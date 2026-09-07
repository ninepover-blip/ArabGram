package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

const (
	readModelRelayBatch          = 256
	readModelRelayLease          = 2 * time.Second
	readModelRelayPublishTimeout = time.Second
	readModelRelayIdleDelay      = 250 * time.Millisecond
	readModelRelayErrorDelay     = 250 * time.Millisecond
)

// ReadModelInvalidationRelay moves pending cache invalidations from PostgreSQL
// to Redis. It never owns version truth and does not hold a PostgreSQL
// transaction while waiting for Redis.
type ReadModelInvalidationRelay struct {
	db        sqlcgen.DBTX
	beginner  txBeginner
	publisher store.ReadModelInvalidationPublisher
	owner     string
	log       *zap.Logger
}

func NewReadModelInvalidationRelay(
	db sqlcgen.DBTX,
	publisher store.ReadModelInvalidationPublisher,
	owner string,
	log *zap.Logger,
) (*ReadModelInvalidationRelay, error) {
	beginner, ok := db.(txBeginner)
	owner = strings.TrimSpace(owner)
	if !ok || publisher == nil || owner == "" || len(owner) > store.MaxDeliveryInstanceIDBytes {
		return nil, fmt.Errorf("read-model invalidation relay requires transactional db, publisher, and bounded owner")
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &ReadModelInvalidationRelay{db: db, beginner: beginner, publisher: publisher, owner: owner, log: log}, nil
}

func (r *ReadModelInvalidationRelay) Run(ctx context.Context) {
	if r == nil {
		return
	}
	for ctx.Err() == nil {
		items, err := r.claim(ctx)
		if err != nil {
			r.log.Warn("claim dialog read-model invalidations", zap.Error(err))
			if !sleepReadModelRelay(ctx, readModelRelayErrorDelay) {
				return
			}
			continue
		}
		if len(items) == 0 {
			if !sleepReadModelRelay(ctx, readModelRelayIdleDelay) {
				return
			}
			continue
		}
		publishCtx, cancel := context.WithTimeout(ctx, readModelRelayPublishTimeout)
		err = r.publisher.PublishReadModelInvalidations(publishCtx, items)
		cancel()
		if err != nil {
			r.log.Warn("publish dialog read-model invalidations; durable lease will retry", zap.Int("items", len(items)), zap.Error(err))
			continue
		}
		if err := r.ack(ctx, items); err != nil {
			// Redis Pub/Sub is idempotent cache invalidation. An ACK failure leaves
			// the durable version pending and deliberately republishes after lease.
			r.log.Warn("ack dialog read-model invalidations", zap.Int("items", len(items)), zap.Error(err))
		}
	}
}

func (r *ReadModelInvalidationRelay) claim(ctx context.Context) ([]store.ReadModelInvalidation, error) {
	tx, err := r.beginner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `
WITH candidates AS MATERIALIZED (
  SELECT model, owner_user_id, peer_type, peer_id
  FROM read_model_versions
  WHERE model = 'dialog_light'
    AND published_version < version
    AND (publish_lease_until IS NULL OR publish_lease_until <= clock_timestamp())
  ORDER BY updated_at, owner_user_id, peer_type, peer_id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE read_model_versions r
  SET publish_owner = $2,
      publish_lease_until = clock_timestamp() + ($3::bigint * interval '1 millisecond')
  FROM candidates c
  WHERE r.model = c.model AND r.owner_user_id = c.owner_user_id
    AND r.peer_type = c.peer_type AND r.peer_id = c.peer_id
  RETURNING r.model, r.owner_user_id, r.peer_type, r.peer_id, r.version, r.hash
)
SELECT model, owner_user_id, peer_type, peer_id, version, hash
FROM claimed
ORDER BY owner_user_id, peer_type, peer_id`, readModelRelayBatch, r.owner, durationMillis(readModelRelayLease))
	if err != nil {
		return nil, fmt.Errorf("query claim: %w", err)
	}
	var items []store.ReadModelInvalidation
	for rows.Next() {
		var item store.ReadModelInvalidation
		var peerType string
		if err := rows.Scan(&item.Key.Model, &item.Key.OwnerUserID, &peerType, &item.Key.PeerID, &item.Version, &item.Hash); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claim: %w", err)
		}
		item.Key.PeerType = domain.PeerType(peerType)
		if items == nil {
			// Empty polls still commit the claim transaction, but need no batch.
			items = make([]store.ReadModelInvalidation, 0, readModelRelayBatch)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate claim: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return items, nil
}

func (r *ReadModelInvalidationRelay) ack(ctx context.Context, items []store.ReadModelInvalidation) error {
	if len(items) == 0 {
		return nil
	}
	models := make([]string, len(items))
	owners := make([]int64, len(items))
	peerTypes := make([]string, len(items))
	peerIDs := make([]int64, len(items))
	versions := make([]int64, len(items))
	for i, item := range items {
		models[i] = item.Key.Model
		owners[i] = item.Key.OwnerUserID
		peerTypes[i] = string(item.Key.PeerType)
		peerIDs[i] = item.Key.PeerID
		versions[i] = item.Version
	}
	tag, err := r.db.Exec(ctx, `
WITH input AS (
  SELECT * FROM unnest(
    $1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::bigint[]
  ) AS i(model, owner_user_id, peer_type, peer_id, version)
)
UPDATE read_model_versions r
SET published_version = GREATEST(r.published_version, i.version),
    publish_owner = '',
    publish_lease_until = NULL
FROM input i
WHERE r.model = i.model AND r.owner_user_id = i.owner_user_id
  AND r.peer_type = i.peer_type AND r.peer_id = i.peer_id
  AND r.publish_owner = $6`, models, owners, peerTypes, peerIDs, versions, r.owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(items)) {
		return fmt.Errorf("acked %d of %d leased invalidations", tag.RowsAffected(), len(items))
	}
	return nil
}

func sleepReadModelRelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
