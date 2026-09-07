package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// ReadModelVersionStore reads durable read-model version tokens.
type ReadModelVersionStore struct {
	db sqlcgen.DBTX
}

func NewReadModelVersionStore(db sqlcgen.DBTX) *ReadModelVersionStore {
	return &ReadModelVersionStore{db: db}
}

func (s *ReadModelVersionStore) ReadModelHash(ctx context.Context, model string, ownerUserID int64, peerType domain.PeerType, peerID int64) (int64, bool, error) {
	if s == nil || s.db == nil || model == "" {
		return 0, false, nil
	}
	var hash int64
	err := s.db.QueryRow(ctx, `
SELECT hash
FROM read_model_versions
WHERE model = $1
  AND owner_user_id = $2
  AND peer_type = $3
  AND peer_id = $4
`, model, ownerUserID, string(peerType), peerID).Scan(&hash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read model hash: %w", err)
	}
	return hash, hash != 0, nil
}

func (s *ReadModelVersionStore) ReadModelHashes(ctx context.Context, keys []store.ReadModelKey) (map[store.ReadModelKey]int64, error) {
	out := make(map[store.ReadModelKey]int64, len(keys))
	if s == nil || s.db == nil || len(keys) == 0 {
		return out, nil
	}
	models := make([]string, 0, len(keys))
	owners := make([]int64, 0, len(keys))
	peerTypes := make([]string, 0, len(keys))
	peerIDs := make([]int64, 0, len(keys))
	seen := make(map[store.ReadModelKey]struct{}, len(keys))
	for _, key := range keys {
		if key.Model == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		models = append(models, key.Model)
		owners = append(owners, key.OwnerUserID)
		peerTypes = append(peerTypes, string(key.PeerType))
		peerIDs = append(peerIDs, key.PeerID)
	}
	if len(models) == 0 {
		return out, nil
	}
	rows, err := s.db.Query(ctx, `
WITH requested AS (
    SELECT *
    FROM unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[])
         AS r(model, owner_user_id, peer_type, peer_id)
)
SELECT v.model, v.owner_user_id, v.peer_type, v.peer_id, v.hash
FROM requested r
JOIN read_model_versions v
  ON v.model = r.model
 AND v.owner_user_id = r.owner_user_id
 AND v.peer_type = r.peer_type
 AND v.peer_id = r.peer_id
WHERE v.hash <> 0`, models, owners, peerTypes, peerIDs)
	if err != nil {
		return nil, fmt.Errorf("read model hashes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key store.ReadModelKey
		var peerType string
		var hash int64
		if err := rows.Scan(&key.Model, &key.OwnerUserID, &peerType, &key.PeerID, &hash); err != nil {
			return nil, fmt.Errorf("scan read model hash: %w", err)
		}
		key.PeerType = domain.PeerType(peerType)
		out[key] = hash
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read model hashes: %w", err)
	}
	return out, nil
}
