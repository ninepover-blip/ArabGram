package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func TestCollectibleUsernameTransferDeliveryAtomicBoundedAndIdempotentPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin collectible delivery fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	prefix := "cu" + randomSuffix(t)
	insertUser := func(phoneSuffix string) int64 {
		t.Helper()
		var id int64
		if err := tx.QueryRow(ctx, `
INSERT INTO users (access_hash, phone, first_name)
VALUES ($1, $2, 'collectible delivery test')
RETURNING id`, time.Now().UnixNano()&0x7fffffffffffffff, prefix+phoneSuffix).Scan(&id); err != nil {
			t.Fatalf("insert collectible delivery user %q: %v", phoneSuffix, err)
		}
		return id
	}
	from := domain.Peer{Type: domain.PeerTypeUser, ID: insertUser("from")}
	to := domain.Peer{Type: domain.PeerTypeUser, ID: insertUser("to")}

	// Self plus 4,100 contacts proves that the aggregate freezes a stable,
	// deduplicated audience capped at 4,096 for each changed user before it
	// produces the one-statement absolute outbox append set.
	if _, err := tx.Exec(ctx, `
WITH viewers AS (
  INSERT INTO users (access_hash, phone, first_name)
  SELECT $1::bigint + n, $2::text || n::text, 'collectible delivery viewer'
  FROM generate_series(1, 4100) AS n
  RETURNING id
)
INSERT INTO contacts (user_id, contact_user_id)
SELECT id, $3 FROM viewers`, time.Now().UnixNano()&0x3fffffffffffffff, prefix+"viewer", from.ID); err != nil {
		t.Fatalf("insert bounded collectible audience: %v", err)
	}

	username := "atomic" + randomSuffix(t)
	payload := []byte("collectible-delivery-" + prefix)
	registry := NewCollectibleUsernameStore(tx)
	seed, created, err := registry.MintCollectibleUsername(ctx,
		mintRequest(username, from, "mint-"+prefix))
	if err != nil || !created {
		t.Fatalf("seed collectible = %+v created:%v err:%v", seed, created, err)
	}
	req := domain.TransferCollectibleUsernameRequest{
		Username:   username,
		To:         to,
		Actor:      "operator",
		Reason:     "atomic delivery integration test",
		CommandKey: "transfer-" + prefix,
	}

	wantErr := errors.New("encode collectible update failed")
	failed, changed, err := registry.TransferCollectibleUsernameWithDelivery(ctx, req,
		func(storepkg.UsernameAudienceDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
			return nil, wantErr
		})
	if !errors.Is(err, wantErr) || changed || failed.ID != 0 {
		t.Fatalf("failed transfer = asset:%+v changed:%v err:%v", failed, changed, err)
	}
	current, err := registry.CollectibleUsername(ctx, username)
	if err != nil || current.ID != seed.ID || current.Owner != from || current.Version != seed.Version {
		t.Fatalf("asset after failed transfer = %+v err=%v", current, err)
	}
	fromRows, err := registry.PeerUsernames(ctx, from)
	if err != nil || len(fromRows) != 1 || fromRows[0].CollectibleID != seed.ID {
		t.Fatalf("old owner registry after rollback = %+v err=%v", fromRows, err)
	}
	toRows, err := registry.PeerUsernames(ctx, to)
	if err != nil || len(toRows) != 0 {
		t.Fatalf("new owner registry after rollback = %+v err=%v", toRows, err)
	}
	var failedOutbox, failedCommand int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&failedOutbox); err != nil {
		t.Fatalf("count failed collectible outbox: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM collectible_username_transfers WHERE command_key=$1`, req.CommandKey).Scan(&failedCommand); err != nil {
		t.Fatalf("count failed collectible command: %v", err)
	}
	if failedOutbox != 0 || failedCommand != 0 {
		t.Fatalf("failed transaction leaked outbox=%d command=%d", failedOutbox, failedCommand)
	}

	buildCalls := 0
	observed := make(map[int64]int)
	build := func(snapshot storepkg.UsernameAudienceDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
		buildCalls++
		if len(snapshot.Users) != 2 {
			return nil, fmt.Errorf("transfer snapshot has %d users, want 2", len(snapshot.Users))
		}
		effects := make([]storepkg.DeliveryEffect, 0)
		for _, changedUser := range snapshot.Users {
			observed[changedUser.User.ID] = len(changedUser.Audience)
			seen := make(map[int64]struct{}, len(changedUser.Audience))
			for _, viewerID := range changedUser.Audience {
				if _, duplicate := seen[viewerID]; duplicate {
					return nil, fmt.Errorf("duplicate viewer %d for user %d", viewerID, changedUser.User.ID)
				}
				seen[viewerID] = struct{}{}
				effects = append(effects, storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
					TargetUserID:   viewerID,
					Payload:        payload,
					RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
				}))
			}
		}
		return effects, nil
	}
	moved, changed, err := registry.TransferCollectibleUsernameWithDelivery(ctx, req, build)
	if err != nil || !changed || moved.ID != seed.ID || moved.Owner != to || buildCalls != 1 {
		t.Fatalf("successful transfer = asset:%+v changed:%v buildCalls:%d err:%v", moved, changed, buildCalls, err)
	}
	if observed[from.ID] != maxModerationFlagAudience {
		t.Fatalf("old owner bounded audience = %d, want %d", observed[from.ID], maxModerationFlagAudience)
	}
	if observed[to.ID] != 1 {
		t.Fatalf("new owner audience = %d, want self only", observed[to.ID])
	}
	wantEffects := maxModerationFlagAudience + 1
	var outboxCount, distinctTargets, commandCount int
	if err := tx.QueryRow(ctx, `
SELECT count(*), count(DISTINCT target_user_id)
FROM edge_delivery_outbox
WHERE payload=$1`, payload).Scan(&outboxCount, &distinctTargets); err != nil {
		t.Fatalf("count successful collectible outbox: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM collectible_username_transfers WHERE command_key=$1`, req.CommandKey).Scan(&commandCount); err != nil {
		t.Fatalf("count successful collectible command: %v", err)
	}
	if outboxCount != wantEffects || distinctTargets != wantEffects || commandCount != 1 {
		t.Fatalf("successful transaction outbox=%d distinct=%d command=%d, want %d/%d/1",
			outboxCount, distinctTargets, commandCount, wantEffects, wantEffects)
	}

	replayed, changed, err := registry.TransferCollectibleUsernameWithDelivery(ctx, req,
		func(storepkg.UsernameAudienceDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
			t.Fatal("command replay invoked delivery builder")
			return nil, nil
		})
	if err != nil || changed || replayed.ID != seed.ID || replayed.Owner != to {
		t.Fatalf("transfer replay = asset:%+v changed:%v err:%v", replayed, changed, err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&outboxCount); err != nil {
		t.Fatalf("count collectible outbox after replay: %v", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM collectible_username_transfers WHERE command_key=$1`, req.CommandKey).Scan(&commandCount); err != nil {
		t.Fatalf("count collectible command after replay: %v", err)
	}
	if outboxCount != wantEffects || commandCount != 1 {
		t.Fatalf("replay duplicated outbox=%d or command=%d", outboxCount, commandCount)
	}
}
