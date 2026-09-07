package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestProfilePhotoDeliveryAtomicityPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	user, err := users.Create(ctx, domain.User{AccessHash: randomAIComposeID(), Phone: "+1773" + randomSuffix(t), FirstName: "PhotoAtomic"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	media := NewMediaStore(pool)
	first := domain.Photo{ID: randomAIComposeID(), AccessHash: randomAIComposeID(), Date: 1700000000, DCID: 2}
	second := domain.Photo{ID: randomAIComposeID(), AccessHash: randomAIComposeID(), Date: 1700000001, DCID: 2}
	for _, photo := range []domain.Photo{first, second} {
		if err := media.PutPhoto(ctx, photo); err != nil {
			t.Fatalf("put photo %d: %v", photo.ID, err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM profile_photos WHERE owner_peer_type = 'user' AND owner_peer_id = $1`, user.ID)
		_, _ = pool.Exec(ctx, `DELETE FROM photos WHERE id = ANY($1::bigint[])`, []int64{first.ID, second.ID})
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, user.ID)
	})

	effects := func(store.ProfilePhotoMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
	if snapshot, found, err := media.SetProfilePhotoKindWithDelivery(ctx, domain.PeerTypeUser, user.ID, domain.ProfilePhotoKindProfile, first.ID, 1700000000, effects); err != nil || !found || snapshot.Current.ID != first.ID {
		t.Fatalf("set first photo = snapshot:%+v found:%v err:%v", snapshot, found, err)
	}
	var outboxRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, user.ID).Scan(&outboxRows); err != nil || outboxRows != 1 {
		t.Fatalf("outbox after set = %d err:%v, want 1", outboxRows, err)
	}

	wantErr := errors.New("projection failed")
	failing := func(store.ProfilePhotoMutation) ([]store.DeliveryEffect, error) { return nil, wantErr }
	if _, _, err := media.SetProfilePhotoKindWithDelivery(ctx, domain.PeerTypeUser, user.ID, domain.ProfilePhotoKindProfile, second.ID, 1700000001, failing); !errors.Is(err, wantErr) {
		t.Fatalf("set second error = %v, want %v", err, wantErr)
	}
	if current, found, err := media.CurrentProfilePhotoKind(ctx, domain.PeerTypeUser, user.ID, domain.ProfilePhotoKindProfile); err != nil || !found || current != first.ID {
		t.Fatalf("current after failed set = %d found:%v err:%v, want first", current, found, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, user.ID).Scan(&outboxRows); err != nil || outboxRows != 1 {
		t.Fatalf("outbox after failed set = %d err:%v, want 1", outboxRows, err)
	}

	if _, err := media.DeleteProfilePhotosKindWithDelivery(ctx, domain.PeerTypeUser, user.ID, domain.ProfilePhotoKindProfile, []int64{first.ID}, failing); !errors.Is(err, wantErr) {
		t.Fatalf("delete error = %v, want %v", err, wantErr)
	}
	if current, found, err := media.CurrentProfilePhotoKind(ctx, domain.PeerTypeUser, user.ID, domain.ProfilePhotoKindProfile); err != nil || !found || current != first.ID {
		t.Fatalf("current after failed delete = %d found:%v err:%v, want first", current, found, err)
	}
}
