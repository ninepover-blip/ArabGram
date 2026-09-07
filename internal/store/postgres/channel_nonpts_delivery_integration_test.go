package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestChannelPendingJoinDeliveryIsAtomicAndConcurrentReplayDoesNotDuplicate(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 9101, Phone: "+1991" + suffix + "01", FirstName: "PendingOwner"})
	if err != nil {
		t.Fatal(err)
	}
	requester, err := users.Create(ctx, domain.User{AccessHash: 9102, Phone: "+1991" + suffix + "02", FirstName: "PendingRequester"})
	if err != nil {
		t.Fatal(err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, []int64{owner.ID, requester.ID})
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "atomic pending " + suffix, Megagroup: true, Date: 1700100000,
	})
	if err != nil {
		t.Fatal(err)
	}
	channelID = created.Channel.ID
	if _, err := pool.Exec(ctx, `UPDATE channels SET username = $2, join_request = true WHERE id = $1`, channelID, "pending"+suffix); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("pending payload failed")
	_, err = channels.JoinChannel(ctx, channelID, requester.ID, 1700100001, func(store.ChannelPendingJoinDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("JoinChannel error = %v, want builder failure", err)
	}
	assertPendingImporterAndPayloadCount(t, ctx, pool, channelID, requester.ID, owner.ID, []byte{0xf1, 0x01}, 0, 0)

	marker := []byte{0xf1, 0x01}
	build := func(snapshot store.ChannelPendingJoinDeliverySnapshot) ([]store.DeliveryEffect, error) {
		if len(snapshot.Targets) != 1 || snapshot.Targets[0].TargetUserID != owner.ID || snapshot.Targets[0].Pending.Count != 1 {
			return nil, fmt.Errorf("unexpected pending snapshot: %+v", snapshot)
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner.ID, Payload: marker, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, callErr := channels.JoinChannel(ctx, channelID, requester.ID, 1700100002, build)
			errs <- callErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for callErr := range errs {
		if !errors.Is(callErr, domain.ErrInviteRequestSent) {
			t.Fatalf("concurrent JoinChannel error = %v, want ErrInviteRequestSent", callErr)
		}
	}
	assertPendingImporterAndPayloadCount(t, ctx, pool, channelID, requester.ID, owner.ID, marker, 1, 1)
}

func TestChannelAvailableMinDeliveryIsAtomicAndConcurrentReplayDoesNotDuplicate(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 9201, Phone: "+1992" + suffix + "01", FirstName: "ClearOwner"})
	if err != nil {
		t.Fatal(err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 9202, Phone: "+1992" + suffix + "02", FirstName: "ClearMember"})
	if err != nil {
		t.Fatal(err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, []int64{owner.ID, member.ID})
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "atomic available min " + suffix, Megagroup: true,
		MemberUserIDs: []int64{member.ID}, Date: 1700100100,
	})
	if err != nil {
		t.Fatal(err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 9203, Message: "visible", Date: 1700100101,
	})
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("available-min payload failed")
	_, err = channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID: member.ID, ChannelID: channelID, MaxID: sent.Message.ID, Date: 1700100102,
	}, func(store.ChannelAvailableMinDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("DeleteChannelHistory error = %v, want builder failure", err)
	}
	assertAvailableMinAndPayloadCount(t, ctx, pool, channelID, member.ID, []byte{0xf2, 0x01}, 0, 0)

	marker := []byte{0xf2, 0x01}
	build := func(snapshot store.ChannelAvailableMinDeliverySnapshot) ([]store.DeliveryEffect, error) {
		if snapshot.TargetUserID == 0 {
			return nil, nil
		}
		if snapshot.TargetUserID != member.ID || snapshot.Channel.ID != channelID || snapshot.AvailableMinID != sent.Message.ID {
			return nil, fmt.Errorf("unexpected available-min snapshot: %+v", snapshot)
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: member.ID, Payload: marker, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, callErr := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
				UserID: member.ID, ChannelID: channelID, MaxID: sent.Message.ID, Date: 1700100103,
			}, build)
			errs <- callErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent DeleteChannelHistory error = %v", callErr)
		}
	}
	assertAvailableMinAndPayloadCount(t, ctx, pool, channelID, member.ID, marker, sent.Message.ID, 1)
}

func assertPendingImporterAndPayloadCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, channelID, requesterID, targetUserID int64, marker []byte, wantPending, wantPayload int) {
	t.Helper()
	var pending, payload int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_invite_importers WHERE channel_id = $1 AND user_id = $2 AND requested`, channelID, requesterID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1 AND payload = $2`, targetUserID, marker).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if pending != wantPending || payload != wantPayload {
		t.Fatalf("pending/payload count = %d/%d, want %d/%d", pending, payload, wantPending, wantPayload)
	}
}

func assertAvailableMinAndPayloadCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, channelID, memberID int64, marker []byte, wantMinID, wantPayload int) {
	t.Helper()
	var availableMinID, payload int
	if err := pool.QueryRow(ctx, `SELECT available_min_id FROM channel_members WHERE channel_id = $1 AND user_id = $2`, channelID, memberID).Scan(&availableMinID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1 AND payload = $2`, memberID, marker).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if availableMinID != wantMinID || payload != wantPayload {
		t.Fatalf("available_min/payload count = %d/%d, want %d/%d", availableMinID, payload, wantMinID, wantPayload)
	}
}
