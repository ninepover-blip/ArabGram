package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func TestPendingJoinDeliveryRollsBackAndReplayDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	channels := newChannelStoreWithDelivery()
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1, Title: "pending delivery", Megagroup: true, Date: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	invited, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID: 1, ChannelID: created.Channel.ID, Title: "request", RequestNeeded: true, Date: 101,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("encode failed")
	_, err = channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: 2, Hash: invited.Invite.Hash, Date: 102,
	}, func(storepkg.ChannelPendingJoinDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ImportInvite error = %v, want builder failure", err)
	}
	pending, err := channels.PendingJoinRequests(ctx, created.Channel.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 || len(channels.deliveryOutbox.Snapshot()) != 0 {
		t.Fatalf("failed transaction leaked pending=%+v outbox=%+v", pending, channels.deliveryOutbox.Snapshot())
	}

	_, err = channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: 2, Hash: invited.Invite.Hash, Date: 103,
	}, testPendingJoinEffects)
	if !errors.Is(err, domain.ErrInviteRequestSent) {
		t.Fatalf("ImportInvite error = %v, want invite request sent", err)
	}
	if got := len(channels.deliveryOutbox.Snapshot()); got != 1 {
		t.Fatalf("outbox rows = %d, want creator notification", got)
	}
	_, err = channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: 2, Hash: invited.Invite.Hash, Date: 104,
	}, testPendingJoinEffects)
	if !errors.Is(err, domain.ErrInviteRequestSent) {
		t.Fatalf("replay error = %v, want invite request sent", err)
	}
	if got := len(channels.deliveryOutbox.Snapshot()); got != 1 {
		t.Fatalf("replay outbox rows = %d, want no duplicate", got)
	}
}

func TestAvailableMinDeliveryFailureRollsBackAndNoopDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	channels := newChannelStoreWithDelivery()
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1, Title: "available min", Megagroup: true, MemberUserIDs: []int64{2}, Date: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: 1, ChannelID: created.Channel.ID, Message: "one", RandomID: 1, Date: 201,
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := channels.GetParticipant(ctx, 2, created.Channel.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("payload failed")
	_, err = channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID: 2, ChannelID: created.Channel.ID, MaxID: sent.Message.ID, Date: 202,
	}, func(storepkg.ChannelAvailableMinDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("DeleteChannelHistory error = %v, want builder failure", err)
	}
	after, err := channels.GetParticipant(ctx, 2, created.Channel.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if after.AvailableMinID != before.AvailableMinID || len(channels.deliveryOutbox.Snapshot()) != 0 {
		t.Fatalf("failed transaction leaked member=%+v before=%+v outbox=%+v", after, before, channels.deliveryOutbox.Snapshot())
	}

	if _, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID: 2, ChannelID: created.Channel.ID, MaxID: sent.Message.ID, Date: 203,
	}, testAvailableMinEffects); err != nil {
		t.Fatal(err)
	}
	if got := len(channels.deliveryOutbox.Snapshot()); got != 1 {
		t.Fatalf("outbox rows = %d, want one available-min notification", got)
	}
	if _, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID: 2, ChannelID: created.Channel.ID, MaxID: sent.Message.ID, Date: 204,
	}, testAvailableMinEffects); err != nil {
		t.Fatal(err)
	}
	if got := len(channels.deliveryOutbox.Snapshot()); got != 1 {
		t.Fatalf("no-op outbox rows = %d, want no duplicate", got)
	}
}
