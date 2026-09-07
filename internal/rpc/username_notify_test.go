package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	usernamesapp "telesrv/internal/app/usernames"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

var _ usernamesapp.PeerUsernameNotifier = (*Router)(nil)

func TestUsernameAudienceDeliveryEffectsProjectsFrozenUsers(t *testing.T) {
	r := New(Config{}, Deps{}, zap.NewNop(), clock.System)
	effects, err := r.UsernameAudienceDeliveryEffects(store.UsernameAudienceDeliverySnapshot{
		Users: []store.UserAudienceDeliverySnapshot{
			{User: domain.User{ID: 10}, Audience: []int64{10, 11}},
			{User: domain.User{ID: 20}, Audience: []int64{20}},
		},
	})
	if err != nil {
		t.Fatalf("build username effects: %v", err)
	}
	if len(effects) != 3 {
		t.Fatalf("effects = %d, want 3", len(effects))
	}
	wantTargets := []int64{10, 11, 20}
	wantUsers := []int64{10, 10, 20}
	for i := range effects {
		if effects[i].TargetUserID != wantTargets[i] || effects[i].Kind != store.DeliveryEffectAbsolute {
			t.Fatalf("effect[%d] = %+v", i, effects[i])
		}
		decoded, err := decodeDeliveryUpdate(effects[i].Payload)
		if err != nil {
			t.Fatalf("decode effect[%d]: %v", i, err)
		}
		updates := decoded.(*tg.Updates)
		refresh := updates.Updates[0].(*tg.UpdateUser)
		if refresh.UserID != wantUsers[i] {
			t.Fatalf("effect[%d] refresh user = %d, want %d", i, refresh.UserID, wantUsers[i])
		}
	}
	empty, err := r.UsernameAudienceDeliveryEffects(store.UsernameAudienceDeliverySnapshot{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("channel-only snapshot = effects:%v err:%v", empty, err)
	}
}

func TestNotifyPeerUsernamesChangedUserIsCacheOnly(t *testing.T) {
	const (
		targetID = int64(2002)
		viewerID = int64(1001)
	)
	sessions := &captureSessions{onlineUserIDs: []int64{targetID, viewerID}}
	r := New(Config{}, Deps{Sessions: sessions}, zap.NewNop(), clock.System)
	seedUserFullProjection(t, r, viewerID, targetID)

	if err := r.NotifyPeerUsernamesChanged(context.Background(), domain.Peer{
		Type: domain.PeerTypeUser, ID: targetID,
	}); err != nil {
		t.Fatalf("notify user usernames: %v", err)
	}
	if _, ok := r.userFullProjectionCache.Lookup(viewerID, targetID); ok {
		t.Fatal("userFull projection survived username mutation")
	}
	if pushed := sessions.pushedUserIDs(); len(pushed) != 0 {
		t.Fatalf("post-commit username hook pushed %v", pushed)
	}
}

func TestNotifyPeerUsernamesChangedChannelPushesPreloadedVector(t *testing.T) {
	const (
		channelID = int64(4004)
		ownerID   = int64(3003)
		memberID  = int64(3004)
	)
	channels := &verifiedNotifyChannels{channel: domain.Channel{
		ID: channelID, AccessHash: 44, CreatorUserID: ownerID,
		Title: "Collectible", Username: "channel_slot", Broadcast: true,
	}}
	registry := newFakeUsernameRegistry()
	registry.byPeer[domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}] = []domain.Username{
		{Username: "channel_slot", Editable: true, Active: true, SortOrder: 1},
		{Username: "collectible", Active: true, SortOrder: 0, CollectibleID: 9},
	}
	sessions := &captureSessions{
		onlineUserIDs:  []int64{ownerID, memberID},
		channelMembers: map[int64][]int64{channelID: {ownerID, memberID}},
	}
	r := New(Config{}, Deps{
		Channels: channels, Usernames: registry, Sessions: sessions,
	}, zap.NewNop(), clock.System)

	if err := r.NotifyPeerUsernamesChanged(context.Background(), domain.Peer{
		Type: domain.PeerTypeChannel, ID: channelID,
	}); err != nil {
		t.Fatalf("notify channel usernames: %v", err)
	}
	if registry.peerCalls != 1 || registry.batchCalls != 0 {
		t.Fatalf("registry reads = peer %d / batch %d, want one peer-wide read", registry.peerCalls, registry.batchCalls)
	}
	updates := sessions.lastUserPush().(*tg.Updates)
	channel := updates.Chats[0].(*tg.Channel)
	assertVectorOnlyUsernames(t, "pushed channel", channel, []string{"collectible", "channel_slot"})
}
