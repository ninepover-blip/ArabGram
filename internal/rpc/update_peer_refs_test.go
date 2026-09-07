package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
)

func TestRemoveKnownChannelRefs(t *testing.T) {
	refs := map[int64]struct{}{
		1001: {},
		1002: {},
		1003: {},
	}

	removeKnownChannelRefs(refs, []domain.Channel{
		{ID: 1002},
		{ID: 0},
		{ID: 1004},
	})

	if _, ok := refs[1002]; ok {
		t.Fatalf("known channel ref was not removed: %+v", refs)
	}
	for _, id := range []int64{1001, 1003} {
		if _, ok := refs[id]; !ok {
			t.Fatalf("unexpectedly removed channel %d from refs %+v", id, refs)
		}
	}
}

func TestCollectMessagePeerRefsIncludesStarGiftServiceActions(t *testing.T) {
	users := map[int64]struct{}{}
	channels := map[int64]struct{}{}
	collectMessagePeerRefs(domain.Message{Media: &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind: domain.MessageServiceActionStarGift,
			StarGift: &domain.MessageStarGiftAction{
				FromUserID: 1001, PeerChannelID: 55,
			},
		},
	}}, 0, users, channels)
	if _, ok := users[1001]; !ok {
		t.Fatalf("ordinary star-gift user refs=%v, missing sender", users)
	}
	if _, ok := channels[55]; !ok {
		t.Fatalf("ordinary star-gift channel refs=%v", channels)
	}
	collectMessagePeerRefs(domain.Message{Media: &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind:     domain.MessageServiceActionStarGift,
			StarGift: &domain.MessageStarGiftAction{PeerUserID: 1002},
		},
	}}, 0, users, channels)
	if _, ok := users[1002]; !ok {
		t.Fatalf("ordinary star-gift recipient refs=%v", users)
	}

	collectMessagePeerRefs(domain.Message{Media: &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind: domain.MessageServiceActionStarGiftUnique,
			StarGiftUnique: &domain.MessageStarGiftUniqueAction{
				FromUserID: 2001,
				Peer:       domain.Peer{Type: domain.PeerTypeChannel, ID: 56},
				Gift: domain.UniqueStarGift{
					Owner:              domain.Peer{Type: domain.PeerTypeChannel, ID: 56},
					OriginalFromUserID: 2002,
					OriginalOwner:      domain.Peer{Type: domain.PeerTypeUser, ID: 2003},
					ReleasedBy:         domain.Peer{Type: domain.PeerTypeUser, ID: 2004},
					ThemePeer:          domain.Peer{Type: domain.PeerTypeChannel, ID: 57},
					Host:               domain.Peer{Type: domain.PeerTypeUser, ID: 2005},
				},
			},
		},
	}}, 0, users, channels)
	for _, id := range []int64{2001, 2002, 2003, 2004, 2005} {
		if _, ok := users[id]; !ok {
			t.Fatalf("unique star-gift user refs=%v, missing %d", users, id)
		}
	}
	for _, id := range []int64{56, 57} {
		if _, ok := channels[id]; !ok {
			t.Fatalf("unique star-gift channel refs=%v, missing %d", channels, id)
		}
	}
}

func TestCollectMessagePeerRefsHidesStarGiftSenderDetails(t *testing.T) {
	users := map[int64]struct{}{}
	channels := map[int64]struct{}{}
	collectMessagePeerRefs(domain.Message{Media: &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind: domain.MessageServiceActionStarGift,
			StarGift: &domain.MessageStarGiftAction{
				FromUserID: 1001, NameHidden: true, PeerChannelID: 55,
			},
		},
	}}, 0, users, channels)
	collectMessagePeerRefs(domain.Message{Media: &domain.MessageMedia{
		Kind: domain.MessageMediaKindService,
		ServiceAction: &domain.MessageServiceAction{
			Kind: domain.MessageServiceActionStarGiftUnique,
			StarGiftUnique: &domain.MessageStarGiftUniqueAction{
				Gift: domain.UniqueStarGift{
					OriginalFromUserID: 2001,
					OriginalNameHidden: true,
				},
			},
		},
	}}, 0, users, channels)
	for _, id := range []int64{1001, 2001} {
		if _, ok := users[id]; ok {
			t.Fatalf("hidden star-gift sender %d leaked into refs=%v", id, users)
		}
	}
	if _, ok := channels[55]; !ok {
		t.Fatalf("hidden ordinary gift lost recipient channel ref=%v", channels)
	}
}

func TestCollectChannelMessagePeerRefsIncludesNestedWireUsers(t *testing.T) {
	const currentChannelID = int64(3001)
	users := map[int64]struct{}{}
	channels := map[int64]struct{}{}
	collectChannelMessagePeerRefs(domain.ChannelMessage{
		SavedPeer: domain.Peer{Type: domain.PeerTypeUser, ID: 1005},
		Forward: &domain.MessageForward{
			From:      domain.Peer{Type: domain.PeerTypeUser, ID: 1001},
			SavedFrom: domain.Peer{Type: domain.PeerTypeUser, ID: 1002},
		},
		Replies: &domain.ChannelMessageReplies{RecentRepliers: []domain.Peer{
			{Type: domain.PeerTypeUser, ID: 1003},
			{Type: domain.PeerTypeChannel, ID: 4001},
			{Type: domain.PeerTypeChannel, ID: currentChannelID},
		}},
		Action: &domain.ChannelMessageAction{InviterUserID: 1004},
	}, currentChannelID, users, channels)

	for _, id := range []int64{1001, 1002, 1003, 1004, 1005} {
		if _, ok := users[id]; !ok {
			t.Fatalf("channel message user refs=%v, missing %d", users, id)
		}
	}
	if _, ok := channels[4001]; !ok {
		t.Fatalf("channel message channel refs=%v, missing recent replier channel", channels)
	}
	if _, ok := channels[currentChannelID]; ok {
		t.Fatalf("current channel leaked into external refs=%v", channels)
	}
}

func TestCollectChannelMessagePeerRefsIncludesVisiblePaidReactorPeers(t *testing.T) {
	const currentChannelID = int64(3001)
	users := map[int64]struct{}{}
	channels := map[int64]struct{}{}
	collectChannelMessagePeerRefs(domain.ChannelMessage{
		Reactions: &domain.ChannelMessageReactions{Paid: &domain.ChannelMessagePaidReactions{
			TopReactors: []domain.PaidReactor{
				{UserID: 1001, Peer: domain.Peer{Type: domain.PeerTypeChannel, ID: 4001}, Stars: 10},
				{UserID: 1002, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1002}, Stars: 9},
				{UserID: 1003, Peer: domain.Peer{Type: domain.PeerTypeChannel, ID: 4002}, Stars: 8, Anonymous: true},
				{UserID: 1004, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1004}, Stars: 7, Anonymous: true, My: true},
				{UserID: 1005, Peer: domain.Peer{Type: domain.PeerTypeChannel, ID: currentChannelID}, Stars: 6},
			},
		}},
	}, currentChannelID, users, channels)

	if _, ok := channels[4001]; !ok {
		t.Fatalf("paid reactor channel refs=%v, missing visible channel", channels)
	}
	if _, ok := channels[4002]; ok {
		t.Fatalf("anonymous other paid reactor leaked channel ref=%v", channels)
	}
	if _, ok := channels[currentChannelID]; ok {
		t.Fatalf("current paid reactor channel leaked into external refs=%v", channels)
	}
	for _, id := range []int64{1002, 1004} {
		if _, ok := users[id]; !ok {
			t.Fatalf("paid reactor user refs=%v, missing %d", users, id)
		}
	}
	if _, ok := users[1003]; ok {
		t.Fatalf("anonymous other paid reactor leaked user ref=%v", users)
	}
}

func TestMessageMentionNameUsersAreProjectedInStrictAndNonStrictEnvelopes(t *testing.T) {
	const (
		viewerID   = int64(1001)
		entityUser = int64(2001)
		quoteUser  = int64(2002)
		channelID  = int64(3001)
	)
	entities := []domain.MessageEntity{{
		Type:   domain.MessageEntityMentionName,
		UserID: entityUser,
	}}
	reply := &domain.MessageReply{
		MessageID: 1,
		QuoteEntities: []domain.MessageEntity{{
			Type:   domain.MessageEntityMentionName,
			UserID: quoteUser,
		}},
	}
	users := mapUsersService{users: map[int64]domain.User{
		entityUser: {ID: entityUser, FirstName: "Projected entity mention"},
		quoteUser:  {ID: quoteUser, FirstName: "Projected quote mention"},
	}}
	r := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	ctx := context.Background()

	tests := []struct {
		name   string
		enrich func() ([]domain.User, error)
	}{
		{
			name: "private non-strict",
			enrich: func() ([]domain.User, error) {
				list := r.enrichMessageList(ctx, viewerID, domain.MessageList{Messages: []domain.Message{{
					Entities: entities,
					ReplyTo:  reply,
				}}})
				return list.Users, nil
			},
		},
		{
			name: "private strict",
			enrich: func() ([]domain.User, error) {
				events, err := r.enrichUpdateEventsWithPeerCacheStrict(ctx, viewerID, []domain.UpdateEvent{{
					Type: domain.UpdateEventNewMessage,
					Message: domain.Message{
						Entities: entities,
						ReplyTo:  reply,
					},
				}}, nil)
				if err != nil {
					return nil, err
				}
				return events[0].Users, nil
			},
		},
		{
			name: "channel non-strict",
			enrich: func() ([]domain.User, error) {
				history := r.enrichChannelHistory(ctx, viewerID, domain.ChannelHistory{
					Channel: domain.Channel{ID: channelID},
					Messages: []domain.ChannelMessage{{
						ChannelID: channelID,
						Entities:  entities,
						ReplyTo:   reply,
					}},
				})
				return history.Users, nil
			},
		},
		{
			name: "channel strict",
			enrich: func() ([]domain.User, error) {
				diff, err := r.enrichChannelDifferenceStrict(ctx, viewerID, domain.ChannelDifference{
					Channel: domain.Channel{ID: channelID},
					NewMessages: []domain.ChannelMessage{{
						ChannelID: channelID,
						Entities:  entities,
						ReplyTo:   reply,
					}},
				})
				return diff.Users, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.enrich()
			if err != nil {
				t.Fatalf("enrich: %v", err)
			}
			byID := make(map[int64]domain.User, len(got))
			for _, user := range got {
				byID[user.ID] = user
			}
			if user := byID[entityUser]; user.FirstName != "Projected entity mention" {
				t.Fatalf("entity mention user = %+v, want viewer projection", user)
			}
			if user := byID[quoteUser]; user.FirstName != "Projected quote mention" {
				t.Fatalf("quote mention user = %+v, want viewer projection", user)
			}
		})
	}
}
