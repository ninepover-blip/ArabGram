package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"
)

func TestValidateTransientChannelUpdatesRejectsPTSAndMixedContainers(t *testing.T) {
	if err := validateTransientChannelUpdates(&tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: 91}},
	}); err != nil {
		t.Fatalf("PTS-less channel invalidation was rejected: %v", err)
	}
	for name, updates := range map[string]*tg.Updates{
		"pts": {
			Updates: []tg.UpdateClass{&tg.UpdateChannelTooLong{ChannelID: 91, Pts: 7}},
		},
		"mixed": {
			Updates: []tg.UpdateClass{
				&tg.UpdateChannel{ChannelID: 91},
				&tg.UpdateChannelTooLong{ChannelID: 91, Pts: 7},
			},
		},
		"read boundary": {
			Updates: []tg.UpdateClass{&tg.UpdateReadChannelInbox{ChannelID: 91, MaxID: 5, Pts: 7}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateTransientChannelUpdates(updates)
			if err == nil {
				t.Fatalf("durable channel container was accepted: %+v", updates)
			}
			var invariant *transientChannelInvariantError
			if !errors.As(err, &invariant) {
				t.Fatalf("error = %T %v, want typed invariant violation", err, err)
			}
		})
	}
}

func TestPushTransientChannelExplicitUpdatesRejectsPTSWithoutPartialFanout(t *testing.T) {
	sessions := &captureSessions{}
	r := New(Config{}, Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	ctx := context.Background()

	for name, updates := range map[string]*tg.Updates{
		"pts": {
			Updates: []tg.UpdateClass{&tg.UpdateChannelTooLong{ChannelID: 91, Pts: 7}},
		},
		"mixed": {
			Updates: []tg.UpdateClass{
				&tg.UpdateChannel{ChannelID: 91},
				&tg.UpdateChannelTooLong{ChannelID: 91, Pts: 7},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			sessions.clearMessages()
			r.pushTransientChannelExplicitUpdates(ctx, 1001, 91, []int64{1002}, func(int64) *tg.Updates {
				return updates
			})
			if got := sessions.pushedUserIDs(); len(got) != 0 {
				t.Fatalf("durable container reached sessions: %v", got)
			}
		})
	}

	sessions.clearMessages()
	r.pushTransientChannelExplicitUpdates(ctx, 1001, 91, []int64{1002, 1003}, func(viewerUserID int64) *tg.Updates {
		if viewerUserID == 1002 {
			return &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: 91}}}
		}
		return &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateChannel{ChannelID: 91},
			&tg.UpdateChannelTooLong{ChannelID: 91, Pts: 7},
		}}
	})
	if got := sessions.pushedUserIDs(); len(got) != 0 {
		t.Fatalf("one invalid viewer container caused partial transient fanout: %v", got)
	}

	sessions.clearMessages()
	r.pushTransientChannelExplicitUpdates(ctx, 1001, 91, []int64{1002}, func(int64) *tg.Updates {
		return &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: 91}}}
	})
	if got := sessions.pushedUserIDs(); len(got) != 1 || got[0] != 1002 {
		t.Fatalf("transient invalidation targets = %v, want [1002]", got)
	}
}
