package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestChannelStoreSetChannelVerifiedRefreshesRowCache(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner := createTestUser(t, ctx, users, "+1886"+suffix+"91", "VerifiedChannelOwner", "")
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	cache := NewChannelRowCache(8)
	channels := newTestChannelStore(pool, WithChannelRowCache(cache))
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "verified channel " + suffix,
		Broadcast:     true,
		Date:          1700001100,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelIDs = append(channelIDs, created.Channel.ID)

	warm, err := channels.GetChannelByID(ctx, created.Channel.ID)
	if err != nil {
		t.Fatalf("warm channel cache: %v", err)
	}
	if warm.Verified {
		t.Fatalf("created channel verified=true, want false")
	}

	updated, err := channels.SetChannelVerified(ctx, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("set verified: %v", err)
	}
	if !updated.Verified {
		t.Fatalf("updated verified=false, want true")
	}
	got, err := channels.GetChannelByID(ctx, created.Channel.ID)
	if err != nil {
		t.Fatalf("get updated channel: %v", err)
	}
	if !got.Verified {
		t.Fatalf("cached channel verified=false after update, want true")
	}
}
