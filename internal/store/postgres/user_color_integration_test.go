package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"telesrv/internal/domain"
)

func TestUserStoreUpdateColorPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := time.Now().UnixNano() % 1_000_000_000

	viewer, err := users.Create(ctx, domain.User{
		AccessHash: 1,
		Phone:      fmt.Sprintf("1777%d", suffix),
		FirstName:  "ViewerColor",
	})
	if err != nil {
		t.Fatalf("create viewer: %v", err)
	}
	target, err := users.Create(ctx, domain.User{
		AccessHash: 2,
		Phone:      fmt.Sprintf("1888%d", suffix),
		FirstName:  fmt.Sprintf("TargetColor%d", suffix),
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{viewer.ID, target.ID})
	})

	updated, err := users.UpdateColor(ctx, target.ID, false, domain.PeerColor{
		HasColor:          true,
		Color:             0,
		BackgroundEmojiID: 100,
	})
	if err != nil {
		t.Fatalf("update message color: %v", err)
	}
	assertPostgresPeerColor(t, updated.Color, true, 0, 100)

	updated, err = users.UpdateColor(ctx, target.ID, true, domain.PeerColor{
		HasColor:          true,
		Color:             3,
		BackgroundEmojiID: 200,
	})
	if err != nil {
		t.Fatalf("update profile color: %v", err)
	}
	assertPostgresPeerColor(t, updated.Color, true, 0, 100)
	assertPostgresPeerColor(t, updated.ProfileColor, true, 3, 200)

	loaded, found, err := users.ByID(ctx, target.ID)
	if err != nil || !found {
		t.Fatalf("load target found=%v err=%v", found, err)
	}
	assertPostgresPeerColor(t, loaded.Color, true, 0, 100)
	assertPostgresPeerColor(t, loaded.ProfileColor, true, 3, 200)

	foundInSearch := false
	search, err := users.Search(ctx, viewer.ID, fmt.Sprintf("targetcolor%d", suffix), "", 10)
	if err != nil {
		t.Fatalf("search users: %v", err)
	}
	for _, u := range search.Results {
		if u.ID == target.ID {
			foundInSearch = true
			assertPostgresPeerColor(t, u.Color, true, 0, 100)
			assertPostgresPeerColor(t, u.ProfileColor, true, 3, 200)
		}
	}
	if !foundInSearch {
		t.Fatalf("target %d not found in search results %+v", target.ID, search.Results)
	}

	cleared, err := users.UpdateColor(ctx, target.ID, true, domain.PeerColor{})
	if err != nil {
		t.Fatalf("clear profile color: %v", err)
	}
	if !cleared.ProfileColor.Empty() {
		t.Fatalf("profile color after clear = %+v, want empty", cleared.ProfileColor)
	}
}

func assertPostgresPeerColor(t *testing.T, got domain.PeerColor, hasColor bool, color int, backgroundEmojiID int64) {
	t.Helper()
	if got.HasColor != hasColor || got.Color != color || got.BackgroundEmojiID != backgroundEmojiID {
		t.Fatalf("peer color = %+v, want has=%v color=%d bg=%d", got, hasColor, color, backgroundEmojiID)
	}
}
