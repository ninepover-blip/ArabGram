package memory

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestUserStoreUpdateColorRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := NewUserStore()
	user, err := store.Create(ctx, domain.User{AccessHash: 1, Phone: "15550003301", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	updated, err := store.UpdateColor(ctx, user.ID, false, domain.PeerColor{
		HasColor:          true,
		Color:             0,
		BackgroundEmojiID: 100,
	})
	if err != nil {
		t.Fatalf("update message color: %v", err)
	}
	if !updated.Color.HasColor || updated.Color.Color != 0 || updated.Color.BackgroundEmojiID != 100 {
		t.Fatalf("message color = %+v, want explicit color=0 bg=100", updated.Color)
	}
	if !updated.ProfileColor.Empty() {
		t.Fatalf("profile color = %+v, want unchanged empty", updated.ProfileColor)
	}

	updated, err = store.UpdateColor(ctx, user.ID, true, domain.PeerColor{
		HasColor:          true,
		Color:             3,
		BackgroundEmojiID: 200,
	})
	if err != nil {
		t.Fatalf("update profile color: %v", err)
	}
	if updated.Color.Color != 0 || updated.Color.BackgroundEmojiID != 100 {
		t.Fatalf("message color after profile update = %+v, want unchanged", updated.Color)
	}
	if !updated.ProfileColor.HasColor || updated.ProfileColor.Color != 3 || updated.ProfileColor.BackgroundEmojiID != 200 {
		t.Fatalf("profile color = %+v, want color=3 bg=200", updated.ProfileColor)
	}

	updated, err = store.UpdateColor(ctx, user.ID, true, domain.PeerColor{})
	if err != nil {
		t.Fatalf("clear profile color: %v", err)
	}
	if !updated.ProfileColor.Empty() {
		t.Fatalf("profile color after clear = %+v, want empty", updated.ProfileColor)
	}
}
