package store

import (
	"fmt"

	"telesrv/internal/domain"
)

type SavedDialogMutationKind string

const (
	SavedDialogSetPinned SavedDialogMutationKind = "set_pinned"
	SavedDialogReorder   SavedDialogMutationKind = "reorder"
)

// SavedDialogMutation is the closed account aggregate for Saved Messages
// sub-dialog pin state.
type SavedDialogMutation struct {
	Kind   SavedDialogMutationKind
	UserID int64
	Date   int
	Peer   domain.Peer
	Pinned bool
	Order  []domain.Peer
	Force  bool
}

type SavedDialogMutationSnapshot struct {
	Mutation SavedDialogMutation
	Changed  bool
	// Order is the authoritative complete pinned order after the mutation.
	Order   []domain.Peer
	Effects []DeliveryEffect
}

func (m SavedDialogMutation) Validate() error {
	if m.UserID <= 0 {
		return fmt.Errorf("saved dialog mutation user id is required")
	}
	switch m.Kind {
	case SavedDialogSetPinned:
		if !validSavedDialogPeer(m.Peer) {
			return fmt.Errorf("saved dialog pin peer is invalid")
		}
	case SavedDialogReorder:
		if len(m.Order) > domain.MaxPinnedSavedDialogs {
			return domain.ErrPinnedSavedDialogsTooMuch
		}
		seen := make(map[domain.Peer]struct{}, len(m.Order))
		for _, peer := range m.Order {
			if !validSavedDialogPeer(peer) {
				return fmt.Errorf("saved dialog order contains invalid peer")
			}
			if _, exists := seen[peer]; exists {
				return fmt.Errorf("saved dialog order contains duplicate peer")
			}
			seen[peer] = struct{}{}
		}
	default:
		return fmt.Errorf("unknown saved dialog mutation kind %q", m.Kind)
	}
	return nil
}

func ValidateSavedDialogEffects(snapshot SavedDialogMutationSnapshot, effects []DeliveryEffect) error {
	want := 0
	if snapshot.Changed {
		want = 1
	}
	if len(effects) != want {
		return fmt.Errorf("saved dialog effects=%d, want %d", len(effects), want)
	}
	if want == 0 {
		return nil
	}
	effect := effects[0]
	if err := effect.Validate(); err != nil {
		return err
	}
	if effect.Kind != DeliveryEffectAccountPTS || effect.TargetUserID != snapshot.Mutation.UserID ||
		effect.ExcludeAuthKeyID != ([8]byte{}) || effect.ExcludeSessionID != 0 {
		return fmt.Errorf("saved dialog effect has wrong target, kind or exclusion")
	}
	event := effect.Event
	if event.Pts != 0 || event.PtsCount != 1 || event.Date != snapshot.Mutation.Date {
		return fmt.Errorf("saved dialog effect has invalid account PTS fields")
	}
	switch snapshot.Mutation.Kind {
	case SavedDialogSetPinned:
		if event.Type != domain.UpdateEventSavedDialogPinned || event.Peer != snapshot.Mutation.Peer || event.Bool != snapshot.Mutation.Pinned {
			return fmt.Errorf("saved dialog pin event does not match mutation")
		}
	case SavedDialogReorder:
		if event.Type != domain.UpdateEventPinnedSavedDialogs || !samePeers(event.Peers, snapshot.Order) {
			return fmt.Errorf("saved dialog order event does not match snapshot")
		}
	}
	return nil
}

func validSavedDialogPeer(peer domain.Peer) bool {
	return peer.ID > 0 && (peer.Type == domain.PeerTypeUser || peer.Type == domain.PeerTypeChannel)
}
