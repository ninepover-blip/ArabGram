package store

import (
	"fmt"
	"reflect"

	"telesrv/internal/domain"
)

type QuickReplyAccountMutationKind string

const (
	QuickReplyAccountSaveText       QuickReplyAccountMutationKind = "save_text"
	QuickReplyAccountRenameShortcut QuickReplyAccountMutationKind = "rename_shortcut"
	QuickReplyAccountReorder        QuickReplyAccountMutationKind = "reorder"
	QuickReplyAccountDeleteShortcut QuickReplyAccountMutationKind = "delete_shortcut"
	QuickReplyAccountDeleteMessages QuickReplyAccountMutationKind = "delete_messages"
)

// QuickReplyAccountMutation is the closed write API for account-owned quick
// replies. The owning store commits the quick-reply rows and Account PTS
// delivery facts in one aggregate transaction.
type QuickReplyAccountMutation struct {
	Kind       QuickReplyAccountMutationKind
	UserID     int64
	Date       int
	Shortcut   string
	ShortcutID int
	Order      []int
	Message    domain.QuickReplyMessage
	MessageIDs []int
}

type QuickReplyAccountMutationSnapshot struct {
	Mutation QuickReplyAccountMutation
	Changed  bool
	Result   domain.QuickReplyMutation
	Effects  []DeliveryEffect
}

// Normalize validates the tagged mutation and returns an ownership-safe copy
// whose shortcut and slice fields are canonical.
func (m QuickReplyAccountMutation) Normalize() (QuickReplyAccountMutation, error) {
	if m.UserID <= 0 {
		return QuickReplyAccountMutation{}, fmt.Errorf("quick reply mutation user id is required")
	}
	if m.Date <= 0 {
		return QuickReplyAccountMutation{}, fmt.Errorf("quick reply mutation date is required")
	}
	m.Order = append([]int(nil), m.Order...)
	m.MessageIDs = append([]int(nil), m.MessageIDs...)
	m.Message.Entities = append([]domain.MessageEntity(nil), m.Message.Entities...)
	switch m.Kind {
	case QuickReplyAccountSaveText:
		shortcut, err := domain.NormalizeQuickReplyShortcut(m.Shortcut)
		if err != nil {
			return QuickReplyAccountMutation{}, err
		}
		if m.Message.Message == "" || len(m.Message.Entities) > domain.MaxMessageEntityCount {
			return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
		}
		m.Shortcut = shortcut
	case QuickReplyAccountRenameShortcut:
		shortcut, err := domain.NormalizeQuickReplyShortcut(m.Shortcut)
		if err != nil {
			return QuickReplyAccountMutation{}, err
		}
		if m.ShortcutID <= 0 {
			return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
		}
		m.Shortcut = shortcut
	case QuickReplyAccountReorder:
		if len(m.Order) > domain.MaxQuickReplies {
			return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
		}
		seen := make(map[int]struct{}, len(m.Order))
		for _, id := range m.Order {
			if id <= 0 {
				return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
			}
			if _, duplicate := seen[id]; duplicate {
				return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
			}
			seen[id] = struct{}{}
		}
	case QuickReplyAccountDeleteShortcut:
		if m.ShortcutID <= 0 {
			return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
		}
	case QuickReplyAccountDeleteMessages:
		if m.ShortcutID <= 0 || len(m.MessageIDs) == 0 || len(m.MessageIDs) > domain.MaxQuickReplyMessages {
			return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
		}
		seen := make(map[int]struct{}, len(m.MessageIDs))
		for _, id := range m.MessageIDs {
			if id <= 0 {
				return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
			}
			if _, duplicate := seen[id]; duplicate {
				return QuickReplyAccountMutation{}, domain.ErrShortcutInvalid
			}
			seen[id] = struct{}{}
		}
	default:
		return QuickReplyAccountMutation{}, fmt.Errorf("unknown quick reply mutation kind %q", m.Kind)
	}
	return m, nil
}

func ValidateQuickReplyAccountEffects(snapshot QuickReplyAccountMutationSnapshot, effects []DeliveryEffect) error {
	want := 0
	if snapshot.Changed {
		want = 1
	}
	if len(effects) != want {
		return fmt.Errorf("quick reply effects=%d, want %d", len(effects), want)
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
		return fmt.Errorf("quick reply effect has wrong target, kind or exclusion")
	}
	event := effect.Event
	if event.Pts != 0 || event.PtsCount != 1 || event.Date != snapshot.Mutation.Date || event.MaxID != snapshot.Result.ShortcutID {
		return fmt.Errorf("quick reply effect has invalid account PTS fields")
	}
	if !sameQuickReplies(event.QuickReplies, snapshot.Result.List.QuickReplies) {
		return fmt.Errorf("quick reply effect list does not match aggregate snapshot")
	}
	switch snapshot.Result.Kind {
	case domain.QuickReplyMutationNew:
		if event.Type != domain.UpdateEventNewQuickReply || !reflect.DeepEqual(event.QuickReply, snapshot.Result.QuickReply) ||
			!reflect.DeepEqual(event.QuickReplyMessage, snapshot.Result.Message) || len(event.MessageIDs) != 0 {
			return fmt.Errorf("new quick reply effect does not match aggregate snapshot")
		}
	case domain.QuickReplyMutationMessage:
		if event.Type != domain.UpdateEventQuickReplyMessage || !reflect.DeepEqual(event.QuickReplyMessage, snapshot.Result.Message) || len(event.MessageIDs) != 0 {
			return fmt.Errorf("quick reply message effect does not match aggregate snapshot")
		}
	case domain.QuickReplyMutationDelete:
		if event.Type != domain.UpdateEventDeleteQuickReply || event.QuickReply.ID != 0 || event.QuickReplyMessage.ID != 0 || len(event.MessageIDs) != 0 {
			return fmt.Errorf("delete quick reply effect does not match aggregate snapshot")
		}
	case domain.QuickReplyMutationIDs:
		if event.Type != domain.UpdateEventDeleteQuickReplyMessages || !sameInts(event.MessageIDs, snapshot.Result.MessageIDs) || event.QuickReply.ID != 0 || event.QuickReplyMessage.ID != 0 {
			return fmt.Errorf("delete quick reply messages effect does not match aggregate snapshot")
		}
	case domain.QuickReplyMutationList:
		if event.Type != domain.UpdateEventQuickReplies || event.QuickReply.ID != 0 || event.QuickReplyMessage.ID != 0 || len(event.MessageIDs) != 0 {
			return fmt.Errorf("quick reply list effect does not match aggregate snapshot")
		}
	default:
		return fmt.Errorf("quick reply snapshot has unknown result kind %q", snapshot.Result.Kind)
	}
	return nil
}

func sameQuickReplies(a, b []domain.QuickReply) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func CloneQuickReplyAccountSnapshot(snapshot QuickReplyAccountMutationSnapshot) QuickReplyAccountMutationSnapshot {
	out := snapshot
	out.Mutation.Order = append([]int(nil), snapshot.Mutation.Order...)
	out.Mutation.MessageIDs = append([]int(nil), snapshot.Mutation.MessageIDs...)
	out.Mutation.Message.Entities = append([]domain.MessageEntity(nil), snapshot.Mutation.Message.Entities...)
	out.Result.List.QuickReplies = append([]domain.QuickReply(nil), snapshot.Result.List.QuickReplies...)
	out.Result.List.Messages = cloneQuickReplyMessages(snapshot.Result.List.Messages)
	out.Result.QuickReply = snapshot.Result.QuickReply
	out.Result.Message = cloneQuickReplyMessage(snapshot.Result.Message)
	out.Result.MessageIDs = append([]int(nil), snapshot.Result.MessageIDs...)
	out.Effects = append([]DeliveryEffect(nil), snapshot.Effects...)
	return out
}

func cloneQuickReplyMessages(in []domain.QuickReplyMessage) []domain.QuickReplyMessage {
	out := make([]domain.QuickReplyMessage, len(in))
	for i := range in {
		out[i] = cloneQuickReplyMessage(in[i])
	}
	return out
}

func cloneQuickReplyMessage(in domain.QuickReplyMessage) domain.QuickReplyMessage {
	out := in
	out.Entities = append([]domain.MessageEntity(nil), in.Entities...)
	return out
}
