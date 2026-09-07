package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"

	"telesrv/internal/domain"
)

const MaxBlocklistEvents = 4096

var (
	ErrBlocklistInvalid  = errors.New("blocklist mutation invalid")
	ErrBlocklistLimit    = errors.New("blocklist mutation limit exceeded")
	ErrBlocklistConflict = errors.New("blocklist snapshot changed")
	ErrBlocklistRequired = errors.New("blocklist aggregate dependencies required")
)

type BlocklistMutationKind uint8

const (
	BlocklistBlock BlocklistMutationKind = iota + 1
	BlocklistUnblock
	BlocklistReplace
)

type BlocklistMutation struct {
	Kind        BlocklistMutationKind
	OwnerUserID int64
	PeerIDs     []int64
	ExpectedIDs []int64
	Date        int
	Stories     []domain.Story
}

// The aggregate freezes these facts before building any delivery effects.
type BlocklistPeerFacts struct {
	PeerUserID    int64
	Contact       bool
	CloseFriend   bool
	KnowsPhone    bool
	Bot           bool
	Premium       bool
	SharedChatIDs []int64
}
type BlocklistChange struct {
	PeerUserID int64
	Blocked    bool
}
type BlocklistMutationSnapshot struct {
	OwnerUserID    int64
	Date           int
	Changes        []BlocklistChange
	Stories        []domain.Story
	PhoneRules     domain.PrivacyRules
	Peers          []BlocklistPeerFacts
	RequiredEvents []ContactMutationRequiredEvent
}

func BlocklistIDs(ids []int64) []int64 {
	out := append([]int64{}, ids...)
	slices.Sort(out)
	return slices.Compact(out)
}
func (m BlocklistMutation) Prepare() (BlocklistMutation, error) {
	if m.OwnerUserID <= 0 || m.Date <= 0 || m.Kind < BlocklistBlock || m.Kind > BlocklistReplace {
		return BlocklistMutation{}, ErrBlocklistInvalid
	}
	if len(m.PeerIDs) > MaxDeliveryTargets || len(m.ExpectedIDs) > MaxDeliveryTargets || len(m.Stories) > domain.MaxStoryListLimit {
		return BlocklistMutation{}, ErrBlocklistLimit
	}
	m.PeerIDs = BlocklistIDs(m.PeerIDs)
	m.ExpectedIDs = BlocklistIDs(m.ExpectedIDs)
	raw, err := json.Marshal(m.Stories)
	if err != nil {
		return BlocklistMutation{}, fmt.Errorf("%w: story snapshot: %v", ErrBlocklistInvalid, err)
	}
	var stories []domain.Story
	if err := json.Unmarshal(raw, &stories); err != nil {
		return BlocklistMutation{}, err
	}
	m.Stories = stories
	if m.Kind != BlocklistReplace && (len(m.PeerIDs) != 1 || len(m.ExpectedIDs) != 0) {
		return BlocklistMutation{}, ErrBlocklistInvalid
	}
	ids := BlocklistIDs(append(append([]int64{}, m.PeerIDs...), m.ExpectedIDs...))
	if len(ids) > MaxDeliveryTargets {
		return BlocklistMutation{}, ErrBlocklistLimit
	}
	for _, id := range ids {
		if id <= 0 || id == m.OwnerUserID {
			return BlocklistMutation{}, ErrBlocklistInvalid
		}
	}
	for i, story := range m.Stories {
		if story.Owner != (domain.Peer{Type: domain.PeerTypeUser, ID: m.OwnerUserID}) || !story.Active(m.Date) || story.ID <= 0 || (i > 0 && m.Stories[i-1].ID >= story.ID) {
			return BlocklistMutation{}, ErrBlocklistInvalid
		}
	}
	return m, nil
}

func BlocklistDiff(m BlocklistMutation, current []int64) ([]BlocklistChange, error) {
	current = BlocklistIDs(current)
	if len(current) > MaxDeliveryTargets {
		return nil, ErrBlocklistLimit
	}
	if m.Kind == BlocklistReplace && !slices.Equal(current, m.ExpectedIDs) {
		return nil, ErrBlocklistConflict
	}
	desired := map[int64]bool{}
	for _, id := range current {
		desired[id] = true
	}
	switch m.Kind {
	case BlocklistBlock:
		desired[m.PeerIDs[0]] = true
	case BlocklistUnblock:
		delete(desired, m.PeerIDs[0])
	case BlocklistReplace:
		clear(desired)
		for _, id := range m.PeerIDs {
			desired[id] = true
		}
	}
	if len(desired) > MaxDeliveryTargets {
		return nil, ErrBlocklistLimit
	}
	var out []BlocklistChange
	for _, id := range current {
		if !desired[id] {
			out = append(out, BlocklistChange{PeerUserID: id})
		}
	}
	for id := range desired {
		if !slices.Contains(current, id) {
			out = append(out, BlocklistChange{PeerUserID: id, Blocked: true})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerUserID < out[j].PeerUserID })
	return out, nil
}

// BuildBlocklistSnapshot is pure. Neither a builder nor a post-commit recorder
// may reload visibility facts or decide which required events to omit.
func BuildBlocklistSnapshot(m BlocklistMutation, changes []BlocklistChange, phone domain.PrivacyRules, peers []BlocklistPeerFacts) (BlocklistMutationSnapshot, error) {
	s := BlocklistMutationSnapshot{OwnerUserID: m.OwnerUserID, Date: m.Date, Changes: append([]BlocklistChange(nil), changes...), Stories: cloneStoreStories(m.Stories), PhoneRules: ClonePrivacyRules(phone), Peers: append([]BlocklistPeerFacts(nil), peers...)}
	if phone.OwnerUserID != m.OwnerUserID || phone.Key != domain.PrivacyKeyPhoneNumber || len(peers) != len(changes) {
		return BlocklistMutationSnapshot{}, ErrBlocklistInvalid
	}
	for i, c := range changes {
		f := peers[i]
		if f.PeerUserID != c.PeerUserID {
			return BlocklistMutationSnapshot{}, ErrBlocklistInvalid
		}
		visible := f.KnowsPhone || domain.EvaluatePrivacy(phone, domain.PrivacyContext{OwnerUserID: m.OwnerUserID, ViewerUserID: f.PeerUserID, ViewerIsContact: f.Contact, ViewerCloseFriend: f.CloseFriend, ViewerIsBot: f.Bot, ViewerIsPremium: f.Premium, SharedChatIDs: f.SharedChatIDs})
		peer := domain.Peer{Type: domain.PeerTypeUser, ID: c.PeerUserID}
		s.RequiredEvents = append(s.RequiredEvents, ContactMutationRequiredEvent{TargetUserID: m.OwnerUserID, Event: domain.UpdateEvent{Type: domain.UpdateEventPeerStoryBlocked, Peer: peer, Bool: c.Blocked, Date: m.Date, PtsCount: 1}}, ContactMutationRequiredEvent{TargetUserID: m.OwnerUserID, Event: domain.UpdateEvent{Type: domain.UpdateEventPeerSettings, Peer: peer, Settings: domain.PeerSettings{AddContact: !f.Contact, BlockContact: !c.Blocked, ShareContact: f.Contact && !visible, NeedContactsException: !visible}, Date: m.Date, PtsCount: 1}})
		for _, story := range s.Stories {
			before := story.VisibleToWithStoryFacts(c.PeerUserID, f.Contact, f.CloseFriend, !c.Blocked)
			after := story.VisibleToWithStoryFacts(c.PeerUserID, f.Contact, f.CloseFriend, c.Blocked)
			if before == after {
				continue
			}
			v := cloneStoreStory(story)
			v.Out = false
			v.Views = domain.StoryViews{}
			v.SentReaction = nil
			v.Deleted = !after
			s.RequiredEvents = append(s.RequiredEvents, ContactMutationRequiredEvent{TargetUserID: c.PeerUserID, Event: domain.UpdateEvent{Type: domain.UpdateEventStory, Peer: story.Owner, Story: v, Date: m.Date, PtsCount: 1}})
			if len(s.RequiredEvents) > MaxBlocklistEvents {
				return BlocklistMutationSnapshot{}, ErrBlocklistLimit
			}
		}
		if len(s.RequiredEvents) > MaxBlocklistEvents {
			return BlocklistMutationSnapshot{}, ErrBlocklistLimit
		}
	}
	sort.Slice(s.RequiredEvents, func(i, j int) bool {
		a, b := s.RequiredEvents[i], s.RequiredEvents[j]
		if a.TargetUserID != b.TargetUserID {
			return a.TargetUserID < b.TargetUserID
		}
		if a.Event.Peer.ID != b.Event.Peer.ID {
			return a.Event.Peer.ID < b.Event.Peer.ID
		}
		if a.Event.Type != b.Event.Type {
			return blocklistEventRank(a.Event.Type) < blocklistEventRank(b.Event.Type)
		}
		return a.Event.Story.ID < b.Event.Story.ID
	})
	return CloneBlocklistSnapshot(s)
}
func blocklistEventRank(t domain.UpdateEventType) int {
	switch t {
	case domain.UpdateEventPeerStoryBlocked:
		return 0
	case domain.UpdateEventPeerSettings:
		return 1
	default:
		return 2
	}
}
func CloneBlocklistSnapshot(s BlocklistMutationSnapshot) (BlocklistMutationSnapshot, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return BlocklistMutationSnapshot{}, fmt.Errorf("clone blocklist snapshot: %w", err)
	}
	var copy BlocklistMutationSnapshot
	if err := json.Unmarshal(raw, &copy); err != nil {
		return BlocklistMutationSnapshot{}, err
	}
	return copy, nil
}
func BlocklistDeliveryEffects(s BlocklistMutationSnapshot) ([]DeliveryEffect, error) {
	e := make([]DeliveryEffect, 0, len(s.RequiredEvents))
	for _, r := range s.RequiredEvents {
		e = append(e, AccountPTSDeliveryEffect(r.TargetUserID, r.Event, [8]byte{}, 0))
	}
	return e, nil
}
func ValidateBlocklistEffects(s BlocklistMutationSnapshot, effects []DeliveryEffect) error {
	if len(effects) != len(s.RequiredEvents) || len(effects) > MaxBlocklistEvents {
		return ErrBlocklistInvalid
	}
	for i, e := range effects {
		r := s.RequiredEvents[i]
		if err := e.Validate(); err != nil {
			return err
		}
		if e.Kind != DeliveryEffectAccountPTS || e.TargetUserID != r.TargetUserID || e.ExcludeAuthKeyID != ([8]byte{}) || e.ExcludeSessionID != 0 || !reflect.DeepEqual(e.Event, r.Event) {
			return fmt.Errorf("%w: effect %d differs from required event", ErrBlocklistInvalid, i)
		}
	}
	return nil
}

func BlocklistPrivacyChatIDs(r domain.PrivacyRules) []int64 {
	var ids []int64
	for _, rule := range r.Rules {
		if rule.Kind == domain.PrivacyRuleAllowChatParticipants || rule.Kind == domain.PrivacyRuleDisallowChatParticipants {
			ids = append(ids, rule.ChatIDs...)
		}
	}
	return BlocklistIDs(ids)
}
