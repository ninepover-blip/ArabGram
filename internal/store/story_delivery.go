package store

import (
	"fmt"
	"reflect"
	"sort"

	"telesrv/internal/domain"
)

// StoryMutationKind identifies the owning story aggregate operation. Every
// production mutation reaches the store through one of these boundaries; the
// mutation-only helpers in concrete stores are deliberately not exported.
type StoryMutationKind uint8

const (
	StoryMutationInvalid StoryMutationKind = iota
	StoryMutationCreate
	StoryMutationEdit
	StoryMutationDelete
	StoryMutationTogglePinned
	StoryMutationRead
	StoryMutationReaction
)

// StoryDeliveryRequirement is an immutable account-lane event derived from the
// post-mutation snapshot. ExcludeOrigin is true only when the target is the RPC
// actor and the synchronous response contains the same absolute result.
type StoryDeliveryRequirement struct {
	TargetUserID int64
	Event        domain.UpdateEvent
	ExcludeOrigin bool
}

// StoryMutationSnapshot is the only input accepted by a story delivery
// builder. Exactly one result field is populated according to Kind.
type StoryMutationSnapshot struct {
	Kind        StoryMutationKind
	ActorUserID int64
	Changed     bool

	Create   domain.StoryCreateResult
	Edit     domain.StoryEditResult
	Mutation domain.StoryMutationResult
	Read     domain.StoryReadResult
	Reaction domain.StoryReactionResult

	Required []StoryDeliveryRequirement
	Effects  []DeliveryEffect
}

// StoryFanoutFact is the bounded, transaction-local visibility fact used for
// user-story privacy/delete fanout.
type StoryFanoutFact struct {
	UserID       int64
	IsContact    bool
	CloseFriend  bool
	StoryBlocked bool
}

func StoryMutationSnapshotForDelivery(in StoryMutationSnapshot) StoryMutationSnapshot {
	out := cloneStoryMutationSnapshot(in)
	out.Effects = nil
	return out
}

func StoryEvent(story domain.Story) domain.UpdateEvent {
	return domain.UpdateEvent{
		Type: domain.UpdateEventStory, Date: story.Date,
		Peer: story.Owner, Story: cloneStoreStory(story), PtsCount: 1,
	}
}

func ReadStoriesEvent(read domain.StoryReadResult) domain.UpdateEvent {
	return domain.UpdateEvent{
		Type: domain.UpdateEventReadStories, Date: read.Date,
		Peer: read.Peer, MaxID: read.MaxReadID, PtsCount: 1,
	}
}

func SentStoryReactionEvent(reaction domain.StoryReactionResult) domain.UpdateEvent {
	return domain.UpdateEvent{
		Type: domain.UpdateEventSentStoryReaction, Date: reaction.Date,
		Peer: reaction.Peer, MaxID: reaction.StoryID,
		Story: cloneStoreStory(reaction.Story), Reaction: cloneStoreReaction(reaction.Reaction), PtsCount: 1,
	}
}

func NewStoryReactionEvent(reaction domain.StoryReactionResult) domain.UpdateEvent {
	return domain.UpdateEvent{
		Type: domain.UpdateEventNewStoryReaction, Date: reaction.Date,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: reaction.ViewerID}, MaxID: reaction.StoryID,
		Story: cloneStoreStory(reaction.Story), Reaction: cloneStoreReaction(reaction.Reaction), PtsCount: 1,
	}
}

// AppendStoryChangeRequirements adds the actor event and, for a channel-owned
// story, one account event per other active member. No channel PTS is created.
func AppendStoryChangeRequirements(snapshot *StoryMutationSnapshot, story domain.Story, channelMemberIDs []int64) {
	if snapshot == nil || snapshot.ActorUserID <= 0 || story.ID <= 0 {
		return
	}
	snapshot.Required = append(snapshot.Required, StoryDeliveryRequirement{
		TargetUserID: snapshot.ActorUserID, Event: StoryEvent(story), ExcludeOrigin: true,
	})
	if story.Owner.Type != domain.PeerTypeChannel {
		return
	}
	projection := StoryFanoutSnapshot(story)
	for _, userID := range channelMemberIDs {
		if userID <= 0 || userID == snapshot.ActorUserID {
			continue
		}
		snapshot.Required = append(snapshot.Required, StoryDeliveryRequirement{
			TargetUserID: userID, Event: StoryEvent(projection),
		})
	}
}

func AppendStoryPrivacyFanoutRequirements(snapshot *StoryMutationSnapshot, before, after domain.Story, facts []StoryFanoutFact, now int) {
	if snapshot == nil || before.Owner != after.Owner || before.ID != after.ID ||
		after.Owner.Type != domain.PeerTypeUser || after.Owner.ID != snapshot.ActorUserID {
		return
	}
	beforeActive, afterActive := before.Active(now), after.Active(now)
	if !beforeActive && !afterActive {
		return
	}
	for _, fact := range facts {
		if fact.UserID <= 0 || fact.UserID == snapshot.ActorUserID {
			continue
		}
		beforeVisible := beforeActive && before.VisibleToWithStoryFacts(fact.UserID, fact.IsContact, fact.CloseFriend, fact.StoryBlocked)
		afterVisible := afterActive && after.VisibleToWithStoryFacts(fact.UserID, fact.IsContact, fact.CloseFriend, fact.StoryBlocked)
		var projection domain.Story
		switch {
		case afterVisible:
			projection = StoryFanoutSnapshot(after)
		case beforeVisible:
			projection = StoryFanoutSnapshot(after)
			projection.Deleted = true
		default:
			continue
		}
		snapshot.Required = append(snapshot.Required, StoryDeliveryRequirement{
			TargetUserID: fact.UserID, Event: StoryEvent(projection),
		})
	}
}

func AppendDeletedStoryFanoutRequirements(snapshot *StoryMutationSnapshot, before, deleted domain.Story, facts []StoryFanoutFact, now int) {
	if snapshot == nil || before.Owner.Type != domain.PeerTypeUser || before.Owner.ID != snapshot.ActorUserID ||
		before.Owner != deleted.Owner || before.ID <= 0 || before.ID != deleted.ID || (!before.Active(now) && !before.Pinned) {
		return
	}
	projection := StoryFanoutSnapshot(deleted)
	projection.Deleted = true
	for _, fact := range facts {
		if fact.UserID <= 0 || fact.UserID == snapshot.ActorUserID ||
			!before.VisibleToWithStoryFacts(fact.UserID, fact.IsContact, fact.CloseFriend, fact.StoryBlocked) {
			continue
		}
		snapshot.Required = append(snapshot.Required, StoryDeliveryRequirement{
			TargetUserID: fact.UserID, Event: StoryEvent(projection),
		})
	}
}

func AppendUnpinnedExpiredStoryFanoutRequirements(snapshot *StoryMutationSnapshot, before, after domain.Story, facts []StoryFanoutFact, now int) {
	if before.ID == 0 || after.ID == 0 || before.Owner != after.Owner || before.ID != after.ID ||
		!before.Pinned || before.Active(now) || after.Active(now) || after.Pinned || after.Deleted {
		return
	}
	AppendDeletedStoryFanoutRequirements(snapshot, before, after, facts, now)
}

func StoryFanoutSnapshot(story domain.Story) domain.Story {
	story = cloneStoreStory(story)
	story.Out = false
	story.Views = domain.StoryViews{}
	story.SentReaction = nil
	return story
}

func SortStoryDeliveryRequirements(items []StoryDeliveryRequirement) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].TargetUserID != items[j].TargetUserID {
			return items[i].TargetUserID < items[j].TargetUserID
		}
		if items[i].Event.Type != items[j].Event.Type {
			return items[i].Event.Type < items[j].Event.Type
		}
		if items[i].Event.Peer.Type != items[j].Event.Peer.Type {
			return items[i].Event.Peer.Type < items[j].Event.Peer.Type
		}
		if items[i].Event.Peer.ID != items[j].Event.Peer.ID {
			return items[i].Event.Peer.ID < items[j].Event.Peer.ID
		}
		return storyEventStableID(items[i].Event) < storyEventStableID(items[j].Event)
	})
}

func storyEventStableID(event domain.UpdateEvent) int {
	if event.MaxID != 0 {
		return event.MaxID
	}
	return event.Story.ID
}

// ValidateStoryMutationDeliveryEffects enforces a one-to-one projection from
// the immutable requirement list. It also prevents a builder from excluding a
// non-origin recipient or smuggling channel/absolute delivery into this lane.
func ValidateStoryMutationDeliveryEffects(snapshot StoryMutationSnapshot, effects []DeliveryEffect) error {
	if !snapshot.Changed {
		if len(snapshot.Required) != 0 || len(effects) != 0 {
			return fmt.Errorf("story mutation noop must not produce delivery effects")
		}
		return nil
	}
	if len(snapshot.Required) == 0 {
		return fmt.Errorf("story mutation changed without delivery requirements")
	}
	if len(effects) != len(snapshot.Required) {
		return fmt.Errorf("story delivery effect count %d does not match required count %d", len(effects), len(snapshot.Required))
	}
	for i := range effects {
		required, effect := snapshot.Required[i], effects[i]
		if err := effect.Validate(); err != nil {
			return fmt.Errorf("story delivery effect %d: %w", i, err)
		}
		if effect.Kind != DeliveryEffectAccountPTS || effect.TargetUserID != required.TargetUserID ||
			!reflect.DeepEqual(effect.Event, required.Event) {
			return fmt.Errorf("story delivery effect %d does not match required account event", i)
		}
		if !required.ExcludeOrigin && (effect.ExcludeAuthKeyID != ([8]byte{}) || effect.ExcludeSessionID != 0) {
			return fmt.Errorf("story delivery effect %d excludes a non-origin recipient", i)
		}
	}
	return nil
}

func cloneStoryMutationSnapshot(in StoryMutationSnapshot) StoryMutationSnapshot {
	out := in
	out.Create.Story = cloneStoreStory(in.Create.Story)
	out.Edit.Story = cloneStoreStory(in.Edit.Story)
	out.Edit.Previous = cloneStoreStory(in.Edit.Previous)
	out.Mutation.IDs = append([]int(nil), in.Mutation.IDs...)
	out.Mutation.Stories = cloneStoreStories(in.Mutation.Stories)
	out.Mutation.Previous = cloneStoreStories(in.Mutation.Previous)
	out.Reaction.Reaction = cloneStoreReaction(in.Reaction.Reaction)
	out.Reaction.Story = cloneStoreStory(in.Reaction.Story)
	out.Required = append([]StoryDeliveryRequirement(nil), in.Required...)
	for i := range out.Required {
		out.Required[i].Event = cloneStoreUpdateEvent(out.Required[i].Event)
	}
	out.Effects = append([]DeliveryEffect(nil), in.Effects...)
	for i := range out.Effects {
		out.Effects[i].Payload = append([]byte(nil), out.Effects[i].Payload...)
		out.Effects[i].Event = cloneStoreUpdateEvent(out.Effects[i].Event)
	}
	return out
}

func cloneStoreStories(in []domain.Story) []domain.Story {
	out := make([]domain.Story, len(in))
	for i := range in {
		out[i] = cloneStoreStory(in[i])
	}
	return out
}

func cloneStoreStory(in domain.Story) domain.Story {
	out := in
	out.PrivacyRules = append([]domain.PrivacyRule(nil), in.PrivacyRules...)
	out.AllowUserIDs = append([]int64(nil), in.AllowUserIDs...)
	out.DisallowUserIDs = append([]int64(nil), in.DisallowUserIDs...)
	out.Entities = append([]domain.MessageEntity(nil), in.Entities...)
	out.MediaAreas = append([]domain.StoryMediaArea(nil), in.MediaAreas...)
	out.Views.Reactions = append([]domain.ChannelMessageReactionCount(nil), in.Views.Reactions...)
	out.Views.RecentViewers = append([]int64(nil), in.Views.RecentViewers...)
	out.SentReaction = cloneStoreReaction(in.SentReaction)
	return out
}

func cloneStoreReaction(in *domain.MessageReaction) *domain.MessageReaction {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneStoreUpdateEvent(in domain.UpdateEvent) domain.UpdateEvent {
	out := in
	out.Story = cloneStoreStory(in.Story)
	out.Reaction = cloneStoreReaction(in.Reaction)
	return out
}
