package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/tg"

	appupdates "telesrv/internal/app/updates"
	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func rpcTestStoryUpdates(stories *memory.StoryStore) *appupdates.Service {
	events := memory.NewUpdateEventStore()
	stories.AttachUpdateEventStore(events)
	return appupdates.NewService(memory.NewUpdateStateStore(), events)
}

// rpcStoryDeliveryCaptureStore lets legacy RPC assertions observe the durable
// effects produced by the new aggregate boundary. It is test-only: production
// still has exactly one mutation + delivery transaction and no Record* path.
type rpcStoryDeliveryCaptureStore struct {
	store.StoryStore
	capture *captureUpdates
}

func (s *rpcStoryDeliveryCaptureStore) captureBuilder(next store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) store.DeliveryEffectsBuilder[store.StoryMutationSnapshot] {
	return func(snapshot store.StoryMutationSnapshot) ([]store.DeliveryEffect, error) {
		effects, err := next(snapshot)
		if err != nil {
			return nil, err
		}
		for _, effect := range effects {
			if effect.Kind != store.DeliveryEffectAccountPTS {
				continue
			}
			s.capture.authKeyID = effect.ExcludeAuthKeyID
			s.capture.userID = effect.TargetUserID
			s.capture.captureExclude(effect.ExcludeAuthKeyID, effect.ExcludeSessionID)
			s.capture.events = append(s.capture.events, effect.Event)
		}
		return effects, nil
	}
}

func (s *rpcStoryDeliveryCaptureStore) CreateStoryWithDelivery(ctx context.Context, actorUserID int64, req domain.StoryCreateRequest, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryCreateResult, error) {
	return s.StoryStore.CreateStoryWithDelivery(ctx, actorUserID, req, s.captureBuilder(build))
}

func (s *rpcStoryDeliveryCaptureStore) EditStoryWithDelivery(ctx context.Context, actorUserID int64, req domain.StoryEditRequest, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryEditResult, error) {
	return s.StoryStore.EditStoryWithDelivery(ctx, actorUserID, req, s.captureBuilder(build))
}

func (s *rpcStoryDeliveryCaptureStore) DeleteStoriesWithDelivery(ctx context.Context, actorUserID int64, peer domain.Peer, ids []int, date int, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryMutationResult, error) {
	return s.StoryStore.DeleteStoriesWithDelivery(ctx, actorUserID, peer, ids, date, s.captureBuilder(build))
}

func (s *rpcStoryDeliveryCaptureStore) TogglePinnedWithDelivery(ctx context.Context, actorUserID int64, peer domain.Peer, ids []int, pinned bool, date int, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryMutationResult, error) {
	return s.StoryStore.TogglePinnedWithDelivery(ctx, actorUserID, peer, ids, pinned, date, s.captureBuilder(build))
}

func (s *rpcStoryDeliveryCaptureStore) MarkReadWithDelivery(ctx context.Context, viewerUserID int64, peer domain.Peer, maxID, date int, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryReadResult, error) {
	return s.StoryStore.MarkReadWithDelivery(ctx, viewerUserID, peer, maxID, date, s.captureBuilder(build))
}

func (s *rpcStoryDeliveryCaptureStore) SetReactionWithDelivery(ctx context.Context, viewerUserID int64, peer domain.Peer, storyID int, reaction *domain.MessageReaction, date int, build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot]) (domain.StoryReactionResult, error) {
	return s.StoryStore.SetReactionWithDelivery(ctx, viewerUserID, peer, storyID, reaction, date, s.captureBuilder(build))
}

func rpcTestStoryDeliveryEffects(snapshot store.StoryMutationSnapshot) ([]store.DeliveryEffect, error) {
	effects := make([]store.DeliveryEffect, 0, len(snapshot.Required))
	for _, required := range snapshot.Required {
		effects = append(effects, store.AccountPTSDeliveryEffect(required.TargetUserID, required.Event, [8]byte{}, 0))
	}
	return effects, nil
}

func rpcTestMarkStoryRead(ctx context.Context, stories *memory.StoryStore, viewerUserID int64, peer domain.Peer, maxID, date int) (domain.StoryReadResult, error) {
	return stories.MarkReadWithDelivery(ctx, viewerUserID, peer, maxID, date, rpcTestStoryDeliveryEffects)
}

func rpcTestSetStoryReaction(ctx context.Context, stories *memory.StoryStore, viewerUserID int64, peer domain.Peer, storyID int, reaction *domain.MessageReaction, date int) (domain.StoryReactionResult, error) {
	return stories.SetReactionWithDelivery(ctx, viewerUserID, peer, storyID, reaction, date, rpcTestStoryDeliveryEffects)
}

func rpcTestCreateStory(ctx context.Context, stories *memory.StoryStore, actorUserID int64, req domain.StoryCreateRequest) (domain.StoryCreateResult, error) {
	return stories.CreateStoryWithDelivery(ctx, actorUserID, req, rpcTestStoryDeliveryEffects)
}

func rpcTestDeleteStories(ctx context.Context, stories *memory.StoryStore, actorUserID int64, peer domain.Peer, ids []int, date int) (domain.StoryMutationResult, error) {
	return stories.DeleteStoriesWithDelivery(ctx, actorUserID, peer, ids, date, rpcTestStoryDeliveryEffects)
}

func requireDeliveryUpdates(t *testing.T, item store.DeliveryOutboxItem) *tg.Updates {
	t.Helper()
	decoded, err := decodeDeliveryUpdate(item.Payload)
	if err != nil {
		t.Fatalf("decode durable delivery for user %d: %v", item.TargetUserID, err)
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok {
		t.Fatalf("durable delivery for user %d = %T, want *tg.Updates", item.TargetUserID, decoded)
	}
	return updates
}

func requireDeliveryItemForUser(t *testing.T, items []store.DeliveryOutboxItem, userID int64) store.DeliveryOutboxItem {
	t.Helper()
	for _, item := range items {
		if item.TargetUserID == userID {
			return item
		}
	}
	t.Fatalf("durable deliveries = %+v, want target user %d", items, userID)
	return store.DeliveryOutboxItem{}
}

func rpcTestBlocklistStores(contacts *memory.ContactStore, users *memory.UserStore, stories *memory.StoryStore, events *memory.UpdateEventStore) {
	contacts.AttachBlocklistStores(stories, memory.NewPrivacyStore(), users, memory.NewChannelStore())
	contacts.AttachUpdateEventStore(events)
}
