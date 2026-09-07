package stories

import (
	"context"
	"errors"
	"strings"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func storyTestDeliveryEffects(snapshot storepkg.StoryMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
	effects := make([]storepkg.DeliveryEffect, 0, len(snapshot.Required))
	for _, required := range snapshot.Required {
		effects = append(effects, storepkg.AccountPTSDeliveryEffect(required.TargetUserID, required.Event, [8]byte{}, 0))
	}
	return effects, nil
}

func TestReadStoriesClampsFutureMaxIDToVisibleActiveStory(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStoryStore()
	service := NewService(store)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	viewerID := int64(2001)
	for _, id := range []int{1, 2} {
		if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
			Owner:      owner,
			ID:         id,
			Date:       100 + id,
			ExpireDate: 1000,
			Public:     true,
		}}); err != nil {
			t.Fatalf("upsert story %d: %v", id, err)
		}
	}

	read, err := service.ReadStories(ctx, viewerID, owner, 99, 200, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("read stories: %v", err)
	}
	if !read.Advanced || read.MaxReadID != 2 {
		t.Fatalf("read = %+v, want advanced max 2", read)
	}

	if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
		Owner:      owner,
		ID:         3,
		Date:       300,
		ExpireDate: 1000,
		Public:     true,
	}}); err != nil {
		t.Fatalf("upsert future story: %v", err)
	}
	peerStories, err := store.GetPeerStories(ctx, viewerID, owner, 400)
	if err != nil {
		t.Fatalf("get peer stories: %v", err)
	}
	if peerStories.MaxReadID != 2 || len(peerStories.Stories) != 3 {
		t.Fatalf("peer stories = %+v, want max read 2 and three active stories", peerStories)
	}
}

func TestReadStoriesWithoutVisibleActiveStoryDoesNotWriteFutureBoundary(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStoryStore()
	service := NewService(store)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	viewerID := int64(2001)

	read, err := service.ReadStories(ctx, viewerID, owner, 99, 200, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("read stories: %v", err)
	}
	if read.Advanced || read.MaxReadID != 0 {
		t.Fatalf("read = %+v, want no-op zero boundary", read)
	}
	states, err := store.ListReadStates(ctx, viewerID)
	if err != nil {
		t.Fatalf("list read states: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("read states = %+v, want none", states)
	}

	if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
		Owner:      owner,
		ID:         1,
		Date:       300,
		ExpireDate: 1000,
		Public:     true,
	}}); err != nil {
		t.Fatalf("upsert story after no-op read: %v", err)
	}
	peerStories, err := store.GetPeerStories(ctx, viewerID, owner, 400)
	if err != nil {
		t.Fatalf("get peer stories: %v", err)
	}
	if peerStories.MaxReadID != 0 || len(peerStories.Stories) != 1 {
		t.Fatalf("peer stories = %+v, want unread first story", peerStories)
	}
}

func TestReadStoriesWithoutStoreIsNoop(t *testing.T) {
	ctx := context.Background()
	service := NewService(nil)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}

	read, err := service.ReadStories(ctx, 2001, owner, 99, 200, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("read stories without store: %v", err)
	}
	if read.Advanced || read.MaxReadID != 0 || read.Peer != owner || read.ViewerID != 2001 {
		t.Fatalf("read without store = %+v, want no-op zero boundary for viewer/peer", read)
	}
}

func TestGetStoryReactionsListRequiresChannelInteractionGrant(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStoryStore()
	service := NewService(store)
	owner := domain.Peer{Type: domain.PeerTypeChannel, ID: 1001}
	if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
		Owner:      owner,
		ID:         1,
		Date:       100,
		ExpireDate: 1000,
		Public:     true,
	}}); err != nil {
		t.Fatalf("upsert story: %v", err)
	}
	if _, err := store.SetReactionWithDelivery(ctx, 2001, owner, 1, &domain.MessageReaction{
		Type:     domain.MessageReactionEmoji,
		Emoticon: "🔥",
	}, 110, storyTestDeliveryEffects); err != nil {
		t.Fatalf("set reaction: %v", err)
	}

	_, err := service.GetStoryReactionsList(ctx, 3001, domain.StoryReactionListRequest{
		Owner:   owner,
		StoryID: 1,
		Limit:   10,
	})
	if !errors.Is(err, domain.ErrStoryPeerInvalid) {
		t.Fatalf("ungranted channel reaction list err = %v, want ErrStoryPeerInvalid", err)
	}

	list, err := service.GetStoryReactionsList(ctx, 3001, domain.StoryReactionListRequest{
		Owner:                    owner,
		StoryID:                  1,
		Limit:                    10,
		CanViewOwnerInteractions: true,
	})
	if err != nil {
		t.Fatalf("granted channel reaction list: %v", err)
	}
	if list.Count != 1 || len(list.Reactions) != 1 || list.Reactions[0].ViewerID != 2001 {
		t.Fatalf("reaction list = %+v, want viewer 2001", list)
	}
}

func TestGetStoryReactionsListRejectsUserPeer(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewStoryStore())
	_, err := service.GetStoryReactionsList(ctx, 1001, domain.StoryReactionListRequest{
		Owner:                    domain.Peer{Type: domain.PeerTypeUser, ID: 1001},
		StoryID:                  1,
		Limit:                    10,
		CanViewOwnerInteractions: true,
	})
	if !errors.Is(err, domain.ErrStoryPeerInvalid) {
		t.Fatalf("user story reaction list err = %v, want ErrStoryPeerInvalid", err)
	}
}

func TestStoryInteractionListsRejectMalformedOffsets(t *testing.T) {
	ctx := context.Background()
	service := NewService(nil)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	if _, err := service.GetStoryViewsList(ctx, owner.ID, domain.StoryViewListRequest{
		Owner:   owner,
		StoryID: 1,
		Offset:  "bad",
		Limit:   10,
	}); !errors.Is(err, domain.ErrStoryOffsetInvalid) {
		t.Fatalf("bad views offset err = %v, want ErrStoryOffsetInvalid", err)
	}
	if _, err := service.GetStoryViewsList(ctx, owner.ID, domain.StoryViewListRequest{
		Owner:   owner,
		StoryID: 1,
		Offset:  strings.Repeat("9", domain.MaxStoryInteractionOffsetLength+1),
		Limit:   10,
	}); !errors.Is(err, domain.ErrStoryOffsetInvalid) {
		t.Fatalf("oversized views offset err = %v, want ErrStoryOffsetInvalid", err)
	}
	if _, err := service.GetStoryReactionsList(ctx, 2001, domain.StoryReactionListRequest{
		Owner:                    domain.Peer{Type: domain.PeerTypeChannel, ID: 3001},
		StoryID:                  1,
		Offset:                   "1:1700000000:2001",
		Limit:                    10,
		CanViewOwnerInteractions: true,
	}); !errors.Is(err, domain.ErrStoryOffsetInvalid) {
		t.Fatalf("reaction group offset err = %v, want ErrStoryOffsetInvalid", err)
	}
	if _, err := service.GetStoryReactionsList(ctx, 2001, domain.StoryReactionListRequest{
		Owner:                    domain.Peer{Type: domain.PeerTypeChannel, ID: 3001},
		StoryID:                  1,
		Offset:                   "1:1700000000:2001",
		Limit:                    10,
		ForwardsFirst:            true,
		CanViewOwnerInteractions: true,
	}); err != nil {
		t.Fatalf("reaction forwards-first group offset err = %v, want nil", err)
	}
}

func TestGetStoriesByIDDedupesAndRejectsEmptyIDs(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStoryStore()
	service := NewService(store)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
		Owner:      owner,
		ID:         3,
		Date:       100,
		ExpireDate: 1000,
		Public:     true,
	}}); err != nil {
		t.Fatalf("upsert story: %v", err)
	}

	list, err := service.GetStoriesByID(ctx, owner.ID, owner, []int{3, 3}, 200)
	if err != nil {
		t.Fatalf("get stories by duplicate ids: %v", err)
	}
	if len(list.Stories) != 1 || list.Stories[0].ID != 3 {
		t.Fatalf("stories = %+v, want one deduped story", list.Stories)
	}

	if _, err := service.GetStoriesByID(ctx, owner.ID, owner, nil, 200); !errors.Is(err, domain.ErrStoryIDInvalid) {
		t.Fatalf("empty get stories by id err = %v, want ErrStoryIDInvalid", err)
	}
}

func TestIncrementViewsDedupeAndRejectsEmptyIDs(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStoryStore()
	service := NewService(store)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}
	if _, err := store.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{
		Owner:      owner,
		ID:         7,
		Date:       100,
		ExpireDate: 1000,
		Public:     true,
	}}); err != nil {
		t.Fatalf("upsert story: %v", err)
	}

	created, err := service.IncrementViews(ctx, 2001, owner, []int{7, 7}, 200)
	if err != nil {
		t.Fatalf("increment views duplicate ids: %v", err)
	}
	if created != 1 {
		t.Fatalf("created views = %d, want one deduped view", created)
	}

	if _, err := service.IncrementViews(ctx, 2001, owner, nil, 200); !errors.Is(err, domain.ErrStoryIDInvalid) {
		t.Fatalf("empty increment views err = %v, want ErrStoryIDInvalid", err)
	}
}

func TestStoryMutationsDedupeIDsBeforeStore(t *testing.T) {
	ctx := context.Background()
	service := NewService(nil)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: 1001}

	pinned, err := service.TogglePinned(ctx, owner.ID, owner, []int{3, 3, 4}, true, 100, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("toggle pinned: %v", err)
	}
	if got, want := pinned.IDs, []int{3, 4}; !intSlicesEqual(got, want) {
		t.Fatalf("pinned ids = %v, want %v", got, want)
	}

	emptyPinned, err := service.TogglePinned(ctx, owner.ID, owner, nil, true, 100, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("empty toggle pinned: %v", err)
	}
	if len(emptyPinned.IDs) != 0 || len(emptyPinned.Stories) != 0 {
		t.Fatalf("empty pinned = %+v, want no-op", emptyPinned)
	}

	deleted, err := service.DeleteStories(ctx, owner.ID, owner, []int{4, 3, 4}, 101, storyTestDeliveryEffects)
	if err != nil {
		t.Fatalf("delete stories: %v", err)
	}
	if got, want := deleted.IDs, []int{4, 3}; !intSlicesEqual(got, want) {
		t.Fatalf("deleted ids = %v, want %v", got, want)
	}

	if err := service.TogglePinnedToTop(ctx, owner.ID, owner, []int{4, 3, 4}); err != nil {
		t.Fatalf("toggle pinned to top deduped ids: %v", err)
	}
	if err := service.TogglePinnedToTop(ctx, owner.ID, owner, []int{1, 2, 3, 4}); !errors.Is(err, domain.ErrStoryIDInvalid) {
		t.Fatalf("oversized pinned-to-top err = %v, want ErrStoryIDInvalid", err)
	}
}

func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
