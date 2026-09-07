package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type blocklistFixture struct {
	contacts *ContactStore
	users    *UserStore
	stories  *StoryStore
	privacy  *PrivacyStore
	channels *ChannelStore
	events   *UpdateEventStore
	owner    domain.User
	peers    []domain.User
}

func newBlocklistFixture(t *testing.T, n int) *blocklistFixture {
	t.Helper()
	f := &blocklistFixture{contacts: NewContactStore(), users: NewUserStore(), stories: NewStoryStore(), privacy: NewPrivacyStore(), channels: NewChannelStore(), events: NewUpdateEventStore()}
	var err error
	f.owner, err = f.users.Create(context.Background(), domain.User{FirstName: "Owner"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		u, err := f.users.Create(context.Background(), domain.User{FirstName: fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
		f.peers = append(f.peers, u)
	}
	f.contacts.AttachBlocklistStores(f.stories, f.privacy, f.users, f.channels)
	f.contacts.AttachUpdateEventStore(f.events)
	return f
}
func (f *blocklistFixture) command(t *testing.T, kind store.BlocklistMutationKind, ids ...int64) store.BlocklistMutation {
	t.Helper()
	stories, err := f.stories.ListOwnerActiveStories(context.Background(), domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}, 1700000100, 100)
	if err != nil {
		t.Fatal(err)
	}
	m := store.BlocklistMutation{Kind: kind, OwnerUserID: f.owner.ID, PeerIDs: ids, Date: 1700000100, Stories: stories.Stories}
	if kind == store.BlocklistReplace {
		m.ExpectedIDs, err = f.contacts.ReadBlocklistIDs(context.Background(), f.owner.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	return m
}
func (f *blocklistFixture) apply(t *testing.T, m store.BlocklistMutation) store.BlocklistMutationSnapshot {
	t.Helper()
	r, err := f.contacts.MutateBlocklist(context.Background(), m, store.BlocklistDeliveryEffects)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBlocklistMemoryAtomicFailuresAndNoop(t *testing.T) {
	f := newBlocklistFixture(t, 2)
	ctx := context.Background()
	m := f.command(t, store.BlocklistBlock, f.peers[0].ID)
	builders := []store.DeliveryEffectsBuilder[store.BlocklistMutationSnapshot]{nil, func(store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, errors.New("builder failure")
	}, func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		e, _ := store.BlocklistDeliveryEffects(s)
		e[0].ExcludeSessionID = 1
		return e, nil
	}, func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		e, _ := store.BlocklistDeliveryEffects(s)
		e[0].Event.Bool = false
		return e, nil
	}}
	for i, b := range builders {
		r, err := f.contacts.MutateBlocklist(ctx, m, b)
		if err == nil || len(r.Changes) != 0 {
			t.Fatalf("failure %d committed: %+v %v", i, r, err)
		}
		ids, _ := f.contacts.ReadBlocklistIDs(ctx, f.owner.ID)
		ev, _ := f.events.ListAfter(ctx, f.owner.ID, 0, 100)
		if len(ids)+len(ev) != 0 {
			t.Fatal("failure left durable state")
		}
	}
	f.apply(t, m)
	row := f.contacts.blocks[f.owner.ID][f.peers[0].ID]
	m.Date++
	noop := func(store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		t.Fatal("no-op called builder")
		return nil, nil
	}
	if r, err := f.contacts.MutateBlocklist(ctx, m, noop); err != nil || len(r.Changes) != 0 || !reflect.DeepEqual(f.contacts.blocks[f.owner.ID][f.peers[0].ID], row) {
		t.Fatal("repeat changed date or state", err)
	}
	f.apply(t, f.command(t, store.BlocklistUnblock, f.peers[0].ID))
	if _, err := f.contacts.MutateBlocklist(ctx, f.command(t, store.BlocklistUnblock, f.peers[0].ID), noop); err != nil {
		t.Fatal(err)
	}
	events, _ := f.events.ListAfter(ctx, f.owner.ID, 0, 100)
	if len(events) != 4 {
		t.Fatal("no-op allocated PTS", len(events))
	}
	for i, e := range events {
		d := f.events.dispatches[f.owner.ID][i]
		if e.Pts != i+1 || e.PtsCount != 1 || d.ExcludeAuthKeyID != ([8]byte{}) || d.ExcludeSessionID != 0 {
			t.Fatal("PTS or exclusion")
		}
	}
	f.contacts.AttachBlocklistStores(nil, f.privacy, f.users, f.channels)
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistRequired) {
		t.Fatal("missing story boundary", err)
	}
}

func TestBlocklistMemoryVisibilityAndImmutableSnapshot(t *testing.T) {
	f := newBlocklistFixture(t, 2)
	ctx := context.Background()
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}
	a, b := f.peers[0].ID, f.peers[1].ID
	_, err := f.contacts.Upsert(ctx, f.owner.ID, domain.ContactInput{ContactUserID: a, FirstName: "Contact"})
	if err != nil {
		t.Fatal(err)
	}
	f.contacts.m[f.owner.ID].Contacts[0].CloseFriend = true
	stories := []domain.Story{{ID: 1, Public: true}, {ID: 2, SelectedContacts: true, AllowUserIDs: []int64{a}}, {ID: 3, Contacts: true}, {ID: 4, CloseFriends: true}, {ID: 5, Public: true, DisallowUserIDs: []int64{a, b}}, {ID: 6, SelectedContacts: true}}
	for _, s := range stories {
		s.Owner = owner
		s.Date = 1700000000
		s.ExpireDate = 1700003600
		_, err := f.stories.UpsertStory(ctx, domain.UpsertStoryRequest{Story: s})
		if err != nil {
			t.Fatal(err)
		}
	}
	m := f.command(t, store.BlocklistReplace, a, b)
	result := f.apply(t, m)
	counts := map[int64]int{}
	for _, r := range result.RequiredEvents {
		counts[r.TargetUserID]++
		if r.Event.Type == domain.UpdateEventStory && (!r.Event.Story.Deleted || r.Event.Story.Out) {
			t.Fatal("wrong hidden story")
		}
	}
	if !reflect.DeepEqual(counts, map[int64]int{f.owner.ID: 4, a: 4, b: 1}) {
		t.Fatal("story visibility diff", counts)
	}
	if list, err := f.stories.GetPeerStories(ctx, a, owner, m.Date); err != nil || len(list.Stories) != 0 {
		t.Fatal("shared block visibility not committed", err)
	}
	m = f.command(t, store.BlocklistReplace)
	if _, err := f.contacts.MutateBlocklist(ctx, m, func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		s.Stories[1].AllowUserIDs[0] = b
		s.RequiredEvents[0].Event.Bool = true
		return store.BlocklistDeliveryEffects(s)
	}); err == nil {
		t.Fatal("mutating builder affected authority")
	}
	ids, _ := f.contacts.ReadBlocklistIDs(ctx, f.owner.ID)
	if len(ids) != 2 || f.stories.stories[storyKey{domain.PeerTypeUser, f.owner.ID, 2}].AllowUserIDs[0] != a {
		t.Fatal("rejected builder mutated snapshots")
	}
	result = f.apply(t, m)
	for _, r := range result.RequiredEvents {
		if r.Event.Type == domain.UpdateEventStory && (r.Event.Story.Deleted || r.Event.Story.Out || !reflect.DeepEqual(r.Event.Story.Views, domain.StoryViews{})) {
			t.Fatal("wrong restored story")
		}
	}
}

func TestBlocklistMemoryCASLimitsAndConcurrentDuplicates(t *testing.T) {
	f := newBlocklistFixture(t, 1025)
	ctx := context.Background()
	ids := make([]int64, len(f.peers))
	for i, p := range f.peers {
		ids[i] = p.ID
	}
	m := f.command(t, store.BlocklistReplace, ids...)
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistLimit) {
		t.Fatal("1025 accepted", err)
	}
	m = f.command(t, store.BlocklistReplace, ids[:1024]...)
	r := f.apply(t, m)
	if len(r.Changes) != 1024 || len(r.RequiredEvents) != 2048 {
		t.Fatal("1024 boundary")
	}
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistConflict) {
		t.Fatal("stale replacement CAS", err)
	}
	m = f.command(t, store.BlocklistReplace)
	f.apply(t, m)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	m = f.command(t, store.BlocklistBlock, ids[0])
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ev, _ := f.events.ListAfter(ctx, f.owner.ID, 4096, 100)
	if len(ev) != 2 {
		t.Fatal("concurrent duplicate PTS", len(ev))
	}
}

func TestBlocklistMemoryCompleteSnapshotAndFanoutBound(t *testing.T) {
	f := newBlocklistFixture(t, 41)
	ctx := context.Background()
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}
	ids := make([]int64, len(f.peers))
	for i, p := range f.peers {
		ids[i] = p.ID
	}
	for i := 1; i <= 100; i++ {
		if _, err := f.stories.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{Owner: owner, ID: i, Date: 1700000000, ExpireDate: 1700003600, Public: true}}); err != nil {
			t.Fatal(err)
		}
	}
	m := f.command(t, store.BlocklistReplace, ids...)
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistLimit) {
		t.Fatal("4182 events accepted", err)
	}
	if len(f.contacts.blocks[f.owner.ID]) != 0 || len(f.events.events[f.owner.ID]) != 0 {
		t.Fatal("oversize fanout partially committed")
	}
	m = f.command(t, store.BlocklistReplace, ids[:40]...)
	stale := m
	stale.Stories = stale.Stories[:99]
	if _, err := f.contacts.MutateBlocklist(ctx, stale, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistConflict) {
		t.Fatal("truncated snapshot accepted", err)
	}
	if got := f.apply(t, m); len(got.RequiredEvents) != 4080 {
		t.Fatal("4080 event boundary", len(got.RequiredEvents))
	}
	if _, err := f.stories.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{Owner: owner, ID: 101, Date: 1700000000, ExpireDate: 1700003600, Public: true}}); err != nil {
		t.Fatal(err)
	}
	// RPC reads at most 100 stories. The aggregate must detect a larger active
	// set itself, rather than silently freezing a truncated page.
	if _, err := f.contacts.MutateBlocklist(ctx, f.command(t, store.BlocklistUnblock, ids[0]), store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistLimit) {
		t.Fatal("active story overflow accepted", err)
	}
}
