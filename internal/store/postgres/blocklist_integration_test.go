package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type pgBlocklistFixture struct {
	pool     *pgxpool.Pool
	contacts *ContactStore
	owner    domain.User
	peers    []domain.User
}

func newPGBlocklistFixture(t *testing.T, n int) *pgBlocklistFixture {
	t.Helper()
	f := &pgBlocklistFixture{pool: testPool(t)}
	f.contacts = NewContactStore(f.pool)
	users := NewUserStore(f.pool)
	var ids []int64
	suffix := randomSuffix(t)
	for i := 0; i <= n; i++ {
		u, err := users.Create(context.Background(), domain.User{AccessHash: int64(900 + i), Phone: fmt.Sprintf("+1779%s%03d", suffix, i), FirstName: "Blocklist"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, u.ID)
		if i == 0 {
			f.owner = u
		} else {
			f.peers = append(f.peers, u)
		}
	}
	t.Cleanup(func() {
		// Stories use a polymorphic owner rather than a users foreign key.
		// Remove only this fixture's stories before removing its users.
		if _, err := f.pool.Exec(context.Background(), "DELETE FROM stories WHERE owner_peer_type='user' AND owner_peer_id=$1", f.owner.ID); err != nil {
			t.Error(err)
		}
		if _, err := f.pool.Exec(context.Background(), "DELETE FROM users WHERE id=ANY($1::bigint[])", ids); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *pgBlocklistFixture) command(t *testing.T, kind store.BlocklistMutationKind, ids ...int64) store.BlocklistMutation {
	t.Helper()
	m := store.BlocklistMutation{Kind: kind, OwnerUserID: f.owner.ID, PeerIDs: ids, Date: 1700000100}
	s, err := NewStoryStore(f.pool).ListOwnerActiveStories(context.Background(), domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}, m.Date, 100)
	if err != nil {
		t.Fatal(err)
	}
	m.Stories = s.Stories
	if kind == store.BlocklistReplace {
		m.ExpectedIDs, err = f.contacts.ReadBlocklistIDs(context.Background(), f.owner.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	return m
}
func (f *pgBlocklistFixture) facts(t *testing.T) string {
	t.Helper()
	var raw string
	err := f.pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
'blocks',(SELECT jsonb_agg(to_jsonb(b) ORDER BY blocked_user_id) FROM contact_blocks b WHERE owner_user_id=$1),
'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY pts) FROM user_update_events e WHERE user_id=$1),
'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY pts) FROM dispatch_outbox o WHERE target_user_id=$1),
'state',(SELECT to_jsonb(u) FROM user_update_watermarks u WHERE user_id=$1))::text`, f.owner.ID).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func (f *pgBlocklistFixture) apply(t *testing.T, m store.BlocklistMutation) store.BlocklistMutationSnapshot {
	t.Helper()
	r, err := f.contacts.MutateBlocklist(context.Background(), m, store.BlocklistDeliveryEffects)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBlocklistPostgresAtomicFailuresNoopAndCommit(t *testing.T) {
	f := newPGBlocklistFixture(t, 2)
	ctx := context.Background()
	m := f.command(t, store.BlocklistReplace, f.peers[0].ID, f.peers[1].ID)
	before := f.facts(t)
	builders := []store.DeliveryEffectsBuilder[store.BlocklistMutationSnapshot]{nil,
		func(store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
			return nil, errors.New("builder failed")
		},
		func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
			e, _ := store.BlocklistDeliveryEffects(s)
			return e[:len(e)-1], nil
		},
		func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
			e, _ := store.BlocklistDeliveryEffects(s)
			e[0], e[1] = e[1], e[0]
			return e, nil
		},
		func(s store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
			e, _ := store.BlocklistDeliveryEffects(s)
			e[0].ExcludeAuthKeyID = [8]byte{1}
			e[0].ExcludeSessionID = 1
			return e, nil
		},
	}
	for i, b := range builders {
		if r, err := f.contacts.MutateBlocklist(ctx, m, b); err == nil || len(r.Changes) != 0 || f.facts(t) != before {
			t.Fatalf("failure %d left state: %v", i, err)
		}
	}
	// A deferred trigger proves failure at actual COMMIT after all four events and
	// both block rows are visible inside that same transaction.
	name := fmt.Sprintf("blocklist_test_%d", f.owner.ID)
	ddl := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NEW.user_id=%d AND NEW.event_type='peer_story_blocked' THEN
 IF (SELECT count(*) FROM contact_blocks WHERE owner_user_id=%d AND xmin=pg_current_xact_id()::text::xid)<>2
 OR (SELECT count(*) FROM user_update_events WHERE user_id=%d AND xmin=pg_current_xact_id()::text::xid)<>4
 OR (SELECT count(*) FROM dispatch_outbox WHERE target_user_id=%d AND xmin=pg_current_xact_id()::text::xid)<>4 THEN
  RAISE EXCEPTION 'incomplete aggregate' USING ERRCODE='P0002';
 END IF;
 RAISE EXCEPTION 'blocklist injected commit' USING ERRCODE='P0001';
END IF; RETURN NEW; END $$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON user_update_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s()`, name, f.owner.ID, f.owner.ID, f.owner.ID, f.owner.ID, name, name)
	if _, err := f.pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	drop := func() {
		if _, err := f.pool.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON user_update_events; DROP FUNCTION IF EXISTS %s()", name, name)); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(drop)
	r, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "P0001" || len(r.Changes) != 0 || f.facts(t) != before {
		t.Fatalf("COMMIT rollback mismatch: %+v %v", r, err)
	}
	drop()
	r = f.apply(t, m)
	if len(r.Changes) != 2 || len(r.RequiredEvents) != 4 {
		t.Fatal("replace incomplete")
	}
	committed := f.facts(t)
	m = f.command(t, store.BlocklistReplace, f.peers[0].ID, f.peers[1].ID)
	m.Date++
	if r, err := f.contacts.MutateBlocklist(ctx, m, func(store.BlocklistMutationSnapshot) ([]store.DeliveryEffect, error) {
		t.Error("noop builder invoked")
		return nil, nil
	}); err != nil || len(r.Changes) != 0 || f.facts(t) != committed {
		t.Fatal("noop changed state", err)
	}
	events, err := NewUpdateEventStore(f.pool).ListAfter(ctx, f.owner.ID, 0, 100)
	if err != nil || len(events) != 4 {
		t.Fatal(events, err)
	}
	for i, e := range events {
		if e.Pts != i+1 || e.PtsCount != 1 {
			t.Fatal("PTS gap", events)
		}
	}
}

func TestBlocklistPostgresSnapshotPrivacyAndConcurrentCAS(t *testing.T) {
	f := newPGBlocklistFixture(t, 2)
	ctx := context.Background()
	a, b := f.peers[0].ID, f.peers[1].ID
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}
	if _, err := f.contacts.Upsert(ctx, f.owner.ID, domain.ContactInput{ContactUserID: a, FirstName: "Contact"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.contacts.SetCloseFriends(ctx, f.owner.ID, []int64{a}); err != nil {
		t.Fatal(err)
	}
	if err := NewPrivacyStore(f.pool).SetPrivacyRules(ctx, domain.PrivacyRules{OwnerUserID: f.owner.ID, Key: domain.PrivacyKeyPhoneNumber, Rules: []domain.PrivacyRule{{Kind: domain.PrivacyRuleAllowContacts}, {Kind: domain.PrivacyRuleDisallowAll}}}); err != nil {
		t.Fatal(err)
	}
	storyStore := NewStoryStore(f.pool)
	for _, s := range []domain.Story{{ID: 1, Public: true}, {ID: 2, SelectedContacts: true, AllowUserIDs: []int64{a}}, {ID: 3, Contacts: true}, {ID: 4, CloseFriends: true}, {ID: 5, Public: true, DisallowUserIDs: []int64{a, b}}} {
		s.Owner = owner
		s.Date = 1700000000
		s.ExpireDate = 1700003600
		if _, err := storyStore.UpsertStory(ctx, domain.UpsertStoryRequest{Story: s}); err != nil {
			t.Fatal(err)
		}
	}
	m := f.command(t, store.BlocklistReplace, a, b)
	stale := m
	stale.Stories = stale.Stories[:len(stale.Stories)-1]
	before := f.facts(t)
	if _, err := f.contacts.MutateBlocklist(ctx, stale, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistConflict) || f.facts(t) != before {
		t.Fatal("stale story accepted", err)
	}
	r := f.apply(t, m)
	counts := map[int64]int{}
	for _, e := range r.RequiredEvents {
		counts[e.TargetUserID]++
		if e.Event.Type == domain.UpdateEventPeerSettings {
			wantHidden := e.Event.Peer.ID == b
			if e.Event.Settings.NeedContactsException != wantHidden || e.Event.Settings.ShareContact {
				t.Fatal("phone privacy", e)
			}
		}
	}
	if !reflect.DeepEqual(counts, map[int64]int{f.owner.ID: 4, a: 4, b: 1}) {
		t.Fatal("visibility", counts)
	}
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistConflict) {
		t.Fatal("stale CAS accepted", err)
	}
	clear := f.command(t, store.BlocklistReplace)
	f.apply(t, clear)
	m = f.command(t, store.BlocklistBlock, a)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
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
			var pe *pgconn.PgError
			if !errors.Is(err, store.ErrBlocklistConflict) && (!errors.As(err, &pe) || pe.Code != "40001") {
				t.Fatal("unexpected concurrent failure", err)
			}
		}
	}
	// Serialization failures expose no partial success. An explicit retry is a
	// no-op after the winning transaction, without allocating another event.
	f.apply(t, m)
	events, err := NewUpdateEventStore(f.pool).ListAfter(ctx, f.owner.ID, 0, 100)
	if err != nil || len(events) != 10 {
		t.Fatal("concurrent duplicate owner PTS", len(events), err)
	}
	for i, e := range events {
		if e.Pts != i+1 {
			t.Fatal("PTS gap")
		}
	}
	for peer, want := range map[int64]int{a: 12, b: 2} {
		events, err := NewUpdateEventStore(f.pool).ListAfter(ctx, peer, 0, 100)
		if err != nil || len(events) != want {
			t.Fatal("concurrent viewer events", peer, len(events), err)
		}
	}
	m = f.command(t, store.BlocklistReplace, a, b)
	errs = make(chan error, 8)
	for i := 0; i < 8; i++ {
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
		var pe *pgconn.PgError
		if err != nil && !errors.Is(err, store.ErrBlocklistConflict) && (!errors.As(err, &pe) || pe.Code != "40001") {
			t.Fatal("concurrent replace", err)
		}
	}
	f.apply(t, f.command(t, store.BlocklistReplace, a, b))
	events, err = NewUpdateEventStore(f.pool).ListAfter(ctx, f.owner.ID, 0, 100)
	if err != nil || len(events) != 12 {
		t.Fatal("duplicate replacement events", len(events), err)
	}
}

func TestBlocklistPostgresLimitsAndCompleteFanout(t *testing.T) {
	f := newPGBlocklistFixture(t, 1025)
	ctx := context.Background()
	ids := make([]int64, len(f.peers))
	for i, p := range f.peers {
		ids[i] = p.ID
	}
	m := f.command(t, store.BlocklistReplace, ids...)
	before := f.facts(t)
	if _, err := f.contacts.MutateBlocklist(ctx, m, store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistLimit) || f.facts(t) != before {
		t.Fatal("1025 accepted", err)
	}
	storyStore := NewStoryStore(f.pool)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: f.owner.ID}
	for i := 1; i <= 2; i++ {
		if _, err := storyStore.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{Owner: owner, ID: i, Date: 1700000000, ExpireDate: 1700003600, Public: true}}); err != nil {
			t.Fatal(err)
		}
	}
	r := f.apply(t, f.command(t, store.BlocklistReplace, ids[:1024]...))
	if len(r.Changes) != 1024 || len(r.RequiredEvents) != 4096 {
		t.Fatal("exact 1024/4096 boundary", len(r.Changes), len(r.RequiredEvents))
	}
	// A third active story would make clearing this list require 5120 events.
	// Validate the complete fanout before changing any block row or PTS.
	if _, err := storyStore.UpsertStory(ctx, domain.UpsertStoryRequest{Story: domain.Story{Owner: owner, ID: 3, Date: 1700000000, ExpireDate: 1700003600, Public: true}}); err != nil {
		t.Fatal(err)
	}
	before = f.facts(t)
	if _, err := f.contacts.MutateBlocklist(ctx, f.command(t, store.BlocklistReplace), store.BlocklistDeliveryEffects); !errors.Is(err, store.ErrBlocklistLimit) || f.facts(t) != before {
		t.Fatal("5120 fanout partially committed", err)
	}
	var count int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM user_update_events WHERE user_id=ANY($1::bigint[])`, ids[:1024]).Scan(&count); err != nil || count != 2048 {
		t.Fatal("viewer fanout incomplete", count, err)
	}
}
