package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestContactMutationPostgresAtomicMutualPTSAndLockOrder(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	alice := createTestUser(t, ctx, users, "+1940"+suffix+"01", "Alice", "")
	bob := createTestUser(t, ctx, users, "+1940"+suffix+"02", "Bob", "")
	ids := []int64{alice.ID, bob.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM account_privacy_rules WHERE owner_user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM dispatch_outbox_attempts WHERE stream_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM dispatch_outbox_lanes WHERE stream_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM user_update_watermarks WHERE user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM contacts WHERE user_id = ANY($1::bigint[]) OR contact_user_id = ANY($1::bigint[])`, ids)
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, ids)
	})

	contacts := NewContactStore(pool)
	if _, err := contacts.Upsert(ctx, bob.ID, domain.ContactInput{ContactUserID: alice.ID, FirstName: "Alice"}); err != nil {
		t.Fatalf("seed reverse: %v", err)
	}
	mutation := store.ContactMutation{
		Kind: store.ContactMutationAdd, OwnerUserID: alice.ID, Date: 1700000000,
		Inputs: []domain.ContactInput{{ContactUserID: bob.ID, FirstName: "Bob"}},
		PeerSettings: []store.ContactMutationPeerSettings{
			{TargetUserID: alice.ID, PeerUserID: bob.ID, Settings: domain.PeerSettings{BlockContact: true}},
			{TargetUserID: bob.ID, PeerUserID: alice.ID, Settings: domain.PeerSettings{BlockContact: true}},
		},
	}
	wantErr := errors.New("encode failed")
	if _, err := contacts.MutateContacts(ctx, mutation, func(store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder failure err = %v", err)
	}
	if _, found, err := contacts.Get(ctx, alice.ID, bob.ID); err != nil || found {
		t.Fatalf("failed aggregate leaked owner row found=%v err=%v", found, err)
	}
	reverse, found, err := contacts.Get(ctx, bob.ID, alice.ID)
	if err != nil || !found || reverse.Mutual {
		t.Fatalf("failed aggregate reverse=%+v found=%v err=%v, want non-mutual", reverse, found, err)
	}

	snapshot, err := contacts.MutateContacts(ctx, mutation, postgresContactMutationEffects)
	if err != nil {
		t.Fatalf("successful aggregate: %v", err)
	}
	if !snapshot.Changed || len(snapshot.RequiredEvents) != 4 {
		t.Fatalf("snapshot = %+v, want both users peer/reset", snapshot)
	}
	forward, found, err := contacts.Get(ctx, alice.ID, bob.ID)
	if err != nil || !found || !forward.Mutual {
		t.Fatalf("forward=%+v found=%v err=%v", forward, found, err)
	}
	reverse, found, err = contacts.Get(ctx, bob.ID, alice.ID)
	if err != nil || !found || !reverse.Mutual {
		t.Fatalf("reverse=%+v found=%v err=%v", reverse, found, err)
	}
	events := NewUpdateEventStore(pool)
	for _, userID := range ids {
		got, err := events.ListAfter(ctx, userID, 0, 10)
		if err != nil || len(got) != 2 || got[0].Pts != 1 || got[1].Pts != 2 ||
			got[0].Type != domain.UpdateEventPeerSettings || got[1].Type != domain.UpdateEventContactsReset {
			t.Fatalf("events user=%d = %+v err=%v", userID, got, err)
		}
		var dispatches int
		if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id=$1 AND exclude_auth_key_id=decode(repeat('00',8),'hex') AND exclude_session_id=0`, userID).Scan(&dispatches); err != nil || dispatches != 2 {
			t.Fatalf("zero-exclusion dispatches user=%d count=%d err=%v", userID, dispatches, err)
		}
	}

	repeated, err := contacts.MutateContacts(ctx, mutation, postgresContactMutationEffects)
	if err != nil || repeated.Changed || len(repeated.RequiredEvents) != 0 {
		t.Fatalf("repeat snapshot=%+v err=%v, want no-op", repeated, err)
	}
	privacyMutation := mutation
	privacyMutation.Inputs = []domain.ContactInput{{ContactUserID: bob.ID, FirstName: "Bob", AddPhonePrivacyException: true}}
	privacyMutation.PeerSettings = []store.ContactMutationPeerSettings{{
		TargetUserID: alice.ID, PeerUserID: bob.ID, Settings: domain.PeerSettings{BlockContact: true},
	}}
	privacyMutation.PhonePrivacyRules = &domain.PrivacyRules{
		OwnerUserID: alice.ID, Key: domain.PrivacyKeyPhoneNumber,
		Rules: []domain.PrivacyRule{{Kind: domain.PrivacyRuleAllowUsers, UserIDs: []int64{bob.ID}}, {Kind: domain.PrivacyRuleDisallowAll}},
	}
	if _, err := contacts.MutateContacts(ctx, privacyMutation, func(store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("privacy builder failure err = %v", err)
	}
	var privacyRows int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM account_privacy_rules WHERE owner_user_id=$1 AND privacy_key=$2`, alice.ID, string(domain.PrivacyKeyPhoneNumber)).Scan(&privacyRows); err != nil || privacyRows != 0 {
		t.Fatalf("privacy rows after rollback=%d err=%v", privacyRows, err)
	}
	privacySnapshot, err := contacts.MutateContacts(ctx, privacyMutation, postgresContactMutationEffects)
	if err != nil || !privacySnapshot.PhonePrivacyChanged {
		t.Fatalf("privacy snapshot=%+v err=%v", privacySnapshot, err)
	}
	var absoluteRows int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM edge_delivery_outbox WHERE target_user_id=$1`, alice.ID).Scan(&absoluteRows); err != nil || absoluteRows != 1 {
		t.Fatalf("privacy absolute rows=%d err=%v", absoluteRows, err)
	}

	deleteMutation := func(owner, peer int64) store.ContactMutation {
		return store.ContactMutation{
			Kind: store.ContactMutationDelete, OwnerUserID: owner, ContactUserIDs: []int64{peer}, Date: 1700000001,
			PeerSettings: []store.ContactMutationPeerSettings{
				{TargetUserID: owner, PeerUserID: peer, Settings: domain.PeerSettings{AddContact: true, BlockContact: true}},
				{TargetUserID: peer, PeerUserID: owner, Settings: domain.PeerSettings{BlockContact: true, ShareContact: true}},
			},
		}
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for _, mutation := range []store.ContactMutation{deleteMutation(alice.ID, bob.ID), deleteMutation(bob.ID, alice.ID)} {
		mutation := mutation
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := contacts.MutateContacts(deadline, mutation, postgresContactMutationEffects)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("opposite delete mutation: %v", err)
		}
	}
	if deadline.Err() != nil {
		t.Fatalf("opposite delete mutations exceeded deadline: %v", deadline.Err())
	}
	for _, userID := range ids {
		got, err := events.ListAfter(ctx, userID, 0, 50)
		if err != nil {
			t.Fatalf("list final events user=%d: %v", userID, err)
		}
		for i, event := range got {
			if event.Pts != i+1 {
				t.Fatalf("non-contiguous pts user=%d events=%+v", userID, got)
			}
		}
	}
}
