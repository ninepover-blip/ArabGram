package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func exactContactMutationEffects(snapshot store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) {
	out := make([]store.DeliveryEffect, 0, len(snapshot.RequiredEvents)+1)
	for _, required := range snapshot.RequiredEvents {
		out = append(out, store.AccountPTSDeliveryEffect(required.TargetUserID, required.Event, [8]byte{}, 0))
	}
	if snapshot.PhonePrivacyChanged {
		out = append(out, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: snapshot.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}))
	}
	return out, nil
}

func TestContactMutationBuilderFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	contacts := NewContactStore()
	events := NewUpdateEventStore()
	contacts.AttachUpdateEventStore(events)
	wantErr := errors.New("encode failed")
	_, err := contacts.MutateContacts(ctx, store.ContactMutation{
		Kind: store.ContactMutationAdd, OwnerUserID: 1001, Date: 1700000000,
		Inputs:       []domain.ContactInput{{ContactUserID: 2002, FirstName: "Peer"}},
		PeerSettings: []store.ContactMutationPeerSettings{{TargetUserID: 1001, PeerUserID: 2002, Settings: domain.PeerSettings{BlockContact: true}}},
	}, func(store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) { return nil, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("MutateContacts err = %v, want builder error", err)
	}
	if _, found, err := contacts.Get(ctx, 1001, 2002); err != nil || found {
		t.Fatalf("contact after rollback found=%v err=%v", found, err)
	}
	if got, _ := events.ListAfter(ctx, 1001, 0, 10); len(got) != 0 {
		t.Fatalf("events after rollback = %+v", got)
	}
}

func TestContactMutationDeliverySnapshotHidesPrivateCard(t *testing.T) {
	ctx := context.Background()
	contacts := NewContactStore()
	_, err := contacts.MutateContacts(ctx, store.ContactMutation{
		Kind: store.ContactMutationAdd, OwnerUserID: 1001, Date: 1700000000,
		Inputs:       []domain.ContactInput{{ContactUserID: 2002, FirstName: "Private alias", Phone: "secret", Note: "private note"}},
		PeerSettings: []store.ContactMutationPeerSettings{{TargetUserID: 1001, PeerUserID: 2002}},
	}, func(snapshot store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) {
		if len(snapshot.Contacts) != 0 {
			return nil, errors.New("delivery builder received viewer-owned contact card")
		}
		return exactContactMutationEffects(snapshot)
	})
	if err != nil {
		t.Fatalf("MutateContacts: %v", err)
	}
	contact, found, err := contacts.Get(ctx, 1001, 2002)
	if err != nil || !found || contact.Note != "private note" || contact.Phone != "secret" {
		t.Fatalf("stored private card=%+v found=%v err=%v", contact, found, err)
	}
}

func TestContactMutationMutualAtomicAndNoopPTS(t *testing.T) {
	ctx := context.Background()
	contacts := NewContactStore()
	if err := contacts.SaveList(ctx, 2002, domain.ContactList{Contacts: []domain.Contact{{
		User: domain.User{ID: 1001, Contact: true}, FirstName: "Owner",
	}}}); err != nil {
		t.Fatalf("seed reverse: %v", err)
	}
	events := NewUpdateEventStore()
	contacts.AttachUpdateEventStore(events)
	mutation := store.ContactMutation{
		Kind: store.ContactMutationAdd, OwnerUserID: 1001, Date: 1700000000,
		Inputs: []domain.ContactInput{{ContactUserID: 2002, FirstName: "Peer"}},
		PeerSettings: []store.ContactMutationPeerSettings{
			{TargetUserID: 1001, PeerUserID: 2002, Settings: domain.PeerSettings{BlockContact: true}},
			{TargetUserID: 2002, PeerUserID: 1001, Settings: domain.PeerSettings{BlockContact: true}},
		},
	}
	snapshot, err := contacts.MutateContacts(ctx, mutation, exactContactMutationEffects)
	if err != nil {
		t.Fatalf("MutateContacts: %v", err)
	}
	if !snapshot.Changed || len(snapshot.RequiredEvents) != 4 {
		t.Fatalf("snapshot = %+v, want both owners peer/reset", snapshot)
	}
	forward, found, err := contacts.Get(ctx, 1001, 2002)
	if err != nil || !found || !forward.Mutual {
		t.Fatalf("forward contact = %+v found=%v err=%v", forward, found, err)
	}
	reverse, found, err := contacts.Get(ctx, 2002, 1001)
	if err != nil || !found || !reverse.Mutual {
		t.Fatalf("reverse contact = %+v found=%v err=%v", reverse, found, err)
	}
	for _, userID := range []int64{1001, 2002} {
		got, err := events.ListAfter(ctx, userID, 0, 10)
		if err != nil || len(got) != 2 || got[0].Pts != 1 || got[1].Pts != 2 {
			t.Fatalf("events user=%d = %+v err=%v, want contiguous 1,2", userID, got, err)
		}
		if len(events.dispatches[userID]) != 2 || events.dispatches[userID][0].ExcludeAuthKeyID != ([8]byte{}) || events.dispatches[userID][0].ExcludeSessionID != 0 {
			t.Fatalf("dispatches user=%d = %+v, want two zero-exclusion rows", userID, events.dispatches[userID])
		}
	}

	repeated, err := contacts.MutateContacts(ctx, mutation, exactContactMutationEffects)
	if err != nil {
		t.Fatalf("repeat MutateContacts: %v", err)
	}
	if repeated.Changed || len(repeated.RequiredEvents) != 0 {
		t.Fatalf("repeat snapshot = %+v, want no-op", repeated)
	}
	for _, userID := range []int64{1001, 2002} {
		got, _ := events.ListAfter(ctx, userID, 0, 10)
		if len(got) != 2 {
			t.Fatalf("repeat advanced user %d events: %+v", userID, got)
		}
	}
}

func TestContactMutationPrivacyExceptionIsAtomic(t *testing.T) {
	ctx := context.Background()
	contacts := NewContactStore()
	privacy := NewPrivacyStore()
	outbox := NewDeliveryOutboxStore()
	events := NewUpdateEventStore()
	contacts.AttachPrivacyStore(privacy)
	contacts.AttachDeliveryOutbox(outbox)
	contacts.AttachUpdateEventStore(events)
	mutation := store.ContactMutation{
		Kind: store.ContactMutationAdd, OwnerUserID: 1001, Date: 1700000000,
		Inputs:       []domain.ContactInput{{ContactUserID: 2002, FirstName: "Peer", AddPhonePrivacyException: true}},
		PeerSettings: []store.ContactMutationPeerSettings{{TargetUserID: 1001, PeerUserID: 2002, Settings: domain.PeerSettings{BlockContact: true}}},
		PhonePrivacyRules: &domain.PrivacyRules{
			OwnerUserID: 1001, Key: domain.PrivacyKeyPhoneNumber,
			Rules: []domain.PrivacyRule{{Kind: domain.PrivacyRuleAllowUsers, UserIDs: []int64{2002}}, {Kind: domain.PrivacyRuleDisallowAll}},
		},
	}
	wantErr := errors.New("encode privacy failed")
	_, err := contacts.MutateContacts(ctx, mutation, func(store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) { return nil, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("failed mutation err = %v", err)
	}
	if _, found, _ := contacts.Get(ctx, 1001, 2002); found {
		t.Fatal("contact committed after privacy builder failure")
	}
	if _, found, _ := privacy.GetPrivacyRules(ctx, 1001, domain.PrivacyKeyPhoneNumber); found {
		t.Fatal("privacy rules committed after builder failure")
	}
	if len(outbox.items) != 0 {
		t.Fatalf("outbox after rollback = %+v", outbox.items)
	}

	snapshot, err := contacts.MutateContacts(ctx, mutation, exactContactMutationEffects)
	if err != nil {
		t.Fatalf("successful mutation: %v", err)
	}
	if !snapshot.PhonePrivacyChanged || len(outbox.items) != 1 {
		t.Fatalf("snapshot=%+v outbox=%+v, want privacy absolute effect", snapshot, outbox.items)
	}
	rules, found, err := privacy.GetPrivacyRules(ctx, 1001, domain.PrivacyKeyPhoneNumber)
	if err != nil || !found || len(rules.Rules) == 0 || rules.Rules[0].UserIDs[0] != 2002 {
		t.Fatalf("privacy rules = %+v found=%v err=%v", rules, found, err)
	}
}

func TestContactMutationBatchLimit(t *testing.T) {
	inputs := make([]domain.ContactInput, store.MaxContactMutationBatch+1)
	settings := make([]store.ContactMutationPeerSettings, len(inputs))
	for i := range inputs {
		inputs[i] = domain.ContactInput{ContactUserID: int64(i + 2), FirstName: "Peer"}
		settings[i] = store.ContactMutationPeerSettings{TargetUserID: 1, PeerUserID: int64(i + 2)}
	}
	_, err := NewContactStore().MutateContacts(context.Background(), store.ContactMutation{
		Kind: store.ContactMutationImport, OwnerUserID: 1, Date: 1700000000, Inputs: inputs, PeerSettings: settings,
	}, exactContactMutationEffects)
	if !errors.Is(err, store.ErrContactMutationInvalid) {
		t.Fatalf("501 import err = %v, want ErrContactMutationInvalid", err)
	}

	contacts := NewContactStore()
	snapshot, err := contacts.MutateContacts(context.Background(), store.ContactMutation{
		Kind: store.ContactMutationImport, OwnerUserID: 1, Date: 1700000000,
		Inputs: inputs[:store.MaxContactMutationBatch], PeerSettings: settings[:store.MaxContactMutationBatch],
	}, exactContactMutationEffects)
	if err != nil {
		t.Fatalf("500 import: %v", err)
	}
	if got := len(snapshot.Contacts); got != store.MaxContactMutationBatch {
		t.Fatalf("500 import contacts = %d", got)
	}
	if got := len(snapshot.RequiredEvents); got != store.MaxContactMutationBatch+1 {
		t.Fatalf("500 import required events = %d, want peer events + one reset", got)
	}
}
