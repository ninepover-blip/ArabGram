package memory

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type contactMutationWorkingSet struct {
	base  map[int64]domain.ContactList
	lists map[int64]domain.ContactList
}

func newContactMutationWorkingSet(base map[int64]domain.ContactList) *contactMutationWorkingSet {
	return &contactMutationWorkingSet{base: base, lists: make(map[int64]domain.ContactList)}
}

func (w *contactMutationWorkingSet) get(userID int64) domain.ContactList {
	if list, ok := w.lists[userID]; ok {
		return list
	}
	list := w.base[userID]
	list.Contacts = cloneContacts(list.Contacts)
	w.lists[userID] = list
	return list
}

func (w *contactMutationWorkingSet) put(userID int64, list domain.ContactList) {
	list.Hash = contactListHash(list.Contacts)
	w.lists[userID] = list
}

func (s *ContactStore) MutateContacts(_ context.Context, mutation store.ContactMutation, build store.DeliveryEffectsBuilder[store.ContactMutationSnapshot]) (store.ContactMutationSnapshot, error) {
	if err := mutation.Validate(); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if build == nil {
		return store.ContactMutationSnapshot{}, store.ErrContactMutationRequired
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.updateEvents == nil {
		return store.ContactMutationSnapshot{}, store.ErrContactMutationRequired
	}
	privacy := s.privacy
	if mutation.PhonePrivacyRules != nil {
		if privacy == nil {
			return store.ContactMutationSnapshot{}, store.ErrPrivacyDeliveryStoreMissing
		}
		privacy.mu.Lock()
		defer privacy.mu.Unlock()
	}

	working := newContactMutationWorkingSet(s.m)
	var snapshot store.ContactMutationSnapshot
	var err error
	switch mutation.Kind {
	case store.ContactMutationAdd, store.ContactMutationImport:
		snapshot, err = mutateMemoryContactUpserts(working, mutation)
	case store.ContactMutationAccept:
		snapshot, err = mutateMemoryContactAccept(working, mutation)
	case store.ContactMutationDelete:
		snapshot, err = mutateMemoryContactDelete(working, mutation)
	case store.ContactMutationNote:
		snapshot, err = mutateMemoryContactNote(working, mutation)
	default:
		err = store.ErrContactMutationInvalid
	}
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}

	var preparedPrivacy domain.PrivacyRules
	if mutation.PhonePrivacyRules != nil {
		preparedPrivacy = store.ClonePrivacyRules(*mutation.PhonePrivacyRules)
		key := privacyStoreKey{ownerUserID: preparedPrivacy.OwnerUserID, key: preparedPrivacy.Key}
		current, found := privacy.rules[key]
		if !found {
			current = domain.PrivacyRules{OwnerUserID: preparedPrivacy.OwnerUserID, Key: preparedPrivacy.Key, Rules: domain.DefaultPrivacyRules(preparedPrivacy.Key)}
		}
		snapshot.PhonePrivacyRules = &preparedPrivacy
		if !reflect.DeepEqual(current.Rules, preparedPrivacy.Rules) {
			snapshot.PhonePrivacyChanged = true
			snapshot.Changed = true
			for _, input := range mutation.Inputs {
				if input.AddPhonePrivacyException {
					if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, input.ContactUserID); err != nil {
						return store.ContactMutationSnapshot{}, err
					}
				}
			}
		}
	}

	store.EnsureContactMutationResetEvents(&snapshot, mutation.Date)
	store.SortContactMutationRequiredEvents(snapshot.RequiredEvents)
	intents, err := build(store.ContactMutationSnapshotForDelivery(snapshot))
	if err != nil {
		return store.ContactMutationSnapshot{}, fmt.Errorf("build contact mutation delivery effects: %w", err)
	}
	if err := store.ValidateContactMutationDeliveryEffects(snapshot, intents); err != nil {
		return store.ContactMutationSnapshot{}, err
	}

	absolute := make([]store.DeliveryEffect, 0, 1)
	for _, effect := range intents {
		if effect.Kind == store.DeliveryEffectAbsolute {
			absolute = append(absolute, effect)
		}
	}
	var outbox *DeliveryOutboxStore
	if len(absolute) > 0 {
		outbox = s.deliveryOutbox
		if outbox == nil {
			return store.ContactMutationSnapshot{}, store.ErrDeliveryOutboxRequired
		}
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		if err := appendAbsoluteDeliveryEffectsLocked(outbox, absolute, time.Now()); err != nil {
			return store.ContactMutationSnapshot{}, err
		}
	}

	events := s.updateEvents
	events.mu.Lock()
	defer events.mu.Unlock()
	for _, effect := range intents {
		if effect.Kind != store.DeliveryEffectAccountPTS {
			continue
		}
		event := events.appendLocked(effect.TargetUserID, effect.Event, true)
		events.dispatches[effect.TargetUserID] = append(events.dispatches[effect.TargetUserID], memoryUpdateDispatch{
			Pts: event.Pts, ExcludeAuthKeyID: effect.ExcludeAuthKeyID, ExcludeSessionID: effect.ExcludeSessionID,
		})
	}
	for userID, list := range working.lists {
		s.m[userID] = list
	}
	if snapshot.PhonePrivacyChanged {
		privacy.rules[privacyStoreKey{ownerUserID: preparedPrivacy.OwnerUserID, key: preparedPrivacy.Key}] = clonePrivacyRules(preparedPrivacy)
	}
	return store.CloneContactMutationSnapshot(snapshot), nil
}

func mutateMemoryContactUpserts(working *contactMutationWorkingSet, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	snapshot := store.ContactMutationSnapshot{
		Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: true,
		Contacts: make([]domain.Contact, 0, len(mutation.Inputs)),
	}
	for _, input := range mutation.Inputs {
		contact, ownerChanged, reverseChanged := upsertMemoryContact(working, mutation.OwnerUserID, input)
		snapshot.Contacts = append(snapshot.Contacts, contact)
		if ownerChanged {
			snapshot.Changed = true
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, input.ContactUserID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
		if reverseChanged {
			snapshot.Changed = true
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, input.ContactUserID, mutation.OwnerUserID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
	}
	return snapshot, nil
}

func mutateMemoryContactAccept(working *contactMutationWorkingSet, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	peerID := mutation.ContactUserIDs[0]
	actorList := working.get(mutation.OwnerUserID)
	actorIndex := contactIndex(actorList, peerID)
	snapshot := store.ContactMutationSnapshot{Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: actorIndex >= 0}
	if actorIndex < 0 {
		return snapshot, nil
	}
	if actorList.Contacts[actorIndex].Mutual || actorList.Contacts[actorIndex].User.Mutual {
		snapshot.Contacts = []domain.Contact{cloneContact(actorList.Contacts[actorIndex])}
		return snapshot, nil
	}
	_, _, reverseChanged := upsertMemoryContact(working, peerID, mutation.Reciprocal)
	if !reverseChanged {
		return store.ContactMutationSnapshot{}, fmt.Errorf("accept contact invariant: reciprocal mutation did not make actor contact mutual")
	}
	actorList = working.get(mutation.OwnerUserID)
	actorIndex = contactIndex(actorList, peerID)
	if actorIndex < 0 || (!actorList.Contacts[actorIndex].Mutual && !actorList.Contacts[actorIndex].User.Mutual) {
		return store.ContactMutationSnapshot{}, fmt.Errorf("accept contact invariant: actor contact is not mutual")
	}
	snapshot.Changed = true
	snapshot.Contacts = []domain.Contact{cloneContact(actorList.Contacts[actorIndex])}
	if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, peerID); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, peerID, mutation.OwnerUserID); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	return snapshot, nil
}

func mutateMemoryContactDelete(working *contactMutationWorkingSet, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	want := make(map[int64]struct{}, len(mutation.ContactUserIDs))
	for _, id := range mutation.ContactUserIDs {
		want[id] = struct{}{}
	}
	owner := working.get(mutation.OwnerUserID)
	kept := make([]domain.Contact, 0, len(owner.Contacts))
	snapshot := store.ContactMutationSnapshot{Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: true}
	for _, contact := range owner.Contacts {
		peerID := contact.User.ID
		if _, remove := want[peerID]; !remove {
			kept = append(kept, contact)
			continue
		}
		snapshot.Deleted++
		snapshot.Changed = true
		if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, peerID); err != nil {
			return store.ContactMutationSnapshot{}, err
		}
		reverse := working.get(peerID)
		idx := contactIndex(reverse, mutation.OwnerUserID)
		if idx >= 0 && (reverse.Contacts[idx].Mutual || reverse.Contacts[idx].User.Mutual) {
			reverse.Contacts[idx].Mutual = false
			reverse.Contacts[idx].User.Mutual = false
			working.put(peerID, reverse)
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, peerID, mutation.OwnerUserID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
	}
	owner.Contacts = kept
	working.put(mutation.OwnerUserID, owner)
	return snapshot, nil
}

func mutateMemoryContactNote(working *contactMutationWorkingSet, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	peerID := mutation.ContactUserIDs[0]
	owner := working.get(mutation.OwnerUserID)
	idx := contactIndex(owner, peerID)
	snapshot := store.ContactMutationSnapshot{Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: idx >= 0}
	if idx < 0 {
		return snapshot, nil
	}
	if owner.Contacts[idx].Note != mutation.Note || !reflect.DeepEqual(owner.Contacts[idx].NoteEntities, mutation.NoteEntities) {
		owner.Contacts[idx].Note = mutation.Note
		owner.Contacts[idx].NoteEntities = append([]domain.MessageEntity(nil), mutation.NoteEntities...)
		working.put(mutation.OwnerUserID, owner)
		snapshot.Changed = true
	}
	snapshot.Contacts = []domain.Contact{cloneContact(owner.Contacts[idx])}
	return snapshot, nil
}

func upsertMemoryContact(working *contactMutationWorkingSet, ownerUserID int64, input domain.ContactInput) (domain.Contact, bool, bool) {
	owner := working.get(ownerUserID)
	reverse := working.get(input.ContactUserID)
	reverseIndex := contactIndex(reverse, ownerUserID)
	reverseExists := reverseIndex >= 0
	reverseChanged := false
	if reverseExists && (!reverse.Contacts[reverseIndex].Mutual || !reverse.Contacts[reverseIndex].User.Mutual) {
		reverse.Contacts[reverseIndex].Mutual = true
		reverse.Contacts[reverseIndex].User.Mutual = true
		working.put(input.ContactUserID, reverse)
		reverseChanged = true
	}

	contact := domain.Contact{
		User:      domain.User{ID: input.ContactUserID, Phone: input.Phone, FirstName: input.FirstName, LastName: input.LastName, Contact: true},
		FirstName: input.FirstName, LastName: input.LastName, Phone: input.Phone,
		Note: input.Note, NoteEntities: append([]domain.MessageEntity(nil), input.NoteEntities...),
		Mutual: reverseExists,
	}
	contact.User.Mutual = contact.Mutual
	idx := contactIndex(owner, input.ContactUserID)
	if idx < 0 {
		owner.Contacts = append(owner.Contacts, contact)
		working.put(ownerUserID, owner)
		return cloneContact(contact), true, reverseChanged
	}
	existing := owner.Contacts[idx]
	contact.User.AccessHash = existing.User.AccessHash
	contact.User.Username = existing.User.Username
	contact.User.CountryCode = existing.User.CountryCode
	contact.User.Verified = existing.User.Verified
	contact.User.Support = existing.User.Support
	contact.User.Bot = existing.User.Bot
	contact.User.BotInfoVersion = existing.User.BotInfoVersion
	contact.User.PremiumUntil = existing.User.PremiumUntil
	contact.User.EmojiStatusDocumentID = existing.User.EmojiStatusDocumentID
	contact.User.EmojiStatusUntil = existing.User.EmojiStatusUntil
	contact.User.PhotoID = existing.User.PhotoID
	contact.User.PhotoDCID = existing.User.PhotoDCID
	contact.User.PhotoStripped = append([]byte(nil), existing.User.PhotoStripped...)
	contact.User.PhotoPersonal = existing.User.PhotoPersonal
	contact.User.PhotoHasVideo = existing.User.PhotoHasVideo
	contact.CloseFriend = existing.CloseFriend
	contact.User.CloseFriend = existing.CloseFriend || existing.User.CloseFriend
	contact.Mutual = contact.Mutual || existing.Mutual || existing.User.Mutual
	contact.User.Mutual = contact.Mutual
	if contact.FirstName == "" {
		contact.User.FirstName = existing.User.FirstName
	}
	if contact.LastName == "" {
		contact.User.LastName = existing.User.LastName
	}
	changed := !reflect.DeepEqual(existing, contact)
	if changed {
		owner.Contacts[idx] = contact
		working.put(ownerUserID, owner)
	}
	return cloneContact(contact), changed, reverseChanged
}

func contactIndex(list domain.ContactList, contactUserID int64) int {
	for i := range list.Contacts {
		if list.Contacts[i].User.ID == contactUserID {
			return i
		}
	}
	return -1
}
