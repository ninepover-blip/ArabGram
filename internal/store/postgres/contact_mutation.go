package postgres

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *ContactStore) MutateContacts(ctx context.Context, mutation store.ContactMutation, build store.DeliveryEffectsBuilder[store.ContactMutationSnapshot]) (store.ContactMutationSnapshot, error) {
	if err := mutation.Validate(); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if build == nil {
		return store.ContactMutationSnapshot{}, store.ErrContactMutationRequired
	}

	var snapshot store.ContactMutationSnapshot
	err := withTx(ctx, s.db, "mutate contacts", func(tx pgx.Tx) error {
		lockIDs := []int64{mutation.OwnerUserID}
		for _, input := range mutation.Inputs {
			lockIDs = append(lockIDs, input.ContactUserID)
		}
		lockIDs = append(lockIDs, mutation.ContactUserIDs...)
		if err := lockUsersForUpdate(ctx, tx, lockIDs...); err != nil {
			return err
		}

		txStore := NewContactStore(tx)
		var err error
		switch mutation.Kind {
		case store.ContactMutationAdd, store.ContactMutationImport:
			snapshot, err = txStore.mutateContactUpserts(ctx, mutation)
		case store.ContactMutationAccept:
			snapshot, err = txStore.mutateContactAccept(ctx, mutation)
		case store.ContactMutationDelete:
			snapshot, err = txStore.mutateContactDelete(ctx, mutation)
		case store.ContactMutationNote:
			snapshot, err = txStore.mutateContactNote(ctx, mutation)
		default:
			err = store.ErrContactMutationInvalid
		}
		if err != nil {
			return err
		}

		if mutation.PhonePrivacyRules != nil {
			changed, rules, err := applyContactPhonePrivacyRules(ctx, tx, *mutation.PhonePrivacyRules)
			if err != nil {
				return err
			}
			snapshot.PhonePrivacyChanged = changed
			snapshot.PhonePrivacyRules = &rules
			if changed {
				snapshot.Changed = true
				for _, input := range mutation.Inputs {
					if input.AddPhonePrivacyException {
						if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, input.ContactUserID); err != nil {
							return err
						}
					}
				}
			}
		}

		store.EnsureContactMutationResetEvents(&snapshot, mutation.Date)
		store.SortContactMutationRequiredEvents(snapshot.RequiredEvents)
		intents, err := build(store.ContactMutationSnapshotForDelivery(snapshot))
		if err != nil {
			return fmt.Errorf("build contact mutation delivery effects: %w", err)
		}
		if err := store.ValidateContactMutationDeliveryEffects(snapshot, intents); err != nil {
			return err
		}
		if _, err := applyDeliveryEffectsTx(ctx, tx, intents); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	return store.CloneContactMutationSnapshot(snapshot), nil
}

func (s *ContactStore) mutateContactUpserts(ctx context.Context, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	rows, err := s.upsertManyWithChanges(ctx, mutation.OwnerUserID, mutation.Inputs)
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	snapshot := store.ContactMutationSnapshot{
		Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: true,
		Contacts: make([]domain.Contact, 0, len(rows)),
	}
	for _, row := range rows {
		snapshot.Contacts = append(snapshot.Contacts, row.Contact)
		peerID := row.Contact.User.ID
		if row.OwnerChanged {
			snapshot.Changed = true
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, peerID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
		if row.ReverseMutualChanged {
			snapshot.Changed = true
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, peerID, mutation.OwnerUserID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
	}
	return snapshot, nil
}

func (s *ContactStore) mutateContactAccept(ctx context.Context, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	peerID := mutation.ContactUserIDs[0]
	actorContact, found, err := s.Get(ctx, mutation.OwnerUserID, peerID)
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	snapshot := store.ContactMutationSnapshot{
		Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: found,
	}
	if !found {
		return snapshot, nil
	}
	if actorContact.Mutual || actorContact.User.Mutual {
		snapshot.Contacts = []domain.Contact{actorContact}
		return snapshot, nil
	}
	rows, err := s.upsertManyWithChanges(ctx, peerID, []domain.ContactInput{mutation.Reciprocal})
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if len(rows) != 1 || !rows[0].ReverseMutualChanged {
		return store.ContactMutationSnapshot{}, fmt.Errorf("accept contact invariant: reciprocal mutation did not make actor contact mutual")
	}
	actorContact, found, err = s.Get(ctx, mutation.OwnerUserID, peerID)
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if !found || (!actorContact.Mutual && !actorContact.User.Mutual) {
		return store.ContactMutationSnapshot{}, fmt.Errorf("accept contact invariant: actor contact is not mutual")
	}
	snapshot.Found = true
	snapshot.Changed = true
	snapshot.Contacts = []domain.Contact{actorContact}
	if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, peerID); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, peerID, mutation.OwnerUserID); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	return snapshot, nil
}

func (s *ContactStore) mutateContactDelete(ctx context.Context, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	rows, err := s.db.Query(ctx, `
WITH input AS (
  SELECT contact_user_id, ord
  FROM unnest($2::bigint[]) WITH ORDINALITY AS i(contact_user_id, ord)
), deleted AS (
  DELETE FROM contacts c
  USING input i
  WHERE c.user_id = $1
    AND c.contact_user_id = i.contact_user_id
  RETURNING c.contact_user_id
), reverse_updated AS (
  UPDATE contacts c
  SET mutual = false, updated_at = now()
  FROM deleted d
  WHERE c.user_id = d.contact_user_id
    AND c.contact_user_id = $1
    AND c.mutual
  RETURNING c.user_id
)
SELECT d.contact_user_id,
       EXISTS (SELECT 1 FROM reverse_updated r WHERE r.user_id = d.contact_user_id)
FROM deleted d
JOIN input i ON i.contact_user_id = d.contact_user_id
ORDER BY i.ord`, mutation.OwnerUserID, mutation.ContactUserIDs)
	if err != nil {
		return store.ContactMutationSnapshot{}, fmt.Errorf("delete contact aggregate rows: %w", err)
	}
	defer rows.Close()
	snapshot := store.ContactMutationSnapshot{Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID, Found: true}
	for rows.Next() {
		var peerID int64
		var reverseChanged bool
		if err := rows.Scan(&peerID, &reverseChanged); err != nil {
			return store.ContactMutationSnapshot{}, err
		}
		snapshot.Deleted++
		snapshot.Changed = true
		if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, mutation.OwnerUserID, peerID); err != nil {
			return store.ContactMutationSnapshot{}, err
		}
		if reverseChanged {
			if err := store.AppendContactMutationPeerEvent(&snapshot, mutation, peerID, mutation.OwnerUserID); err != nil {
				return store.ContactMutationSnapshot{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	return snapshot, nil
}

func (s *ContactStore) mutateContactNote(ctx context.Context, mutation store.ContactMutation) (store.ContactMutationSnapshot, error) {
	peerID := mutation.ContactUserIDs[0]
	raw, err := encodeMessageEntities(mutation.NoteEntities)
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	var changed bool
	var snapshotFound bool
	err = s.db.QueryRow(ctx, `
WITH updated AS (
  UPDATE contacts
  SET note = $3, note_entities = $4::jsonb, updated_at = now()
  WHERE user_id = $1 AND contact_user_id = $2
    AND (note IS DISTINCT FROM $3 OR note_entities IS DISTINCT FROM $4::jsonb)
  RETURNING true
), existing AS (
  SELECT true AS found FROM contacts WHERE user_id = $1 AND contact_user_id = $2
)
SELECT EXISTS (SELECT 1 FROM updated), EXISTS (SELECT 1 FROM existing)`,
		mutation.OwnerUserID, peerID, mutation.Note, raw).Scan(&changed, &snapshotFound)
	if err != nil {
		return store.ContactMutationSnapshot{}, fmt.Errorf("update contact note aggregate: %w", err)
	}
	snapshot := store.ContactMutationSnapshot{
		Kind: mutation.Kind, OwnerUserID: mutation.OwnerUserID,
		Found: snapshotFound, Changed: changed,
	}
	if !snapshotFound {
		return snapshot, nil
	}
	contact, found, err := s.Get(ctx, mutation.OwnerUserID, peerID)
	if err != nil {
		return store.ContactMutationSnapshot{}, err
	}
	if !found {
		return store.ContactMutationSnapshot{}, fmt.Errorf("update contact note invariant: row disappeared")
	}
	snapshot.Contacts = []domain.Contact{contact}
	return snapshot, nil
}

func applyContactPhonePrivacyRules(ctx context.Context, tx pgx.Tx, prepared domain.PrivacyRules) (bool, domain.PrivacyRules, error) {
	current, found, err := NewPrivacyStore(tx).GetPrivacyRules(ctx, prepared.OwnerUserID, prepared.Key)
	if err != nil {
		return false, domain.PrivacyRules{}, err
	}
	if !found {
		current = domain.PrivacyRules{OwnerUserID: prepared.OwnerUserID, Key: prepared.Key, Rules: domain.DefaultPrivacyRules(prepared.Key)}
	}
	prepared = store.ClonePrivacyRules(prepared)
	if reflect.DeepEqual(current.Rules, prepared.Rules) {
		return false, prepared, nil
	}
	if err := setPrivacyRules(ctx, tx, prepared); err != nil {
		return false, domain.PrivacyRules{}, err
	}
	return true, prepared, nil
}
