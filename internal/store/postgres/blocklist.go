package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *ContactStore) ReadBlocklistIDs(ctx context.Context, owner int64) ([]int64, error) {
	if owner <= 0 {
		return nil, store.ErrBlocklistInvalid
	}
	rows, err := s.db.Query(ctx, `SELECT blocked_user_id FROM contact_blocks WHERE owner_user_id=$1 ORDER BY blocked_user_id LIMIT $2`, owner, store.MaxDeliveryTargets+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) > store.MaxDeliveryTargets {
		return nil, store.ErrBlocklistLimit
	}
	return ids, nil
}

func (s *ContactStore) MutateBlocklist(ctx context.Context, m store.BlocklistMutation, build store.DeliveryEffectsBuilder[store.BlocklistMutationSnapshot]) (store.BlocklistMutationSnapshot, error) {
	m, err := m.Prepare()
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	if build == nil {
		return store.BlocklistMutationSnapshot{}, store.ErrBlocklistRequired
	}
	var snapshot store.BlocklistMutationSnapshot
	err = withTx(ctx, s.db, "mutate blocklist", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
			return err
		}
		ids := append([]int64{m.OwnerUserID}, m.ExpectedIDs...)
		ids = append(ids, m.PeerIDs...)
		if err := lockUsersForUpdate(ctx, tx, ids...); err != nil {
			return err
		}
		contacts := NewContactStore(tx)
		current, err := contacts.ReadBlocklistIDs(ctx, m.OwnerUserID)
		if err != nil {
			return err
		}
		changes, err := store.BlocklistDiff(m, current)
		if err != nil {
			return err
		}
		var storyCount int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM stories WHERE owner_peer_type='user' AND owner_peer_id=$1 AND NOT deleted AND expire_date>$2`, m.OwnerUserID, m.Date).Scan(&storyCount); err != nil {
			return err
		}
		if storyCount > domain.MaxStoryListLimit {
			return store.ErrBlocklistLimit
		}
		stories, err := NewStoryStore(tx).ListOwnerActiveStories(ctx, domain.Peer{Type: domain.PeerTypeUser, ID: m.OwnerUserID}, m.Date, domain.MaxStoryListLimit)
		if err != nil {
			return err
		}
		if !slices.EqualFunc(stories.Stories, m.Stories, func(a, b domain.Story) bool { return reflect.DeepEqual(a, b) }) {
			return store.ErrBlocklistConflict
		}
		if len(changes) == 0 {
			snapshot = store.BlocklistMutationSnapshot{OwnerUserID: m.OwnerUserID, Date: m.Date}
			return nil
		}
		phone, found, err := NewPrivacyStore(tx).GetPrivacyRules(ctx, m.OwnerUserID, domain.PrivacyKeyPhoneNumber)
		if err != nil {
			return err
		}
		if !found {
			phone = domain.PrivacyRules{OwnerUserID: m.OwnerUserID, Key: domain.PrivacyKeyPhoneNumber, Rules: domain.DefaultPrivacyRules(domain.PrivacyKeyPhoneNumber)}
		}
		facts, err := contacts.blocklistPeerFacts(ctx, m, changes, phone)
		if err != nil {
			return err
		}
		snapshot, err = store.BuildBlocklistSnapshot(m, changes, phone, facts)
		if err != nil {
			return err
		}
		frozen, err := store.CloneBlocklistSnapshot(snapshot)
		if err != nil {
			return err
		}
		effects, err := build(frozen)
		if err != nil {
			return fmt.Errorf("build blocklist delivery: %w", err)
		}
		if err := store.ValidateBlocklistEffects(snapshot, effects); err != nil {
			return err
		}
		add, remove := []int64{}, []int64{}
		for _, c := range changes {
			if c.Blocked {
				add = append(add, c.PeerUserID)
			} else {
				remove = append(remove, c.PeerUserID)
			}
		}
		if len(remove) > 0 {
			tag, err := tx.Exec(ctx, `DELETE FROM contact_blocks WHERE owner_user_id=$1 AND blocked_user_id=ANY($2::bigint[])`, m.OwnerUserID, remove)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != int64(len(remove)) {
				return store.ErrBlocklistConflict
			}
		}
		if len(add) > 0 {
			tag, err := tx.Exec(ctx, `INSERT INTO contact_blocks(owner_user_id,blocked_user_id,date) SELECT $1,peer,$3 FROM unnest($2::bigint[]) AS peer ORDER BY peer`, m.OwnerUserID, add, m.Date)
			if err != nil {
				// REPEATABLE READ can retain a snapshot from before the advisory
				// lock wait. A concurrent insert then conflicts at the unique
				// constraint; report the stale snapshot, never skip that write.
				var pgerr *pgconn.PgError
				if errors.As(err, &pgerr) && pgerr.Code == "23505" && pgerr.ConstraintName == "contact_blocks_pkey" {
					return fmt.Errorf("%w: %w", store.ErrBlocklistConflict, err)
				}
				return err
			}
			if tag.RowsAffected() != int64(len(add)) {
				return store.ErrBlocklistConflict
			}
		}
		_, err = applyDeliveryEffectsTx(ctx, tx, effects)
		return err
	})
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	return store.CloneBlocklistSnapshot(snapshot)
}

func (s *ContactStore) blocklistPeerFacts(ctx context.Context, m store.BlocklistMutation, changes []store.BlocklistChange, phone domain.PrivacyRules) ([]store.BlocklistPeerFacts, error) {
	ids := make([]int64, len(changes))
	for i, c := range changes {
		ids[i] = c.PeerUserID
	}
	rows, err := s.db.Query(ctx, `SELECT u.id,u.is_bot,COALESCE(EXTRACT(EPOCH FROM u.premium_expires_at),0)::bigint,
c.contact_user_id IS NOT NULL,COALESCE(c.close_friend,false),COALESCE(reverse.contact_phone,'')<>''
FROM users u LEFT JOIN contacts c ON c.user_id=$1 AND c.contact_user_id=u.id
LEFT JOIN contacts reverse ON reverse.user_id=u.id AND reverse.contact_user_id=$1
WHERE u.id=ANY($2::bigint[]) ORDER BY u.id`, m.OwnerUserID, ids)
	if err != nil {
		return nil, err
	}
	facts := make([]store.BlocklistPeerFacts, 0, len(ids))
	for rows.Next() {
		var f store.BlocklistPeerFacts
		var premium int64
		if err := rows.Scan(&f.PeerUserID, &f.Bot, &premium, &f.Contact, &f.CloseFriend, &f.KnowsPhone); err != nil {
			rows.Close()
			return nil, err
		}
		f.Premium = !f.Bot && premium > int64(m.Date)
		facts = append(facts, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(facts) != len(ids) {
		return nil, store.ErrBlocklistInvalid
	}
	chats := store.BlocklistPrivacyChatIDs(phone)
	if len(chats) == 0 {
		return facts, nil
	}
	if len(chats) > store.MaxActiveChannelMemberPairs/len(ids) {
		return nil, store.ErrActiveChannelMemberPairsLimit
	}
	rows, err = s.db.Query(ctx, `SELECT user_id,channel_id FROM channel_members WHERE user_id=ANY($1::bigint[]) AND channel_id=ANY($2::bigint[]) AND status='active' ORDER BY user_id,channel_id`, ids, chats)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	index := map[int64]int{}
	for i, f := range facts {
		index[f.PeerUserID] = i
	}
	for rows.Next() {
		var user, channel int64
		if err := rows.Scan(&user, &channel); err != nil {
			return nil, err
		}
		i := index[user]
		facts[i].SharedChatIDs = append(facts[i].SharedChatIDs, channel)
	}
	return facts, rows.Err()
}
