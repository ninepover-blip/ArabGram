package postgres

import (
	"context"
	"fmt"
	"sort"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *StoryStore) CreateStoryWithDelivery(
	ctx context.Context,
	actorUserID int64,
	req domain.StoryCreateRequest,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryCreateResult, error) {
	return runStoryDeliveryTx(ctx, s, storyOwnerMutationLock(req.Owner), build,
		func(txStore *StoryStore) (domain.StoryCreateResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.createStory(ctx, req)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationCreate, ActorUserID: actorUserID,
				Changed: err == nil && !result.Duplicate, Create: result,
			}
			if err != nil || result.Duplicate {
				return result, snapshot, err
			}
			members, err := txStore.storyChannelDeliveryMembers(ctx, result.Story.Owner)
			if err != nil {
				return domain.StoryCreateResult{}, snapshot, err
			}
			store.AppendStoryChangeRequirements(&snapshot, result.Story, members)
			return result, snapshot, nil
		})
}

func (s *StoryStore) EditStoryWithDelivery(
	ctx context.Context,
	actorUserID int64,
	req domain.StoryEditRequest,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryEditResult, error) {
	return runStoryDeliveryTx(ctx, s, storyOwnerMutationLock(req.Owner), build,
		func(txStore *StoryStore) (domain.StoryEditResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.editStory(ctx, req)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationEdit, ActorUserID: actorUserID,
				Changed: err == nil, Edit: result,
			}
			if err != nil {
				return result, snapshot, err
			}
			members, err := txStore.storyChannelDeliveryMembers(ctx, result.Story.Owner)
			if err != nil {
				return domain.StoryEditResult{}, snapshot, err
			}
			store.AppendStoryChangeRequirements(&snapshot, result.Story, members)
			if req.UpdatePrivacy && result.Story.Owner.Type == domain.PeerTypeUser {
				facts, err := txStore.storyUserDeliveryFacts(ctx, result.Story.Owner.ID,
					[]int{result.Story.ID}, storyAudienceSeedIDs(result.Previous, result.Story))
				if err != nil {
					return domain.StoryEditResult{}, snapshot, err
				}
				store.AppendStoryPrivacyFanoutRequirements(&snapshot, result.Previous, result.Story, facts, result.Story.Date)
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) DeleteStoriesWithDelivery(
	ctx context.Context,
	actorUserID int64,
	peer domain.Peer,
	ids []int,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryMutationResult, error) {
	return runStoryDeliveryTx(ctx, s, storyOwnerMutationLock(peer), build,
		func(txStore *StoryStore) (domain.StoryMutationResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.deleteStories(ctx, peer, ids, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationDelete, ActorUserID: actorUserID,
				Changed: err == nil && len(result.Stories) > 0, Mutation: result,
			}
			if err != nil || len(result.Stories) == 0 {
				return result, snapshot, err
			}
			members, facts, err := txStore.storyMutationAudience(ctx, peer, result.Previous, result.Stories)
			if err != nil {
				return domain.StoryMutationResult{}, snapshot, err
			}
			previous := storiesByID(result.Previous)
			for _, story := range result.Stories {
				store.AppendStoryChangeRequirements(&snapshot, story, members)
				if peer.Type == domain.PeerTypeUser {
					store.AppendDeletedStoryFanoutRequirements(&snapshot, previous[story.ID], story, facts, date)
				}
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) TogglePinnedWithDelivery(
	ctx context.Context,
	actorUserID int64,
	peer domain.Peer,
	ids []int,
	pinned bool,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryMutationResult, error) {
	return runStoryDeliveryTx(ctx, s, storyOwnerMutationLock(peer), build,
		func(txStore *StoryStore) (domain.StoryMutationResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.togglePinned(ctx, peer, ids, pinned, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationTogglePinned, ActorUserID: actorUserID,
				Changed: err == nil && len(result.Stories) > 0, Mutation: result,
			}
			if err != nil || len(result.Stories) == 0 {
				return result, snapshot, err
			}
			members, facts, err := txStore.storyMutationAudience(ctx, peer, result.Previous, result.Stories)
			if err != nil {
				return domain.StoryMutationResult{}, snapshot, err
			}
			previous := storiesByID(result.Previous)
			for _, story := range result.Stories {
				store.AppendStoryChangeRequirements(&snapshot, story, members)
				if peer.Type == domain.PeerTypeUser {
					store.AppendUnpinnedExpiredStoryFanoutRequirements(&snapshot, previous[story.ID], story, facts, date)
				}
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) MarkReadWithDelivery(
	ctx context.Context,
	viewerUserID int64,
	peer domain.Peer,
	maxID, date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryReadResult, error) {
	return runStoryDeliveryTx(ctx, s, storyReadMutationLock(viewerUserID, peer), build,
		func(txStore *StoryStore) (domain.StoryReadResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.markRead(ctx, viewerUserID, peer, maxID, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationRead, ActorUserID: viewerUserID,
				Changed: err == nil && result.Advanced, Read: result,
			}
			if err == nil && result.Advanced {
				snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
					TargetUserID: viewerUserID, Event: store.ReadStoriesEvent(result), ExcludeOrigin: true,
				})
			}
			return result, snapshot, err
		})
}

func (s *StoryStore) SetReactionWithDelivery(
	ctx context.Context,
	viewerUserID int64,
	peer domain.Peer,
	storyID int,
	reaction *domain.MessageReaction,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryReactionResult, error) {
	return runStoryDeliveryTx(ctx, s, storyReactionMutationLock(viewerUserID, peer, storyID), build,
		func(txStore *StoryStore) (domain.StoryReactionResult, store.StoryMutationSnapshot, error) {
			result, err := txStore.setReaction(ctx, viewerUserID, peer, storyID, reaction, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationReaction, ActorUserID: viewerUserID,
				Changed: err == nil && result.Changed, Reaction: result,
			}
			if err != nil || !result.Changed {
				return result, snapshot, err
			}
			snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
				TargetUserID: viewerUserID, Event: store.SentStoryReactionEvent(result), ExcludeOrigin: true,
			})
			owner := result.Story.Owner
			if owner.ID == 0 {
				owner = result.Peer
			}
			if owner.Type == domain.PeerTypeUser && owner.ID > 0 && owner.ID != viewerUserID && result.Reaction != nil {
				snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
					TargetUserID: owner.ID, Event: store.NewStoryReactionEvent(result),
				})
			}
			return result, snapshot, nil
		})
}

type storyDeliveryMutation[T any] func(*StoryStore) (T, store.StoryMutationSnapshot, error)

func runStoryDeliveryTx[T any](
	ctx context.Context,
	s *StoryStore,
	lockKey string,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
	mutate storyDeliveryMutation[T],
) (T, error) {
	var zero T
	if s == nil || build == nil {
		return zero, store.ErrDeliveryOutboxRequired
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return zero, fmt.Errorf("story store: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("begin story delivery transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if lockKey != "" {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
			return zero, fmt.Errorf("lock story aggregate: %w", err)
		}
	}
	result, snapshot, err := mutate(NewStoryStore(tx))
	if err != nil {
		return zero, err
	}
	store.SortStoryDeliveryRequirements(snapshot.Required)
	effects, err := build(store.StoryMutationSnapshotForDelivery(snapshot))
	if err != nil {
		return zero, fmt.Errorf("build story delivery effects: %w", err)
	}
	if err := store.ValidateStoryMutationDeliveryEffects(snapshot, effects); err != nil {
		return zero, err
	}
	applied, err := applyDeliveryEffectsTx(ctx, tx, effects)
	if err != nil {
		return zero, err
	}
	snapshot.Effects = applied
	if err := tx.Commit(ctx); err != nil {
		return zero, fmt.Errorf("commit story delivery transaction: %w", err)
	}
	return result, nil
}

func storyOwnerMutationLock(peer domain.Peer) string {
	return fmt.Sprintf("story-owner:%s:%d", peer.Type, peer.ID)
}

func storyReadMutationLock(viewerUserID int64, peer domain.Peer) string {
	return fmt.Sprintf("story-read:%d:%s:%d", viewerUserID, peer.Type, peer.ID)
}

func storyReactionMutationLock(viewerUserID int64, peer domain.Peer, storyID int) string {
	return fmt.Sprintf("story-reaction:%d:%s:%d:%d", viewerUserID, peer.Type, peer.ID, storyID)
}

func (s *StoryStore) storyChannelDeliveryMembers(ctx context.Context, peer domain.Peer) ([]int64, error) {
	if peer.Type != domain.PeerTypeChannel || peer.ID <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id
FROM channel_members
WHERE channel_id = $1
  AND status = 'active'
  AND NOT COALESCE((banned_rights->>'ViewMessages')::boolean, false)
  AND user_id > 0
ORDER BY user_id
LIMIT $2`, peer.ID, int32(domain.MaxChannelRealtimeFanout+1))
	if err != nil {
		return nil, fmt.Errorf("list channel story delivery members: %w", err)
	}
	defer rows.Close()
	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan channel story delivery member: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan channel story delivery members: %w", err)
	}
	if len(ids) > domain.MaxChannelRealtimeFanout {
		return nil, fmt.Errorf("channel story delivery audience exceeds %d", domain.MaxChannelRealtimeFanout)
	}
	return ids, nil
}

func (s *StoryStore) storyMutationAudience(ctx context.Context, peer domain.Peer, before, after []domain.Story) ([]int64, []store.StoryFanoutFact, error) {
	if peer.Type == domain.PeerTypeChannel {
		members, err := s.storyChannelDeliveryMembers(ctx, peer)
		return members, nil, err
	}
	storyIDs := make([]int, 0, len(before))
	seedIDs := make([]int64, 0)
	for _, story := range before {
		storyIDs = append(storyIDs, story.ID)
		seedIDs = append(seedIDs, storyAudienceSeedIDs(story)...)
	}
	for _, story := range after {
		seedIDs = append(seedIDs, storyAudienceSeedIDs(story)...)
	}
	facts, err := s.storyUserDeliveryFacts(ctx, peer.ID, storyIDs, seedIDs)
	return nil, facts, err
}

func (s *StoryStore) storyUserDeliveryFacts(ctx context.Context, ownerUserID int64, storyIDs []int, seedIDs []int64) ([]store.StoryFanoutFact, error) {
	if ownerUserID <= 0 || len(storyIDs) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
WITH candidates AS (
  SELECT DISTINCT user_id
  FROM (
    SELECT unnest($3::bigint[]) AS user_id
    UNION ALL
    SELECT contact_user_id FROM contacts WHERE user_id = $1
    UNION ALL
    SELECT viewer_user_id FROM story_views
      WHERE owner_peer_type = 'user' AND owner_peer_id = $1 AND story_id = ANY($2::int[])
    UNION ALL
    SELECT viewer_user_id FROM story_exposures
      WHERE owner_peer_type = 'user' AND owner_peer_id = $1 AND story_id = ANY($2::int[])
  ) source
  WHERE user_id > 0 AND user_id <> $1
)
SELECT c.user_id,
       EXISTS (SELECT 1 FROM contacts x WHERE x.user_id = $1 AND x.contact_user_id = c.user_id),
       EXISTS (SELECT 1 FROM contacts x WHERE x.user_id = $1 AND x.contact_user_id = c.user_id AND x.close_friend),
       EXISTS (SELECT 1 FROM contact_blocks b WHERE b.owner_user_id = $1 AND b.blocked_user_id = c.user_id)
FROM candidates c
ORDER BY c.user_id
LIMIT $4`, ownerUserID, int32s(storyIDs), nonNullInt64s(uniquePositiveInt64s(seedIDs)), int32(domain.MaxStoryPrivacyFanoutTargets+1))
	if err != nil {
		return nil, fmt.Errorf("list user story delivery audience: %w", err)
	}
	defer rows.Close()
	facts := make([]store.StoryFanoutFact, 0)
	for rows.Next() {
		var fact store.StoryFanoutFact
		if err := rows.Scan(&fact.UserID, &fact.IsContact, &fact.CloseFriend, &fact.StoryBlocked); err != nil {
			return nil, fmt.Errorf("scan user story delivery audience: %w", err)
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan user story delivery audience: %w", err)
	}
	if len(facts) > domain.MaxStoryPrivacyFanoutTargets {
		return nil, fmt.Errorf("user story delivery audience exceeds %d", domain.MaxStoryPrivacyFanoutTargets)
	}
	return facts, nil
}

func storyAudienceSeedIDs(stories ...domain.Story) []int64 {
	out := make([]int64, 0)
	for _, story := range stories {
		out = append(out, story.AllowUserIDs...)
		out = append(out, story.DisallowUserIDs...)
	}
	return out
}

func uniquePositiveInt64s(in []int64) []int64 {
	seen := make(map[int64]struct{}, len(in))
	out := make([]int64, 0, len(in))
	for _, id := range in {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func storiesByID(items []domain.Story) map[int]domain.Story {
	out := make(map[int]domain.Story, len(items))
	for _, story := range items {
		out[story.ID] = story
	}
	return out
}
