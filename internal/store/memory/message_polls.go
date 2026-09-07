package memory

import (
	"context"
	"sort"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// 私聊消息 poll 投票/关闭：消息可见性在本 store 校验，poll 级校验与状态全部
// 委托共享 PollStore（与 postgres 实现共用 domain 纯函数语义）。

func (s *MessageStore) VoteMessagePoll(ctx context.Context, req domain.VotePrivateMessagePollRequest, effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult]) (domain.PrivateMessagePollResult, error) {
	target, err := s.pollMessageTarget(req.UserID, req.Peer, req.MessageID)
	if err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	pollID := target.Media.Poll.ID
	previous, existed := s.snapshotPollVote(pollID, req.UserID)
	if err := s.polls.Vote(pollID, req.UserID, req.Options, req.Date); err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	res, err := s.privatePollResultWithEffects(ctx, target.UID, pollID, req.Date, effects)
	if err != nil {
		s.restorePollVote(pollID, req.UserID, previous, existed)
		return domain.PrivateMessagePollResult{}, err
	}
	return res, nil
}

func (s *MessageStore) CloseMessagePoll(ctx context.Context, req domain.ClosePrivateMessagePollRequest, effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult]) (domain.PrivateMessagePollResult, error) {
	target, err := s.pollMessageTarget(req.UserID, req.Peer, req.MessageID)
	if err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	pollID := target.Media.Poll.ID
	previous, existed := s.snapshotPollDefinition(pollID)
	if err := s.polls.Close(pollID, req.UserID); err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	res, err := s.privatePollResultWithEffects(ctx, target.UID, pollID, req.Date, effects)
	if err != nil {
		s.restorePollDefinition(pollID, previous, existed)
		return domain.PrivateMessagePollResult{}, err
	}
	return res, nil
}

func (s *MessageStore) privatePollResultWithEffects(ctx context.Context, uid, pollID int64, now int, effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult]) (domain.PrivateMessagePollResult, error) {
	if effects == nil {
		return domain.PrivateMessagePollResult{}, store.ErrDeliveryOutboxRequired
	}
	out := s.privatePollResult(uid, pollID, now)
	sort.Slice(out.Messages, func(i, j int) bool { return out.Messages[i].OwnerUserID < out.Messages[j].OwnerUserID })
	intents, err := effects(out)
	if err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	if len(intents) == 0 && len(out.Messages) > 0 {
		return domain.PrivateMessagePollResult{}, store.ErrDeliveryOutboxRequired
	}
	s.mu.RLock()
	outbox := s.deliveryOutbox
	s.mu.RUnlock()
	if _, err := applyDeliveryEffects(ctx, intents, outbox, nil); err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	return out, nil
}

func (s *MessageStore) snapshotPollVote(pollID, userID int64) (domain.PollVote, bool) {
	s.polls.mu.RLock()
	defer s.polls.mu.RUnlock()
	vote, ok := s.polls.votes[pollID][userID]
	return clonePollVote(vote), ok
}

func (s *MessageStore) restorePollVote(pollID, userID int64, vote domain.PollVote, existed bool) {
	s.polls.mu.Lock()
	defer s.polls.mu.Unlock()
	if !existed {
		delete(s.polls.votes[pollID], userID)
		return
	}
	if s.polls.votes[pollID] == nil {
		s.polls.votes[pollID] = make(map[int64]domain.PollVote)
	}
	s.polls.votes[pollID][userID] = clonePollVote(vote)
}

func (s *MessageStore) snapshotPollDefinition(pollID int64) (domain.PollDefinition, bool) {
	s.polls.mu.RLock()
	defer s.polls.mu.RUnlock()
	def, ok := s.polls.polls[pollID]
	return clonePollDefinition(def), ok
}

func (s *MessageStore) restorePollDefinition(pollID int64, def domain.PollDefinition, existed bool) {
	s.polls.mu.Lock()
	defer s.polls.mu.Unlock()
	if !existed {
		delete(s.polls.polls, pollID)
		return
	}
	s.polls.polls[pollID] = clonePollDefinition(def)
}

// pollMessageTarget 定位 viewer box 中带 poll 的目标消息。
func (s *MessageStore) pollMessageTarget(userID int64, peer domain.Peer, messageID int) (domain.Message, error) {
	if s == nil || s.polls == nil {
		return domain.Message{}, domain.ErrMessageIDInvalid
	}
	if userID == 0 || peer.Type != domain.PeerTypeUser || peer.ID == 0 || messageID <= 0 || messageID > domain.MaxMessageBoxID {
		return domain.Message{}, domain.ErrMessageIDInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, msg := range s.m[userID] {
		if msg.ID != messageID || msg.Peer != peer {
			continue
		}
		if msg.Media == nil || msg.Media.Kind != domain.MessageMediaKindPoll || msg.Media.Poll == nil || msg.Media.Poll.ID == 0 {
			return domain.Message{}, domain.ErrMessageIDInvalid
		}
		return msg, nil
	}
	return domain.Message{}, domain.ErrMessageIDInvalid
}

// privatePollResult 收集同一 UID 的全部 owner 副本，并按各自 owner 视角 enrich poll。
func (s *MessageStore) privatePollResult(uid, pollID int64, now int) domain.PrivateMessagePollResult {
	out := domain.PrivateMessagePollResult{PollID: pollID}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, messages := range s.m {
		for _, msg := range messages {
			if msg.UID != uid {
				continue
			}
			item := cloneMessage(msg)
			item.Media = enrichPollMediaForViewer(s.polls, item.Media, item.OwnerUserID, now)
			out.Messages = append(out.Messages, item)
		}
	}
	return out
}

// enrichPrivateMessagePolls 是私聊读路径的 poll enrichment 入口（与 reactions 填充并列）。
func (s *MessageStore) enrichPrivateMessagePolls(messages []domain.Message, now int) {
	if s == nil || s.polls == nil {
		return
	}
	for i := range messages {
		messages[i].Media = enrichPollMediaForViewer(s.polls, messages[i].Media, messages[i].OwnerUserID, now)
	}
}
