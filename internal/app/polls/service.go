package polls

import (
	"context"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service 提供 poll 权威态的发送时创建与投票人列表查询。
// 投票/关闭走 messages/channels 服务（需要消息可见性），不经本服务。
type Service struct {
	polls store.PollStore
}

// NewService 创建 poll 服务。
func NewService(polls store.PollStore) *Service {
	return &Service{polls: polls}
}

// CreatePoll 在消息发送前落 poll 权威行（消息发送失败产生的孤儿行无害）。
func (s *Service) CreatePoll(ctx context.Context, def domain.PollDefinition) error {
	if s == nil || s.polls == nil {
		return domain.ErrPollInvalid
	}
	if def.ID == 0 || def.CreatorUserID == 0 || len(def.Options) < domain.MinPollAnswers || len(def.Options) > domain.MaxPollAnswers {
		return domain.ErrPollInvalid
	}
	return s.polls.CreatePoll(ctx, def)
}

// GetPollDefinition 返回权威定义（getPollVotes 的 public_voters/broadcast 校验用）。
func (s *Service) GetPollDefinition(ctx context.Context, pollID int64) (domain.PollDefinition, bool, error) {
	if s == nil || s.polls == nil || pollID == 0 {
		return domain.PollDefinition{}, false, nil
	}
	return s.polls.GetPollDefinition(ctx, pollID)
}

// ListPollVotes 分页列出投票人（仅 public_voters poll 由 rpc 层放行）。
func (s *Service) ListPollVotes(ctx context.Context, req domain.PollVotesListRequest) (domain.PollVotesList, error) {
	if s == nil || s.polls == nil {
		return domain.PollVotesList{}, domain.ErrPollNotFound
	}
	if req.PollID == 0 || req.Limit <= 0 || req.Limit > domain.MaxPollVotesPageLimit {
		return domain.PollVotesList{}, domain.ErrPollInvalid
	}
	return s.polls.ListPollVotes(ctx, req)
}
