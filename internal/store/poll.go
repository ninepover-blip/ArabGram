package store

import (
	"context"

	"telesrv/internal/domain"
)

// PollStore 持久化 poll 权威态（定义 + closed + 机密）与投票人列表查询。
// 投票/关闭走 MessageStore.VoteMessagePoll / ChannelStore.VoteChannelMessagePoll 等
// 消息侧入口（需要消息可见性校验）；本接口只承担发送时建 poll 与 getPollVotes 列表。
type PollStore interface {
	CreatePoll(ctx context.Context, def domain.PollDefinition) error
	GetPollDefinition(ctx context.Context, pollID int64) (domain.PollDefinition, bool, error)
	ListPollVotes(ctx context.Context, req domain.PollVotesListRequest) (domain.PollVotesList, error)
}
