package domain

import (
	"bytes"
	"errors"
)

// 本文件定义投票（poll）的业务对象与共享语义：
//
//   - MessagePoll 是随消息 media JSONB 落库的「渲染安全定义快照」——不含 quiz 正确答案与
//     solution 等服务端机密；Closed 仅是发送时刻快照，读路径必须用 polls 权威态覆盖。
//   - PollDefinition 是 polls 权威表行（含机密与可变 closed），投票校验与结果门控只信它。
//   - 投票校验（ValidatePollVote）与结果门控（ResolvePollResults）是纯函数，由 memory 与
//     postgres 两个 store 共用，杜绝双实现行为漂移。
//
// 转发不复制 poll：转发快照携带同一 poll id，投票/关闭全局聚合（与官方一致）。

const (
	// MaxPollQuestionLength 与官方 poll question 上限一致。
	MaxPollQuestionLength = 255
	// MinPollAnswers / MaxPollAnswers 是答案个数边界（官方 2..10）。
	MinPollAnswers = 2
	MaxPollAnswers = 10
	// MaxPollAnswerTextLength 是单个答案文本上限（官方 100）。
	MaxPollAnswerTextLength = 100
	// MaxPollSolutionLength 是 quiz 解释文本上限（官方 200）。
	MaxPollSolutionLength = 200
	// MinPollClosePeriod / MaxPollClosePeriod 是自动关闭倒计时边界（官方 5..600 秒）。
	MinPollClosePeriod = 5
	MaxPollClosePeriod = 600
	// MaxPollRecentVoters 是 recent_voters 截断（与 reaction recent 同款，官方 3）。
	MaxPollRecentVoters = 3
	// MaxPollVotesPageLimit 是 messages.getPollVotes 单页上限。
	MaxPollVotesPageLimit = 50
)

// 投票链路业务错误；rpc 层映射为对应 RPC error 文本。
var (
	ErrPollInvalid          = errors.New("poll invalid")
	ErrPollNotFound         = errors.New("poll not found")
	ErrPollClosed           = errors.New("poll closed")
	ErrPollOptionInvalid    = errors.New("poll option invalid")
	ErrPollRevoteNotAllowed = errors.New("poll revote not allowed")
	ErrPollNotCreator       = errors.New("poll can only be closed by creator")
	ErrPollNotPublic        = errors.New("poll voters are not public")
)

// MessagePollAnswer 是一个候选答案（文本 + option 字节键 + 可选配图）。
// Media 仅允许 photo/document 快照（rpc 层限制输入类型）。
type MessagePollAnswer struct {
	Text     string          `json:"text"`
	Entities []MessageEntity `json:"entities,omitempty"`
	Option   []byte          `json:"option"`
	Media    *MessageMedia   `json:"media,omitempty"`
}

// MessagePoll 是消息 media 上的 poll 定义快照（渲染安全，不含机密）。
type MessagePoll struct {
	ID               int64               `json:"id"`
	Question         string              `json:"question"`
	QuestionEntities []MessageEntity     `json:"question_entities,omitempty"`
	Answers          []MessagePollAnswer `json:"answers"`
	// Closed 是发送时刻快照；读路径以 polls 权威态覆盖（转发副本/关闭后历史一致性靠它）。
	Closed                bool `json:"closed,omitempty"`
	PublicVoters          bool `json:"public_voters,omitempty"`
	MultipleChoice        bool `json:"multiple_choice,omitempty"`
	Quiz                  bool `json:"quiz,omitempty"`
	RevotingDisabled      bool `json:"revoting_disabled,omitempty"`
	ShuffleAnswers        bool `json:"shuffle_answers,omitempty"`
	HideResultsUntilClose bool `json:"hide_results_until_close,omitempty"`
	ClosePeriod           int  `json:"close_period,omitempty"`
	CloseDate             int  `json:"close_date,omitempty"`

	// AttachedMedia 是 poll 题干配图（messageMediaPoll.attached_media），photo/document 快照。
	AttachedMedia *MessageMedia `json:"attached_media,omitempty"`

	// Results 在读路径按 viewer 填充（含 chosen/correct/solution 门控），不落库。
	Results *MessagePollResults `json:"-"`
}

// MessagePollAnswerVoters 是一个答案的聚合结果（已按 viewer 门控）。
type MessagePollAnswerVoters struct {
	Option  []byte
	Voters  int
	Chosen  bool
	Correct bool
}

// MessagePollResults 是按 viewer 解析后的聚合结果。
type MessagePollResults struct {
	TotalVoters      int
	Voters           []MessagePollAnswerVoters // 与 MessagePoll.Answers 同序
	RecentVoters     []int64                   // 仅 public_voters 填充，截断 MaxPollRecentVoters
	ViewerVoted      bool
	Solution         string // 仅 quiz 且 (viewer 已投 || 已关闭) 下发
	SolutionEntities []MessageEntity
}

// PollDefinition 是 polls 权威表行：可变状态（closed）+ 服务端机密 + 校验所需选项集。
type PollDefinition struct {
	ID                    int64
	CreatorUserID         int64
	Options               [][]byte // 合法选项集合（与答案顺序一致）
	PublicVoters          bool
	MultipleChoice        bool
	Quiz                  bool
	RevotingDisabled      bool
	HideResultsUntilClose bool
	Closed                bool
	ClosePeriod           int
	CloseDate             int
	CorrectOptions        [][]byte
	Solution              string
	SolutionEntities      []MessageEntity
}

// ClosedAt 返回 now 时刻 poll 是否应视为已关闭（显式关闭或 close_date 已过）。
func (d PollDefinition) ClosedAt(now int) bool {
	return d.Closed || (d.CloseDate > 0 && now >= d.CloseDate)
}

// HasOption 判断 option 是否在合法选项集内。
func (d PollDefinition) HasOption(option []byte) bool {
	for _, candidate := range d.Options {
		if bytes.Equal(candidate, option) {
			return true
		}
	}
	return false
}

// PollVote 是一个用户的投票行。Options 保留客户端提交顺序。
type PollVote struct {
	PollID  int64
	UserID  int64
	Options [][]byte
	Date    int
}

// PollAggregates 是 store 层产出的原始聚合，交由 ResolvePollResults 做 viewer 门控。
// Counts 的 key 是 string(option)。
type PollAggregates struct {
	Counts        map[string]int
	TotalVoters   int
	RecentVoters  []int64  // 按 vote_date DESC 截断 MaxPollRecentVoters；仅 public_voters 需要
	ViewerOptions [][]byte // viewer 自己的投票（nil = 未投）
}

// ValidatePollVote 校验一次投票提交；existing 是 viewer 当前投票（nil=未投），options 为空表示撤票。
// memory 与 postgres store 必须共用本函数，保持双实现一致。
func ValidatePollVote(def PollDefinition, existing [][]byte, options [][]byte, now int) error {
	if def.ID == 0 {
		return ErrPollNotFound
	}
	if def.ClosedAt(now) {
		return ErrPollClosed
	}
	if def.Quiz || def.RevotingDisabled {
		// quiz / revoting_disabled：不可撤票、不可改票。
		if len(existing) > 0 || len(options) == 0 {
			return ErrPollRevoteNotAllowed
		}
	}
	if def.Quiz {
		if len(options) != 1 {
			return ErrPollOptionInvalid
		}
	} else if len(options) > 1 && !def.MultipleChoice {
		return ErrPollOptionInvalid
	}
	if len(options) > len(def.Options) {
		return ErrPollOptionInvalid
	}
	seen := make(map[string]struct{}, len(options))
	for _, option := range options {
		if !def.HasOption(option) {
			return ErrPollOptionInvalid
		}
		key := string(option)
		if _, dup := seen[key]; dup {
			return ErrPollOptionInvalid
		}
		seen[key] = struct{}{}
	}
	return nil
}

// ResolvePollResults 把原始聚合解析成 viewer 视角结果：chosen 标 viewer 自己的选项；
// correct/solution 仅 quiz 且 (viewer 已投 || 已关闭) 揭示；
// hide_results_until_close 在关闭前对非创建者隐藏计数（防恶意客户端绕过 UI）。
func ResolvePollResults(def PollDefinition, agg PollAggregates, viewerUserID int64, now int) MessagePollResults {
	closed := def.ClosedAt(now)
	viewerVoted := len(agg.ViewerOptions) > 0
	reveal := def.Quiz && (viewerVoted || closed)
	hideCounts := def.HideResultsUntilClose && !closed && viewerUserID != def.CreatorUserID
	chosen := make(map[string]struct{}, len(agg.ViewerOptions))
	for _, option := range agg.ViewerOptions {
		chosen[string(option)] = struct{}{}
	}
	correct := make(map[string]struct{}, len(def.CorrectOptions))
	if reveal {
		for _, option := range def.CorrectOptions {
			correct[string(option)] = struct{}{}
		}
	}
	out := MessagePollResults{
		TotalVoters: agg.TotalVoters,
		Voters:      make([]MessagePollAnswerVoters, 0, len(def.Options)),
		ViewerVoted: viewerVoted,
	}
	for _, option := range def.Options {
		key := string(option)
		_, isChosen := chosen[key]
		_, isCorrect := correct[key]
		voters := agg.Counts[key]
		if hideCounts {
			voters = 0
		}
		out.Voters = append(out.Voters, MessagePollAnswerVoters{
			Option:  option,
			Voters:  voters,
			Chosen:  isChosen,
			Correct: isCorrect,
		})
	}
	if def.PublicVoters && !hideCounts && len(agg.RecentVoters) > 0 {
		recent := agg.RecentVoters
		if len(recent) > MaxPollRecentVoters {
			recent = recent[:MaxPollRecentVoters]
		}
		out.RecentVoters = append([]int64(nil), recent...)
	}
	if reveal {
		out.Solution = def.Solution
		out.SolutionEntities = append([]MessageEntity(nil), def.SolutionEntities...)
	}
	return out
}

// ApplyPollState 把权威态与解析结果写回 media 上的定义快照（读路径 enrichment 终点）。
func ApplyPollState(poll *MessagePoll, def PollDefinition, results MessagePollResults, now int) {
	if poll == nil {
		return
	}
	poll.Closed = def.ClosedAt(now)
	poll.Results = &results
}

// VotePrivateMessagePollRequest 是私聊消息投票请求（msg id 为 viewer box id）。
type VotePrivateMessagePollRequest struct {
	UserID    int64
	Peer      Peer
	MessageID int
	Options   [][]byte // 空 = 撤票
	Date      int
}

// PrivateMessagePollResult 返回两个 owner 视角的消息（media 已按各自 owner enrich）。
type PrivateMessagePollResult struct {
	PollID   int64
	Messages []Message
}

// ClosePrivateMessagePollRequest 关闭私聊消息上的 poll（仅 poll 创建者）。
type ClosePrivateMessagePollRequest struct {
	UserID    int64
	Peer      Peer
	MessageID int
	Date      int
}

// VoteChannelMessagePollRequest 是频道/超级群消息投票请求。
type VoteChannelMessagePollRequest struct {
	UserID    int64
	ChannelID int64
	MessageID int
	Options   [][]byte
	Date      int
}

// ChannelMessagePollResult 返回当前 RPC 调用者视角的消息投影。
type ChannelMessagePollResult struct {
	PollID  int64
	Channel Channel
	Message ChannelMessage
}

// CloseChannelMessagePollRequest 关闭频道消息上的 poll（仅 poll 创建者）。
type CloseChannelMessagePollRequest struct {
	UserID    int64
	ChannelID int64
	MessageID int
	Date      int
}

// PollVotesListRequest 是 messages.getPollVotes 的分页请求。
type PollVotesListRequest struct {
	PollID       int64
	Option       []byte // 可选：仅列出投了该选项的人
	OffsetDate   int    // 0 = 第一页；翻页用上页末行 (date,user_id)
	OffsetUserID int64
	Limit        int
}

// PollVotesList 是投票人分页结果。
type PollVotesList struct {
	Count int // 满足过滤条件的总数
	Votes []PollVote
	// HasMore 为 true 时 rpc 层用末行 (Date,UserID) 编码 next_offset。
	HasMore bool
}
