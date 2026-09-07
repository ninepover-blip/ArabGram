package domain

// SavedHiddenAuthorUserID 是官方 hidden-author 收藏夹子会话的占位 user id
// （TDesktop kSavedHiddenAuthorId / TDLib HIDDEN_AUTHOR_DIALOG_ID）。
// 运行时新写入永远能确定源会话，不产生该值；仅存量回填中
// 「fwd 头只有 from_name、源会话已不可知」的消息归入此子会话。
const SavedHiddenAuthorUserID int64 = 2666000

// MaxPinnedSavedDialogs 是收藏夹子会话置顶上限（官方 premium 上限同级）。
const MaxPinnedSavedDialogs = 100

// MaxSavedDialogsLimit 是 messages.getSavedDialogs 单页上限。
const MaxSavedDialogsLimit = 100

// SavedPeerForSelfChat 计算 self-chat 新消息的 saved 子会话分组键，与
// TDLib SavedMessagesTopicId 语义对齐：转发带源会话（saved_from_peer）→
// 源会话；fwd 头仅 from_name 且源会话不可知 → hidden author 占位（防御
// 分支，运行时转发总是带源会话）；其余（直发、drop_author）→ self。
func SavedPeerForSelfChat(selfUserID int64, forward *MessageForward) Peer {
	if forward != nil {
		if forward.SavedFrom.ID != 0 {
			return forward.SavedFrom
		}
		if forward.From.ID == 0 && forward.FromName != "" {
			return Peer{Type: PeerTypeUser, ID: SavedHiddenAuthorUserID}
		}
	}
	return Peer{Type: PeerTypeUser, ID: selfUserID}
}

// SavedDialog 是收藏夹的一个子会话（savedDialog TL 构造器的业务形态）。
type SavedDialog struct {
	Peer Peer
	// TopMessage 是该子会话当前最新可见消息的 owner 视角 box id。
	TopMessage int
	Pinned     bool
}

// SavedDialogList 是 getSavedDialogs/getPinnedSavedDialogs/getSavedDialogsByID
// 的业务层结果。Messages 与 Dialogs 一一对应（top message 全量行）。
type SavedDialogList struct {
	Dialogs  []SavedDialog
	Messages []Message
	Users    []User
	Channels []Channel
	// Count 是过滤口径下（含/不含 pinned）的子会话总数。
	Count int
	// Full 为 true 表示本页已含过滤口径下的全部剩余子会话
	// （映射 messages.savedDialogs；false 映射 savedDialogsSlice）。
	Full bool
}

// SavedDialogsFilter 描述 getSavedDialogs 分页条件。
type SavedDialogsFilter struct {
	ExcludePinned bool
	// OffsetID 是上一页最后一个子会话的 top message box id；0 或 >= MaxMessageBoxID
	// 视为从最新开始（TDesktop 首页传 0，DrKLO Android 首页传 int32 max）。
	OffsetID int
	// OffsetDate/OffsetPeer 仅作合法性校验；box id 单调于时间，分页统一按
	// top box id 严格降序推进。
	OffsetDate int
	OffsetPeer Peer
	Limit      int
}

// DeleteSavedHistoryRequest 是 messages.deleteSavedHistory 的业务命令：
// 删除 self-chat 中一个 saved 子会话的消息。self-chat 单侧，无 revoke 语义。
type DeleteSavedHistoryRequest struct {
	OwnerUserID int64
	// SavedPeer 是要清空的子会话分组键。
	SavedPeer       Peer
	MaxID           int
	MinDate         int
	MaxDate         int
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// DeleteSavedHistoryResult 描述一次 saved 子会话删除批次。
type DeleteSavedHistoryResult struct {
	// MessageIDs 是本批被删除的 owner 视角 box id。
	MessageIDs []int
	Event      UpdateEvent
	// More 为 true 表示仍有剩余批次，客户端按 affectedHistory.offset 续发。
	More bool
}
