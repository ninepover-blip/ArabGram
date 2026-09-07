package domain

// NotifyScopeKind 区分通知设置的作用域：具体 peer / 三类全局默认（私聊/群/频道）。
type NotifyScopeKind string

const (
	NotifyScopePeer       NotifyScopeKind = "peer"
	NotifyScopeUsers      NotifyScopeKind = "users"
	NotifyScopeChats      NotifyScopeKind = "chats"
	NotifyScopeBroadcasts NotifyScopeKind = "broadcasts"
)

// NotifyScope 唯一标识一条通知设置：peer 作用域用 Peer(+TopicID 区分 forum 话题，
// 0=整 peer)，三类默认作用域 Peer/TopicID 为空。
type NotifyScope struct {
	Kind    NotifyScopeKind
	Peer    Peer
	TopicID int
}

// PeerNotifySettings 是 peerNotifySettings 的业务层表达。各字段为指针=可选：
// nil 表示该项未设置（客户端按所属类别默认继承）。声音字段恒为默认，v1 不建模
// 自定义铃声（custom sound 罕见，留后续）。
type PeerNotifySettings struct {
	ShowPreviews      *bool
	Silent            *bool
	MuteUntil         *int
	StoriesMuted      *bool
	StoriesHideSender *bool
}

// IsZero 报告该设置是否全部未设置（无需持久化/可视为继承默认）。
func (s PeerNotifySettings) IsZero() bool {
	return s.ShowPreviews == nil && s.Silent == nil && s.MuteUntil == nil &&
		s.StoriesMuted == nil && s.StoriesHideSender == nil
}

// NotifyException 是一个 peer（可含 forum 话题）的非默认通知设置，供
// account.getNotifyExceptions 列出"有自定义通知设置的会话"全局索引。
type NotifyException struct {
	Peer     Peer
	TopicID  int
	Settings PeerNotifySettings
}

// Clone 深拷贝指针字段，供内存 store 隔离。
func (s PeerNotifySettings) Clone() PeerNotifySettings {
	out := PeerNotifySettings{}
	if s.ShowPreviews != nil {
		v := *s.ShowPreviews
		out.ShowPreviews = &v
	}
	if s.Silent != nil {
		v := *s.Silent
		out.Silent = &v
	}
	if s.MuteUntil != nil {
		v := *s.MuteUntil
		out.MuteUntil = &v
	}
	if s.StoriesMuted != nil {
		v := *s.StoriesMuted
		out.StoriesMuted = &v
	}
	if s.StoriesHideSender != nil {
		v := *s.StoriesHideSender
		out.StoriesHideSender = &v
	}
	return out
}
