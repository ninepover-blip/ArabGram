package memory

import (
	"testing"

	"telesrv/internal/domain"
)

// TestChannelUnreadCountClampedToMax 锁定 P1-v：未读 COUNT 钳到 MaxDialogUnreadCount（min(actual,cap)），
// 与 postgres 的 LIMIT 子查询同语义。直接喂 s.messages 避免 1000+ 次 send 建链路。
func TestChannelUnreadCountClampedToMax(t *testing.T) {
	const (
		channelID = int64(7000000001)
		viewer    = int64(5001)
		sender    = int64(5002)
	)
	s := NewChannelStore()

	over := domain.MaxDialogUnreadCount + 25
	msgs := make([]domain.ChannelMessage, 0, over)
	for i := 1; i <= over; i++ {
		msgs = append(msgs, domain.ChannelMessage{ChannelID: channelID, ID: i, SenderUserID: sender, ReplyTo: &domain.MessageReply{TopMessageID: 100}})
	}
	s.mu.Lock()
	s.messages[channelID] = msgs
	s.mu.Unlock()

	// 全量未读（readMaxID=0, topID 覆盖全部）→ 应钳到 cap，而非 over。
	if got := s.channelUnreadCountLocked(viewer, channelID, 0, over); got != domain.MaxDialogUnreadCount {
		t.Fatalf("channelUnreadCountLocked = %d, want clamped %d", got, domain.MaxDialogUnreadCount)
	}
	// thread 未读同样钳到 cap（全部消息挂在 root=100 线程）。
	if got := s.channelThreadUnreadCountLocked(viewer, channelID, 100, 0); got != domain.MaxDialogUnreadCount {
		t.Fatalf("channelThreadUnreadCountLocked = %d, want clamped %d", got, domain.MaxDialogUnreadCount)
	}

	// 未超上界时返回精确值（cap 内不被钳）。
	const fewChannel = int64(7000000002)
	few := make([]domain.ChannelMessage, 0, 3)
	for i := 1; i <= 3; i++ {
		few = append(few, domain.ChannelMessage{ChannelID: fewChannel, ID: i, SenderUserID: sender})
	}
	s.mu.Lock()
	s.messages[fewChannel] = few
	s.mu.Unlock()
	if got := s.channelUnreadCountLocked(viewer, fewChannel, 0, 3); got != 3 {
		t.Fatalf("channelUnreadCountLocked(few) = %d, want exact 3 (below cap unaffected)", got)
	}
}
