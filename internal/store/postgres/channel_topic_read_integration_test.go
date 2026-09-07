package postgres

import (
	"context"
	"telesrv/internal/domain"
	"testing"
)

// TestChannelTopicReadIsolation 验证 forum 话题级已读不被频道级单一水位串扰：
// 交错发送使 topicB 的消息 id 都小于 topicA 的最新消息 id，读 topicA 到最新后，
// 旧的频道级水位会把 topicB 误判已读，而 per-topic 水位下 topicB 未读保持不变。
// 同时验证 outbox 已读回执（S6）。
func TestChannelTopicReadIsolation(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 81, Phone: "+1778" + suffix + "81", FirstName: "TROwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 82, Phone: "+1778" + suffix + "82", FirstName: "TRMember"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "TopicRead " + suffix,
		Megagroup:     true,
		Forum:         true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700002100,
	})
	if err != nil {
		t.Fatalf("create forum channel: %v", err)
	}
	channelID = created.Channel.ID

	topicA, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{UserID: owner.ID, ChannelID: channelID, Title: "A " + suffix, RandomID: 222001, Date: 1700002101})
	if err != nil {
		t.Fatalf("topic A: %v", err)
	}
	topicB, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{UserID: owner.ID, ChannelID: channelID, Title: "B " + suffix, RandomID: 222002, Date: 1700002102})
	if err != nil {
		t.Fatalf("topic B: %v", err)
	}

	send := func(rid int64, topicID, date int) domain.ChannelMessage {
		t.Helper()
		res, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
			UserID:    owner.ID,
			ChannelID: channelID,
			RandomID:  rid,
			Message:   "m",
			ReplyTo:   &domain.MessageReply{TopMessageID: topicID, ForumTopic: true},
			Date:      date,
		})
		if err != nil {
			t.Fatalf("send to topic %d: %v", topicID, err)
		}
		return res.Message
	}
	// 交错发送：topicB 两条都在 topicA 最新一条之前，id 更小。
	_ = send(222003, topicA.Topic.TopicID, 1700002103)   // A1
	_ = send(222004, topicB.Topic.TopicID, 1700002104)   // B1
	_ = send(222005, topicB.Topic.TopicID, 1700002105)   // B2
	a2 := send(222006, topicA.Topic.TopicID, 1700002106) // A2（全局最大 id）

	if u := topicUnread(t, channels, ctx, member.ID, channelID, topicA.Topic.TopicID); u != 2 {
		t.Fatalf("before read: topicA unread = %d, want 2", u)
	}
	if u := topicUnread(t, channels, ctx, member.ID, channelID, topicB.Topic.TopicID); u != 2 {
		t.Fatalf("before read: topicB unread = %d, want 2", u)
	}

	res, err := channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		TopicID:   topicA.Topic.TopicID,
		MaxID:     a2.ID,
		Date:      1700002110,
	}, testChannelTopicReadEffects)

	if err != nil {
		t.Fatalf("read topic A: %v", err)
	}
	if !res.Changed || res.MaxID != a2.ID {
		t.Fatalf("read topic A result = %+v, want changed with maxID %d", res, a2.ID)
	}

	// 关键断言：读 topicA 后 topicA 未读=0，topicB 未读仍=2（不被频道级高水位污染）。
	if u := topicUnread(t, channels, ctx, member.ID, channelID, topicA.Topic.TopicID); u != 0 {
		t.Fatalf("after read: topicA unread = %d, want 0", u)
	}
	if u := topicUnread(t, channels, ctx, member.ID, channelID, topicB.Topic.TopicID); u != 2 {
		t.Fatalf("after read: topicB unread = %d, want 2 (cross-topic contamination)", u)
	}

	// 幂等：再读一次同水位 → Changed=false。
	again, err := channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID: member.ID, ChannelID: channelID, TopicID: topicA.Topic.TopicID, MaxID: a2.ID, Date: 1700002111,
	}, testChannelTopicReadEffects)

	if err != nil || again.Changed {
		t.Fatalf("re-read topic A = %+v err %v, want not changed", again, err)
	}

	// outbox 回执（S6）：owner 在 topicA 的消息被 member 读到 a2 → 回执含 owner。
	found := false
	for _, o := range res.OutboxUpdates {
		if o.UserID == owner.ID && o.MaxID == a2.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("outbox updates = %+v, want owner %d at %d", res.OutboxUpdates, owner.ID, a2.ID)
	}
}

func topicUnread(t *testing.T, channels *ChannelStore, ctx context.Context, userID, channelID int64, topicID int) int {
	t.Helper()
	list, err := channels.GetForumTopicsByID(ctx, userID, channelID, []int{topicID})
	if err != nil {
		t.Fatalf("get topic %d: %v", topicID, err)
	}
	for _, topic := range list.Topics {
		if topic.TopicID == topicID {
			return topic.UnreadCount
		}
	}
	t.Fatalf("topic %d not found in %+v", topicID, list.Topics)
	return -1
}

// TestChannelGeneralTopicRead 验证 General 话题（id=1）独立已读：General 未读排除其它话题的根
// 服务消息，且 ReadChannelTopicHistory(General) 只推进 General 自己的水位。
func TestChannelGeneralTopicRead(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 83, Phone: "+1778" + suffix + "83", FirstName: "GenOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 84, Phone: "+1778" + suffix + "84", FirstName: "GenMember"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "General " + suffix, Megagroup: true, Forum: true,
		MemberUserIDs: []int64{member.ID}, Date: 1700003100,
	})
	if err != nil {
		t.Fatalf("create forum channel: %v", err)
	}
	channelID = created.Channel.ID

	// 建一个普通话题：其根服务消息 reply_to_top_id=0，必须被 General 现算排除。
	if _, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{
		UserID: owner.ID, ChannelID: channelID, Title: "Topic " + suffix, RandomID: 333009, Date: 1700003101,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	sendGeneral := func(rid int64, date int) domain.ChannelMessage {
		t.Helper()
		// General 消息按真实客户端行为不带 top_msg_id（reply_to_top_id=0），由 General 现算归并。
		res, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
			UserID:    owner.ID,
			ChannelID: channelID,
			RandomID:  rid,
			Message:   "g",
			Date:      date,
		})
		if err != nil {
			t.Fatalf("send general: %v", err)
		}
		return res.Message
	}
	// 基线：建话题后、发 General 消息前的 General 未读（话题根已排除，仅含频道创建系统消息）。
	base, err := channels.GeneralForumTopic(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("general baseline: %v", err)
	}

	_ = sendGeneral(333010, 1700003110)
	g2 := sendGeneral(333011, 1700003111)

	// 基线之后新建话题 A2 并发一条消息：A2 的根服务消息与普通消息都绝不能计入 General。
	topicA2, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{
		UserID: owner.ID, ChannelID: channelID, Title: "A2 " + suffix, RandomID: 333012, Date: 1700003112,
	})
	if err != nil {
		t.Fatalf("create topic A2: %v", err)
	}
	a1, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 333013, Message: "a",
		ReplyTo: &domain.MessageReply{TopMessageID: topicA2.Topic.TopicID, ForumTopic: true}, Date: 1700003113,
	})
	if err != nil {
		t.Fatalf("send topic A2 message: %v", err)
	}

	gen, err := channels.GeneralForumTopic(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("general topic: %v", err)
	}
	// 只多 2 条 General 消息；A2 根服务消息与 a1 都不计入（否则差为 4）。
	if gen.UnreadCount != base.UnreadCount+2 {
		t.Fatalf("general unread = %d, want baseline %d + 2 (A2 root+message excluded)", gen.UnreadCount, base.UnreadCount)
	}
	if gen.TopMessageID != g2.ID {
		t.Fatalf("general top message = %d, want %d (A2 root/message excluded)", gen.TopMessageID, g2.ID)
	}

	// 读话题 A2 不污染 General。
	if _, err := channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID: member.ID, ChannelID: channelID, TopicID: topicA2.Topic.TopicID, MaxID: a1.Message.ID, Date: 1700003120,
	}, testChannelTopicReadEffects); err != nil {
		t.Fatalf("read topic A2: %v", err)
	}
	genAfterTopic, err := channels.GeneralForumTopic(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("general after topic read: %v", err)
	}
	if genAfterTopic.UnreadCount != gen.UnreadCount {
		t.Fatalf("reading topic A2 changed general unread %d -> %d (contamination)", gen.UnreadCount, genAfterTopic.UnreadCount)
	}

	// 读 General 清零并推进 General 自己的水位。
	res, err := channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID: member.ID, ChannelID: channelID, TopicID: domain.ForumGeneralTopicID, MaxID: g2.ID, Date: 1700003121,
	}, testChannelTopicReadEffects)

	if err != nil || !res.Changed {
		t.Fatalf("read general = %+v err %v, want changed", res, err)
	}
	genAfter, err := channels.GeneralForumTopic(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("general after read: %v", err)
	}
	if genAfter.UnreadCount != 0 || genAfter.ReadInboxMaxID != g2.ID {
		t.Fatalf("general after read = %+v, want unread 0 read_inbox %d", genAfter, g2.ID)
	}
}
