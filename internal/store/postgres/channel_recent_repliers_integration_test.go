package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

// TestChannelStoreRecentRepliersPostgres 回归 populateChannelMessageReplies:此前它只算
// Replies/MaxID/RepliesPts,从不填 RecentRepliers(恒 nil),导致 Postgres(生产)下频道帖
// 评论入口拿不到「最近回复者头像」——而 memory store 一直在填,属双 store 不一致。
// 这里验证 Postgres 也按「各回复者最新一条回复 id 倒序、去重、取前 3」回填,与 memory 对齐。
func TestChannelStoreRecentRepliersPostgres(t *testing.T) {
	pool := testPool(t) // 未设 TELESRV_TEST_POSTGRES_DSN 会 t.Skip
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 61, Phone: "+1779" + suffix + "01", FirstName: "ReplyStatOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	alice, err := users.Create(ctx, domain.User{AccessHash: 62, Phone: "+1779" + suffix + "02", FirstName: "ReplyStatAlice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 63, Phone: "+1779" + suffix + "03", FirstName: "ReplyStatBob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, alice.ID, bob.ID})
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Recent Repliers " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{alice.ID, bob.ID},
		Date:          1700000800,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	root, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 9301, Message: "root post", Date: 1700000801,
	})
	if err != nil {
		t.Fatalf("send root: %v", err)
	}
	reply := func(u, rid int64, date int) {
		t.Helper()
		if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
			UserID: u, ChannelID: channelID, RandomID: rid, Message: "c",
			ReplyTo: &domain.MessageReply{MessageID: root.Message.ID}, Date: date,
		}); err != nil {
			t.Fatalf("reply from %d: %v", u, err)
		}
	}
	reply(alice.ID, 9302, 1700000802) // alice 最旧
	reply(bob.ID, 9303, 1700000803)   // bob 居中
	reply(alice.ID, 9304, 1700000804) // alice 最新

	got, err := channels.GetChannelMessages(ctx, owner.ID, channelID, []int{root.Message.ID})
	if err != nil {
		t.Fatalf("get channel messages: %v", err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("get messages = %d, want 1", len(got.Messages))
	}
	replies := got.Messages[0].Replies
	if replies == nil {
		t.Fatalf("root.Replies == nil, want reply stats")
	}
	if replies.Replies != 3 {
		t.Fatalf("replies count = %d, want 3", replies.Replies)
	}
	// 去重 + newest-first:alice(最新一条 9304)排首、bob(9303)次之;alice 那条更旧的(9302)
	// 因 alice 已出现被去重,故只剩两个去重回复者。
	want := []domain.Peer{
		{Type: domain.PeerTypeUser, ID: alice.ID},
		{Type: domain.PeerTypeUser, ID: bob.ID},
	}
	if len(replies.RecentRepliers) != len(want) {
		t.Fatalf("recent repliers = %+v, want %+v", replies.RecentRepliers, want)
	}
	for i := range want {
		if replies.RecentRepliers[i] != want[i] {
			t.Fatalf("recent repliers[%d] = %+v, want %+v (full=%+v)", i, replies.RecentRepliers[i], want[i], replies.RecentRepliers)
		}
	}
}
