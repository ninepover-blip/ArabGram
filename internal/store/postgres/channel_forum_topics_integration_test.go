package postgres

import (
	"context"
	"telesrv/internal/domain"
	"testing"
)

func TestChannelStoreForumTopicsBatchViewerCounters(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 91,
		Phone:      "+1777" + suffix + "91",
		FirstName:  "ForumOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 92,
		Phone:      "+1777" + suffix + "92",
		FirstName:  "ForumMember",
	})
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
		Title:         "Forum Counters " + suffix,
		Megagroup:     true,
		Forum:         true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700001100,
	})
	if err != nil {
		t.Fatalf("create forum channel: %v", err)
	}
	channelID = created.Channel.ID

	topicA, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Title:     "Alpha " + suffix,
		RandomID:  111001,
		Date:      1700001101,
	})
	if err != nil {
		t.Fatalf("create topic A: %v", err)
	}
	topicB, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Title:     "Beta " + suffix,
		RandomID:  111002,
		Date:      1700001102,
	})
	if err != nil {
		t.Fatalf("create topic B: %v", err)
	}

	memberReply, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		RandomID:  111003,
		Message:   "member message in alpha",
		ReplyTo: &domain.MessageReply{
			TopMessageID: topicA.Topic.TopicID,
			ForumTopic:   true,
		},
		Date: 1700001103,
	})
	if err != nil {
		t.Fatalf("send member topic reply: %v", err)
	}
	if _, err := channels.SetChannelMessageReactions(ctx, domain.SetChannelMessageReactionsRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MessageID: memberReply.Message.ID,
		Reactions: []domain.MessageReaction{{
			Type:     domain.MessageReactionEmoji,
			Emoticon: "\U0001f525",
		}},
		Date: 1700001104,
	}); err != nil {
		t.Fatalf("set topic reaction: %v", err)
	}
	if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:         owner.ID,
		ChannelID:      channelID,
		RandomID:       111005,
		Message:        "alpha mention",
		MentionUserIDs: []int64{member.ID},
		ReplyTo: &domain.MessageReply{
			TopMessageID: topicA.Topic.TopicID,
			ForumTopic:   true,
		},
		Date: 1700001105,
	}); err != nil {
		t.Fatalf("send alpha mention: %v", err)
	}
	if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  111006,
		Message:   "alpha plain",
		ReplyTo: &domain.MessageReply{
			TopMessageID: topicA.Topic.TopicID,
			ForumTopic:   true,
		},
		Date: 1700001106,
	}); err != nil {
		t.Fatalf("send alpha plain: %v", err)
	}
	if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  111007,
		Message:   "beta plain",
		ReplyTo: &domain.MessageReply{
			TopMessageID: topicB.Topic.TopicID,
			ForumTopic:   true,
		},
		Date: 1700001107,
	}); err != nil {
		t.Fatalf("send beta plain: %v", err)
	}

	list, err := channels.ListForumTopics(ctx, member.ID, domain.ChannelForumTopicFilter{
		ChannelID: channelID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list forum topics: %v", err)
	}
	if list.Count != 2 || len(list.Topics) != 2 {
		t.Fatalf("forum topics count=%d len=%d topics=%+v, want count 2 len 2", list.Count, len(list.Topics), list.Topics)
	}
	assertForumTopicCounter(t, list.Topics, topicA.Topic.TopicID, 2, 1, 1)
	assertForumTopicCounter(t, list.Topics, topicB.Topic.TopicID, 1, 0, 0)

	byID, err := channels.GetForumTopicsByID(ctx, member.ID, channelID, []int{topicA.Topic.TopicID, topicB.Topic.TopicID})
	if err != nil {
		t.Fatalf("get forum topics by id: %v", err)
	}
	if byID.Count != 2 || len(byID.Topics) != 2 {
		t.Fatalf("forum topics by id count=%d len=%d topics=%+v, want count 2 len 2", byID.Count, len(byID.Topics), byID.Topics)
	}
	assertForumTopicCounter(t, byID.Topics, topicA.Topic.TopicID, 2, 1, 1)
	assertForumTopicCounter(t, byID.Topics, topicB.Topic.TopicID, 1, 0, 0)
}

func assertForumTopicCounter(t *testing.T, topics []domain.ChannelForumTopic, topicID, unread, mentions, reactions int) {
	t.Helper()
	for _, topic := range topics {
		if topic.TopicID != topicID {
			continue
		}
		if topic.UnreadCount != unread || topic.UnreadMentionsCount != mentions || topic.UnreadReactionsCount != reactions {
			t.Fatalf("topic %d counters = unread %d mentions %d reactions %d, want %d/%d/%d",
				topicID, topic.UnreadCount, topic.UnreadMentionsCount, topic.UnreadReactionsCount, unread, mentions, reactions)
		}
		return
	}
	t.Fatalf("topic %d not found in %+v", topicID, topics)
}
