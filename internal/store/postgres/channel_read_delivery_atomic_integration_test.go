package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestChannelReadWatermarksAndAbsoluteDeliveryCommitAtomicallyPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	owner, err := users.Create(ctx, domain.User{AccessHash: 131, Phone: "+1899" + suffix + "01", FirstName: "Read Atomic Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 132, Phone: "+1899" + suffix + "02", FirstName: "Read Atomic Member"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	userIDs := []int64{owner.ID, member.ID}
	var channelID int64
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "Read Atomic " + suffix, Megagroup: true,
		MemberUserIDs: []int64{member.ID}, Date: 1_700_030_000,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 840001, Message: "read me", Date: 1_700_030_001,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	var beforeMemberInbox, beforeOwnerOutbox, beforeDelivery, beforeChannelPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT read_inbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $2),
  (SELECT read_outbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $3),
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($4::bigint[])),
  (SELECT pts FROM channels WHERE id = $1)`, channelID, member.ID, owner.ID, userIDs).
		Scan(&beforeMemberInbox, &beforeOwnerOutbox, &beforeDelivery, &beforeChannelPTS); err != nil {
		t.Fatalf("read channel read baselines: %v", err)
	}
	projectionErr := errors.New("channel read projection failed")
	_, err = channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID: member.ID, ChannelID: channelID, MaxID: sent.Message.ID, Date: 1_700_030_002,
	}, func(domain.ReadChannelHistoryResult) ([]store.DeliveryEffect, error) { return nil, projectionErr })
	if !errors.Is(err, projectionErr) {
		t.Fatalf("failed channel read err = %v, want projection error", err)
	}
	var memberInbox, ownerOutbox, deliveryRows int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT read_inbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $2),
  (SELECT read_outbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $3),
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($4::bigint[]))`, channelID, member.ID, owner.ID, userIDs).
		Scan(&memberInbox, &ownerOutbox, &deliveryRows); err != nil {
		t.Fatalf("read rolled back channel facts: %v", err)
	}
	if memberInbox != beforeMemberInbox || ownerOutbox != beforeOwnerOutbox || deliveryRows != beforeDelivery {
		t.Fatalf("channel read survived failed builder: inbox %d->%d outbox %d->%d delivery %d->%d",
			beforeMemberInbox, memberInbox, beforeOwnerOutbox, ownerOutbox, beforeDelivery, deliveryRows)
	}

	result, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID: member.ID, ChannelID: channelID, MaxID: sent.Message.ID, Date: 1_700_030_003,
	}, func(read domain.ReadChannelHistoryResult) ([]store.DeliveryEffect, error) {
		effects := []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: member.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}
		for _, update := range read.OutboxUpdates {
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: update.UserID, Payload: []byte{2}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
		return effects, nil
	})
	if err != nil {
		t.Fatalf("read channel with delivery: %v", err)
	}
	if !result.Changed || len(result.OutboxUpdates) != 1 || result.OutboxUpdates[0].UserID != owner.ID {
		t.Fatalf("channel read result = %+v", result)
	}
	var afterChannelPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT read_inbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $2),
  (SELECT read_outbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $3),
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($4::bigint[])),
  (SELECT pts FROM channels WHERE id = $1)`, channelID, member.ID, owner.ID, userIDs).
		Scan(&memberInbox, &ownerOutbox, &deliveryRows, &afterChannelPTS); err != nil {
		t.Fatalf("read committed channel facts: %v", err)
	}
	if memberInbox != sent.Message.ID || ownerOutbox != sent.Message.ID || deliveryRows-beforeDelivery != 2 {
		t.Fatalf("channel read committed facts: inbox=%d outbox=%d delivery delta=%d", memberInbox, ownerOutbox, deliveryRows-beforeDelivery)
	}
	if afterChannelPTS != beforeChannelPTS {
		t.Fatalf("channel read changed channel pts %d -> %d", beforeChannelPTS, afterChannelPTS)
	}
}

func TestForumTopicReadWatermarksAndAbsoluteDeliveryCommitAtomicallyPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	owner, err := users.Create(ctx, domain.User{AccessHash: 141, Phone: "+1900" + suffix + "01", FirstName: "Topic Atomic Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 142, Phone: "+1900" + suffix + "02", FirstName: "Topic Atomic Member"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	userIDs := []int64{owner.ID, member.ID}
	var channelID int64
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "Topic Atomic " + suffix, Megagroup: true, Forum: true,
		MemberUserIDs: []int64{member.ID}, Date: 1_700_040_000,
	})
	if err != nil {
		t.Fatalf("create forum: %v", err)
	}
	channelID = created.Channel.ID
	topic, err := channels.CreateForumTopic(ctx, domain.CreateChannelForumTopicRequest{
		UserID: owner.ID, ChannelID: channelID, Title: "Atomic Topic", RandomID: 850001, Date: 1_700_040_001,
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 850002, Message: "topic read me",
		ReplyTo: &domain.MessageReply{TopMessageID: topic.Topic.TopicID, ForumTopic: true}, Date: 1_700_040_002,
	})
	if err != nil {
		t.Fatalf("send topic message: %v", err)
	}
	beforeMemberInbox, err := channels.channelTopicReadInbox(ctx, pool, channelID, member.ID, topic.Topic.TopicID, 0)
	if err != nil {
		t.Fatalf("read member topic baseline: %v", err)
	}
	beforeOwnerWater, err := channels.channelTopicReadBatch(ctx, channelID, owner.ID, []int32{int32(topic.Topic.TopicID)}, 0)
	if err != nil {
		t.Fatalf("read owner topic baseline: %v", err)
	}
	var beforeDelivery, beforeChannelPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
  (SELECT pts FROM channels WHERE id = $2)`, userIDs, channelID).Scan(&beforeDelivery, &beforeChannelPTS); err != nil {
		t.Fatalf("read topic durable baselines: %v", err)
	}
	projectionErr := errors.New("topic read projection failed")
	_, err = channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID: member.ID, ChannelID: channelID, TopicID: topic.Topic.TopicID, MaxID: sent.Message.ID, Date: 1_700_040_003,
	}, func(domain.ReadChannelTopicHistoryResult) ([]store.DeliveryEffect, error) { return nil, projectionErr })
	if !errors.Is(err, projectionErr) {
		t.Fatalf("failed topic read err = %v, want projection error", err)
	}
	afterFailedInbox, err := channels.channelTopicReadInbox(ctx, pool, channelID, member.ID, topic.Topic.TopicID, 0)
	if err != nil {
		t.Fatalf("read member topic after rollback: %v", err)
	}
	afterFailedOwner, err := channels.channelTopicReadBatch(ctx, channelID, owner.ID, []int32{int32(topic.Topic.TopicID)}, 0)
	if err != nil {
		t.Fatalf("read owner topic after rollback: %v", err)
	}
	if afterFailedInbox != beforeMemberInbox || afterFailedOwner[topic.Topic.TopicID].Outbox != beforeOwnerWater[topic.Topic.TopicID].Outbox {
		t.Fatalf("topic read survived failed builder: member %d->%d owner outbox %d->%d",
			beforeMemberInbox, afterFailedInbox, beforeOwnerWater[topic.Topic.TopicID].Outbox, afterFailedOwner[topic.Topic.TopicID].Outbox)
	}

	result, err := channels.ReadChannelTopicHistory(ctx, domain.ReadChannelTopicHistoryRequest{
		UserID: member.ID, ChannelID: channelID, TopicID: topic.Topic.TopicID, MaxID: sent.Message.ID, Date: 1_700_040_004,
	}, func(read domain.ReadChannelTopicHistoryResult) ([]store.DeliveryEffect, error) {
		effects := []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: member.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}
		for _, update := range read.OutboxUpdates {
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: update.UserID, Payload: []byte{2}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
		return effects, nil
	})
	if err != nil {
		t.Fatalf("read topic with delivery: %v", err)
	}
	if !result.Changed || len(result.OutboxUpdates) != 1 || result.OutboxUpdates[0].UserID != owner.ID {
		t.Fatalf("topic read result = %+v", result)
	}
	memberInbox, err := channels.channelTopicReadInbox(ctx, pool, channelID, member.ID, topic.Topic.TopicID, 0)
	if err != nil {
		t.Fatalf("read committed member topic: %v", err)
	}
	ownerWater, err := channels.channelTopicReadBatch(ctx, channelID, owner.ID, []int32{int32(topic.Topic.TopicID)}, 0)
	if err != nil {
		t.Fatalf("read committed owner topic: %v", err)
	}
	var deliveryRows, afterChannelPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
  (SELECT pts FROM channels WHERE id = $2)`, userIDs, channelID).Scan(&deliveryRows, &afterChannelPTS); err != nil {
		t.Fatalf("read committed topic durable facts: %v", err)
	}
	if memberInbox != sent.Message.ID || ownerWater[topic.Topic.TopicID].Outbox != sent.Message.ID || deliveryRows-beforeDelivery != 2 {
		t.Fatalf("topic read committed facts: member=%d owner=%d delivery delta=%d",
			memberInbox, ownerWater[topic.Topic.TopicID].Outbox, deliveryRows-beforeDelivery)
	}
	if afterChannelPTS != beforeChannelPTS {
		t.Fatalf("topic read changed channel pts %d -> %d", beforeChannelPTS, afterChannelPTS)
	}
}

func TestChannelMessageContentsAndAbsoluteDeliveryCommitAtomicallyPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	owner, err := users.Create(ctx, domain.User{AccessHash: 151, Phone: "+1901" + suffix + "01", FirstName: "Contents Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 152, Phone: "+1901" + suffix + "02", FirstName: "Contents Member"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	userIDs := []int64{owner.ID, member.ID}
	var channelID int64
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "Contents Atomic " + suffix, Megagroup: true,
		MemberUserIDs: []int64{member.ID}, Date: 1_700_050_000,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: channelID, RandomID: 860001, Message: "watch", Date: 1_700_050_001,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	if _, err := channels.SetChannelMessageReactions(ctx, domain.SetChannelMessageReactionsRequest{
		UserID: member.ID, ChannelID: channelID, MessageID: sent.Message.ID,
		Reactions: []domain.MessageReaction{{Type: domain.MessageReactionEmoji, Emoticon: "👍"}}, Date: 1_700_050_002,
	}); err != nil {
		t.Fatalf("set channel reaction: %v", err)
	}
	var beforeDelivery int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs).Scan(&beforeDelivery); err != nil {
		t.Fatalf("read delivery baseline: %v", err)
	}
	projectionErr := errors.New("channel contents projection failed")
	_, err = channels.ReadChannelMessageContents(ctx, domain.ReadChannelMessageContentsRequest{
		UserID: owner.ID, ChannelID: channelID, IDs: []int{sent.Message.ID},
	}, func(domain.ReadChannelMessageContentsResult) ([]store.DeliveryEffect, error) {
		return nil, projectionErr
	})
	if !errors.Is(err, projectionErr) {
		t.Fatalf("failed channel contents err = %v, want projection error", err)
	}
	var unread bool
	var deliveryRows int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT unread FROM channel_message_reactions WHERE channel_id = $1 AND message_id = $2 AND reacted_user_id = $3),
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($4::bigint[]))`,
		channelID, sent.Message.ID, member.ID, userIDs).Scan(&unread, &deliveryRows); err != nil {
		t.Fatalf("read rolled back channel contents facts: %v", err)
	}
	if !unread || deliveryRows != beforeDelivery {
		t.Fatalf("channel contents survived failed builder: unread=%v delivery %d->%d", unread, beforeDelivery, deliveryRows)
	}
	result, err := channels.ReadChannelMessageContents(ctx, domain.ReadChannelMessageContentsRequest{
		UserID: owner.ID, ChannelID: channelID, IDs: []int{sent.Message.ID},
	}, func(domain.ReadChannelMessageContentsResult) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	})
	if err != nil {
		t.Fatalf("read channel contents with delivery: %v", err)
	}
	if len(result.ClearedUnreadReactionMessageIDs) != 1 || result.ClearedUnreadReactionMessageIDs[0] != sent.Message.ID {
		t.Fatalf("channel contents result = %+v", result)
	}
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT unread FROM channel_message_reactions WHERE channel_id = $1 AND message_id = $2 AND reacted_user_id = $3),
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($4::bigint[]))`,
		channelID, sent.Message.ID, member.ID, userIDs).Scan(&unread, &deliveryRows); err != nil {
		t.Fatalf("read committed channel contents facts: %v", err)
	}
	if unread || deliveryRows-beforeDelivery != 1 {
		t.Fatalf("channel contents committed facts: unread=%v delivery delta=%d", unread, deliveryRows-beforeDelivery)
	}
}
