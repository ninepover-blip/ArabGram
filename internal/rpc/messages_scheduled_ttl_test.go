package rpc

import (
	"context"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"
	"telesrv/internal/domain"
	"testing"
	"time"
)

func TestScheduledMessagesLifecycleUsesScheduledStore(t *testing.T) {
	const (
		aliceID = int64(1000000001)
		bobID   = int64(1000000002)
		now     = int64(1700001000)
	)
	alice := domain.User{ID: aliceID, AccessHash: 11, FirstName: "Alice"}
	bob := domain.User{ID: bobID, AccessHash: 22, FirstName: "Bob"}
	messages := &scheduledCaptureMessages{captureMessages: &captureMessages{}}
	outbox := attachCaptureMessagesDeliveryOutbox(messages.captureMessages)
	r := New(Config{}, Deps{
		Messages:       messages,
		Users:          mapUsersService{users: map[int64]domain.User{aliceID: alice, bobID: bob}},
		DeliveryOutbox: outbox,
	}, zaptest.NewLogger(t), fixedClock{now: time.Unix(now, 0)})
	ctx := WithUserID(context.Background(), aliceID)

	sendReq := &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerUser{UserID: bobID, AccessHash: bob.AccessHash},
		Message:  "later",
		RandomID: 123456,
	}
	sendReq.SetScheduleDate(int(now + 3600))
	updates, err := r.onMessagesSendMessage(ctx, sendReq)
	if err != nil {
		t.Fatalf("send scheduled message: %v", err)
	}
	if messages.scheduleReq.OwnerUserID != aliceID || messages.scheduleReq.Peer.ID != bobID || messages.scheduleReq.ScheduleDate != int(now+3600) {
		t.Fatalf("schedule req = %+v, want alice->bob at requested date", messages.scheduleReq)
	}
	gotUpdates := updates.(*tg.Updates).Updates
	if len(gotUpdates) != 2 {
		t.Fatalf("scheduled send updates = %+v, want id + new scheduled", gotUpdates)
	}
	newScheduled, ok := gotUpdates[1].(*tg.UpdateNewScheduledMessage)
	if !ok {
		t.Fatalf("scheduled update = %T, want UpdateNewScheduledMessage", gotUpdates[1])
	}
	scheduledMsg, ok := newScheduled.Message.(*tg.Message)
	if !ok || !scheduledMsg.FromScheduled || scheduledMsg.Message != "later" || scheduledMsg.Date != int(now+3600) {
		t.Fatalf("scheduled message = %#v, want from_scheduled later at schedule date", newScheduled.Message)
	}

	history, err := r.onMessagesGetScheduledHistory(ctx, &tg.MessagesGetScheduledHistoryRequest{
		Peer: &tg.InputPeerUser{UserID: bobID, AccessHash: bob.AccessHash},
	})
	if err != nil {
		t.Fatalf("get scheduled history: %v", err)
	}
	if list := history.(*tg.MessagesMessages).Messages; len(list) != 1 {
		t.Fatalf("scheduled history = %+v, want one message", list)
	}

	editReq := &tg.MessagesEditMessageRequest{
		Peer: &tg.InputPeerUser{UserID: bobID, AccessHash: bob.AccessHash},
		ID:   scheduledMsg.ID,
	}
	editReq.SetMessage("edited")
	editReq.SetScheduleDate(int(now + 7200))
	edited, err := r.onMessagesEditMessage(ctx, editReq)
	if err != nil {
		t.Fatalf("edit scheduled message: %v", err)
	}
	if messages.editScheduledReq.ID != scheduledMsg.ID || !messages.editScheduledReq.SetMessage || messages.editScheduledReq.Message != "edited" || messages.editScheduledReq.ScheduleDate != int(now+7200) {
		t.Fatalf("edit scheduled req = %+v, want updated scheduled text/date", messages.editScheduledReq)
	}
	editedMessage := edited.(*tg.Updates).Updates[0].(*tg.UpdateNewScheduledMessage).Message.(*tg.Message)
	if !editedMessage.FromScheduled || editedMessage.Message != "edited" || editedMessage.Date != int(now+7200) {
		t.Fatalf("edited scheduled message = %#v, want edited future scheduled item", editedMessage)
	}

	sent, err := r.onMessagesSendScheduledMessages(ctx, &tg.MessagesSendScheduledMessagesRequest{
		Peer: &tg.InputPeerUser{UserID: bobID, AccessHash: bob.AccessHash},
		ID:   []int{scheduledMsg.ID},
	})
	if err != nil {
		t.Fatalf("send scheduled now: %v", err)
	}
	if messages.sendReq.Message != "edited" || messages.markedScheduledID != scheduledMsg.ID || messages.markedSentID == 0 {
		t.Fatalf("sent scheduled state send=%+v marked scheduled=%d sent=%d", messages.sendReq, messages.markedScheduledID, messages.markedSentID)
	}
	var deleteUpdate *tg.UpdateDeleteScheduledMessages
	for _, update := range sent.(*tg.Updates).Updates {
		if v, ok := update.(*tg.UpdateDeleteScheduledMessages); ok {
			deleteUpdate = v
		}
	}
	if deleteUpdate == nil || len(deleteUpdate.Messages) != 1 || deleteUpdate.Messages[0] != scheduledMsg.ID {
		t.Fatalf("send scheduled delete update = %#v, want deleted scheduled id", deleteUpdate)
	}
	if sentIDs, ok := deleteUpdate.GetSentMessages(); !ok || len(sentIDs) != 1 || sentIDs[0] != messages.markedSentID {
		t.Fatalf("delete scheduled sent ids = %+v ok %v, want sent message id %d", sentIDs, ok, messages.markedSentID)
	}
	if items := outbox.Snapshot(); len(items) != 3 {
		t.Fatalf("scheduled lifecycle durable deliveries = %+v, want create/edit/sent", items)
	}
}

func TestScheduledMessageEditDateOnlyPreservesContent(t *testing.T) {
	const (
		aliceID = int64(1000000001)
		bobID   = int64(1000000002)
		now     = int64(1700003000)
	)
	media := &domain.MessageMedia{
		Kind:     domain.MessageMediaKindDocument,
		Document: &domain.Document{ID: 910000000000000001, AccessHash: 91, DCID: 2},
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: bobID}
	messages := &scheduledCaptureMessages{
		captureMessages: &captureMessages{},
		scheduled: []domain.ScheduledMessage{{
			OwnerUserID:  aliceID,
			ID:           77,
			Peer:         peer,
			Message:      "",
			Media:        media,
			ScheduleDate: int(now + 3600),
			State:        "pending",
		}},
	}
	outbox := attachCaptureMessagesDeliveryOutbox(messages.captureMessages)
	r := New(Config{}, Deps{Messages: messages, DeliveryOutbox: outbox}, zaptest.NewLogger(t), fixedClock{now: time.Unix(now, 0)})
	ctx := WithUserID(context.Background(), aliceID)

	editReq := &tg.MessagesEditMessageRequest{
		Peer: &tg.InputPeerUser{UserID: bobID, AccessHash: 22},
		ID:   77,
	}
	editReq.SetScheduleDate(int(now + 7200))
	updates, err := r.onMessagesEditMessage(ctx, editReq)
	if err != nil {
		t.Fatalf("date-only scheduled edit: %v", err)
	}
	if messages.editScheduledReq.SetMessage {
		t.Fatalf("edit scheduled req = %+v, want date-only edit without content overwrite", messages.editScheduledReq)
	}
	if messages.scheduled[0].Message != "" || messages.scheduled[0].Media != media || messages.scheduled[0].ScheduleDate != int(now+7200) {
		t.Fatalf("scheduled item after date-only edit = %+v, want original content and new date", messages.scheduled[0])
	}
	editedMessage := updates.(*tg.Updates).Updates[0].(*tg.UpdateNewScheduledMessage).Message.(*tg.Message)
	if !editedMessage.FromScheduled || editedMessage.Date != int(now+7200) {
		t.Fatalf("date-only edit update = %#v, want scheduled message at new date", editedMessage)
	}
	if items := outbox.Snapshot(); len(items) != 1 || items[0].TargetUserID != aliceID {
		t.Fatalf("date-only edit durable deliveries = %+v, want one owner delivery", items)
	}
}

func TestScheduledMessageEditAllowsEmptyCaptionForMedia(t *testing.T) {
	const (
		aliceID = int64(1000000001)
		bobID   = int64(1000000002)
		now     = int64(1700004000)
	)
	media := &domain.MessageMedia{
		Kind:     domain.MessageMediaKindDocument,
		Document: &domain.Document{ID: 910000000000000002, AccessHash: 92, DCID: 2},
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: bobID}
	messages := &scheduledCaptureMessages{
		captureMessages: &captureMessages{},
		scheduled: []domain.ScheduledMessage{{
			OwnerUserID:  aliceID,
			ID:           78,
			Peer:         peer,
			Message:      "caption",
			Media:        media,
			ScheduleDate: int(now + 3600),
			State:        "pending",
		}},
	}
	outbox := attachCaptureMessagesDeliveryOutbox(messages.captureMessages)
	r := New(Config{}, Deps{Messages: messages, DeliveryOutbox: outbox}, zaptest.NewLogger(t), fixedClock{now: time.Unix(now, 0)})
	ctx := WithUserID(context.Background(), aliceID)

	editReq := &tg.MessagesEditMessageRequest{
		Peer: &tg.InputPeerUser{UserID: bobID, AccessHash: 22},
		ID:   78,
	}
	editReq.SetMessage("")
	editReq.SetScheduleDate(int(now + 7200))
	if _, err := r.onMessagesEditMessage(ctx, editReq); err != nil {
		t.Fatalf("empty-caption scheduled edit: %v", err)
	}
	if !messages.editScheduledReq.SetMessage || messages.editScheduledReq.Message != "" {
		t.Fatalf("edit scheduled req = %+v, want explicit empty caption update", messages.editScheduledReq)
	}
	if messages.scheduled[0].Message != "" || messages.scheduled[0].Media != media || messages.scheduled[0].ScheduleDate != int(now+7200) {
		t.Fatalf("scheduled item after empty-caption edit = %+v, want media kept and caption cleared", messages.scheduled[0])
	}
	if items := outbox.Snapshot(); len(items) != 1 || items[0].TargetUserID != aliceID {
		t.Fatalf("empty-caption edit durable deliveries = %+v, want one owner delivery", items)
	}
}

func TestMessagesSetHistoryTTLDurablyQueuesPrivatePeerBothSides(t *testing.T) {
	const (
		aliceID = int64(1000000001)
		bobID   = int64(1000000002)
		now     = int64(1700002000)
		period  = 86400
	)
	messages := &ttlCaptureMessages{captureMessages: &captureMessages{}}
	outbox := attachCaptureMessagesDeliveryOutbox(messages.captureMessages)
	r := New(Config{}, Deps{Messages: messages, DeliveryOutbox: outbox}, zaptest.NewLogger(t), fixedClock{now: time.Unix(now, 0)})
	authKeyID := [8]byte{7, 7}
	ctx := WithSessionID(WithAuthKeyID(WithUserID(context.Background(), aliceID), authKeyID), 77)

	out, err := r.onMessagesSetHistoryTTL(ctx, &tg.MessagesSetHistoryTTLRequest{
		Peer:   &tg.InputPeerUser{UserID: bobID, AccessHash: 22},
		Period: period,
	})
	if err != nil {
		t.Fatalf("set private history ttl: %v", err)
	}
	if messages.setTTLUserID != aliceID || messages.setTTLPeer != (domain.Peer{Type: domain.PeerTypeUser, ID: bobID}) || messages.setTTLPeriod != period {
		t.Fatalf("set ttl context user=%d peer=%+v period=%d", messages.setTTLUserID, messages.setTTLPeer, messages.setTTLPeriod)
	}
	selfUpdate := out.(*tg.Updates).Updates[0].(*tg.UpdatePeerHistoryTTL)
	if peer, ok := selfUpdate.Peer.(*tg.PeerUser); !ok || peer.UserID != bobID {
		t.Fatalf("self ttl peer = %#v, want bob", selfUpdate.Peer)
	}
	if ttl, ok := selfUpdate.GetTTLPeriod(); !ok || ttl != period {
		t.Fatalf("self ttl period = %d ok %v, want %d", ttl, ok, period)
	}
	items := outbox.Snapshot()
	if len(items) != 2 {
		t.Fatalf("ttl durable deliveries = %+v, want alice and bob", items)
	}
	aliceItem := requireDeliveryItemForUser(t, items, aliceID)
	if aliceItem.ExcludeAuthKeyID != authKeyID || aliceItem.ExcludeSessionID != 77 {
		t.Fatalf("alice ttl delivery exclusion = %x/%d, want %x/77", aliceItem.ExcludeAuthKeyID, aliceItem.ExcludeSessionID, authKeyID)
	}
	bobItem := requireDeliveryItemForUser(t, items, bobID)
	if bobItem.ExcludeAuthKeyID != ([8]byte{}) || bobItem.ExcludeSessionID != 0 {
		t.Fatalf("bob ttl delivery exclusion = %x/%d, want none", bobItem.ExcludeAuthKeyID, bobItem.ExcludeSessionID)
	}
	peerUpdates := requireDeliveryUpdates(t, bobItem)
	peerUpdate := peerUpdates.Updates[0].(*tg.UpdatePeerHistoryTTL)
	if peer, ok := peerUpdate.Peer.(*tg.PeerUser); !ok || peer.UserID != aliceID {
		t.Fatalf("peer ttl update peer = %#v, want alice", peerUpdate.Peer)
	}
}

func TestScheduledAndTTLRPCsFailClosedWithoutMessagesService(t *testing.T) {
	const userID int64 = 1000000001
	r := New(Config{}, Deps{}, zaptest.NewLogger(t), fixedClock{now: time.Unix(1_700_005_000, 0)})
	ctx := WithUserID(context.Background(), userID)

	if ok, err := r.onMessagesSetDefaultHistoryTTL(ctx, 3600); err == nil || ok {
		t.Fatalf("set default TTL without messages service = ok %v err %v, want fail closed", ok, err)
	}
	updates, err := r.onMessagesDeleteScheduledMessages(ctx, &tg.MessagesDeleteScheduledMessagesRequest{
		Peer: &tg.InputPeerUser{UserID: 1000000002, AccessHash: 22},
		ID:   []int{41},
	})
	if err == nil || updates != nil {
		t.Fatalf("delete scheduled without messages service = updates %#v err %v, want no fabricated update", updates, err)
	}
}
