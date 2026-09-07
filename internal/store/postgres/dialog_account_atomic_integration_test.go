package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func createDialogAggregateUsers(t *testing.T, pool *pgxpool.Pool, count int) []domain.User {
	t.Helper()
	ctx := context.Background()
	users := make([]domain.User, 0, count)
	base := time.Now().UnixNano() % 1_000_000_000
	userStore := NewUserStore(pool)
	for i := 0; i < count; i++ {
		user, err := userStore.Create(ctx, domain.User{
			AccessHash: int64(900 + i),
			Phone:      fmt.Sprintf("+1888%09d%02d", base, i),
			FirstName:  fmt.Sprintf("DialogAggregate%d", i),
		})
		if err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		users = append(users, user)
	}
	ids := make([]int64, 0, len(users))
	for _, user := range users {
		ids = append(ids, user.ID)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", ids)
	})
	return users
}

func postgresDialogTestEffects(snapshot storepkg.DialogAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	m := snapshot.Mutation
	event := domain.UpdateEvent{Date: m.Date, PtsCount: 1}
	switch m.Kind {
	case storepkg.DialogAccountSaveDraft:
		event.Type, event.Peer, event.MaxID = domain.UpdateEventDraftMessage, m.Draft.Peer, m.Draft.TopMessageID
	case storepkg.DialogAccountDeleteDraft:
		event.Type, event.Peer, event.MaxID = domain.UpdateEventDraftMessage, m.Peer, m.TopMessageID
	case storepkg.DialogAccountSetPinned:
		event.Type, event.Peer, event.Bool, event.FolderID = domain.UpdateEventDialogPinned, m.Peer, m.Value, snapshot.FolderID
	default:
		return nil, fmt.Errorf("unsupported test mutation %q", m.Kind)
	}
	return []storepkg.DeliveryEffect{storepkg.AccountPTSDeliveryEffect(m.UserID, event, [8]byte{}, 0)}, nil
}

func postgresSavedDialogTestEffects(snapshot storepkg.SavedDialogMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	event := domain.UpdateEvent{Date: snapshot.Mutation.Date, PtsCount: 1}
	switch snapshot.Mutation.Kind {
	case storepkg.SavedDialogSetPinned:
		event.Type = domain.UpdateEventSavedDialogPinned
		event.Peer = snapshot.Mutation.Peer
		event.Bool = snapshot.Mutation.Pinned
	case storepkg.SavedDialogReorder:
		event.Type = domain.UpdateEventPinnedSavedDialogs
		event.Peers = append([]domain.Peer(nil), snapshot.Order...)
	default:
		return nil, fmt.Errorf("unsupported saved-dialog test mutation %q", snapshot.Mutation.Kind)
	}
	return []storepkg.DeliveryEffect{storepkg.AccountPTSDeliveryEffect(snapshot.Mutation.UserID, event, [8]byte{}, 0)}, nil
}

func postgresQuickReplyTestEffects(snapshot storepkg.QuickReplyAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	result := snapshot.Result
	event := domain.UpdateEvent{
		Date: snapshot.Mutation.Date, PtsCount: 1, MaxID: result.ShortcutID,
		QuickReplies: append([]domain.QuickReply(nil), result.List.QuickReplies...),
		QuickReply:   result.QuickReply, QuickReplyMessage: result.Message,
		MessageIDs: append([]int(nil), result.MessageIDs...),
	}
	switch result.Kind {
	case domain.QuickReplyMutationNew:
		event.Type = domain.UpdateEventNewQuickReply
	case domain.QuickReplyMutationMessage:
		event.Type = domain.UpdateEventQuickReplyMessage
	case domain.QuickReplyMutationDelete:
		event.Type = domain.UpdateEventDeleteQuickReply
	case domain.QuickReplyMutationIDs:
		event.Type = domain.UpdateEventDeleteQuickReplyMessages
	default:
		event.Type = domain.UpdateEventQuickReplies
	}
	return []storepkg.DeliveryEffect{storepkg.AccountPTSDeliveryEffect(snapshot.Mutation.UserID, event, [8]byte{}, 0)}, nil
}

func TestDialogAccountAggregatePostgresBuilderRollbackAndPTSContinuity(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := createDialogAggregateUsers(t, pool, 2)
	owner, peerUser := users[0], users[1]
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: peerUser.ID}
	dialogs := NewDialogStore(pool)
	draft := domain.DialogDraft{Peer: peer, Message: "pending", Date: 10}
	wantBuilderErr := errors.New("projection failed")
	if _, err := dialogs.MutateAccountDialogs(ctx, storepkg.DialogAccountMutation{
		Kind: storepkg.DialogAccountSaveDraft, UserID: owner.ID, Date: 10, Draft: draft,
	}, func(storepkg.DialogAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		return nil, wantBuilderErr
	}); !errors.Is(err, wantBuilderErr) {
		t.Fatalf("builder error=%v, want %v", err, wantBuilderErr)
	}
	if _, found, _ := dialogs.GetDraft(ctx, owner.ID, peer, 0); found {
		t.Fatal("failed builder committed draft")
	}

	saved, err := dialogs.MutateAccountDialogs(ctx, storepkg.DialogAccountMutation{
		Kind: storepkg.DialogAccountSaveDraft, UserID: owner.ID, Date: 11, Draft: draft,
	}, postgresDialogTestEffects)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Effects) != 1 || saved.Effects[0].Event.Pts != 1 {
		t.Fatalf("save effects=%+v, want pts=1", saved.Effects)
	}
	if _, err := dialogs.MutateAccountDialogs(ctx, storepkg.DialogAccountMutation{
		Kind: storepkg.DialogAccountDeleteDraft, UserID: owner.ID, Date: 12, Peer: peer,
	}, func(snapshot storepkg.DialogAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		effects, err := postgresDialogTestEffects(snapshot)
		if len(effects) == 1 {
			effects[0].TargetUserID = peerUser.ID
		}
		return effects, err
	}); err == nil {
		t.Fatal("wrong-target builder unexpectedly committed")
	}
	if _, found, _ := dialogs.GetDraft(ctx, owner.ID, peer, 0); !found {
		t.Fatal("wrong-target builder deleted draft")
	}
	deleted, err := dialogs.MutateAccountDialogs(ctx, storepkg.DialogAccountMutation{
		Kind: storepkg.DialogAccountDeleteDraft, UserID: owner.ID, Date: 13, Peer: peer,
	}, postgresDialogTestEffects)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Effects) != 1 || deleted.Effects[0].Event.Pts != 2 {
		t.Fatalf("delete effects=%+v, want contiguous pts=2", deleted.Effects)
	}
	if max, err := NewUpdateEventStore(pool).MaxContiguousPts(ctx, owner.ID); err != nil || max != 2 {
		t.Fatalf("contiguous pts=%d err=%v, want 2", max, err)
	}
}

func TestSavedDialogAggregatePostgresExactRollbackAndPTSContinuity(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := createDialogAggregateUsers(t, pool, 2)
	owner, peerUser := users[0], users[1]
	first := domain.Peer{Type: domain.PeerTypeUser, ID: peerUser.ID}
	second := domain.Peer{Type: domain.PeerTypeChannel, ID: 9_000_000 + owner.ID}
	messages := newTestMessageStore(pool)

	mutation := storepkg.SavedDialogMutation{
		Kind: storepkg.SavedDialogSetPinned, UserID: owner.ID, Date: 21, Peer: first, Pinned: true,
	}
	wantErr := errors.New("encode saved-dialog update failed")
	if _, err := messages.MutateSavedDialogs(ctx, mutation, func(storepkg.SavedDialogMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder error=%v, want %v", err, wantErr)
	}
	if _, err := messages.MutateSavedDialogs(ctx, mutation, func(snapshot storepkg.SavedDialogMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		effects, err := postgresSavedDialogTestEffects(snapshot)
		if len(effects) == 1 {
			effects[0].TargetUserID = peerUser.ID
		}
		return effects, err
	}); err == nil {
		t.Fatal("wrong-target saved-dialog effect unexpectedly committed")
	}
	var pinRows, eventRows int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM saved_dialog_pins WHERE user_id=$1`, owner.ID).Scan(&pinRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM user_update_events WHERE user_id=$1`, owner.ID).Scan(&eventRows); err != nil {
		t.Fatal(err)
	}
	if pinRows != 0 || eventRows != 0 {
		t.Fatalf("rejected mutation leaked pins/events: pins=%d events=%d", pinRows, eventRows)
	}

	firstPin, err := messages.MutateSavedDialogs(ctx, mutation, postgresSavedDialogTestEffects)
	if err != nil {
		t.Fatal(err)
	}
	if !firstPin.Changed || len(firstPin.Effects) != 1 || firstPin.Effects[0].Event.Pts != 1 {
		t.Fatalf("first pin=%+v, want pts=1", firstPin)
	}
	noop, err := messages.MutateSavedDialogs(ctx, storepkg.SavedDialogMutation{
		Kind: storepkg.SavedDialogSetPinned, UserID: owner.ID, Date: 22, Peer: first, Pinned: true,
	}, postgresSavedDialogTestEffects)
	if err != nil || noop.Changed || len(noop.Effects) != 0 {
		t.Fatalf("no-op pin=%+v err=%v", noop, err)
	}
	secondPin, err := messages.MutateSavedDialogs(ctx, storepkg.SavedDialogMutation{
		Kind: storepkg.SavedDialogSetPinned, UserID: owner.ID, Date: 23, Peer: second, Pinned: true,
	}, postgresSavedDialogTestEffects)
	if err != nil || len(secondPin.Effects) != 1 || secondPin.Effects[0].Event.Pts != 2 {
		t.Fatalf("second pin=%+v err=%v, want pts=2", secondPin, err)
	}
	reordered, err := messages.MutateSavedDialogs(ctx, storepkg.SavedDialogMutation{
		Kind: storepkg.SavedDialogReorder, UserID: owner.ID, Date: 24,
		Order: []domain.Peer{first, second}, Force: true,
	}, postgresSavedDialogTestEffects)
	if err != nil {
		t.Fatal(err)
	}
	if !reordered.Changed || len(reordered.Effects) != 1 || reordered.Effects[0].Event.Pts != 3 ||
		len(reordered.Effects[0].Event.Peers) != 2 || reordered.Effects[0].Event.Peers[0] != first || reordered.Effects[0].Event.Peers[1] != second {
		t.Fatalf("reorder=%+v, want exact authoritative order at pts=3", reordered)
	}
	rows, err := pool.Query(ctx, `SELECT peer_type,peer_id FROM saved_dialog_pins WHERE user_id=$1 ORDER BY pinned_order`, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []domain.Peer
	for rows.Next() {
		var peerType string
		var peerID int64
		if err := rows.Scan(&peerType, &peerID); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		persisted = append(persisted, domain.Peer{Type: domain.PeerType(peerType), ID: peerID})
	}
	rows.Close()
	if len(persisted) != 2 || persisted[0] != first || persisted[1] != second {
		t.Fatalf("persisted order=%+v", persisted)
	}
	events, err := NewUpdateEventStore(pool).ListAfter(ctx, owner.ID, 0, 10)
	if err != nil || len(events) != 3 || events[0].Pts != 1 || events[1].Pts != 2 || events[2].Pts != 3 {
		t.Fatalf("saved-dialog events=%+v err=%v", events, err)
	}
	var dispatches int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id=$1 AND event_type IN ('saved_dialog_pinned','pinned_saved_dialogs')`, owner.ID).Scan(&dispatches); err != nil {
		t.Fatal(err)
	}
	if dispatches != 3 {
		t.Fatalf("saved-dialog dispatch rows=%d, want 3", dispatches)
	}
}

func TestQuickReplyAccountAggregatePostgresExactRollbackNoopAndPTS(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := createDialogAggregateUsers(t, pool, 2)
	owner, other := users[0], users[1]
	quickReplies := NewPasswordStore(pool)
	firstSave := storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountSaveText, UserID: owner.ID, Date: 31, Shortcut: "alpha",
		Message: domain.QuickReplyMessage{RandomID: 11, Date: 31, Message: "first"},
	}
	wantErr := errors.New("quick reply projection failed")
	if _, err := quickReplies.MutateQuickReplies(ctx, firstSave, func(storepkg.QuickReplyAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder error=%v, want %v", err, wantErr)
	}
	if _, err := quickReplies.MutateQuickReplies(ctx, firstSave, func(snapshot storepkg.QuickReplyAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
		effects, err := postgresQuickReplyTestEffects(snapshot)
		if len(effects) == 1 {
			effects[0].TargetUserID = other.ID
		}
		return effects, err
	}); err == nil {
		t.Fatal("wrong-target quick reply effect unexpectedly committed")
	}
	var replies, messages, events int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM quick_replies WHERE owner_user_id=$1`, owner.ID).Scan(&replies); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM quick_reply_messages WHERE owner_user_id=$1`, owner.ID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM user_update_events WHERE user_id=$1`, owner.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if replies != 0 || messages != 0 || events != 0 {
		t.Fatalf("rejected aggregate leaked rows: replies=%d messages=%d events=%d", replies, messages, events)
	}

	apply := func(mutation storepkg.QuickReplyAccountMutation) storepkg.QuickReplyAccountMutationSnapshot {
		t.Helper()
		snapshot, err := quickReplies.MutateQuickReplies(ctx, mutation, postgresQuickReplyTestEffects)
		if err != nil {
			t.Fatalf("mutate quick replies (%s): %v", mutation.Kind, err)
		}
		return snapshot
	}
	first := apply(firstSave)
	if first.Result.ShortcutID != 1 || first.Result.Message.ID != 1 || first.Effects[0].Event.Pts != 1 {
		t.Fatalf("first save=%+v", first)
	}
	noopRename := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountRenameShortcut, UserID: owner.ID, Date: 32, ShortcutID: 1, Shortcut: "alpha",
	})
	if noopRename.Changed || len(noopRename.Effects) != 0 {
		t.Fatalf("same-name rename consumed PTS: %+v", noopRename)
	}
	renamed := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountRenameShortcut, UserID: owner.ID, Date: 33, ShortcutID: 1, Shortcut: "renamed",
	})
	if renamed.Effects[0].Event.Pts != 2 || renamed.Effects[0].Event.Type != domain.UpdateEventQuickReplies {
		t.Fatalf("rename=%+v", renamed)
	}
	second := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountSaveText, UserID: owner.ID, Date: 34, Shortcut: "beta",
		Message: domain.QuickReplyMessage{RandomID: 12, Date: 34, Message: "second"},
	})
	if second.Result.ShortcutID != 2 || second.Result.Message.ID != 2 || second.Effects[0].Event.Pts != 3 {
		t.Fatalf("second save=%+v", second)
	}
	reordered := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountReorder, UserID: owner.ID, Date: 35, Order: []int{2, 1},
	})
	if reordered.Effects[0].Event.Pts != 4 || len(reordered.Result.List.QuickReplies) != 2 ||
		reordered.Result.List.QuickReplies[0].ID != 2 || reordered.Result.List.QuickReplies[1].ID != 1 {
		t.Fatalf("reorder=%+v", reordered)
	}
	noopReorder := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountReorder, UserID: owner.ID, Date: 36, Order: []int{2, 1},
	})
	if noopReorder.Changed || len(noopReorder.Effects) != 0 {
		t.Fatalf("same order consumed PTS: %+v", noopReorder)
	}
	_, err := quickReplies.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountDeleteMessages, UserID: owner.ID, Date: 37,
		ShortcutID: 1, MessageIDs: []int{1, 999},
	}, postgresQuickReplyTestEffects)
	if !errors.Is(err, domain.ErrShortcutInvalid) {
		t.Fatalf("mixed valid/invalid delete err=%v", err)
	}
	if got, err := quickReplies.GetQuickReplyMessages(ctx, owner.ID, 1, nil); err != nil || len(got.Messages) != 1 || got.Messages[0].ID != 1 {
		t.Fatalf("failed delete partially removed message: %+v err=%v", got, err)
	}
	deletedMessages := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountDeleteMessages, UserID: owner.ID, Date: 38,
		ShortcutID: 1, MessageIDs: []int{1},
	})
	if deletedMessages.Effects[0].Event.Pts != 5 || deletedMessages.Effects[0].Event.Type != domain.UpdateEventDeleteQuickReplyMessages {
		t.Fatalf("delete messages=%+v", deletedMessages)
	}
	deletedShortcut := apply(storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountDeleteShortcut, UserID: owner.ID, Date: 39, ShortcutID: 2,
	})
	if deletedShortcut.Effects[0].Event.Pts != 6 || deletedShortcut.Effects[0].Event.Type != domain.UpdateEventDeleteQuickReply {
		t.Fatalf("delete shortcut=%+v", deletedShortcut)
	}

	durable, err := NewUpdateEventStore(pool).ListAfter(ctx, owner.ID, 0, 10)
	if err != nil || len(durable) != 6 {
		t.Fatalf("durable quick reply events=%+v err=%v", durable, err)
	}
	for i, event := range durable {
		if event.Pts != i+1 || event.PtsCount != 1 {
			t.Fatalf("event[%d]=%+v", i, event)
		}
	}
	if durable[0].QuickReply.ID != 1 || durable[0].QuickReplyMessage.ID != 1 || durable[1].QuickReplies[0].Shortcut != "renamed" {
		t.Fatalf("hydrated quick reply evidence is not exact: %+v", durable)
	}
	var dispatches int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id=$1 AND event_type IN ('quick_replies','new_quick_reply','delete_quick_reply','quick_reply_message','delete_quick_reply_messages')`, owner.ID).Scan(&dispatches); err != nil {
		t.Fatal(err)
	}
	if dispatches != 6 {
		t.Fatalf("quick reply dispatches=%d, want 6", dispatches)
	}
}

func TestPrivateSendClearDraftPostgresRollsBackMessageAndDraftTogether(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := createDialogAggregateUsers(t, pool, 2)
	owner, recipient := users[0], users[1]
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID}
	dialogs := NewDialogStore(pool)
	if err := dialogs.SaveDraft(ctx, owner.ID, domain.DialogDraft{Peer: peer, Message: "pending", Date: 10}); err != nil {
		t.Fatal(err)
	}

	fn := pgx.Identifier{fmt.Sprintf("test_fail_draft_event_%d", owner.ID)}.Sanitize()
	trigger := pgx.Identifier{fmt.Sprintf("test_fail_draft_event_trigger_%d", owner.ID)}.Sanitize()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION `+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced draft event failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON user_update_events
FOR EACH ROW WHEN (NEW.event_type = 'draft_message' AND NEW.user_id = %d) EXECUTE FUNCTION %s()`, trigger, owner.ID, fn)); err != nil {
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS `+fn+`() CASCADE`)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON user_update_events`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS `+fn+`() CASCADE`)
	})

	randomID := time.Now().UnixNano()
	_, err := newTestMessageStore(pool).SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: owner.ID, RecipientUserID: recipient.ID, RandomID: randomID,
		Message: "must roll back", Date: 20, ClearDraft: true,
	})
	if err == nil {
		t.Fatal("forced draft event failure unexpectedly committed")
	}
	if _, found, getErr := dialogs.GetDraft(ctx, owner.ID, peer, 0); getErr != nil || !found {
		t.Fatalf("rolled-back send lost draft: found=%v err=%v", found, getErr)
	}
	var messages, events int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM private_messages WHERE sender_user_id=$1 AND random_id=$2`, owner.ID, randomID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM user_update_events WHERE user_id=$1`, owner.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if messages != 0 || events != 0 {
		t.Fatalf("rollback leaked message/events: messages=%d events=%d", messages, events)
	}
}

func TestChannelAndScheduledClearDraftPostgresUseAccountPTSDomain(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner := createDialogAggregateUsers(t, pool, 1)[0]
	dialogs := NewDialogStore(pool)
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "account draft domain", Megagroup: true, Date: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	channelPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: created.Channel.ID}
	if err := dialogs.SaveDraft(ctx, owner.ID, domain.DialogDraft{Peer: channelPeer, Message: "channel pending", Date: 11}); err != nil {
		t.Fatal(err)
	}
	channelSend, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: owner.ID, ChannelID: created.Channel.ID, RandomID: time.Now().UnixNano(),
		Message: "channel sent", Date: 12, ClearDraft: true, SkipRecipientLookup: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if channelSend.DraftEvent.Pts != 1 || channelSend.DraftEvent.Type != domain.UpdateEventDraftMessage || channelSend.DraftEvent.Peer != channelPeer {
		t.Fatalf("channel draft event=%+v, want account pts=1", channelSend.DraftEvent)
	}
	if _, found, _ := dialogs.GetDraft(ctx, owner.ID, channelPeer, 0); found {
		t.Fatal("channel send left draft behind")
	}

	privatePeer := domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}
	if err := dialogs.SaveDraft(ctx, owner.ID, domain.DialogDraft{Peer: privatePeer, Message: "scheduled pending", Date: 13}); err != nil {
		t.Fatal(err)
	}
	messages := newTestMessageStore(pool)
	_, err = messages.CreateScheduledMessage(ctx, domain.ScheduleMessageRequest{
		OwnerUserID: owner.ID, Peer: privatePeer, RandomID: time.Now().UnixNano(), Message: "later",
		ScheduleDate: 1_800_000_000, Date: 14, ClearDraft: true,
	}, func(msg domain.ScheduledMessage) ([]storepkg.DeliveryEffect, error) {
		return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
			TargetUserID: msg.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
		})}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := dialogs.GetDraft(ctx, owner.ID, privatePeer, 0); found {
		t.Fatal("scheduled-message aggregate left draft behind")
	}
	events, err := NewUpdateEventStore(pool).ListAfter(ctx, owner.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Pts != 1 || events[1].Pts != 2 || events[0].Type != domain.UpdateEventDraftMessage || events[1].Type != domain.UpdateEventDraftMessage {
		t.Fatalf("account draft events=%+v, want contiguous pts 1,2", events)
	}
	var dispatches int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id=$1 AND event_type='draft_message'`, owner.ID).Scan(&dispatches); err != nil {
		t.Fatal(err)
	}
	if dispatches != 2 {
		t.Fatalf("draft dispatch rows=%d, want 2", dispatches)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id=$1", owner.ID)
	})
}

func TestDialogAccountAndChannelSendLockOrderNoDeadlock(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	ctx, cancel := context.WithTimeout(baseCtx, 20*time.Second)
	defer cancel()
	owner := createDialogAggregateUsers(t, pool, 1)[0]
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(baseCtx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "lock order", Megagroup: true, Date: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: created.Channel.ID}
	dialogs := NewDialogStore(pool)
	if err := dialogs.SaveDraft(baseCtx, owner.ID, domain.DialogDraft{Peer: peer, Message: "pending", Date: 11}); err != nil {
		t.Fatal(err)
	}

	const rounds = 40
	var wg sync.WaitGroup
	errCh := make(chan error, rounds*2)
	start := make(chan struct{})
	// The test channel-id allocator persists counters through this same pool,
	// unlike the production Redis allocator. Keep spare pool connections while
	// still forcing owner-fence/channel-row interleaving.
	sem := make(chan struct{}, 4)
	for i := 0; i < rounds; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			sem <- struct{}{}
			defer func() { <-sem }()
			_, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
				UserID: owner.ID, ChannelID: created.Channel.ID, RandomID: int64(10_000 + i),
				Message: "concurrent send", Date: 20 + i, ClearDraft: true, SkipRecipientLookup: true,
			})
			if err != nil {
				errCh <- err
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			sem <- struct{}{}
			defer func() { <-sem }()
			_, err := dialogs.MutateAccountDialogs(ctx, storepkg.DialogAccountMutation{
				Kind: storepkg.DialogAccountSetPinned, UserID: owner.ID, Date: 20 + i,
				Peer: peer, Value: i%2 == 0, PinnedLimit: 100,
			}, postgresDialogTestEffects)
			if err != nil {
				errCh <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	var failures []error
	for err := range errCh {
		failures = append(failures, err)
	}
	if len(failures) != 0 {
		t.Fatalf("concurrent dialog/channel aggregates failed (%d): %v", len(failures), failures[0])
	}
}
