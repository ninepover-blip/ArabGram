package account

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func TestBusinessProfileAndChatLinks(t *testing.T) {
	ctx := context.Background()
	const userID int64 = 1001
	store := memory.NewPasswordStore()
	svc := NewService(store, WithBusinessAutomation(store))

	profile, err := svc.UpdateBusinessWorkHours(ctx, userID, &domain.BusinessWorkHours{
		TimezoneID: "Asia/Shanghai",
		WeeklyOpen: []domain.BusinessWeeklyOpen{{
			StartMinute: 6*24*60 + 21*60,
			EndMinute:   7*24*60 + 4*60,
		}},
		OpenNow: true,
	})
	if err != nil {
		t.Fatalf("UpdateBusinessWorkHours: %v", err)
	}
	if profile.WorkHours == nil || profile.WorkHours.OpenNow {
		t.Fatalf("WorkHours = %+v, want persisted hours with OpenNow cleared", profile.WorkHours)
	}
	if _, err := svc.UpdateBusinessLocation(ctx, userID, &domain.BusinessLocation{
		Address: "No. 1 Test Road",
		Geo:     &domain.GeoPoint{Lat: 31.2, Long: 121.5},
	}); err != nil {
		t.Fatalf("UpdateBusinessLocation: %v", err)
	}
	if _, err := svc.UpdateBusinessIntro(ctx, userID, &domain.BusinessIntro{
		Title:       "Support",
		Description: "Fast replies",
	}); err != nil {
		t.Fatalf("UpdateBusinessIntro: %v", err)
	}
	got, found, err := svc.GetBusinessProfile(ctx, userID)
	if err != nil || !found {
		t.Fatalf("GetBusinessProfile found=%v err=%v", found, err)
	}
	if got.Location == nil || got.Intro == nil {
		t.Fatalf("profile = %+v, want location and intro", got)
	}

	link, err := svc.CreateBusinessChatLink(ctx, userID, domain.BusinessChatLinkInput{
		Message: "Hello from link",
		Title:   "Support link",
		Entities: []domain.MessageEntity{{
			Type:   domain.MessageEntityBold,
			Offset: 0,
			Length: 5,
		}},
	})
	if err != nil {
		t.Fatalf("CreateBusinessChatLink: %v", err)
	}
	if link.Slug == "" || link.Link == "" {
		t.Fatalf("created link = %+v, want slug/link", link)
	}
	links, err := svc.ListBusinessChatLinks(ctx, userID)
	if err != nil || len(links) != 1 {
		t.Fatalf("ListBusinessChatLinks len=%d err=%v", len(links), err)
	}
	resolved, found, err := svc.ResolveBusinessChatLink(ctx, link.Slug, true)
	if err != nil || !found || resolved.Views != 1 {
		t.Fatalf("ResolveBusinessChatLink found=%v link=%+v err=%v", found, resolved, err)
	}
	edited, err := svc.EditBusinessChatLink(ctx, userID, link.Slug, domain.BusinessChatLinkInput{
		Message: "Edited",
		Title:   "New title",
	})
	if err != nil {
		t.Fatalf("EditBusinessChatLink: %v", err)
	}
	if edited.Message != "Edited" || edited.Title != "New title" {
		t.Fatalf("edited link = %+v", edited)
	}
	deleted, err := svc.DeleteBusinessChatLink(ctx, userID, link.Slug)
	if err != nil || !deleted {
		t.Fatalf("DeleteBusinessChatLink deleted=%v err=%v", deleted, err)
	}
	if _, found, err := svc.ResolveBusinessChatLink(ctx, link.Slug, false); err != nil || found {
		t.Fatalf("Resolve deleted found=%v err=%v", found, err)
	}
}

func TestQuickRepliesLifecycle(t *testing.T) {
	ctx := context.Background()
	const userID int64 = 1002
	store := memory.NewPasswordStore()
	svc := NewService(store, WithBusinessAutomation(store))

	available, err := svc.CheckQuickReplyShortcut(ctx, userID, "hello")
	if err != nil || !available {
		t.Fatalf("CheckQuickReplyShortcut available=%v err=%v", available, err)
	}
	first, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountSaveText, UserID: userID, Date: 123, Shortcut: "hello",
		Message: domain.QuickReplyMessage{
			RandomID: 11,
			Date:     123,
			Message:  "First template",
			Entities: []domain.MessageEntity{{Type: domain.MessageEntityItalic, Offset: 0, Length: 5}},
		},
	}, accountQuickReplyTestEffects)
	if err != nil {
		t.Fatalf("SaveQuickReplyText first: %v", err)
	}
	mutation := first.Result
	if mutation.Kind != domain.QuickReplyMutationNew || mutation.ShortcutID == 0 || mutation.Message.ID == 0 {
		t.Fatalf("first mutation = %+v", mutation)
	}
	shortcutID := mutation.ShortcutID
	secondSnapshot, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountSaveText, UserID: userID, Date: 124, Shortcut: "hello",
		Message: domain.QuickReplyMessage{RandomID: 12, Date: 124, Message: "Second template"},
	}, accountQuickReplyTestEffects)
	if err != nil {
		t.Fatalf("SaveQuickReplyText second: %v", err)
	}
	second := secondSnapshot.Result
	if second.Kind != domain.QuickReplyMutationMessage || second.ShortcutID != shortcutID {
		t.Fatalf("second mutation = %+v", second)
	}
	if available, err := svc.CheckQuickReplyShortcut(ctx, userID, "hello"); err != nil || available {
		t.Fatalf("CheckQuickReplyShortcut duplicate available=%v err=%v", available, err)
	}
	list, err := svc.ListQuickReplies(ctx, userID)
	if err != nil || len(list.QuickReplies) != 1 || list.QuickReplies[0].Count != 2 || list.Hash == 0 {
		t.Fatalf("ListQuickReplies = %+v err=%v", list, err)
	}
	msgs, err := svc.GetQuickReplyMessages(ctx, userID, shortcutID, nil)
	if err != nil || msgs.Count != 2 || len(msgs.Messages) != 2 || msgs.Hash == 0 {
		t.Fatalf("GetQuickReplyMessages = %+v err=%v", msgs, err)
	}
	if _, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountRenameShortcut, UserID: userID, Date: 125, ShortcutID: shortcutID, Shortcut: "renamed",
	}, accountQuickReplyTestEffects); err != nil {
		t.Fatalf("RenameQuickReplyShortcut: %v", err)
	}
	if _, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountReorder, UserID: userID, Date: 126, Order: []int{shortcutID},
	}, accountQuickReplyTestEffects); err != nil {
		t.Fatalf("ReorderQuickReplies: %v", err)
	}
	deletedMessages, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountDeleteMessages, UserID: userID, Date: 127,
		ShortcutID: shortcutID, MessageIDs: []int{msgs.Messages[0].ID},
	}, accountQuickReplyTestEffects)
	if err != nil {
		t.Fatalf("DeleteQuickReplyMessages: %v", err)
	}
	deleteMutation := deletedMessages.Result
	if deleteMutation.Kind != domain.QuickReplyMutationIDs || len(deleteMutation.MessageIDs) != 1 {
		t.Fatalf("delete mutation = %+v", deleteMutation)
	}
	if _, err := svc.MutateQuickReplies(ctx, storepkg.QuickReplyAccountMutation{
		Kind: storepkg.QuickReplyAccountDeleteShortcut, UserID: userID, Date: 128, ShortcutID: shortcutID,
	}, accountQuickReplyTestEffects); err != nil {
		t.Fatalf("DeleteQuickReplyShortcut: %v", err)
	}
	if _, err := svc.GetQuickReplyMessages(ctx, userID, shortcutID, nil); !errors.Is(err, domain.ErrShortcutInvalid) {
		t.Fatalf("GetQuickReplyMessages deleted err = %v, want ErrShortcutInvalid", err)
	}
}

func accountQuickReplyTestEffects(snapshot storepkg.QuickReplyAccountMutationSnapshot) ([]storepkg.DeliveryEffect, error) {
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
