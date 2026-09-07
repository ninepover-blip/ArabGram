package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"go.uber.org/zap/zaptest"

	appaccount "telesrv/internal/app/account"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func stickerCreatorRouter(t *testing.T) (*Router, *fakeFiles, *memory.DeliveryOutboxStore) {
	t.Helper()
	files := &fakeFiles{
		docs: map[int64]domain.Document{
			101: {ID: 101, AccessHash: 11, Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrSticker}}},
			102: {ID: 102, AccessHash: 12, Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrSticker}}},
			103: {ID: 103, AccessHash: 13, Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrSticker}}},
		},
		sets: map[domain.StickerSetKind][]domain.StickerSet{},
	}
	passwordStore := memory.NewPasswordStore()
	delivery := memory.NewDeliveryOutboxStore()
	passwordStore.AttachDeliveryOutbox(delivery)
	files.deliveryOutbox = delivery
	router := New(Config{}, Deps{
		Account: appaccount.NewService(passwordStore, appaccount.WithUserStickerSets(passwordStore)),
		Files:   files, DeliveryOutbox: delivery,
	}, zaptest.NewLogger(t), clock.System)
	return router, files, delivery
}

func TestStickersCreateStickerSetInstallsAndInvalidatesCatalog(t *testing.T) {
	r, _, delivery := stickerCreatorRouter(t)
	ctx := WithUserID(context.Background(), 1000000001)

	before, err := r.onMessagesGetAllStickers(ctx, 0)
	if err != nil {
		t.Fatalf("get all before create: %v", err)
	}
	if full, ok := before.(*tg.MessagesAllStickers); ok && len(full.Sets) != 0 {
		t.Fatalf("all stickers before create = %+v, want empty", full.Sets)
	}

	out, err := r.onStickersCreateStickerSet(ctx, &tg.StickersCreateStickerSetRequest{
		UserID:    &tg.InputUserSelf{},
		Title:     "Fresh Pack",
		ShortName: "fresh_pack",
		Stickers: []tg.InputStickerSetItem{{
			Document: &tg.InputDocument{ID: 101, AccessHash: 11},
			Emoji:    "🙂",
			Keywords: "fresh,happy",
		}},
	})
	if err != nil {
		t.Fatalf("create sticker set: %v", err)
	}
	full, ok := out.(*tg.MessagesStickerSet)
	if !ok {
		t.Fatalf("create result = %T, want *tg.MessagesStickerSet", out)
	}
	if full.Set.ShortName != "fresh_pack" || !full.Set.Creator || full.Set.InstalledDate == 0 {
		t.Fatalf("created set = %+v, want creator installed fresh_pack", full.Set)
	}
	if len(full.Packs) != 1 || len(full.Keywords) != 1 || len(full.Documents) != 1 {
		t.Fatalf("created payload packs=%d keywords=%d docs=%d, want 1/1/1", len(full.Packs), len(full.Keywords), len(full.Documents))
	}
	assertStickerSetsUpdate(t, lastQueuedDeliveryUpdates(t, delivery), domain.StickerSetKindStickers, nil)

	owned, err := r.onMessagesGetMyStickers(ctx, &tg.MessagesGetMyStickersRequest{Limit: 10})
	if err != nil {
		t.Fatalf("get my stickers: %v", err)
	}
	if owned.Count != 1 || len(owned.Sets) != 1 {
		t.Fatalf("my stickers = count %d sets %d, want one created set", owned.Count, len(owned.Sets))
	}

	available, err := r.onStickersCheckShortName(ctx, "fresh_pack")
	if err != nil {
		t.Fatalf("check short name: %v", err)
	}
	if available {
		t.Fatalf("fresh_pack available = true, want false after create")
	}
}

func TestStickersCreateStickerSetRejectsBadDocumentAccessHash(t *testing.T) {
	r, _, _ := stickerCreatorRouter(t)
	ctx := WithUserID(context.Background(), 1000000001)

	out, err := r.onStickersCreateStickerSet(ctx, &tg.StickersCreateStickerSetRequest{
		UserID:    &tg.InputUserSelf{},
		Title:     "Fresh Pack",
		ShortName: "fresh_pack",
		Stickers: []tg.InputStickerSetItem{{
			Document: &tg.InputDocument{ID: 101, AccessHash: 999},
			Emoji:    "🙂",
		}},
	})
	if out != nil || !tgerr.Is(err, "STICKER_FILE_INVALID") {
		t.Fatalf("create with bad document hash = %T %v, want STICKER_FILE_INVALID", out, err)
	}
}

func TestStickersCreateStickerSetQueuesAtomicOriginExcludedDelivery(t *testing.T) {
	r, _, delivery := stickerCreatorRouter(t)
	rawAuthKeyID := [8]byte{1, 3, 5, 7, 9}
	const sessionID = int64(5150)
	ctx := WithSessionID(WithRawAuthKeyID(WithUserID(context.Background(), 1000000001), rawAuthKeyID), sessionID)
	if _, err := r.onStickersCreateStickerSet(ctx, &tg.StickersCreateStickerSetRequest{
		UserID: &tg.InputUserSelf{}, Title: "Atomic Pack", ShortName: "atomic_pack",
		Stickers: []tg.InputStickerSetItem{{Document: &tg.InputDocument{ID: 101, AccessHash: 11}, Emoji: "🙂"}},
	}); err != nil {
		t.Fatalf("create sticker set: %v", err)
	}
	items := delivery.Snapshot()
	if len(items) != 1 {
		t.Fatalf("delivery items = %d, want 1", len(items))
	}
	if items[0].ExcludeAuthKeyID != rawAuthKeyID || items[0].ExcludeSessionID != sessionID {
		t.Fatalf("delivery exclusion = %x/%d", items[0].ExcludeAuthKeyID, items[0].ExcludeSessionID)
	}
	updates := requireDeliveryUpdates(t, items[0])
	if len(updates.Updates) != 1 {
		t.Fatalf("delivery updates = %d, want 1", len(updates.Updates))
	}
	if _, ok := updates.Updates[0].(*tg.UpdateStickerSets); !ok {
		t.Fatalf("delivery update = %T, want *tg.UpdateStickerSets", updates.Updates[0])
	}
}

func TestStickersSuggestAndCheckShortNameValidation(t *testing.T) {
	r, _, _ := stickerCreatorRouter(t)
	ctx := WithUserID(context.Background(), 1000000001)

	suggested, err := r.onStickersSuggestShortName(ctx, "Fresh Pack")
	if err != nil {
		t.Fatalf("suggest short name: %v", err)
	}
	if suggested.ShortName == "" {
		t.Fatalf("suggested short name empty")
	}
	if ok, err := r.onStickersCheckShortName(ctx, "bad!"); ok || !tgerr.Is(err, "SHORT_NAME_INVALID") {
		t.Fatalf("check invalid short name = %v %v, want SHORT_NAME_INVALID", ok, err)
	}
}

func TestStickersManageCreatedStickerSetRPCs(t *testing.T) {
	r, files, delivery := stickerCreatorRouter(t)
	ctx := WithUserID(context.Background(), 1000000001)

	created, err := r.onStickersCreateStickerSet(ctx, &tg.StickersCreateStickerSetRequest{
		UserID:    &tg.InputUserSelf{},
		Title:     "Fresh Pack",
		ShortName: "fresh_pack",
		Stickers: []tg.InputStickerSetItem{{
			Document: &tg.InputDocument{ID: 101, AccessHash: 11},
			Emoji:    "🙂",
		}},
	})
	if err != nil {
		t.Fatalf("create sticker set: %v", err)
	}
	full := created.(*tg.MessagesStickerSet)
	setInput := &tg.InputStickerSetID{ID: full.Set.ID, AccessHash: full.Set.AccessHash}

	added, err := r.onStickersAddStickerToSet(ctx, &tg.StickersAddStickerToSetRequest{
		Stickerset: setInput,
		Sticker: tg.InputStickerSetItem{
			Document: &tg.InputDocument{ID: 102, AccessHash: 12},
			Emoji:    "😄",
			Keywords: "smile",
		},
	})
	if err != nil {
		t.Fatalf("add sticker: %v", err)
	}
	addedFull := added.(*tg.MessagesStickerSet)
	if addedFull.Set.Count != 2 || len(addedFull.Documents) != 2 || len(addedFull.Keywords) != 1 {
		t.Fatalf("after add count=%d docs=%d keywords=%d, want 2/2/1", addedFull.Set.Count, len(addedFull.Documents), len(addedFull.Keywords))
	}
	assertStickerSetsUpdate(t, lastQueuedDeliveryUpdates(t, delivery), domain.StickerSetKindStickers, nil)

	moved, err := r.onStickersChangeStickerPosition(ctx, &tg.StickersChangeStickerPositionRequest{
		Sticker:  &tg.InputDocument{ID: 102, AccessHash: 12},
		Position: 0,
	})
	if err != nil {
		t.Fatalf("change sticker position: %v", err)
	}
	movedFull := moved.(*tg.MessagesStickerSet)
	if len(movedFull.Documents) != 2 || tgDocumentID(movedFull.Documents[0]) != 102 {
		t.Fatalf("documents after move = %+v, want doc 102 first", movedFull.Documents)
	}

	renamed, err := r.onStickersRenameStickerSet(ctx, &tg.StickersRenameStickerSetRequest{
		Stickerset: &tg.InputStickerSetShortName{ShortName: "fresh_pack"},
		Title:      "Renamed Pack",
	})
	if err != nil {
		t.Fatalf("rename sticker set: %v", err)
	}
	renamedFull := renamed.(*tg.MessagesStickerSet)
	if renamedFull.Set.Title != "Renamed Pack" {
		t.Fatalf("renamed title = %q, want Renamed Pack", renamedFull.Set.Title)
	}

	removed, err := r.onStickersRemoveStickerFromSet(ctx, &tg.InputDocument{ID: 102, AccessHash: 12})
	if err != nil {
		t.Fatalf("remove sticker: %v", err)
	}
	removedFull := removed.(*tg.MessagesStickerSet)
	if removedFull.Set.Count != 1 || len(removedFull.Documents) != 1 || tgDocumentID(removedFull.Documents[0]) != 101 {
		t.Fatalf("after remove count=%d docs=%+v, want only doc 101", removedFull.Set.Count, removedFull.Documents)
	}

	ok, err := r.onStickersDeleteStickerSet(ctx, setInput)
	if err != nil || !ok {
		t.Fatalf("delete sticker set = %v %v, want true nil", ok, err)
	}
	if _, _, found, err := files.ResolveStickerSet(ctx, domain.StickerSetRef{Kind: domain.StickerSetRefByShortName, ShortName: "fresh_pack"}); err != nil || found {
		t.Fatalf("resolve deleted set = found %v err %v, want miss", found, err)
	}
}

func TestStickersManageCreatedStickerSetRejectsNonCreator(t *testing.T) {
	r, _, _ := stickerCreatorRouter(t)
	ownerCtx := WithUserID(context.Background(), 1000000001)

	created, err := r.onStickersCreateStickerSet(ownerCtx, &tg.StickersCreateStickerSetRequest{
		UserID:    &tg.InputUserSelf{},
		Title:     "Fresh Pack",
		ShortName: "fresh_pack",
		Stickers: []tg.InputStickerSetItem{{
			Document: &tg.InputDocument{ID: 101, AccessHash: 11},
			Emoji:    "🙂",
		}},
	})
	if err != nil {
		t.Fatalf("create sticker set: %v", err)
	}
	full := created.(*tg.MessagesStickerSet)
	otherCtx := WithUserID(context.Background(), 1000000002)
	out, err := r.onStickersAddStickerToSet(otherCtx, &tg.StickersAddStickerToSetRequest{
		Stickerset: &tg.InputStickerSetID{ID: full.Set.ID, AccessHash: full.Set.AccessHash},
		Sticker: tg.InputStickerSetItem{
			Document: &tg.InputDocument{ID: 102, AccessHash: 12},
			Emoji:    "😄",
		},
	})
	if out != nil || !tgerr.Is(err, "STICKERSET_INVALID") {
		t.Fatalf("non-creator add = %T %v, want STICKERSET_INVALID", out, err)
	}
}

func tgDocumentID(doc tg.DocumentClass) int64 {
	if d, ok := doc.(*tg.Document); ok && d != nil {
		return d.ID
	}
	return 0
}
