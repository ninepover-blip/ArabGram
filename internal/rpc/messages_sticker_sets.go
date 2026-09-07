package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (r *Router) onMessagesInstallStickerSet(ctx context.Context, req *tg.MessagesInstallStickerSetRequest) (tg.MessagesStickerSetInstallResultClass, error) {
	if req == nil {
		return nil, stickersetInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	set, _, err := r.resolveInstallableStickerSet(ctx, req.Stickerset)
	if err != nil {
		return nil, err
	}
	kind := userStickerSetKind(set)
	if err := r.mutateUserStickerSets(ctx, domain.UserStickerSetMutation{
		OwnerUserID: userID, Mutation: domain.UserStickerSetMutationInstall,
		Items:    []domain.UserStickerSetMutationItem{{StickerSetID: set.ID, Kind: kind}},
		Archived: req.Archived, Date: int(r.clock.Now().Unix()),
	}); err != nil {
		return nil, err
	}
	return &tg.MessagesStickerSetInstallResultSuccess{}, nil
}

func (r *Router) onMessagesUninstallStickerSet(ctx context.Context, input tg.InputStickerSetClass) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	set, _, err := r.resolveInstallableStickerSet(ctx, input)
	if err != nil {
		return false, err
	}
	if err := r.mutateUserStickerSets(ctx, domain.UserStickerSetMutation{
		OwnerUserID: userID, Mutation: domain.UserStickerSetMutationUninstall,
		Items: []domain.UserStickerSetMutationItem{{StickerSetID: set.ID, Kind: userStickerSetKind(set)}},
		Date:  int(r.clock.Now().Unix()),
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesReorderStickerSets(ctx context.Context, req *tg.MessagesReorderStickerSetsRequest) (bool, error) {
	if req == nil {
		return false, inputRequestInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	kind := stickerSetKindFromFlags(req.Masks, req.Emojis)
	if len(req.Order) > domain.MaxInstalledStickerSets {
		return false, limitInvalidErr()
	}
	if len(req.Order) == 0 {
		return true, nil
	}
	order := append([]int64(nil), req.Order...)
	if !validUniqueStickerSetIDs(order) {
		return false, inputRequestInvalidErr()
	}
	if err := r.mutateUserStickerSets(ctx, domain.UserStickerSetMutation{
		OwnerUserID: userID, Mutation: domain.UserStickerSetMutationReorder,
		Kind: kind, Order: order, Date: int(r.clock.Now().Unix()),
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesToggleStickerSets(ctx context.Context, req *tg.MessagesToggleStickerSetsRequest) (bool, error) {
	if req == nil {
		return false, inputRequestInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if len(req.Stickersets) > domain.MaxInstalledStickerSets {
		return false, limitInvalidErr()
	}
	modeFlags := 0
	for _, enabled := range []bool{req.Uninstall, req.Archive, req.Unarchive} {
		if enabled {
			modeFlags++
		}
	}
	if modeFlags > 1 {
		return false, inputRequestInvalidErr()
	}
	items := make([]domain.UserStickerSetMutationItem, 0, len(req.Stickersets))
	seen := make(map[int64]struct{}, len(req.Stickersets))
	now := int(r.clock.Now().Unix())
	for _, input := range req.Stickersets {
		set, _, err := r.resolveInstallableStickerSet(ctx, input)
		if err != nil {
			return false, err
		}
		kind := userStickerSetKind(set)
		if _, duplicate := seen[set.ID]; duplicate {
			return false, inputRequestInvalidErr()
		}
		seen[set.ID] = struct{}{}
		items = append(items, domain.UserStickerSetMutationItem{StickerSetID: set.ID, Kind: kind})
	}
	if len(items) == 0 {
		return true, nil
	}
	mutationKind := domain.UserStickerSetMutationInstall
	switch {
	case req.Uninstall:
		mutationKind = domain.UserStickerSetMutationUninstall
	case req.Archive:
		mutationKind = domain.UserStickerSetMutationArchive
	case req.Unarchive:
		mutationKind = domain.UserStickerSetMutationUnarchive
	}
	if err := r.mutateUserStickerSets(ctx, domain.UserStickerSetMutation{
		OwnerUserID: userID, Mutation: mutationKind, Items: items, Date: now,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesGetMyStickers(ctx context.Context, req *tg.MessagesGetMyStickersRequest) (*tg.MessagesMyStickers, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if r.deps.Files == nil {
		return nil, internalErr()
	}
	limit := 50
	var offsetID int64
	if req != nil {
		if req.OffsetID < 0 {
			return nil, offsetInvalidErr()
		}
		if req.Limit < 0 || req.Limit > domain.MaxCreatedStickerSets {
			return nil, limitInvalidErr()
		}
		if req.Limit > 0 {
			limit = req.Limit
		}
		offsetID = req.OffsetID
	}
	sets, total, err := r.deps.Files.ListCreatedStickerSets(ctx, userID, offsetID, limit)
	if err != nil {
		return nil, internalErr()
	}
	covered := make([]tg.StickerSetCoveredClass, 0, len(sets))
	for _, set := range sets {
		set.Creator = true
		covered = append(covered, &tg.StickerSetNoCovered{Set: tgStickerSet(set)})
	}
	return &tg.MessagesMyStickers{Count: total, Sets: covered}, nil
}

func (r *Router) onMessagesGetArchivedStickers(ctx context.Context, req *tg.MessagesGetArchivedStickersRequest) (*tg.MessagesArchivedStickers, error) {
	if req == nil || r.deps.Account == nil || r.deps.Files == nil {
		return nil, internalErr()
	}
	if req.Masks && req.Emojis {
		return nil, inputRequestInvalidErr()
	}
	if req.OffsetID < 0 {
		return nil, offsetInvalidErr()
	}
	limit := req.Limit
	if limit < 0 || limit > domain.MaxInstalledStickerSets {
		return nil, limitInvalidErr()
	}
	if limit == 0 {
		limit = 50
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	kind := stickerSetKindFromFlags(req.Masks, req.Emojis)
	archived := true
	items, total, err := r.deps.Account.ListUserStickerSets(ctx, userID, kind, &archived, req.OffsetID, limit)
	if err != nil {
		return nil, internalErr()
	}
	covered := make([]tg.StickerSetCoveredClass, 0, len(items))
	for _, item := range items {
		set, _, found, err := r.deps.Files.ResolveStickerSet(ctx, domain.StickerSetRef{Kind: domain.StickerSetRefByID, ID: item.StickerSetID})
		if err != nil || !found || set.ID == 0 || set.Deleted || userStickerSetKind(set) != kind {
			return nil, internalErr()
		}
		set = stickerSetWithViewerInstallItem(stickerSetWithoutViewerInstallState(set), item)
		covered = append(covered, &tg.StickerSetNoCovered{Set: tgStickerSet(set)})
	}
	return &tg.MessagesArchivedStickers{Count: total, Sets: covered}, nil
}

func (r *Router) resolveInstallableStickerSet(ctx context.Context, input tg.InputStickerSetClass) (domain.StickerSet, []domain.Document, error) {
	ref, ok := stickerSetRefFromInput(input)
	if !ok || (ref.Kind != domain.StickerSetRefByID && ref.Kind != domain.StickerSetRefByShortName) {
		return domain.StickerSet{}, nil, stickersetInvalidErr()
	}
	if r.deps.Files == nil {
		return domain.StickerSet{}, nil, stickersetInvalidErr()
	}
	set, docs, found, err := r.deps.Files.ResolveStickerSet(ctx, ref)
	if err != nil {
		return domain.StickerSet{}, nil, internalErr()
	}
	if !found || set.ID == 0 {
		return domain.StickerSet{}, nil, stickersetInvalidErr()
	}
	if ref.Kind == domain.StickerSetRefByID && set.AccessHash != ref.AccessHash {
		return domain.StickerSet{}, nil, stickersetInvalidErr()
	}
	return set, docs, nil
}

func userStickerSetKind(set domain.StickerSet) domain.StickerSetKind {
	switch {
	case set.Kind == domain.StickerSetKindMasks || set.Masks:
		return domain.StickerSetKindMasks
	case set.Kind == domain.StickerSetKindEmoji || set.Emojis:
		return domain.StickerSetKindEmoji
	default:
		return domain.StickerSetKindStickers
	}
}

func stickerSetKindFromFlags(masks, emojis bool) domain.StickerSetKind {
	switch {
	case masks:
		return domain.StickerSetKindMasks
	case emojis:
		return domain.StickerSetKindEmoji
	default:
		return domain.StickerSetKindStickers
	}
}

func validUniqueStickerSetIDs(in []int64) bool {
	seen := make(map[int64]struct{}, len(in))
	for _, v := range in {
		if v <= 0 {
			return false
		}
		if _, ok := seen[v]; ok {
			return false
		}
		seen[v] = struct{}{}
	}
	return true
}

func stickerSetsUpdate(kind domain.StickerSetKind) *tg.UpdateStickerSets {
	update := &tg.UpdateStickerSets{}
	switch kind {
	case domain.StickerSetKindMasks:
		update.SetMasks(true)
	case domain.StickerSetKindEmoji:
		update.SetEmojis(true)
	}
	return update
}

func stickerSetsOrderUpdate(kind domain.StickerSetKind, order []int64) *tg.UpdateStickerSetsOrder {
	update := &tg.UpdateStickerSetsOrder{Order: append([]int64(nil), order...)}
	switch kind {
	case domain.StickerSetKindMasks:
		update.SetMasks(true)
	case domain.StickerSetKindEmoji:
		update.SetEmojis(true)
	}
	return update
}

func userStickerSetMutationKinds(mutation domain.UserStickerSetMutation) []domain.StickerSetKind {
	if mutation.Mutation == domain.UserStickerSetMutationReorder {
		return []domain.StickerSetKind{mutation.Kind}
	}
	out := make([]domain.StickerSetKind, 0, len(mutation.Items))
	seen := make(map[domain.StickerSetKind]struct{}, len(mutation.Items))
	for _, item := range mutation.Items {
		if _, exists := seen[item.Kind]; exists {
			continue
		}
		seen[item.Kind] = struct{}{}
		out = append(out, item.Kind)
	}
	return out
}

func (r *Router) stickerSetDeliveryEffects(ctx context.Context, userID int64) store.DeliveryEffectsBuilder[domain.UserStickerSetMutation] {
	excludeAuthKeyID, excludeSessionID := deliveryExclusionFromContext(ctx)
	return func(snapshot domain.UserStickerSetMutation) ([]store.DeliveryEffect, error) {
		updates := make([]tg.UpdateClass, 0, 3)
		if snapshot.Mutation == domain.UserStickerSetMutationReorder {
			updates = append(updates, stickerSetsOrderUpdate(snapshot.Kind, snapshot.Order))
		} else {
			for _, kind := range userStickerSetMutationKinds(snapshot) {
				updates = append(updates, stickerSetsUpdate(kind))
			}
		}
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: updates, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}, Date: int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, err
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: userID, ExcludeAuthKeyID: excludeAuthKeyID, ExcludeSessionID: excludeSessionID,
			Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}

func (r *Router) stickerSetMutationDeliveryEffects(ctx context.Context, userID int64) store.DeliveryEffectsBuilder[store.StickerSetMutation] {
	excludeAuthKeyID, excludeSessionID := deliveryExclusionFromContext(ctx)
	return func(snapshot store.StickerSetMutation) ([]store.DeliveryEffect, error) {
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{stickerSetsUpdate(userStickerSetKind(snapshot.Set))},
			Users:   []tg.UserClass{}, Chats: []tg.ChatClass{}, Date: int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, err
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: userID, ExcludeAuthKeyID: excludeAuthKeyID, ExcludeSessionID: excludeSessionID,
			Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}

func (r *Router) mutateUserStickerSets(ctx context.Context, mutation domain.UserStickerSetMutation) error {
	if r.deps.Account == nil {
		return internalErr()
	}
	if err := r.requireAccountDelivery(mutation.OwnerUserID, "messages.userStickerSetMutation"); err != nil {
		return err
	}
	if err := r.deps.Account.MutateUserStickerSets(ctx, mutation, r.stickerSetDeliveryEffects(ctx, mutation.OwnerUserID)); err != nil {
		return internalErr()
	}
	for _, kind := range userStickerSetMutationKinds(mutation) {
		r.invalidateStickerCatalog(kind)
	}
	return nil
}
