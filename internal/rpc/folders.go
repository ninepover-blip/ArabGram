package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"

	"github.com/iamxvbaba/td/tlprofile"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (r *Router) registerFolders(d *tlprofile.Dispatcher) {
	registerRPC[*tg.FoldersEditPeerFoldersRequest](d, tlprofile.SemanticMethodFoldersEditPeerFolders, func(ctx context.Context, layerRequest *tg.FoldersEditPeerFoldersRequest) (any, error) {
		return r.onFoldersEditPeerFolders(ctx, layerRequest.
			FolderPeers)
	})

}

func (r *Router) onFoldersEditPeerFolders(ctx context.Context, folderPeers []tg.InputFolderPeer) (tg.UpdatesClass, error) {
	if len(folderPeers) > maxDialogInputPeers {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	updates := make([]domain.FolderPeerUpdate, 0, len(folderPeers))
	seen := make(map[domain.Peer]struct{}, len(folderPeers))
	for _, item := range folderPeers {
		if item.FolderID != domain.DialogMainFolderID && item.FolderID != domain.DialogArchiveFolderID {
			return nil, folderIDInvalidErr()
		}
		peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, item.Peer)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		updates = append(updates, domain.FolderPeerUpdate{Peer: peer, FolderID: item.FolderID})
	}
	if len(updates) == 0 {
		return &tg.Updates{Date: int(r.clock.Now().Unix()), Seq: 0}, nil
	}
	if r.deps.Dialogs == nil {
		return nil, internalErr()
	}
	result, err := r.deps.Dialogs.MutateAccountDialogs(ctx, store.DialogAccountMutation{
		Kind: store.DialogAccountEditPeerFolders, UserID: userID,
		Date: int(r.clock.Now().Unix()), FolderPeers: updates,
	}, dialogAccountDeliveryEffects)
	if err != nil {
		return nil, internalErr()
	}
	event := allocatedDialogEvent(result)
	if event.Pts == 0 {
		return &tg.Updates{Date: int(r.clock.Now().Unix()), Seq: 0}, nil
	}
	out := tgUpdateForOutboxEvent(event)
	if out == nil {
		out = &tg.Updates{Date: event.Date, Seq: 0}
	}
	return out, nil
}
