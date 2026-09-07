package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestDialogFoldersRoundTrip(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 11, Phone: "+1666" + suffix + "01", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{AccessHash: 22, Phone: "+1666" + suffix + "02", FirstName: "Friend"})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	stranger, err := users.Create(ctx, domain.User{AccessHash: 33, Phone: "+1666" + suffix + "03", FirstName: "Stranger"})
	if err != nil {
		t.Fatalf("create stranger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID, stranger.ID})
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO contacts (user_id, contact_user_id, mutual)
		VALUES ($1, $2, true)
	`, owner.ID, friend.ID); err != nil {
		t.Fatalf("insert contact: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO dialogs (user_id, peer_type, peer_id, folder_id, top_message_id, top_message_date, unread_count)
		VALUES
			($1, 'user', $2, 0, 10, 1000, 0),
			($1, 'user', $3, 1, 9, 900, 1)
	`, owner.ID, friend.ID, stranger.ID); err != nil {
		t.Fatalf("insert dialogs: %v", err)
	}

	dialogs := NewDialogStore(pool)
	main, err := dialogs.ListByUser(ctx, owner.ID, domain.DialogFilter{HasFolderID: true, FolderID: domain.DialogMainFolderID, Limit: 10})
	if err != nil {
		t.Fatalf("list main folder: %v", err)
	}
	if len(main.Dialogs) != 1 || main.Dialogs[0].Peer.ID != friend.ID {
		t.Fatalf("main dialogs = %+v, want friend only", main.Dialogs)
	}
	archive, err := dialogs.ListByUser(ctx, owner.ID, domain.DialogFilter{HasFolderID: true, FolderID: domain.DialogArchiveFolderID, Limit: 10})
	if err != nil {
		t.Fatalf("list archive folder: %v", err)
	}
	if len(archive.Dialogs) != 1 || archive.Dialogs[0].Peer.ID != stranger.ID || archive.Dialogs[0].FolderID != domain.DialogArchiveFolderID {
		t.Fatalf("archive dialogs = %+v, want archived stranger", archive.Dialogs)
	}

	folder := domain.DialogFolder{
		ID:              2,
		Title:           "Work",
		Contacts:        true,
		ExcludeArchived: true,
		IncludePeers:    []domain.DialogFolderPeer{{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: stranger.ID}, AccessHash: stranger.AccessHash}},
	}
	if err := dialogs.UpsertFolder(ctx, owner.ID, folder); err != nil {
		t.Fatalf("upsert folder: %v", err)
	}
	if err := dialogs.SetFolderTagsEnabled(ctx, owner.ID, true); err != nil {
		t.Fatalf("set tags: %v", err)
	}
	folders, err := dialogs.ListFolders(ctx, owner.ID)
	if err != nil {
		t.Fatalf("list folders: %v", err)
	}
	if !folders.TagsEnabled || len(folders.Folders) != 1 || folders.Folders[0].Title != "Work" {
		t.Fatalf("folders = %+v, want tags plus work folder", folders)
	}
	custom, err := dialogs.ListByUser(ctx, owner.ID, domain.DialogFilter{HasFolderID: true, FolderID: 2, Folder: &folder, Limit: 10})
	if err != nil {
		t.Fatalf("list custom folder: %v", err)
	}
	if len(custom.Dialogs) != 1 || custom.Dialogs[0].Peer.ID != friend.ID {
		t.Fatalf("custom dialogs = %+v, want contact only because archived explicit peer is excluded", custom.Dialogs)
	}
	if err := dialogs.EditPeerFolders(ctx, owner.ID, []domain.FolderPeerUpdate{{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: stranger.ID}, FolderID: domain.DialogMainFolderID}}); err != nil {
		t.Fatalf("edit peer folders: %v", err)
	}
	archive, err = dialogs.ListByUser(ctx, owner.ID, domain.DialogFilter{HasFolderID: true, FolderID: domain.DialogArchiveFolderID, Limit: 10})
	if err != nil {
		t.Fatalf("list archive after edit: %v", err)
	}
	if len(archive.Dialogs) != 0 {
		t.Fatalf("archive after edit = %+v, want empty", archive.Dialogs)
	}
}
