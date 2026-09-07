package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestMessageStoreUnpinAllBatchBoundary(t *testing.T) {
	f := newPGBlocklistFixture(t, 1)
	ctx := context.Background()
	a, b := f.owner.ID, f.peers[0].ID
	t.Cleanup(func() {
		// Remove only messages made by this two-user fixture before its user
		// cleanup. Unbind lane heads before message cascades remove their outbox
		// rows; the peer's from_user_id also prevents deleting the users first.
		if _, err := f.pool.Exec(ctx, "DELETE FROM dispatch_outbox_lanes WHERE stream_id=ANY($1::bigint[])", []int64{a, b}); err != nil {
			t.Error(err)
		}
		if _, err := f.pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id=ANY($1::bigint[])", []int64{a, b}); err != nil {
			t.Error(err)
		}
		if _, err := f.pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id=ANY($1::bigint[]) AND recipient_user_id=ANY($1::bigint[])", []int64{a, b}); err != nil {
			t.Error(err)
		}
	})
	messages := newTestMessageStore(f.pool)
	want := map[int64]map[int]bool{a: {}, b: {}}
	for i := 0; i <= domain.MaxUnpinAllBatch; i++ {
		sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: a, RecipientUserID: b, RandomID: int64(i + 1), Message: "owned batch boundary", Date: 1700000100})
		if err != nil {
			t.Fatal(err)
		}
		for _, side := range []domain.Message{sent.SenderMessage, sent.RecipientMessage} {
			owner, peer := a, b
			if !side.Out {
				owner, peer = b, a
			}
			want[owner][side.ID] = true
			if _, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{OwnerUserID: owner, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: peer}, MessageID: side.ID, Pinned: true, PmOneside: true, Date: 1700000101}); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := map[int64]int{}
	for _, user := range []int64{a, b} {
		var pts int
		if err := f.pool.QueryRow(ctx, "SELECT contiguous_pts FROM user_update_watermarks WHERE user_id=$1", user).Scan(&pts); err != nil {
			t.Fatal(err)
		}
		before[user] = pts
	}
	req := domain.UnpinAllPrivateMessagesRequest{OwnerUserID: a, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: b}, Date: 1700000102, OriginAuthKeyID: [8]byte{1}, OriginSessionID: 7}
	for batch, count := range []int{domain.MaxUnpinAllBatch, 1} {
		result, err := messages.UnpinAllPrivateMessages(ctx, req)
		if err != nil || len(result.Updated) != 2 || result.Offset != 1-batch {
			t.Fatalf("batch %d: %+v %v", batch, result, err)
		}
		for _, side := range result.Updated {
			if len(side.MessageIDs) != count || side.Pinned || side.Event.Pts != before[side.UserID]+batch+1 || side.Event.PtsCount != 1 {
				t.Fatalf("batch receipt: %+v", side)
			}
			for _, id := range side.MessageIDs {
				if !want[side.UserID][id] {
					t.Fatal("foreign/duplicate box ID", side.UserID, id)
				}
				delete(want[side.UserID], id)
			}
			var excluded []byte
			var session int64
			if err := f.pool.QueryRow(ctx, "SELECT exclude_auth_key_id,exclude_session_id FROM dispatch_outbox WHERE target_user_id=$1 AND pts=$2", side.UserID, side.Event.Pts).Scan(&excluded, &session); err != nil {
				t.Fatal(err)
			}
			if len(excluded) != 8 || (side.UserID == a && (excluded[0] != 1 || session != 7)) || (side.UserID == b && (excluded[0] != 0 || session != 0)) {
				t.Fatal("unpin batch origin exclusion")
			}
		}
		var left int
		if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM message_boxes WHERE owner_user_id=ANY($1::bigint[]) AND pinned", []int64{a, b}).Scan(&left); err != nil || left != 2*(1-batch) {
			t.Fatal("remaining batch", left, err)
		}
	}
	result, err := messages.UnpinAllPrivateMessages(ctx, req)
	if err != nil || result.Changed() || result.Offset != 0 || len(want[a])+len(want[b]) != 0 {
		t.Fatal("empty batch", result, err)
	}
	for _, user := range []int64{a, b} {
		events, err := NewUpdateEventStore(f.pool).ListAfter(ctx, user, before[user], 10)
		if err != nil || len(events) != 2 {
			t.Fatal("batch/noop PTS", user, len(events), err)
		}
	}
}
