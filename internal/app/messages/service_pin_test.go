package messages

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestServiceSharedPinChecksSendPermissionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	st := memory.NewMessageStore(memory.NewDialogStore())
	sent, err := st.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: 1001, RecipientUserID: 1002, RandomID: 77, Message: "pin target", Date: 1700000000})
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(st, nil, WithSendPermissionChecker(denySendChecker{}))
	req := domain.PinPrivateMessageRequest{OwnerUserID: 1001, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1002}, MessageID: sent.SenderMessage.ID, Pinned: true, Date: 1700000001}
	res, err := svc.PinPrivateMessage(ctx, 1001, req)
	if !errors.Is(err, domain.ErrUserFrozen) || res.Changed() {
		t.Fatalf("frozen shared pin: %+v %v", res, err)
	}
	for _, user := range []int64{1001, 1002} {
		list, err := st.ListByUser(ctx, user, domain.MessageFilter{Limit: 10})
		if err != nil || len(list.Messages) != 1 || list.Messages[0].Pinned {
			t.Fatalf("denied pin changed user %d: %+v %v", user, list, err)
		}
	}
	req.PmOneside = true
	res, err = svc.PinPrivateMessage(ctx, 1001, req)
	if err != nil || !res.Changed() || res.ServiceMessage.SenderMessage.ID != 0 {
		t.Fatalf("oneside requires no send: %+v %v", res, err)
	}
}
