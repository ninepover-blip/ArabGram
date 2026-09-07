package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"go.uber.org/zap/zaptest"
	"telesrv/internal/domain"
)

func TestPrivatePinRequiresMessageBoundary(t *testing.T) {
	r := New(Config{}, Deps{}, zaptest.NewLogger(t), clock.System)
	ctx := WithUserID(context.Background(), 1001)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 1002}
	if out, err := r.updatePrivatePinnedMessage(ctx, 1001, peer, &tg.MessagesUpdatePinnedMessageRequest{ID: 1}); !tgerr.Is(err, "NOT_IMPLEMENTED") || out != nil {
		t.Fatal("pin missing dependency", out, err)
	}
	if out, err := r.unpinAllPrivateMessages(ctx, 1001, peer); !tgerr.Is(err, "NOT_IMPLEMENTED") || out != nil {
		t.Fatal("unpin all missing dependency returned success", out, err)
	}
}
