package coreexec

import (
	"context"
	"sync"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/rpc"
)

func TestGRPCRemoteReportsSessionOfflineBatchWithOriginalTimestamp(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	capture := &captureSessionOfflineHandler{Handler: coreRouter}
	remote, cleanup := newBufGRPCRemote(t, capture, edgeRouter, "secret", "secret")
	defer cleanup()

	events := []SessionOfflineEvent{
		{RawAuthKeyID: [8]byte{1}, SessionID: 101, UserID: 1001, DisconnectedAt: 1770000001},
		{RawAuthKeyID: [8]byte{2}, SessionID: 202, UserID: 2002, LastForUser: true, DisconnectedAt: 1770000002},
	}
	if err := remote.ReportSessionOfflineBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if got := capture.snapshot(); len(got) != len(events) || got[0] != events[0] || got[1] != events[1] {
		t.Fatalf("reported events = %+v, want %+v", got, events)
	}
}

func TestSessionOfflineBatchServerValidatesWholeBatchBeforeMutation(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	capture := &captureSessionOfflineHandler{Handler: coreRouter}
	server := newCoreExecGRPCServer(capture)
	_, err := server.ReportSessionOffline(context.Background(), &coreexecpb.SessionOfflineBatchRequest{Events: []*coreexecpb.SessionOfflineEvent{
		{RawAuthKeyId: []byte{1, 0, 0, 0, 0, 0, 0, 0}, SessionId: 1, UserId: 2, DisconnectedAt: 3},
		{RawAuthKeyId: []byte{2}, SessionId: 4, UserId: 5, DisconnectedAt: 6},
	}})
	if err == nil {
		t.Fatal("invalid batch was accepted")
	}
	if got := capture.snapshot(); len(got) != 0 {
		t.Fatalf("partial batch applied: %+v", got)
	}
}

func TestSessionOfflineBatchClientRejectsUnboundedOrInvalidInput(t *testing.T) {
	if _, err := sessionOfflineBatchToPB(nil); err != ErrSessionOfflineBatchEmpty {
		t.Fatalf("empty error = %v", err)
	}
	tooMany := make([]SessionOfflineEvent, MaxSessionOfflineBatchEvents+1)
	if _, err := sessionOfflineBatchToPB(tooMany); err != ErrSessionOfflineBatchTooLarge {
		t.Fatalf("oversize error = %v", err)
	}
	if _, err := sessionOfflineBatchToPB([]SessionOfflineEvent{{RawAuthKeyID: [8]byte{1}}}); err == nil {
		t.Fatal("invalid event was accepted")
	}
}

type captureSessionOfflineHandler struct {
	Handler
	mu     sync.Mutex
	events []SessionOfflineEvent
}

func (h *captureSessionOfflineHandler) SessionOfflineAt(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool, disconnectedAt int) {
	h.mu.Lock()
	h.events = append(h.events, SessionOfflineEvent{
		RawAuthKeyID:   rawAuthKeyID,
		SessionID:      sessionID,
		UserID:         userID,
		LastForUser:    lastForUser,
		DisconnectedAt: disconnectedAt,
	})
	h.mu.Unlock()
}

func (h *captureSessionOfflineHandler) snapshot() []SessionOfflineEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]SessionOfflineEvent(nil), h.events...)
}
