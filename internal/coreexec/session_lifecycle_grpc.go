package coreexec

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"telesrv/internal/coreexec/coreexecpb"
)

const MaxSessionOfflineBatchEvents = 1024

var (
	ErrSessionOfflineBatchEmpty    = errors.New("coreexec: session offline batch is empty")
	ErrSessionOfflineBatchTooLarge = errors.New("coreexec: session offline batch exceeds limit")
	ErrSessionOfflineEventInvalid  = errors.New("coreexec: invalid session offline event")
)

// SessionOfflineEvent is the identity-only Edge observation carried to Core.
// It intentionally contains neither TL nor domain objects.
type SessionOfflineEvent struct {
	RawAuthKeyID   [8]byte
	SessionID      int64
	UserID         int64
	LastForUser    bool
	DisconnectedAt int
}

// SessionOfflineAt completes Handler for composition-heavy tests and adapters.
// Production Edge installs its bounded lifecycle reporter as the SessionManager
// observer, so this compatibility-shaped method is not the disconnect hot
// path. A direct caller still gets an explicit error log instead of a silent
// local business fallback.
func (r *GRPCRemote) SessionOfflineAt(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool, disconnectedAt int) {
	if r == nil {
		return
	}
	if err := r.ReportSessionOfflineBatch(context.Background(), []SessionOfflineEvent{{
		RawAuthKeyID:   rawAuthKeyID,
		SessionID:      sessionID,
		UserID:         userID,
		LastForUser:    lastForUser,
		DisconnectedAt: disconnectedAt,
	}}); err != nil {
		r.log.Error("direct session offline lifecycle report failed", zap.Error(err))
	}
}

func (r *GRPCRemote) ReportSessionOfflineBatch(ctx context.Context, events []SessionOfflineEvent) error {
	if r == nil || r.client == nil {
		return ErrRemoteRPCUnavailable
	}
	req, err := sessionOfflineBatchToPB(events)
	if err != nil {
		return err
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.ReportSessionOffline(callCtx, req)
	if err != nil {
		callErr := grpcRemoteCallError("report_session_offline", err)
		observeCoreExecGRPCCall(r.metrics, "client", "report_session_offline", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	if res.GetError() != "" {
		controlErr := fmt.Errorf("coreexec grpc report_session_offline: %s", res.GetError())
		observeCoreExecGRPCCall(r.metrics, "client", "report_session_offline", start, grpcRemoteResponseErrorOutcome(controlErr))
		return controlErr
	}
	observeCoreExecGRPCCall(r.metrics, "client", "report_session_offline", start, "ok")
	return nil
}

func (s *coreExecGRPCServer) ReportSessionOffline(_ context.Context, req *coreexecpb.SessionOfflineBatchRequest) (*coreexecpb.ErrorResponse, error) {
	if s == nil || s.handler == nil {
		return nil, status.Error(codes.Internal, ErrNilHandler.Error())
	}
	events, err := sessionOfflineBatchFromPB(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Validate the complete batch before applying any lifecycle transition.
	for _, event := range events {
		s.handler.SessionOfflineAt(
			event.RawAuthKeyID,
			event.SessionID,
			event.UserID,
			event.LastForUser,
			event.DisconnectedAt,
		)
	}
	return &coreexecpb.ErrorResponse{}, nil
}

func sessionOfflineBatchToPB(events []SessionOfflineEvent) (*coreexecpb.SessionOfflineBatchRequest, error) {
	if err := validateSessionOfflineBatchSize(len(events)); err != nil {
		return nil, err
	}
	out := &coreexecpb.SessionOfflineBatchRequest{Events: make([]*coreexecpb.SessionOfflineEvent, len(events))}
	for i, event := range events {
		if err := validateSessionOfflineEvent(event); err != nil {
			return nil, fmt.Errorf("%w at index %d", err, i)
		}
		out.Events[i] = &coreexecpb.SessionOfflineEvent{
			RawAuthKeyId:   append([]byte(nil), event.RawAuthKeyID[:]...),
			SessionId:      event.SessionID,
			UserId:         event.UserID,
			LastForUser:    event.LastForUser,
			DisconnectedAt: int32(event.DisconnectedAt),
		}
	}
	return out, nil
}

func sessionOfflineBatchFromPB(req *coreexecpb.SessionOfflineBatchRequest) ([]SessionOfflineEvent, error) {
	if req == nil {
		return nil, ErrSessionOfflineBatchEmpty
	}
	items := req.GetEvents()
	if err := validateSessionOfflineBatchSize(len(items)); err != nil {
		return nil, err
	}
	out := make([]SessionOfflineEvent, len(items))
	for i, item := range items {
		if item == nil {
			return nil, fmt.Errorf("%w at index %d", ErrSessionOfflineEventInvalid, i)
		}
		rawAuthKeyID, err := authKeyIDFromBytes(item.GetRawAuthKeyId())
		if err != nil {
			return nil, fmt.Errorf("%w at index %d: raw auth key", ErrSessionOfflineEventInvalid, i)
		}
		event := SessionOfflineEvent{
			RawAuthKeyID:   rawAuthKeyID,
			SessionID:      item.GetSessionId(),
			UserID:         item.GetUserId(),
			LastForUser:    item.GetLastForUser(),
			DisconnectedAt: int(item.GetDisconnectedAt()),
		}
		if err := validateSessionOfflineEvent(event); err != nil {
			return nil, fmt.Errorf("%w at index %d", err, i)
		}
		out[i] = event
	}
	return out, nil
}

func validateSessionOfflineBatchSize(size int) error {
	switch {
	case size == 0:
		return ErrSessionOfflineBatchEmpty
	case size > MaxSessionOfflineBatchEvents:
		return ErrSessionOfflineBatchTooLarge
	default:
		return nil
	}
}

func validateSessionOfflineEvent(event SessionOfflineEvent) error {
	if event.RawAuthKeyID == ([8]byte{}) || event.SessionID == 0 || event.UserID <= 0 || event.DisconnectedAt <= 0 {
		return ErrSessionOfflineEventInvalid
	}
	return nil
}
