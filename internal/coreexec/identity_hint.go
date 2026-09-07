package coreexec

import (
	"context"
	"fmt"

	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/mtprotoedge"
)

type identityHintKey struct{}

func (l *Local) WithLayerRPCIdentityHint(ctx context.Context, hint mtprotoedge.LayerRPCIdentityHint) context.Context {
	if l == nil || l.handler == nil {
		return ctx
	}
	return l.handler.WithLayerRPCIdentityHint(ctx, hint)
}

func (r *GRPCRemote) WithLayerRPCIdentityHint(ctx context.Context, hint mtprotoedge.LayerRPCIdentityHint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if local := r.edgeAdmitter(); local != nil {
		ctx = local.WithLayerRPCIdentityHint(ctx, hint)
	}
	return context.WithValue(ctx, identityHintKey{}, hint)
}

func identityHintFromContext(ctx context.Context) (mtprotoedge.LayerRPCIdentityHint, bool) {
	if ctx == nil {
		return mtprotoedge.LayerRPCIdentityHint{}, false
	}
	hint, ok := ctx.Value(identityHintKey{}).(mtprotoedge.LayerRPCIdentityHint)
	return hint, ok
}

func identityHintToPB(ctx context.Context) *coreexecpb.IdentityHint {
	hint, ok := identityHintFromContext(ctx)
	if !ok {
		return nil
	}
	out := &coreexecpb.IdentityHint{
		BusinessAuthKeyResolved:     hint.BusinessAuthKeyResolved,
		UserId:                      hint.UserID,
		UserIdResolved:              hint.UserIDResolved,
		RawAuthKeyExpiresAt:         int32(hint.RawAuthKeyExpiresAt),
		RawAuthKeyExpiresAtResolved: hint.RawAuthKeyExpiresAtResolved,
		SessionUpdatesReady:         hint.SessionUpdatesReady,
	}
	if hint.BusinessAuthKeyResolved {
		out.BusinessAuthKeyId = append([]byte(nil), hint.BusinessAuthKeyID[:]...)
	}
	return out
}

func identityHintFromPB(rawAuthKeyID [8]byte, sessionID int64, pb *coreexecpb.IdentityHint) (mtprotoedge.LayerRPCIdentityHint, bool, error) {
	if pb == nil {
		return mtprotoedge.LayerRPCIdentityHint{}, false, nil
	}
	hint := mtprotoedge.LayerRPCIdentityHint{
		RawAuthKeyID:                rawAuthKeyID,
		SessionID:                   sessionID,
		BusinessAuthKeyResolved:     pb.GetBusinessAuthKeyResolved(),
		UserID:                      pb.GetUserId(),
		UserIDResolved:              pb.GetUserIdResolved(),
		RawAuthKeyExpiresAt:         int(pb.GetRawAuthKeyExpiresAt()),
		RawAuthKeyExpiresAtResolved: pb.GetRawAuthKeyExpiresAtResolved(),
		SessionUpdatesReady:         pb.GetSessionUpdatesReady(),
	}
	if hint.BusinessAuthKeyResolved {
		id := pb.GetBusinessAuthKeyId()
		if len(id) != 8 {
			return mtprotoedge.LayerRPCIdentityHint{}, false, fmt.Errorf("coreexec: bad identity hint business auth key length")
		}
		copy(hint.BusinessAuthKeyID[:], id)
	}
	return hint, true, nil
}

func applyIdentityHintPB(ctx context.Context, handler Handler, rawAuthKeyID [8]byte, sessionID int64, pb *coreexecpb.IdentityHint) (context.Context, error) {
	hint, ok, err := identityHintFromPB(rawAuthKeyID, sessionID, pb)
	if err != nil || !ok || handler == nil {
		return ctx, err
	}
	decorated := handler.WithLayerRPCIdentityHint(ctx, hint)
	if decorated == nil {
		return ctx, nil
	}
	return decorated, nil
}
