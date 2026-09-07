package coreexec

import (
	"context"
	"errors"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tlprofile"

	"telesrv/internal/mtprotoedge"
	"telesrv/internal/postresponse"
)

var ErrNilHandler = errors.New("coreexec: nil handler")

// Handler is the Core RPC execution contract exposed by the CoreExec service.
//
// Every capability in this contract is mandatory. A wrapper that hid any of
// them would silently downgrade replay, profile evidence, destroy-auth-key
// cleanup, or post-response durability.
type Handler interface {
	mtprotoedge.LayerRPCHandler
	mtprotoedge.LayerRPCOptionsAdmitter
	mtprotoedge.LayerRPCDefaultProfileAdmitter
	mtprotoedge.LayerRPCDefaultProfileOptionsAdmitter
	mtprotoedge.LayerRPCFlatBytesPayloadSizer
	mtprotoedge.LayerRPCSessionProfileRegistry
	mtprotoedge.LayerRPCOrderedSessionProfileRegistry
	mtprotoedge.LayerRPCDurableSessionProfileResolver
	mtprotoedge.LayerRPCInheritedAuthKeyProfileResolver
	mtprotoedge.LayerRPCDurableSessionProfileAdvancer
	mtprotoedge.LayerRPCDurableSessionProfileDeleter
	mtprotoedge.LayerRPCReplayPreparer
	mtprotoedge.LayerRPCProfileEvidenceContext
	mtprotoedge.LayerRPCIdentityHintContext
	mtprotoedge.LayerRPCAdmissionProfilePublisher
	mtprotoedge.RPCInitConnectionObserver
	postresponse.ActionExecutor
	SessionOfflineAt(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool, disconnectedAt int)
}

// Local adapts a Core router to the CoreExec server contract. Edge processes use
// the gRPC client; this adapter is only for Core-side server wiring and tests.
type Local struct {
	handler Handler
}

func NewLocal(handler Handler) *Local {
	return &Local{handler: handler}
}

func (l *Local) AdmitLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitLayer(profile, b, limits)
}

func (l *Local) AdmitLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitLayerWithOptions(profile, b, options)
}

func (l *Local) AdmitDefaultLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitDefaultLayer(profile, b, limits)
}

func (l *Local) AdmitDefaultLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitDefaultLayerWithOptions(profile, b, options)
}

func (l *Local) AdmitUnprofiled(b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitUnprofiled(b, limits)
}

func (l *Local) AdmitUnprofiledWithOptions(b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if l == nil || l.handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	return l.handler.AdmitUnprofiledWithOptions(b, options)
}

func (l *Local) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	if l == nil || l.handler == nil {
		return nil, "", ErrNilHandler
	}
	return l.handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (l *Local) LayerRPCFlatBytesPayloadSize(wire []byte) (int, bool) {
	if l == nil || l.handler == nil {
		return 0, false
	}
	return l.handler.LayerRPCFlatBytesPayloadSize(wire)
}

func (l *Local) NegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) (int, bool) {
	if l == nil || l.handler == nil {
		return 0, false
	}
	return l.handler.NegotiatedSessionLayer(authKeyID, sessionID)
}

func (l *Local) NegotiatedSessionLayerEvidence(authKeyID [8]byte, sessionID int64) (layer int, msgID int64, ok bool) {
	if l == nil || l.handler == nil {
		return 0, 0, false
	}
	return l.handler.NegotiatedSessionLayerEvidence(authKeyID, sessionID)
}

func (l *Local) ResolveNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (layer int, msgID int64, found bool, err error) {
	if l == nil || l.handler == nil {
		return 0, 0, false, ErrNilHandler
	}
	return l.handler.ResolveNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
}

func (l *Local) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (layer int, found bool, err error) {
	if l == nil || l.handler == nil {
		return 0, false, ErrNilHandler
	}
	return l.handler.ResolveInheritedAuthKeyLayer(ctx, rawAuthKeyID)
}

func (l *Local) FreezeNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64, layer int) error {
	if l == nil || l.handler == nil {
		return ErrNilHandler
	}
	return l.handler.FreezeNegotiatedSessionLayer(authKeyID, sessionID, layer)
}

func (l *Local) FreezeNegotiatedSessionLayerAt(authKeyID [8]byte, sessionID int64, layer int, msgID int64) (bool, error) {
	if l == nil || l.handler == nil {
		return false, ErrNilHandler
	}
	return l.handler.FreezeNegotiatedSessionLayerAt(authKeyID, sessionID, layer, msgID)
}

func (l *Local) ForgetNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) {
	if l == nil || l.handler == nil {
		return
	}
	l.handler.ForgetNegotiatedSessionLayer(authKeyID, sessionID)
}

func (l *Local) ForgetNegotiatedAuthKey(authKeyID [8]byte) {
	if l == nil || l.handler == nil {
		return
	}
	l.handler.ForgetNegotiatedAuthKey(authKeyID)
}

func (l *Local) AdvanceNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, layer int, msgID int64) (currentLayer int, currentMsgID int64, publishShared bool, err error) {
	if l == nil || l.handler == nil {
		return 0, 0, false, ErrNilHandler
	}
	return l.handler.AdvanceNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID, layer, msgID)
}

func (l *Local) DeleteNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (deleted bool, err error) {
	if l == nil || l.handler == nil {
		return false, ErrNilHandler
	}
	return l.handler.DeleteNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
}

func (l *Local) PrepareAdmittedReplay(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (func() error, error) {
	if l == nil || l.handler == nil {
		return nil, ErrNilHandler
	}
	return l.handler.PrepareAdmittedReplay(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (l *Local) WithLayerRPCProfileEvidenceFresh(ctx context.Context, fresh bool) context.Context {
	if l == nil || l.handler == nil {
		return ctx
	}
	return l.handler.WithLayerRPCProfileEvidenceFresh(ctx, fresh)
}

func (l *Local) PublishAdmittedLayerProfileEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, safeFloor uint64, layer int) error {
	if l == nil || l.handler == nil {
		return ErrNilHandler
	}
	return l.handler.PublishAdmittedLayerProfileEvidence(ctx, rawAuthKeyID, sessionID, msgID, admissionSeq, safeFloor, layer)
}

func (l *Local) ObserveInitConnection(ctx context.Context, authKeyID [8]byte, sessionID int64, layer, apiID int, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode string) error {
	if l == nil || l.handler == nil {
		return ErrNilHandler
	}
	return l.handler.ObserveInitConnection(ctx, authKeyID, sessionID, layer, apiID, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode)
}

func (l *Local) RunPostResponseActions(ctx context.Context, actions []postresponse.Action) error {
	if len(actions) == 0 {
		return nil
	}
	if l == nil || l.handler == nil {
		return ErrNilHandler
	}
	return l.handler.RunPostResponseActions(ctx, actions)
}

func (l *Local) SessionOfflineAt(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool, disconnectedAt int) {
	if l == nil || l.handler == nil {
		return
	}
	l.handler.SessionOfflineAt(rawAuthKeyID, sessionID, userID, lastForUser, disconnectedAt)
}
