package coreexec_test

import (
	"testing"

	"telesrv/internal/coreexec"
	"telesrv/internal/mtprotoedge"
	"telesrv/internal/rpc"
)

var (
	_ coreexec.Handler = (*rpc.Router)(nil)

	_ mtprotoedge.LayerRPCHandler                         = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCOptionsAdmitter                 = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCDefaultProfileAdmitter          = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCDefaultProfileOptionsAdmitter   = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCFlatBytesPayloadSizer           = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCSessionProfileRegistry          = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCOrderedSessionProfileRegistry   = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileResolver   = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCInheritedAuthKeyProfileResolver = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileAdvancer   = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileDeleter    = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCReplayPreparer                  = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCProfileEvidenceContext          = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCIdentityHintContext             = (*coreexec.Local)(nil)
	_ mtprotoedge.LayerRPCAdmissionProfilePublisher       = (*coreexec.Local)(nil)
	_ mtprotoedge.RPCInitConnectionObserver               = (*coreexec.Local)(nil)

	_ mtprotoedge.LayerRPCHandler                         = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCOptionsAdmitter                 = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCDefaultProfileAdmitter          = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCDefaultProfileOptionsAdmitter   = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCFlatBytesPayloadSizer           = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCSessionProfileRegistry          = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCOrderedSessionProfileRegistry   = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileResolver   = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCInheritedAuthKeyProfileResolver = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileAdvancer   = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCDurableSessionProfileDeleter    = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCReplayPreparer                  = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCProfileEvidenceContext          = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCIdentityHintContext             = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.LayerRPCAdmissionProfilePublisher       = (*coreexec.GRPCRemote)(nil)
	_ mtprotoedge.RPCInitConnectionObserver               = (*coreexec.GRPCRemote)(nil)
)

func TestLocalContracts(t *testing.T) {}
