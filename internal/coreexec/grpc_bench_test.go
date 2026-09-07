package coreexec

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"

	"telesrv/internal/rpc"
)

func BenchmarkCoreExecHelpGetConfigLocalAdmitDispatch(b *testing.B) {
	router := newBenchmarkRouter(b)
	benchCoreExecHelpGetConfigAdmitDispatch(b, NewLocal(router))
}

func BenchmarkCoreExecHelpGetConfigGRPCBufconnAdmitDispatch(b *testing.B) {
	coreRouter := newBenchmarkRouter(b)
	edgeRouter := newBenchmarkRouter(b)
	remote, cleanup := newBufGRPCRemote(b, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()
	benchCoreExecHelpGetConfigAdmitDispatch(b, remote)
}

func benchCoreExecHelpGetConfigAdmitDispatch(b *testing.B, handler Handler) {
	b.Helper()
	wire := encodeBenchmarkHelpGetConfigWire(b)
	ctx := handler.WithLayerRPCProfileEvidenceFresh(context.Background(), false)
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	var body bin.Buffer
	var encoded bin.Buffer

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body.ResetTo(wire)
		admitted, err := handler.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
		if err != nil {
			b.Fatal(err)
		}
		if body.Len() != 0 {
			b.Fatalf("admission left %d bytes", body.Len())
		}
		result, method, err := handler.DispatchAdmitted(ctx, authKeyID, 10, int64(20+i), uint64(30+i), admitted)
		if err != nil {
			b.Fatal(err)
		}
		if method != "help.getConfig" || result == nil {
			b.Fatalf("dispatch result = method:%q result:%T", method, result)
		}
		encoded.Reset()
		if err := result.Encode(&encoded); err != nil {
			b.Fatal(err)
		}
		if encoded.Len() == 0 {
			b.Fatal("encoded result is empty")
		}
	}
}

func encodeBenchmarkHelpGetConfigWire(b *testing.B) []byte {
	b.Helper()
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		b.Fatal(err)
	}
	return body.Copy()
}

func newBenchmarkRouter(b *testing.B) *rpc.Router {
	b.Helper()
	return rpc.New(rpc.Config{
		DC:   2,
		IP:   "127.0.0.1",
		Port: 2398,
	}, rpc.Deps{}, zap.NewNop(), clock.System)
}
