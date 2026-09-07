package redisstore

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestRedisLoginTokenRegistryCrossInstanceCAS(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientA, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()
	clientB, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()
	a, b := NewLoginTokenRegistryStore(clientA), NewLoginTokenRegistryStore(clientB)
	now := time.Now()
	token := bytes.Repeat([]byte{0x4c}, 32)
	target := store.LoginTokenTarget{
		RawAuthKeyID: [8]byte{0x51},
		AuthKeyID:    [8]byte{0x52},
		SessionID:    now.UnixNano(),
	}
	record := store.LoginTokenRecord{
		Token:              token,
		Target:             target,
		Authorization:      domain.Authorization{AuthKeyID: target.AuthKeyID, DeviceModel: "QR target"},
		ExceptIDs:          []int64{1000000001},
		CreatedAtUnixMilli: now.UnixMilli(),
		ExpiresAtUnixMilli: now.Add(30 * time.Second).UnixMilli(),
	}
	defer a.DeleteLoginToken(context.Background(), token, target)

	created, err := a.PutLoginToken(ctx, record, 30*time.Second)
	if err != nil || !created {
		t.Fatalf("put login token created=%v err=%v", created, err)
	}
	if duplicate, err := b.PutLoginToken(ctx, record, 30*time.Second); err != nil || duplicate {
		t.Fatalf("duplicate put created=%v err=%v", duplicate, err)
	}
	byTarget, found, err := b.GetLoginTokenByTarget(ctx, target)
	if err != nil || !found || !bytes.Equal(byTarget.Token, token) || byTarget.Authorization.DeviceModel != "QR target" {
		t.Fatalf("by target = %+v found=%v err=%v", byTarget, found, err)
	}
	if _, status, err := b.BeginLoginTokenAccept(ctx, token, 1000000001, now); err != nil || status != store.LoginTokenAcceptAlreadyAccepted {
		t.Fatalf("except-id begin status=%s err=%v, want already accepted", status, err)
	}
	start, status, err := b.BeginLoginTokenAccept(ctx, token, 1000000002, now)
	if err != nil || status != store.LoginTokenAcceptStarted {
		t.Fatalf("begin status=%s err=%v", status, err)
	}
	if start.Target != target || start.Authorization.AuthKeyID != target.AuthKeyID {
		t.Fatalf("start = %+v, want target/auth", start)
	}
	if _, status, err := a.BeginLoginTokenAccept(ctx, token, 1000000003, now); err != nil || status != store.LoginTokenAcceptAlreadyAccepted {
		t.Fatalf("second begin status=%s err=%v, want already accepted", status, err)
	}
	accepted := start.Authorization
	accepted.UserID = 1000000002
	done, err := b.FinishLoginTokenAccept(ctx, token, accepted.UserID, accepted, now.Add(time.Second))
	if err != nil || !done {
		t.Fatalf("finish done=%v err=%v", done, err)
	}
	stored, found, err := a.GetLoginToken(ctx, token)
	if err != nil || !found || !stored.Accepted || stored.AcceptedAuthorization.UserID != accepted.UserID {
		t.Fatalf("stored after finish = %+v found=%v err=%v", stored, found, err)
	}
	if _, status, err := a.BeginLoginTokenAccept(ctx, token, 1000000002, now.Add(2*time.Second)); err != nil || status != store.LoginTokenAcceptAlreadyAccepted {
		t.Fatalf("accepted begin status=%s err=%v, want already accepted", status, err)
	}
}
