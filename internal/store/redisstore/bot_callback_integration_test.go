package redisstore

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestRedisBotCallbackRegistryCrossInstanceCASAndBlockingWake(t *testing.T) {
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
	a, b := NewBotCallbackRegistryStore(clientA), NewBotCallbackRegistryStore(clientB)
	queryID := time.Now().UnixNano()
	defer a.DeleteBotCallbackPending(context.Background(), 1001, queryID)
	created, err := a.PutBotCallbackPending(ctx, store.BotCallbackPending{QueryID: queryID, BotUserID: 1001, UserID: 2001}, time.Second)
	if err != nil || !created {
		t.Fatalf("put created=%v err=%v", created, err)
	}
	if duplicate, err := b.PutBotCallbackPending(ctx, store.BotCallbackPending{QueryID: queryID, BotUserID: 1001, UserID: 2002}, time.Second); err != nil || duplicate {
		t.Fatalf("duplicate=%v err=%v", duplicate, err)
	}
	waitCh := make(chan struct {
		answer domain.BotCallbackAnswer
		found  bool
		err    error
	}, 1)
	go func() {
		answer, found, err := b.WaitBotCallbackAnswer(ctx, 1001, queryID)
		waitCh <- struct {
			answer domain.BotCallbackAnswer
			found  bool
			err    error
		}{answer: answer, found: found, err: err}
	}()
	answer := domain.BotCallbackAnswer{Message: "done", CacheTime: 3}
	if resolved, err := b.ResolveBotCallback(ctx, 9999, queryID, answer); err != nil || resolved {
		t.Fatalf("foreign resolve=%v err=%v", resolved, err)
	}
	if resolved, err := b.ResolveBotCallback(ctx, 1001, queryID, answer); err != nil || !resolved {
		t.Fatalf("owner resolve=%v err=%v", resolved, err)
	}
	if second, err := a.ResolveBotCallback(ctx, 1001, queryID, domain.BotCallbackAnswer{Message: "second"}); err != nil || second {
		t.Fatalf("second resolve=%v err=%v", second, err)
	}
	stored, found, err := a.GetBotCallbackAnswer(ctx, 1001, queryID)
	if err != nil || !found || stored.Message != "done" {
		t.Fatalf("stored=%#v found=%v err=%v", stored, found, err)
	}
	select {
	case got := <-waitCh:
		if got.err != nil || !got.found || got.answer.Message != "done" {
			t.Fatalf("wait answer=%#v found=%v err=%v", got.answer, got.found, got.err)
		}
	case <-ctx.Done():
		t.Fatal("missing cross-instance callback wake")
	}
	late, found, err := a.WaitBotCallbackAnswer(ctx, 1001, queryID)
	if err != nil || !found || late.Message != "done" {
		t.Fatalf("late wait answer=%#v found=%v err=%v", late, found, err)
	}
}
