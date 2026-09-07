package redisstore

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestRedisInlineRegistryCrossInstanceBlockingWake(t *testing.T) {
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
	a, b := NewInlineRegistryStore(clientA), NewInlineRegistryStore(clientB)
	queryID := time.Now().UnixNano()
	defer func() {
		_ = a.DeleteInlinePending(context.Background(), queryID)
		_ = a.DeleteInlineResult(context.Background(), queryID)
	}()
	if err := a.PutInlinePending(ctx, store.InlinePending{
		QueryID:   queryID,
		BotUserID: 1001,
		UserID:    2001,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: 3001},
	}, time.Second); err != nil {
		t.Fatalf("put pending: %v", err)
	}
	if err := b.PutInlinePending(ctx, store.InlinePending{
		QueryID:   queryID,
		BotUserID: 1001,
		UserID:    2002,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: 3002},
	}, time.Second); err == nil {
		t.Fatal("duplicate pending overwrite succeeded, want error")
	}
	waitCh := make(chan struct {
		results domain.BotInlineResults
		found   bool
		err     error
	}, 1)
	go func() {
		results, found, err := b.WaitInlineResult(ctx, 2001, queryID)
		waitCh <- struct {
			results domain.BotInlineResults
			found   bool
			err     error
		}{results: results, found: found, err: err}
	}()
	want := domain.BotInlineResults{
		QueryID:   queryID,
		BotUserID: 1001,
		UserID:    2001,
		CacheTime: 60,
		Results:   []domain.BotInlineResult{{ID: "article-1", Type: "article", Message: "hello"}},
	}
	if err := a.PutInlineResult(ctx, want, time.Second); err != nil {
		t.Fatalf("put result: %v", err)
	}
	select {
	case got := <-waitCh:
		if got.err != nil || !got.found || got.results.QueryID != queryID || got.results.Results[0].Message != "hello" {
			t.Fatalf("wait results=%#v found=%v err=%v", got.results, got.found, got.err)
		}
	case <-ctx.Done():
		t.Fatal("missing cross-instance inline result wake")
	}
	late, found, err := a.WaitInlineResult(ctx, 2001, queryID)
	if err != nil || !found || late.Results[0].ID != "article-1" {
		t.Fatalf("late wait results=%#v found=%v err=%v", late, found, err)
	}
}
