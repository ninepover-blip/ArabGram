package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestMessageStoreBidirectionalConcurrencyNoDeadlock 验证 watermark/dialog 死锁修复（advisory lock）。
//
// 背景：send/read/edit/delete 在一个事务内会按业务顺序锁住收发双方的 user_update_watermarks 与
// channel/private dialog 行。A→B 与 B→A 反向并发时，两个事务以相反顺序竞争同一对用户的这些行
// （watermark[A]→watermark[B] vs watermark[B]→watermark[A]，dialog(A,B)→dialog(B,A) vs 反向），
// 形成 AB-BA 死锁——PostgreSQL 会检测并 abort 其中一个事务（SQLSTATE 40P01），表现为操作返回错误。
//
// 修复：每个写事务在任何行锁之前，用事务级 advisory lock 按 user_id 升序锁住涉及的用户
// （lockUsersForUpdate），把同一对用户的并发写事务串行化。advisory 与行锁处于独立锁空间且升序获取，
// 既不与行锁交叉成新死锁，也消除了 watermark 与 dialog 的 AB-BA。本测试在高并发反向负载下应
// 全部成功、零错误；若死锁回归，会以 40P01 错误形式被捕获。
func TestMessageStoreBidirectionalConcurrencyNoDeadlock(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	a, err := users.Create(ctx, domain.User{AccessHash: 71, Phone: "+1997" + suffix + "01", FirstName: "BidiA"})
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	b, err := users.Create(ctx, domain.User{AccessHash: 72, Phone: "+1997" + suffix + "02", FirstName: "BidiB"})
	if err != nil {
		t.Fatalf("create b: %v", err)
	}
	ids := []int64{a.ID, b.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_watermarks WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", ids)
	})

	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	date := int(time.Now().Unix())

	var ridMu sync.Mutex
	rid := time.Now().UnixNano()
	nextRID := func() int64 {
		ridMu.Lock()
		defer ridMu.Unlock()
		rid++
		return rid
	}

	// 预热：双向各发若干条，建立双向 dialog 与未读历史，使后续 read 真正推进 watermark（命中行锁）。
	for i := 0; i < 4; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: a.ID, RecipientUserID: b.ID, RandomID: nextRID(), Message: "warmup a->b", Date: date}); err != nil {
			t.Fatalf("warmup a->b: %v", err)
		}
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: b.ID, RecipientUserID: a.ID, RandomID: nextRID(), Message: "warmup b->a", Date: date}); err != nil {
			t.Fatalf("warmup b->a: %v", err)
		}
	}

	// 反向并发：每轮同时发起 A→B send、B→A send、A 读 B、B 读 A，goroutine 一起抢同一对用户的行锁。
	const rounds = 80
	var wg sync.WaitGroup
	errCh := make(chan error, rounds*4)
	sem := make(chan struct{}, 24)

	submit := func(op func() error) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if e := op(); e != nil {
				errCh <- e
			}
		}()
	}

	for r := 0; r < rounds; r++ {
		submit(func() error {
			_, e := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: a.ID, RecipientUserID: b.ID, RandomID: nextRID(), Message: "a->b", Date: date})
			return e
		})
		submit(func() error {
			_, e := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: b.ID, RecipientUserID: a.ID, RandomID: nextRID(), Message: "b->a", Date: date})
			return e
		})
		submit(func() error {
			_, e := messages.ReadHistory(ctx, domain.ReadHistoryRequest{OwnerUserID: a.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: b.ID}, MaxID: domain.MaxMessageBoxID, Date: date})
			return e
		})
		submit(func() error {
			_, e := messages.ReadHistory(ctx, domain.ReadHistoryRequest{OwnerUserID: b.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: a.ID}, MaxID: domain.MaxMessageBoxID, Date: date})
			return e
		})
	}
	wg.Wait()
	close(errCh)

	failed := 0
	for e := range errCh {
		failed++
		if failed <= 5 {
			t.Errorf("反向并发操作失败（疑似死锁回归）: %v", e)
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d 反向并发操作失败", failed, rounds*4)
	}
}

func TestMessageStoreBidirectionalRevokeDeleteNoDeadlock(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	ctx, cancel := context.WithTimeout(baseCtx, 20*time.Second)
	defer cancel()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	a, err := users.Create(baseCtx, domain.User{AccessHash: 73, Phone: "+1997" + suffix + "11", FirstName: "DeleteA"})
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	b, err := users.Create(baseCtx, domain.User{AccessHash: 74, Phone: "+1997" + suffix + "12", FirstName: "DeleteB"})
	if err != nil {
		t.Fatalf("create b: %v", err)
	}
	ids := []int64{a.ID, b.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(baseCtx, "DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM user_update_watermarks WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM private_messages WHERE sender_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(baseCtx, "DELETE FROM users WHERE id = ANY($1::bigint[])", ids)
	})

	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	date := int(time.Now().Unix())
	type pair struct {
		senderBoxID    int
		recipientBoxID int
	}
	const count = 60
	pairs := make([]pair, 0, count)
	for i := 0; i < count; i++ {
		sent, err := messages.SendPrivateText(baseCtx, domain.SendPrivateTextRequest{
			SenderUserID:    a.ID,
			RecipientUserID: b.ID,
			RandomID:        time.Now().UnixNano() + int64(i),
			Message:         "revoke-delete",
			Date:            date,
		})
		if err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
		pairs = append(pairs, pair{senderBoxID: sent.SenderMessage.ID, recipientBoxID: sent.RecipientMessage.ID})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, count*2)
	sem := make(chan struct{}, 24)
	submit := func(op func() error) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if e := op(); e != nil {
				errCh <- e
			}
		}()
	}

	for _, p := range pairs {
		p := p
		submit(func() error {
			_, e := messages.DeleteMessages(ctx, domain.DeleteMessagesRequest{
				OwnerUserID: a.ID,
				IDs:         []int{p.senderBoxID},
				Revoke:      true,
				Date:        date,
			})
			return e
		})
		submit(func() error {
			_, e := messages.DeleteMessages(ctx, domain.DeleteMessagesRequest{
				OwnerUserID: b.ID,
				IDs:         []int{p.recipientBoxID},
				Revoke:      true,
				Date:        date,
			})
			return e
		})
	}
	wg.Wait()
	close(errCh)

	failed := 0
	for e := range errCh {
		failed++
		if failed <= 5 {
			t.Errorf("双端 revoke delete 失败（疑似死锁回归）: %v", e)
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d 双端 revoke delete 操作失败", failed, count*2)
	}
}
