package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store/redisstore"
)

func TestOpenCounterRecoveryRejectsImplicitPoolSize(t *testing.T) {
	if _, err := OpenCounterRecovery(context.Background(), "", 0); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("OpenCounterRecovery maxConns=0 error = %v, want explicit size rejection", err)
	}
}

func TestCounterRecoveryClosedSourcesFailClosedPostgres(t *testing.T) {
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TELESRV_TEST_POSTGRES_DSN to run postgres integration test")
	}
	recovery, err := OpenCounterRecovery(context.Background(), dsn, 1)
	if err != nil {
		t.Fatalf("open counter recovery: %v", err)
	}
	boxSource := recovery.MessageBoxSource()
	channelSource := recovery.ChannelIDSource()
	recovery.Close()
	recovery.Close()

	if _, err := boxSource.Current(context.Background(), 1); !errors.Is(err, ErrCounterRecoveryClosed) {
		t.Fatalf("message box source after close error = %v, want ErrCounterRecoveryClosed", err)
	}
	if _, err := channelSource.CurrentBatch(context.Background(), []int64{1}); !errors.Is(err, ErrCounterRecoveryClosed) {
		t.Fatalf("channel source after close error = %v, want ErrCounterRecoveryClosed", err)
	}
}

func TestCounterRecoveryAvoidsMaxConnsOneTransactionSelfLockPostgres(t *testing.T) {
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	redisAddr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		t.Skip("set TELESRV_TEST_POSTGRES_DSN and TELESRV_TEST_REDIS_ADDR to run counter recovery integration test")
	}
	if err := Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := Open(ctx, dsn, WithMaxConns(1), WithMinConns(1))
	if err != nil {
		t.Fatalf("open one-connection business pool: %v", err)
	}
	t.Cleanup(pool.Close)
	recovery, err := OpenCounterRecovery(ctx, dsn, 1)
	if err != nil {
		t.Fatalf("open one-connection counter recovery pool: %v", err)
	}
	t.Cleanup(recovery.Close)
	rdb, err := redisstore.Open(ctx, redisAddr, os.Getenv("TELESRV_TEST_REDIS_PASSWORD"), 0)
	if err != nil {
		t.Fatalf("open redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	sender := createTestUser(t, ctx, users, "+1760"+suffix+"00", "RecoverySender", "")
	const workers = 12
	recipients := make([]domain.User, workers)
	userIDs := make([]int64, 1, workers+1)
	userIDs[0] = sender.ID
	for i := range recipients {
		recipients[i] = createTestUser(t, ctx, users, fmt.Sprintf("+1760%s%02d", suffix, i+1), "RecoveryRecipient", "")
		userIDs = append(userIDs, recipients[i].ID)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})

	redisKeys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		redisKeys = append(redisKeys, fmt.Sprintf("counter:box_id:{%d}", userID))
	}
	if err := rdb.Del(ctx, redisKeys...).Err(); err != nil {
		t.Fatalf("clear box counters: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), redisKeys...).Err() })

	boxIDs := redisstore.NewBoxIDAllocator(rdb, recovery.MessageBoxSource())
	messages := NewMessageStore(pool, WithMessageAllocators(boxIDs))
	type sendResult struct {
		message domain.Message
		err     error
	}
	results := make(chan sendResult, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, recipient := range recipients {
		wg.Add(1)
		go func(index int, recipient domain.User) {
			defer wg.Done()
			<-start
			result, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
				SenderUserID: sender.ID, RecipientUserID: recipient.ID,
				RandomID: int64(9_760_000 + index), Message: "counter recovery", Date: 1_700_006_000 + index,
			})
			results <- sendResult{message: result.SenderMessage, err: err}
		}(i, recipient)
	}
	close(start)
	wg.Wait()
	close(results)

	senderBoxIDs := make([]int, 0, workers)
	for result := range results {
		if result.err != nil {
			t.Fatalf("send through MaxConns=1 business pool: %v", result.err)
		}
		senderBoxIDs = append(senderBoxIDs, result.message.ID)
	}
	sort.Ints(senderBoxIDs)
	for i, boxID := range senderBoxIDs {
		if i > 0 && boxID <= senderBoxIDs[i-1] {
			t.Fatalf("sender box ids are not strictly increasing: %v", senderBoxIDs)
		}
	}

	var maxChannelID int64
	if err := pool.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM channels").Scan(&maxChannelID); err != nil {
		t.Fatalf("load max channel id: %v", err)
	}
	channelIDs := &counterRecoveryChannelIDAllocator{}
	channelIDs.next.Store(maxChannelID)
	channelMessageIDs := redisstore.NewChannelMessageIDAllocator(rdb, recovery.ChannelMessageIDSource())
	channels := NewChannelStore(pool, WithChannelAllocators(channelIDs, channelMessageIDs))
	const channelWorkers = 8
	channelCounterKeys := make([]string, 0, channelWorkers)
	for i := 1; i <= channelWorkers; i++ {
		channelCounterKeys = append(channelCounterKeys, fmt.Sprintf("counter:channel_msg_id:{%d}", maxChannelID+int64(i)))
	}
	if err := rdb.Del(ctx, channelCounterKeys...).Err(); err != nil {
		t.Fatalf("clear channel message counters: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), channelCounterKeys...).Err() })

	channelResults := make(chan struct {
		id  int64
		err error
	}, channelWorkers)
	startChannels := make(chan struct{})
	for i := 0; i < channelWorkers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-startChannels
			created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
				CreatorUserID: sender.ID, Title: fmt.Sprintf("Counter Recovery %d", index),
				Megagroup: true, Date: 1_700_007_000 + index,
			})
			channelResults <- struct {
				id  int64
				err error
			}{id: created.Channel.ID, err: err}
		}(i)
	}
	close(startChannels)
	wg.Wait()
	close(channelResults)
	createdChannelIDs := make([]int64, 0, channelWorkers)
	for result := range channelResults {
		if result.err != nil {
			t.Fatalf("create channel through MaxConns=1 business pool: %v", result.err)
		}
		createdChannelIDs = append(createdChannelIDs, result.id)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM channels WHERE id = ANY($1::bigint[])", createdChannelIDs)
	})
	sort.Slice(createdChannelIDs, func(i, j int) bool { return createdChannelIDs[i] < createdChannelIDs[j] })
	for i, channelID := range createdChannelIDs {
		if i > 0 && channelID <= createdChannelIDs[i-1] {
			t.Fatalf("channel ids are not strictly increasing: %v", createdChannelIDs)
		}
	}
	if stat := pool.Stat(); stat.MaxConns() != 1 {
		t.Fatalf("business pool max conns = %d, want 1", stat.MaxConns())
	}
	if stat := recovery.Stat(); stat == nil || stat.MaxConns() != 1 {
		t.Fatalf("recovery pool stat = %+v, want max conns 1", stat)
	}
}

type counterRecoveryChannelIDAllocator struct {
	next atomic.Int64
}

func (a *counterRecoveryChannelIDAllocator) NextChannelID(context.Context) (int64, error) {
	return a.next.Add(1), nil
}

func (a *counterRecoveryChannelIDAllocator) CurrentChannelID(context.Context) (int64, error) {
	return a.next.Load(), nil
}
