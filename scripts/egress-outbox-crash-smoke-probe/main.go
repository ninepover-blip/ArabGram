package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres"
)

const defaultPostgresDSN = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	postgresDSN := flag.String("postgres-dsn", defaultProbePostgresDSN(), "PostgreSQL DSN")
	timeout := flag.Duration("timeout", 15*time.Second, "overall probe timeout")
	leaseTimeout := flag.Duration("lease-timeout", time.Second, "durable lane lease duration")
	physicalDuration := flag.Duration("physical-duration", 200*time.Millisecond, "frozen physical command lifetime")
	clockSkewAllowance := flag.Duration("clock-skew-allowance", 100*time.Millisecond, "cross-host evidence recovery allowance")
	staleAge := flag.Duration("stale-age", 2*time.Second, "age used to expire the claimed lane")
	skipMigrate := flag.Bool("skip-migrate", false, "skip embedded PostgreSQL migrations")
	flag.Parse()
	if strings.TrimSpace(*postgresDSN) == "" || *timeout <= 0 || *leaseTimeout <= 0 || *physicalDuration <= 0 ||
		*clockSkewAllowance <= 0 || *physicalDuration+*clockSkewAllowance+100*time.Millisecond >= *leaseTimeout || *staleAge <= *leaseTimeout {
		return fmt.Errorf("postgres DSN and positive deadlines are required; physical+skew+safety must fit the lease and stale-age must exceed it")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	migration := "skipped"
	if !*skipMigrate {
		status, err := postgres.MigrateAndStatus(*postgresDSN)
		if err != nil {
			return fmt.Errorf("migrate postgres: %w", err)
		}
		migration = fmt.Sprintf("version=%d dirty=%t empty=%t", status.Version, status.Dirty, status.Empty)
	}
	pool, err := postgres.Open(ctx, *postgresDSN, postgres.WithMaxConns(4), postgres.WithMinConns(1))
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()
	user, err := createProbeUser(ctx, pool)
	if err != nil {
		return err
	}
	defer cleanupProbeUser(pool, user.ID)

	event, err := postgres.NewUpdateEventStore(pool).AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
		Type: domain.UpdateEventNoop, PtsCount: 1, Date: int(time.Now().Unix()),
	}, [8]byte{}, 0)
	if err != nil {
		return fmt.Errorf("append durable PTS event: %w", err)
	}
	queue := postgres.NewDispatchOutboxStore(pool, postgres.WithLeaseTimeout(*leaseTimeout))
	shard := int(user.ID % store.DispatchOutboxLogicalShards)
	claim := func(owner string) ([]store.OutboxClaimWindow, error) {
		return queue.ClaimWindows(ctx, store.OutboxClaimRequest{
			QueueKind: store.OutboxQueueDispatchPTS, LogicalShardCount: store.DispatchOutboxLogicalShards,
			LogicalShardIDs: []int{shard}, LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1 << 20,
			LeaseDuration: *leaseTimeout, PhysicalDuration: *physicalDuration,
			ClockSkewAllowance: *clockSkewAllowance, Owner: owner,
		})
	}
	windows, err := claim("probe-owner-a")
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		return fmt.Errorf("initial v3 claim=%+v err=%v", windows, err)
	}
	first := windows[0].Items[0].Ref
	if first.QueueKind != store.OutboxQueueDispatchPTS || first.StreamID != user.ID || first.Sequence != int64(event.Pts) || first.Attempt != 1 || first.LeaseFence == 0 {
		return fmt.Errorf("invalid initial attempt ref: %+v", first)
	}
	firstTarget := store.OutboxAttemptTarget{
		TargetInstanceID: "probe-edge", TargetUserID: user.ID,
		BatchID: fixedID(1), CommandID: fixedID(2),
	}
	if err := bindTarget(ctx, queue, first, "probe-source-a", firstTarget); err != nil {
		return err
	}

	live, err := queue.RecoverBoundWindows(ctx, store.OutboxRecoverBoundRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Mode: store.OutboxBoundRecoveryExactOwnerLive,
		Owner: "probe-owner-a", LogicalShardCount: store.DispatchOutboxLogicalShards,
		LogicalShardIDs: []int{shard}, LaneLimit: 1, LeaseDuration: *leaseTimeout,
	})
	if err != nil || len(live) != 1 || len(live[0].Items) != 1 || live[0].Items[0].Ref != first ||
		live[0].Items[0].SourceInstanceID != "probe-source-a" || len(live[0].Items[0].Targets) != 1 || live[0].Items[0].Targets[0] != firstTarget {
		return fmt.Errorf("same-owner live exact recovery=%+v err=%v", live, err)
	}
	if err := expireLane(ctx, pool, user.ID, *staleAge); err != nil {
		return err
	}
	if stolen, err := claim("probe-owner-b"); err != nil || len(stolen) != 0 {
		return fmt.Errorf("ordinary claim stole bound attempt=%+v err=%v", stolen, err)
	}
	if err := waitForDeadline(ctx, windows[0].Items[0].EvidenceDeadline); err != nil {
		return err
	}
	expired, err := queue.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind:         store.OutboxQueueDispatchPTS,
		LogicalShardCount: store.DispatchOutboxLogicalShards, LogicalShardIDs: []int{shard},
		LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != first {
		return fmt.Errorf("expire evidence deadline=%+v err=%v", expired, err)
	}
	finalized, err := queue.FinalizeAttempts(ctx, expired)
	if err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		return fmt.Errorf("finalize expired attempt=%+v err=%v", finalized, err)
	}

	freshWindows, err := claim("probe-owner-b")
	if err != nil || len(freshWindows) != 1 || len(freshWindows[0].Items) != 1 {
		return fmt.Errorf("fresh claim=%+v err=%v", freshWindows, err)
	}
	fresh := freshWindows[0].Items[0].Ref
	if fresh.ItemID != first.ItemID || fresh.StreamID != first.StreamID || fresh.Sequence != first.Sequence ||
		fresh.Attempt != first.Attempt+1 || fresh.LeaseFence <= first.LeaseFence ||
		freshWindows[0].Items[0].SourceInstanceID != "" || len(freshWindows[0].Items[0].Targets) != 0 {
		return fmt.Errorf("fresh claim reused or corrupted expired identity: old=%+v fresh=%+v", first, freshWindows[0].Items[0])
	}
	freshTarget := firstTarget
	freshTarget.BatchID, freshTarget.CommandID = fixedID(3), fixedID(4)
	if err := bindTarget(ctx, queue, fresh, "probe-source-b", freshTarget); err != nil {
		return err
	}
	stale, err := queue.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: first, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "probe-source-a", TargetInstanceID: firstTarget.TargetInstanceID,
		TargetUserID: firstTarget.TargetUserID, BatchID: firstTarget.BatchID, CommandID: firstTarget.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 9001, ObservedAt: time.Now(),
	}})
	if err != nil || len(stale) != 1 || stale[0].Outcome != store.OutboxEvidenceFenced {
		return fmt.Errorf("stale physical evidence outcome=%+v err=%v", stale, err)
	}
	current, err := queue.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: fresh, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "probe-source-b", TargetInstanceID: freshTarget.TargetInstanceID,
		TargetUserID: freshTarget.TargetUserID, BatchID: freshTarget.BatchID, CommandID: freshTarget.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 9002, ObservedAt: time.Now(),
	}})
	if err != nil || len(current) != 1 || current[0].Outcome != store.OutboxEvidenceRecorded {
		return fmt.Errorf("current physical evidence outcome=%+v err=%v", current, err)
	}
	finalizable, err := queue.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueDispatchPTS, LogicalShardCount: store.DispatchOutboxLogicalShards,
		LogicalShardIDs: []int{shard}, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(finalizable) != 1 || finalizable[0].Ref != fresh {
		return fmt.Errorf("current finalizable=%+v err=%v", finalizable, err)
	}
	finalized, err = queue.FinalizeAttempts(ctx, finalizable)
	if err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		return fmt.Errorf("finalize current attempt=%+v err=%v", finalized, err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox WHERE id=$1`, first.ItemID).Scan(&remaining); err != nil || remaining != 0 {
		return fmt.Errorf("finalized item remains=%d err=%v", remaining, err)
	}
	fmt.Printf("egress v3 outbox crash recovery probe ok: user_id=%d pts=%d item_id=%d old_fence=%d new_fence=%d attempts=%d migration=%s\n",
		user.ID, event.Pts, first.ItemID, first.LeaseFence, fresh.LeaseFence, fresh.Attempt, migration)
	return nil
}

func bindTarget(ctx context.Context, queue store.DurableOutboxStateStore, ref store.OutboxAttemptRef, source string, target store.OutboxAttemptTarget) error {
	results, err := queue.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{Ref: ref, SourceInstanceID: source, Targets: []store.OutboxAttemptTarget{target}}})
	if err != nil || len(results) != 1 || results[0].Outcome != store.OutboxBindTargetBound {
		return fmt.Errorf("bind attempt target=%+v err=%v", results, err)
	}
	return nil
}

func expireLane(ctx context.Context, db postgresDB, streamID int64, staleAge time.Duration) error {
	tag, err := db.Exec(ctx, `UPDATE dispatch_outbox_lanes SET lease_until=clock_timestamp()-$2::interval, updated_at=clock_timestamp()-$2::interval WHERE stream_id=$1`, streamID, staleAge.String())
	if err != nil {
		return fmt.Errorf("expire dispatch lane: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expire dispatch lane affected %d rows", tag.RowsAffected())
	}
	return nil
}

func fixedID(value byte) [16]byte {
	var id [16]byte
	id[0] = value
	return id
}

func waitForDeadline(ctx context.Context, deadline time.Time) error {
	if deadline.IsZero() {
		return fmt.Errorf("attempt has no persisted evidence deadline")
	}
	delay := time.Until(deadline) + 10*time.Millisecond
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for evidence deadline: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func defaultProbePostgresDSN() string {
	if value := strings.TrimSpace(os.Getenv("TELESRV_TEST_POSTGRES_DSN")); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("TELESRV_POSTGRES_DSN")); value != "" {
		return value
	}
	return defaultPostgresDSN
}

func createProbeUser(ctx context.Context, pool *pgxpool.Pool) (domain.User, error) {
	now := time.Now().UnixNano()
	user, err := postgres.NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: now & 0x7fffffffffffffff, Phone: fmt.Sprintf("+190090%012d", now%1_000_000_000_000), FirstName: "EgressV3Probe",
	})
	if err != nil {
		return domain.User{}, fmt.Errorf("create probe user: %w", err)
	}
	return user, nil
}

type postgresDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func cleanupProbeUser(db postgresDB, userID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = db.Exec(ctx, `DELETE FROM users WHERE id=$1`, userID)
}
