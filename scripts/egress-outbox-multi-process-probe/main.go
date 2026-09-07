package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamxvbaba/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisbus"
	"telesrv/internal/edgecontrol/redisregistry"
	"telesrv/internal/egress"
	"telesrv/internal/store/postgres"
	"telesrv/internal/store/redisstore"
)

const (
	defaultPostgresDSN      = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable"
	defaultRedisAddr        = "127.0.0.1:6399"
	defaultRedisBusPrefix   = "edge:control"
	defaultProbeUserCount   = 16
	defaultEventsPerUser    = 2
	defaultProbeTimeout     = 30 * time.Second
	defaultLocationTTL      = 30 * time.Second
	defaultClientAckDelay   = 25 * time.Millisecond
	defaultGRPCRequestDelay = 2 * time.Second
	fakeEdgeMailbox         = 2048
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	postgresDSN := flag.String("postgres-dsn", defaultProbePostgresDSN(), "PostgreSQL DSN")
	redisAddr := flag.String("redis-addr", defaultProbeRedisAddr(), "Redis address")
	redisPassword := flag.String("redis-password", os.Getenv("TELESRV_REDIS_PASSWORD"), "Redis password")
	redisDB := flag.Int("redis-db", envInt("TELESRV_REDIS_DB", 0), "Redis DB")
	redisBusPrefix := flag.String("redis-bus-prefix", defaultRedisBusPrefix, "Redis delivery v3 command bus prefix")
	deliveryTargetsRaw := flag.String("egress-delivery-targets", defaultEgressDeliveryTargets(), "Egress delivery v3 gRPC targets")
	deliveryResolver := flag.String("egress-delivery-resolver", envOr("TELESRV_EGRESS_DELIVERY_GRPC_RESOLVER", "static"), "Egress delivery gRPC resolver")
	deliveryToken := flag.String("egress-delivery-token", os.Getenv("TELESRV_EGRESS_DELIVERY_TOKEN"), "Egress delivery bearer token")
	users := flag.Int("users", defaultProbeUserCount, "probe users")
	eventsPerUser := flag.Int("events-per-user", defaultEventsPerUser, "durable account PTS events per user")
	expectSourceInstances := flag.Int("expect-source-instances", 1, "minimum distinct frozen source instance IDs for the normal multi-process gate")
	timeout := flag.Duration("timeout", defaultProbeTimeout, "overall probe timeout")
	locationTTL := flag.Duration("location-ttl", defaultLocationTTL, "fake Edge location TTL")
	admissionDelay := flag.Duration("admission-delay", 0, "delay before v3 command admission")
	clientAckDelay := flag.Duration("client-ack-delay", defaultClientAckDelay, "delay between physical receipt and late client ACK observation")
	crashRecovery := flag.Bool("crash-recovery", false, "verify fencing and fresh v3 attempt creation after its Egress owner crashes")
	crashSignalFile := flag.String("crash-signal-file", "", "coordination JSON path; required with -crash-recovery")
	skipMigrate := flag.Bool("skip-migrate", false, "skip embedded PostgreSQL migrations")
	tlsCAFile := flag.String("tls-ca-file", os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_TLS_CA_FILE"), "Egress delivery root CA")
	tlsServerName := flag.String("tls-server-name", os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_TLS_SERVER_NAME"), "Egress delivery TLS server name")
	tlsClientCertFile := flag.String("tls-client-cert-file", os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_TLS_CLIENT_CERT_FILE"), "Egress delivery client certificate")
	tlsClientKeyFile := flag.String("tls-client-key-file", os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_TLS_CLIENT_KEY_FILE"), "Egress delivery client key")
	flag.Parse()

	if strings.TrimSpace(*postgresDSN) == "" || strings.TrimSpace(*redisAddr) == "" || strings.TrimSpace(*deliveryTargetsRaw) == "" || strings.TrimSpace(*deliveryToken) == "" {
		return fmt.Errorf("postgres, redis, Egress delivery targets and token are required")
	}
	if *users <= 0 || *eventsPerUser <= 0 || *expectSourceInstances <= 0 || *timeout <= 0 || *locationTTL <= 0 {
		return fmt.Errorf("users, events-per-user, expect-source-instances, timeout and location-ttl must be positive")
	}
	if *admissionDelay < 0 || *clientAckDelay < 0 {
		return fmt.Errorf("delivery delays must not be negative")
	}
	if !*crashRecovery && *expectSourceInstances > *users {
		return fmt.Errorf("-expect-source-instances cannot exceed -users")
	}
	if *crashRecovery && (strings.TrimSpace(*crashSignalFile) == "" || *users != 1 || *eventsPerUser != 1) {
		return fmt.Errorf("-crash-recovery requires a signal file, one user and one event")
	}
	if *crashRecovery && *expectSourceInstances < 2 {
		return fmt.Errorf("-crash-recovery requires -expect-source-instances >= 2")
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
	pool, err := postgres.Open(ctx, *postgresDSN, postgres.WithMaxConns(8), postgres.WithMinConns(1))
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	rdb, err := redisstore.Open(ctx, *redisAddr, *redisPassword, *redisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	deliveryTargets, err := egress.ParseGRPCDeliveryTargetsForResolver(*deliveryTargetsRaw, *deliveryResolver)
	if err != nil {
		return fmt.Errorf("parse Egress delivery targets: %w", err)
	}
	remote, conn, err := egress.DialGRPCDeliveryRemote(ctx, egress.GRPCDeliveryClientConfig{
		Targets: deliveryTargets, ResolverKind: strings.TrimSpace(*deliveryResolver), Token: *deliveryToken,
		RequestTimeout: defaultGRPCRequestDelay, TLSCAFile: *tlsCAFile, TLSServerName: *tlsServerName,
		TLSCertFile: *tlsClientCertFile, TLSKeyFile: *tlsClientKeyFile,
	})
	if err != nil {
		return fmt.Errorf("dial Egress delivery gRPC: %w", err)
	}
	defer func() { _ = conn.Close() }()

	probeUsers, err := createProbeUsers(ctx, pool, *users)
	if err != nil {
		return err
	}
	ids := userIDs(probeUsers)
	defer cleanupProbeUsers(pool, ids)
	edgeInstanceID := fmt.Sprintf("probe-edge-%d-%d", os.Getpid(), time.Now().UnixNano())
	registry := redisregistry.New(rdb)
	leaseID := fmt.Sprintf("probe-lease-%d-%d", os.Getpid(), time.Now().UnixNano())
	if err := registry.AcquireInstanceLease(ctx, edgeInstanceID, leaseID, *locationTTL); err != nil {
		return fmt.Errorf("acquire fake Edge lease: %w", err)
	}
	locations := make(map[int64]edgecontrol.LocationRecord, len(probeUsers))
	mutations := make([]edgecontrol.LocationMutation, 0, len(probeUsers))
	for i, user := range probeUsers {
		record := probeLocation(edgeInstanceID, user.ID, i)
		locations[user.ID] = record
		mutations = append(mutations, edgecontrol.LocationMutation{Record: record})
	}
	if err := registry.ApplyLocationMutations(ctx, edgeInstanceID, leaseID, mutations); err != nil {
		return fmt.Errorf("publish fake Edge locations: %w", err)
	}
	defer cleanupProbeLocations(registry, edgeInstanceID, leaseID)

	var crash *crashCoordinator
	if *crashRecovery {
		crash = newCrashCoordinator(pool, *crashSignalFile, *clientAckDelay)
	}
	recorder := newCommandRecorder()
	subCtx, stopSubscriber := context.WithCancel(ctx)
	actor := newFakeEdgeDeliveryActor(subCtx, edgeInstanceID, locations, recorder, remote, *admissionDelay, *clientAckDelay, crash)
	go actor.run()
	defer func() {
		stopSubscriber()
		actor.wait()
	}()
	deliveryBus := redisbus.New(rdb, redisbus.WithPrefix(*redisBusPrefix))
	subscriberErr := make(chan error, 1)
	go func() { subscriberErr <- deliveryBus.SubscribeDeliveryBatches(subCtx, edgeInstanceID, actor.admit) }()
	if err := waitDeliverySubscriberReady(ctx, rdb, *redisBusPrefix, edgeInstanceID, subscriberErr); err != nil {
		return err
	}

	expected, err := appendProbeEvents(ctx, pool, probeUsers, *eventsPerUser)
	if err != nil {
		return err
	}
	if len(expected) == 0 {
		return fmt.Errorf("no probe events appended")
	}
	if *crashRecovery {
		if err := waitForRecords(ctx, recorder, subscriberErr, 2, *expectSourceInstances); err != nil {
			return err
		}
		records := recorder.snapshot()
		if err := validateCrashRecoveryRecords(records, expected[0]); err != nil {
			return err
		}
		if err := waitOutboxEmpty(ctx, pool, ids); err != nil {
			return err
		}
		fmt.Printf("egress v3 fenced crash recovery probe ok: user_id=%d pts=%d item_id=%d old_source=%s recovery_source=%s old_batch=%x new_batch=%x fake_edge=%s targets=%s migration=%s\n",
			expected[0].userID, expected[0].pts, records[0].item.Ref.OutboxID,
			records[0].batch.SourceInstanceID, records[1].batch.SourceInstanceID,
			records[0].batch.BatchID, records[1].batch.BatchID,
			edgeInstanceID, strings.Join(deliveryTargets, ","), migration)
		return nil
	}
	if err := waitForRecords(ctx, recorder, subscriberErr, len(expected), *expectSourceInstances); err != nil {
		return err
	}
	records := recorder.snapshot()
	if err := validateRecordedCommands(records, expected, *expectSourceInstances); err != nil {
		return err
	}
	if err := waitOutboxEmpty(ctx, pool, ids); err != nil {
		return err
	}
	fmt.Printf("egress delivery v3 multi-process probe ok: users=%d items=%d frozen_sources=%s fake_edge=%s targets=%s migration=%s\n",
		len(probeUsers), len(expected), strings.Join(sortedSources(records), ","), edgeInstanceID, strings.Join(deliveryTargets, ","), migration)
	return nil
}

type deliveryEvidenceRemote interface {
	edgecontrol.PhysicalReceiptReporter
	edgecontrol.ClientAckObservationReporter
}

type fakeEdgeDeliveryActor struct {
	ctx            context.Context
	instanceID     string
	locations      map[int64]edgecontrol.LocationRecord
	recorder       *commandRecorder
	remote         deliveryEvidenceRemote
	admissionDelay time.Duration
	clientAckDelay time.Duration
	crash          *crashCoordinator
	commands       chan edgecontrol.DeliveryBatch
	done           chan struct{}
}

func newFakeEdgeDeliveryActor(ctx context.Context, instanceID string, locations map[int64]edgecontrol.LocationRecord, recorder *commandRecorder, remote deliveryEvidenceRemote, admissionDelay, clientAckDelay time.Duration, crash *crashCoordinator) *fakeEdgeDeliveryActor {
	return &fakeEdgeDeliveryActor{
		ctx: ctx, instanceID: instanceID, locations: locations, recorder: recorder, remote: remote,
		admissionDelay: admissionDelay, clientAckDelay: clientAckDelay, crash: crash,
		commands: make(chan edgecontrol.DeliveryBatch, fakeEdgeMailbox), done: make(chan struct{}),
	}
}

func (a *fakeEdgeDeliveryActor) admit(ctx context.Context, batch edgecontrol.DeliveryBatch) edgecontrol.DeliveryAdmission {
	admission := deliveryAdmission(batch, edgecontrol.AdmissionRejected, edgecontrol.DetailInvalidIdentity)
	if a == nil || batch.TargetInstanceID != a.instanceID || batch.SourceInstanceID == "" || batch.BatchID.Empty() || batch.CommandID.Empty() {
		return admission
	}
	if _, ok := a.locations[batch.TargetUserID]; !ok {
		return admission
	}
	if err := edgecontrol.ValidateDeliveryBatch(batch); err != nil {
		admission.Detail = edgecontrol.DetailInvalidPayload
		return admission
	}
	if err := sleepContext(ctx, a.admissionDelay); err != nil {
		return deliveryAdmission(batch, edgecontrol.AdmissionOverloaded, edgecontrol.DetailDeadline)
	}
	select {
	case a.commands <- batch:
		return deliveryAdmission(batch, edgecontrol.AdmissionAccepted, edgecontrol.DetailNone)
	case <-ctx.Done():
		return deliveryAdmission(batch, edgecontrol.AdmissionOverloaded, edgecontrol.DetailDeadline)
	default:
		return deliveryAdmission(batch, edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity)
	}
}

func (a *fakeEdgeDeliveryActor) run() {
	defer close(a.done)
	for {
		select {
		case <-a.ctx.Done():
			return
		case batch := <-a.commands:
			err := a.process(batch)
			a.recorder.add(batch, err)
		}
	}
}

func (a *fakeEdgeDeliveryActor) process(batch edgecontrol.DeliveryBatch) error {
	if a.crash != nil {
		return a.crash.handle(a.ctx, batch, a.locations[batch.TargetUserID], a.remote)
	}
	return reportDeliveryEvidence(a.ctx, a.remote, batch, a.locations[batch.TargetUserID], a.clientAckDelay)
}

func (a *fakeEdgeDeliveryActor) wait() { <-a.done }

func deliveryAdmission(batch edgecontrol.DeliveryBatch, outcome edgecontrol.AdmissionOutcome, detail edgecontrol.DetailCode) edgecontrol.DeliveryAdmission {
	return edgecontrol.DeliveryAdmission{
		BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID,
		TargetInstanceID: batch.TargetInstanceID, Outcome: outcome, Detail: detail,
	}
}

func reportDeliveryEvidence(ctx context.Context, remote deliveryEvidenceRemote, batch edgecontrol.DeliveryBatch, location edgecontrol.LocationRecord, clientAckDelay time.Duration) error {
	if remote == nil {
		return fmt.Errorf("Egress delivery reporter is nil")
	}
	receipts := make([]edgecontrol.PhysicalReceipt, len(batch.Items))
	observations := make([]edgecontrol.ClientAckObservation, len(batch.Items))
	for i, item := range batch.Items {
		observedAt := time.Now()
		serverMsgID := probeServerMsgID(item.Ref)
		receipts[i] = edgecontrol.PhysicalReceipt{
			BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID,
			TargetInstanceID: batch.TargetInstanceID, Ref: item.Ref, Outcome: edgecontrol.PhysicalWritten,
			EligibleSessions: 1, WrittenSessions: 1, FirstServerMsgID: serverMsgID, ObservedAt: observedAt,
		}
		observations[i] = edgecontrol.ClientAckObservation{
			Tracking:         edgecontrol.DeliveryTracking{BatchID: batch.BatchID, CommandID: batch.CommandID, Ref: item.Ref},
			TargetInstanceID: batch.TargetInstanceID, AuthKeyID: location.RawAuthKeyID, SessionID: location.SessionID,
			ServerMsgID: serverMsgID, ObservedAt: observedAt,
		}
	}
	if err := reportPhysicalReceiptsWithRetry(ctx, remote, receipts); err != nil {
		return err
	}
	if err := sleepContext(ctx, clientAckDelay); err != nil {
		return err
	}
	for i := range observations {
		observations[i].ObservedAt = time.Now()
	}
	return reportClientAcksWithRetry(ctx, remote, observations)
}

func reportPhysicalReceiptsWithRetry(ctx context.Context, remote edgecontrol.PhysicalReceiptReporter, receipts []edgecontrol.PhysicalReceipt) error {
	for {
		results, err := remote.ReportPhysicalReceipts(ctx, receipts)
		if err == nil && len(results) == len(receipts) {
			retry := false
			for _, result := range results {
				switch result.Outcome {
				case edgecontrol.PhysicalReceiptApplied:
				case edgecontrol.PhysicalReceiptRetryable:
					retry = true
				default:
					return fmt.Errorf("physical receipt outcome=%d detail=%d", result.Outcome, result.Detail)
				}
			}
			if !retry {
				return nil
			}
		}
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("report physical receipts: %w", err)
		}
	}
}

func reportClientAcksWithRetry(ctx context.Context, remote edgecontrol.ClientAckObservationReporter, observations []edgecontrol.ClientAckObservation) error {
	for {
		results, err := remote.ReportClientAcks(ctx, observations)
		if err == nil && len(results) == len(observations) {
			retry := false
			for _, result := range results {
				switch result.Outcome {
				case edgecontrol.ClientAckObservationApplied:
				case edgecontrol.ClientAckObservationRetryable:
					retry = true
				default:
					return fmt.Errorf("client ACK outcome=%d detail=%d", result.Outcome, result.Detail)
				}
			}
			if !retry {
				return nil
			}
		}
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("report client ACK observations: %w", err)
		}
	}
}

func probeServerMsgID(ref edgecontrol.DeliveryRef) int64 {
	id := (ref.OutboxID%9_000_000_000_000)*1_000 + int64(ref.Attempt)
	if id <= 0 {
		return int64(ref.Attempt)
	}
	return id
}

type recordedCommand struct {
	batch      edgecontrol.DeliveryBatch
	item       edgecontrol.DeliveryItem
	receivedAt time.Time
	err        string
}

type commandRecorder struct {
	mu      sync.Mutex
	records []recordedCommand
}

func newCommandRecorder() *commandRecorder { return &commandRecorder{} }

func (r *commandRecorder) add(batch edgecontrol.DeliveryBatch, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range batch.Items {
		record := recordedCommand{batch: batch, item: item, receivedAt: time.Now()}
		if err != nil {
			record.err = err.Error()
		}
		r.records = append(r.records, record)
	}
}

func (r *commandRecorder) snapshot() []recordedCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedCommand(nil), r.records...)
}

type crashCoordinator struct {
	pool           *pgxpool.Pool
	signalFile     string
	clientAckDelay time.Duration
	mu             sync.Mutex
	firstBatch     edgecontrol.DeliveryBatch
	firstRef       edgecontrol.DeliveryRef
	signaled       bool
}

type crashSignal struct {
	Status           string `json:"status"`
	SourceInstanceID string `json:"source_instance_id"`
	TargetUserID     int64  `json:"target_user_id"`
	OutboxID         int64  `json:"outbox_id"`
	Pts              int    `json:"pts"`
	Attempt          uint32 `json:"attempt"`
	LeaseFence       uint64 `json:"lease_fence"`
	BatchID          string `json:"batch_id"`
	CommandID        string `json:"command_id"`
	WrittenAtUnixNS  int64  `json:"written_at_unix_ns"`
}

func newCrashCoordinator(pool *pgxpool.Pool, signalFile string, clientAckDelay time.Duration) *crashCoordinator {
	signalFile = strings.TrimSpace(signalFile)
	if signalFile != "" {
		_ = os.Remove(signalFile)
	}
	return &crashCoordinator{pool: pool, signalFile: signalFile, clientAckDelay: clientAckDelay}
}

func (c *crashCoordinator) handle(ctx context.Context, batch edgecontrol.DeliveryBatch, location edgecontrol.LocationRecord, remote deliveryEvidenceRemote) error {
	if len(batch.Items) != 1 {
		return fmt.Errorf("crash recovery requires one delivery item, got %d", len(batch.Items))
	}
	ref := batch.Items[0].Ref
	c.mu.Lock()
	if !c.signaled {
		c.firstBatch, c.firstRef, c.signaled = batch, ref, true
		c.mu.Unlock()
		if _, err := waitOutboxState(ctx, c.pool, ref.TargetUserID, ref.PTS, "dispatching", int(ref.Attempt)); err != nil {
			return err
		}
		return c.writeSignal(crashSignal{
			Status: "dispatching", SourceInstanceID: batch.SourceInstanceID, TargetUserID: ref.TargetUserID,
			OutboxID: ref.OutboxID, Pts: ref.PTS, Attempt: ref.Attempt, LeaseFence: ref.LeaseFence,
			BatchID: fmt.Sprintf("%x", batch.BatchID), CommandID: fmt.Sprintf("%x", batch.CommandID), WrittenAtUnixNS: time.Now().UnixNano(),
		})
	}
	firstBatch, firstRef := c.firstBatch, c.firstRef
	c.mu.Unlock()
	if ref.OutboxID != firstRef.OutboxID || ref.TargetUserID != firstRef.TargetUserID || ref.PTS != firstRef.PTS ||
		ref.Domain != firstRef.Domain || ref.Attempt != firstRef.Attempt+1 || ref.LeaseFence <= firstRef.LeaseFence ||
		batch.BatchID == firstBatch.BatchID || batch.CommandID == firstBatch.CommandID ||
		batch.SourceInstanceID == firstBatch.SourceInstanceID || batch.TargetInstanceID != firstBatch.TargetInstanceID {
		return fmt.Errorf("expired cross-owner recovery did not create a fresh fenced attempt: first=%+v/%x/%x recovered=%+v/%x/%x",
			firstRef, firstBatch.BatchID, firstBatch.CommandID, ref, batch.BatchID, batch.CommandID)
	}
	return reportDeliveryEvidence(ctx, remote, batch, location, c.clientAckDelay)
}

func (c *crashCoordinator) writeSignal(signal crashSignal) error {
	if c == nil || c.signalFile == "" {
		return fmt.Errorf("crash signal file is not configured")
	}
	dir := filepath.Dir(c.signalFile)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create crash signal dir: %w", err)
		}
	}
	raw, err := json.MarshalIndent(signal, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.signalFile + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.signalFile)
}

type probeEvent struct {
	userID int64
	pts    int
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

func defaultProbeRedisAddr() string {
	if value := strings.TrimSpace(os.Getenv("TELESRV_REDIS_ADDR")); value != "" {
		return value
	}
	return defaultRedisAddr
}

func defaultEgressDeliveryTargets() string {
	if value := strings.TrimSpace(os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_TARGETS")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("TELESRV_EGRESS_DELIVERY_GRPC_ADDR"))
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func createProbeUsers(ctx context.Context, pool *pgxpool.Pool, count int) ([]domain.User, error) {
	store := postgres.NewUserStore(pool)
	out := make([]domain.User, 0, count)
	stamp := time.Now().UnixNano() % 1_000_000_000_000
	for i := 0; i < count; i++ {
		user, err := store.Create(ctx, domain.User{
			AccessHash: int64(stamp*100 + int64(i) + 1), Phone: fmt.Sprintf("+190092%012d%02d", stamp, i), FirstName: "EgressV3",
		})
		if err != nil {
			return nil, fmt.Errorf("create probe user %d: %w", i, err)
		}
		out = append(out, user)
	}
	return out, nil
}

func userIDs(users []domain.User) []int64 {
	ids := make([]int64, len(users))
	for i := range users {
		ids[i] = users[i].ID
	}
	return ids
}

func cleanupProbeUsers(pool *pgxpool.Pool, ids []int64) {
	if pool == nil || len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, ids)
}

func cleanupProbeLocations(registry *redisregistry.Registry, instanceID, leaseID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = registry.ReleaseInstanceLease(ctx, instanceID, leaseID)
}

func probeLocation(instanceID string, userID int64, index int) edgecontrol.LocationRecord {
	return edgecontrol.LocationRecord{
		InstanceID: instanceID, UserID: userID, RawAuthKeyID: probeAuthKeyID(userID, index),
		SessionID: 900_000_000 + int64(index+1), ReceivesUpdates: true, Layer: tg.Layer,
	}
}

func probeAuthKeyID(userID int64, index int) [8]byte {
	var out [8]byte
	value := uint64(userID)<<16 | uint64(index+1)
	for i := 7; i >= 0; i-- {
		out[i], value = byte(value), value>>8
	}
	if out == ([8]byte{}) {
		out[7] = 1
	}
	return out
}

func appendProbeEvents(ctx context.Context, pool *pgxpool.Pool, users []domain.User, count int) ([]probeEvent, error) {
	store := postgres.NewUpdateEventStore(pool)
	out := make([]probeEvent, 0, len(users)*count)
	for _, user := range users {
		for i := 0; i < count; i++ {
			event, err := store.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
				Type: domain.UpdateEventReadHistoryInbox, PtsCount: 1, Date: int(time.Now().Unix()),
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: user.ID}, MaxID: i + 1,
			}, [8]byte{}, 0)
			if err != nil {
				return nil, fmt.Errorf("append probe event user_id=%d: %w", user.ID, err)
			}
			out = append(out, probeEvent{userID: user.ID, pts: event.Pts})
		}
	}
	return out, nil
}

func waitDeliverySubscriberReady(ctx context.Context, rdb *redis.Client, prefix, instanceID string, subscriberErr <-chan error) error {
	prefix = strings.TrimRight(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		prefix = defaultRedisBusPrefix
	}
	channel := fmt.Sprintf("%s:delivery:v3:command:%s", prefix, instanceID)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		counts, err := rdb.PubSubNumSub(ctx, channel).Result()
		if err == nil && counts[channel] > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for v3 delivery subscriber: %w", ctx.Err())
		case err := <-subscriberErr:
			return fmt.Errorf("v3 delivery subscriber exited: %w", err)
		case <-ticker.C:
		}
	}
}

func waitForRecords(ctx context.Context, recorder *commandRecorder, subscriberErr <-chan error, count, sources int) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		records := recorder.snapshot()
		if err := firstRecordError(records); err != nil {
			return err
		}
		if len(records) >= count && len(sourceSet(records)) >= sources {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for v3 delivery records: got=%d/%d sources=%d/%d: %w", len(records), count, len(sourceSet(records)), sources, ctx.Err())
		case err := <-subscriberErr:
			return fmt.Errorf("v3 delivery subscriber exited: %w", err)
		case <-ticker.C:
		}
	}
}

func waitOutboxEmpty(ctx context.Context, pool *pgxpool.Pool, ids []int64) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])`, ids).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for dispatch_outbox drain, remaining=%d: %w", count, ctx.Err())
		case <-ticker.C:
		}
	}
}

type outboxState struct {
	id       int64
	status   string
	attempts int
}

func waitOutboxState(ctx context.Context, pool *pgxpool.Pool, userID int64, pts int, status string, attempts int) (outboxState, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state outboxState
		err := pool.QueryRow(ctx, `SELECT id, status, attempts FROM dispatch_outbox WHERE target_user_id=$1 AND pts=$2`, userID, int32(pts)).Scan(&state.id, &state.status, &state.attempts)
		if err != nil {
			return state, err
		}
		if state.status == status && state.attempts == attempts {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return state, fmt.Errorf("wait outbox state got=%s/%d want=%s/%d: %w", state.status, state.attempts, status, attempts, ctx.Err())
		case <-ticker.C:
		}
	}
}

func validateRecordedCommands(records []recordedCommand, expected []probeEvent, expectSources int) error {
	if err := firstRecordError(records); err != nil {
		return err
	}
	if len(records) != len(expected) {
		return fmt.Errorf("recorded v3 items=%d want=%d", len(records), len(expected))
	}
	want := make(map[int64][]int)
	for _, event := range expected {
		want[event.userID] = append(want[event.userID], event.pts)
	}
	for userID := range want {
		sort.Ints(want[userID])
	}
	got := make(map[int64][]int)
	seen := make(map[edgecontrol.DeliveryRef]struct{}, len(records))
	for _, record := range records {
		ref := record.item.Ref
		if !ref.Valid() || ref.Domain.Kind != edgecontrol.QueueAccountPTS || ref.Domain.StreamID != ref.TargetUserID || ref.Attempt != 1 {
			return fmt.Errorf("invalid account PTS delivery ref: %+v", ref)
		}
		if record.batch.BatchID.Empty() || record.batch.CommandID.Empty() {
			return fmt.Errorf("empty v3 batch/command identity")
		}
		if _, exists := seen[ref]; exists {
			return fmt.Errorf("duplicate delivery ref: %+v", ref)
		}
		seen[ref] = struct{}{}
		got[ref.TargetUserID] = append(got[ref.TargetUserID], ref.PTS)
	}
	for userID, expectedPTS := range want {
		if !intsEqual(got[userID], expectedPTS) {
			return fmt.Errorf("user_id=%d PTS order=%v want=%v", userID, got[userID], expectedPTS)
		}
	}
	if len(sourceSet(records)) < expectSources {
		return fmt.Errorf("frozen sources=%d want at least %d", len(sourceSet(records)), expectSources)
	}
	return nil
}

func validateCrashRecoveryRecords(records []recordedCommand, expected probeEvent) error {
	if err := firstRecordError(records); err != nil {
		return err
	}
	if len(records) != 2 {
		return fmt.Errorf("crash recovery records=%d want=2", len(records))
	}
	first, replay := records[0], records[1]
	if first.item.Ref.TargetUserID != expected.userID || first.item.Ref.PTS != expected.pts {
		return fmt.Errorf("first frozen ref=%+v does not match probe event", first.item.Ref)
	}
	if replay.item.Ref.OutboxID != first.item.Ref.OutboxID || replay.item.Ref.TargetUserID != first.item.Ref.TargetUserID ||
		replay.item.Ref.Domain != first.item.Ref.Domain || replay.item.Ref.PTS != first.item.Ref.PTS ||
		replay.item.Ref.Attempt != first.item.Ref.Attempt+1 || replay.item.Ref.LeaseFence <= first.item.Ref.LeaseFence ||
		replay.batch.BatchID == first.batch.BatchID || replay.batch.CommandID == first.batch.CommandID ||
		replay.batch.SourceInstanceID == first.batch.SourceInstanceID || replay.batch.TargetInstanceID != first.batch.TargetInstanceID {
		return fmt.Errorf("cross-owner recovery did not fence the old attempt and allocate fresh command identities")
	}
	return nil
}

func firstRecordError(records []recordedCommand) error {
	for _, record := range records {
		if record.err != "" {
			return fmt.Errorf("fake Edge v3 command failed batch=%x command=%x source=%s target_user=%d: %s",
				record.batch.BatchID, record.batch.CommandID, record.batch.SourceInstanceID, record.item.Ref.TargetUserID, record.err)
		}
	}
	return nil
}

func sourceSet(records []recordedCommand) map[string]struct{} {
	out := make(map[string]struct{})
	for _, record := range records {
		if source := strings.TrimSpace(record.batch.SourceInstanceID); source != "" {
			out[source] = struct{}{}
		}
	}
	return out
}

func sortedSources(records []recordedCommand) []string {
	out := make([]string, 0, len(sourceSet(records)))
	for source := range sourceSet(records) {
		out = append(out, source)
	}
	sort.Strings(out)
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
