package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"telesrv/internal/store"
)

var errOutboxQueryAllocationSentinel = errors.New("outbox allocation SQL captured")

type outboxQueryCorpusCase struct {
	name   string
	db     *idlePollDB
	invoke func() error
}

type outboxQueryArgument struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type outboxQueryFingerprint struct {
	Name       string                `json:"name"`
	SQLSHA256  string                `json:"sql_sha256"`
	SQLBytes   int                   `json:"sql_bytes"`
	ArgsSHA256 string                `json:"args_sha256"`
	Args       []outboxQueryArgument `json:"args"`
}

func outboxAllocationConfigs() []outboxStateConfig {
	return []outboxStateConfig{dispatchOutboxStateConfig, deliveryOutboxStateConfig}
}

func outboxAllocationQueueName(cfg outboxStateConfig) string {
	if cfg.kind == store.OutboxQueueDispatchPTS {
		return "dispatch"
	}
	return "delivery"
}

func outboxAllocationID(value byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = value + byte(i)
	}
	return out
}

func outboxAllocationRef(cfg outboxStateConfig, index int) store.OutboxAttemptRef {
	sequence := int64(0)
	if cfg.kind == store.OutboxQueueDispatchPTS {
		sequence = int64(index + 1)
	}
	return store.OutboxAttemptRef{
		QueueKind: cfg.kind, StreamID: int64(1000 + index), ItemID: int64(2000 + index),
		Sequence: sequence, LeaseFence: uint64(3000 + index), Attempt: 2,
	}
}

func outboxAllocationTarget(ref store.OutboxAttemptRef, index int) store.OutboxAttemptTarget {
	return store.OutboxAttemptTarget{
		TargetInstanceID: fmt.Sprintf("edge-allocation-%02d", index), TargetUserID: ref.StreamID,
		BatchID: outboxAllocationID(byte(index + 1)), CommandID: outboxAllocationID(byte(index + 65)),
	}
}

func outboxAllocationTargetSets(cfg outboxStateConfig, count int) []store.OutboxAttemptTargetSet {
	sets := make([]store.OutboxAttemptTargetSet, count)
	for i := range sets {
		ref := outboxAllocationRef(cfg, i)
		sets[i] = store.OutboxAttemptTargetSet{
			Ref: ref, SourceInstanceID: "egress-allocation",
			Targets: []store.OutboxAttemptTarget{outboxAllocationTarget(ref, i)},
		}
	}
	return sets
}

func outboxAllocationEvidence(cfg outboxStateConfig, count int) []store.OutboxAttemptEvidence {
	observedAt := time.Date(2030, time.January, 2, 3, 4, 5, 6000000, time.UTC)
	evidence := make([]store.OutboxAttemptEvidence, count)
	for i := range evidence {
		ref := outboxAllocationRef(cfg, i)
		target := outboxAllocationTarget(ref, i)
		evidence[i] = store.OutboxAttemptEvidence{
			Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-allocation",
			TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
			BatchID: target.BatchID, CommandID: target.CommandID,
			EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: int64(4000 + i),
			ObservedAt: observedAt.Add(time.Duration(i) * time.Nanosecond),
		}
	}
	return evidence
}

func outboxAllocationClaimRequest(cfg outboxStateConfig, scoped bool) store.OutboxClaimRequest {
	req := store.OutboxClaimRequest{
		QueueKind: cfg.kind, LaneLimit: 17, WindowSize: 16, WindowByteLimit: 1 << 20,
		LeaseDuration: 45 * time.Second, PhysicalDuration: 5 * time.Second,
		ClockSkewAllowance: time.Second, Owner: "egress-allocation/q0/w0",
	}
	if scoped {
		req.LogicalShardCount = store.DispatchOutboxLogicalShards
		req.LogicalShardIDs = []int{7, 2, 7, -1, store.DispatchOutboxLogicalShards}
	}
	return req
}

func outboxAllocationFingerprint(name, query string, args []any) (outboxQueryFingerprint, error) {
	fingerprint := outboxQueryFingerprint{Name: name, SQLBytes: len(query), Args: make([]outboxQueryArgument, len(args))}
	sqlHash := sha256.Sum256([]byte(query))
	fingerprint.SQLSHA256 = hex.EncodeToString(sqlHash[:])
	for i, arg := range args {
		encoded, err := json.Marshal(arg)
		if err != nil {
			return outboxQueryFingerprint{}, fmt.Errorf("marshal argument %d: %w", i, err)
		}
		fingerprint.Args[i] = outboxQueryArgument{Type: fmt.Sprintf("%T", arg), Value: encoded}
	}
	encoded, err := json.Marshal(fingerprint.Args)
	if err != nil {
		return outboxQueryFingerprint{}, err
	}
	argsHash := sha256.Sum256(encoded)
	fingerprint.ArgsSHA256 = hex.EncodeToString(argsHash[:])
	return fingerprint, nil
}

// This intentionally has no generated expected SQL. It calls each production
// method twice and proves the captured query and typed parameters are stable.
// The verbose fingerprints are the one-time baseline used to freeze golden
// values before the production query construction changes.
func TestOutboxAllocationQueryCorpus(t *testing.T) {
	var cases []outboxQueryCorpusCase
	for _, cfg := range outboxAllocationConfigs() {
		cfg := cfg
		queue := outboxAllocationQueueName(cfg)
		for _, scoped := range []bool{false, true} {
			scoped := scoped
			db := &idlePollDB{capture: true, queryErr: errOutboxQueryAllocationSentinel}
			state := newDurableOutboxState(db, cfg, time.Minute)
			req := outboxAllocationClaimRequest(cfg, scoped)
			cases = append(cases, outboxQueryCorpusCase{
				name: fmt.Sprintf("claim/%s/scoped_%t", queue, scoped), db: db,
				invoke: func() error { _, err := state.ClaimWindows(context.Background(), req); return err },
			})
		}
		for _, count := range []int{1, 16} {
			count := count
			db := &idlePollDB{capture: true, queryErr: errOutboxQueryAllocationSentinel}
			state := newDurableOutboxState(db, cfg, time.Minute)
			sets := outboxAllocationTargetSets(cfg, count)
			cases = append(cases, outboxQueryCorpusCase{
				name: fmt.Sprintf("bind/%s/%d", queue, count), db: db,
				invoke: func() error { _, err := state.BindAttemptTargets(context.Background(), sets); return err },
			})
		}
		for _, count := range []int{1, 16} {
			count := count
			db := &idlePollDB{capture: true, queryErr: errOutboxQueryAllocationSentinel}
			state := newDurableOutboxState(db, cfg, time.Minute)
			evidence := outboxAllocationEvidence(cfg, count)
			cases = append(cases, outboxQueryCorpusCase{
				name: fmt.Sprintf("evidence/%s/%d", queue, count), db: db,
				invoke: func() error { _, err := state.RecordAttemptEvidenceBatch(context.Background(), evidence); return err },
			})
		}
	}
	if len(cases) != 12 {
		t.Fatalf("query corpus has %d cases, want 12", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.invoke(); !errors.Is(err, errOutboxQueryAllocationSentinel) {
				t.Fatalf("first production query error = %v", err)
			}
			if tc.db.queries != 1 || tc.db.sql == "" {
				t.Fatalf("first invocation queries=%d SQL bytes=%d", tc.db.queries, len(tc.db.sql))
			}
			firstSQL, firstArgs := tc.db.sql, append([]any(nil), tc.db.args...)
			tc.db.queries, tc.db.sql, tc.db.args = 0, "", nil
			if err := tc.invoke(); !errors.Is(err, errOutboxQueryAllocationSentinel) {
				t.Fatalf("second production query error = %v", err)
			}
			if tc.db.queries != 1 || firstSQL != tc.db.sql || !reflect.DeepEqual(firstArgs, tc.db.args) {
				t.Fatal("production query or typed arguments were not reproducible")
			}
			fingerprint, err := outboxAllocationFingerprint(tc.name, firstSQL, firstArgs)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(fingerprint)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("OUTBOX_QUERY %s", encoded)
		})
	}
}

func TestOutboxAllocationShardAndEvidenceBoundariesDoNotQuery(t *testing.T) {
	for _, cfg := range outboxAllocationConfigs() {
		cfg := cfg
		t.Run(outboxAllocationQueueName(cfg), func(t *testing.T) {
			db := &idlePollDB{capture: true, queryErr: errOutboxQueryAllocationSentinel}
			state := newDurableOutboxState(db, cfg, time.Minute)
			invalid := outboxAllocationClaimRequest(cfg, false)
			invalid.LogicalShardCount, invalid.LogicalShardIDs = 3, []int{1}
			if windows, err := state.ClaimWindows(context.Background(), invalid); err == nil || windows != nil || db.queries != 0 {
				t.Fatalf("invalid shards reached DB: windows=%v err=%v queries=%d", windows, err, db.queries)
			}
			empty := outboxAllocationClaimRequest(cfg, false)
			empty.LogicalShardCount = store.DispatchOutboxLogicalShards
			empty.LogicalShardIDs = []int{-1, store.DispatchOutboxLogicalShards}
			if windows, err := state.ClaimWindows(context.Background(), empty); err != nil || len(windows) != 0 || db.queries != 0 {
				t.Fatalf("empty shards reached DB: windows=%v err=%v queries=%d", windows, err, db.queries)
			}
			evidence := outboxAllocationEvidence(cfg, 1)[0]
			conflict := evidence
			conflict.ServerMsgID++
			if results, err := state.RecordAttemptEvidenceBatch(context.Background(), []store.OutboxAttemptEvidence{evidence, conflict}); err == nil || results != nil || db.queries != 0 {
				t.Fatalf("conflicting evidence reached DB: results=%v err=%v queries=%d", results, err, db.queries)
			}
		})
	}
}

func BenchmarkOutboxQueryAllocation(b *testing.B) {
	for _, cfg := range outboxAllocationConfigs() {
		cfg := cfg
		queue := outboxAllocationQueueName(cfg)
		for _, scoped := range []bool{false, true} {
			scoped := scoped
			b.Run(fmt.Sprintf("claim/%s/scoped_%t", queue, scoped), func(b *testing.B) {
				db := &idlePollDB{queryErr: errOutboxQueryAllocationSentinel}
				state := newDurableOutboxState(db, cfg, time.Minute)
				req := outboxAllocationClaimRequest(cfg, scoped)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := state.ClaimWindows(ctx, req); !errors.Is(err, errOutboxQueryAllocationSentinel) {
						b.Fatal(err)
					}
				}
			})
		}
		for _, count := range []int{1, 16} {
			count := count
			b.Run(fmt.Sprintf("bind/%s/%d", queue, count), func(b *testing.B) {
				db := &idlePollDB{queryErr: errOutboxQueryAllocationSentinel}
				state := newDurableOutboxState(db, cfg, time.Minute)
				sets := outboxAllocationTargetSets(cfg, count)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := state.BindAttemptTargets(ctx, sets); !errors.Is(err, errOutboxQueryAllocationSentinel) {
						b.Fatal(err)
					}
				}
			})
		}
		for _, count := range []int{1, 16} {
			count := count
			b.Run(fmt.Sprintf("evidence/%s/%d", queue, count), func(b *testing.B) {
				db := &idlePollDB{queryErr: errOutboxQueryAllocationSentinel}
				state := newDurableOutboxState(db, cfg, time.Minute)
				evidence := outboxAllocationEvidence(cfg, count)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := state.RecordAttemptEvidenceBatch(ctx, evidence); !errors.Is(err, errOutboxQueryAllocationSentinel) {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
