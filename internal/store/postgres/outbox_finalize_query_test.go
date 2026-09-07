package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errOutboxFinalizeQueryCaptured = errors.New("outbox finalize SQL captured")

type outboxFinalizeQueryCaptureTx struct {
	pgx.Tx
	capture bool
	query   string
	args    []any
}

func (tx *outboxFinalizeQueryCaptureTx) reset() {
	tx.query = ""
	tx.args = nil
}

func (tx *outboxFinalizeQueryCaptureTx) record(query string, args []any) {
	if !tx.capture {
		return
	}
	tx.query = query
	tx.args = append(tx.args[:0], args...)
}

func (tx *outboxFinalizeQueryCaptureTx) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	tx.record(query, args)
	return nil, errOutboxFinalizeQueryCaptured
}

func (tx *outboxFinalizeQueryCaptureTx) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	tx.record(query, args)
	return outboxFinalizeQueryCaptureRow{}
}

func (tx *outboxFinalizeQueryCaptureTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.record(query, args)
	return pgconn.CommandTag{}, errOutboxFinalizeQueryCaptured
}

type outboxFinalizeQueryCaptureRow struct{}

func (outboxFinalizeQueryCaptureRow) Scan(...any) error { return errOutboxFinalizeQueryCaptured }

type outboxFinalizeQueryFixture struct {
	state      durableOutboxState
	tx         *outboxFinalizeQueryCaptureTx
	lane       outboxLaneRow
	plan       outboxBatchLanePlan
	streamIDs  []int64
	itemIDs    []int64
	fences     []int64
	outcomes   []string
	successors map[int64]outboxBatchSuccessor
}

func newOutboxFinalizeQueryFixture(cfg outboxStateConfig, capture bool) outboxFinalizeQueryFixture {
	sequence := int64(0)
	if cfg.kind == dispatchOutboxStateConfig.kind {
		sequence = 51
	}
	endItemID, endSequence := int64(2001), sequence
	lane := outboxLaneRow{
		streamID: 1001, headItemID: 2001, headSequence: sequence,
		state: "leased", leaseFence: 3001,
		windowEndItemID: &endItemID, windowEndSequence: &endSequence,
	}
	row := outboxWindowAttemptRow{itemID: 2001, sequence: sequence}
	return outboxFinalizeQueryFixture{
		state: newDurableOutboxState(nil, cfg, time.Minute),
		tx:    &outboxFinalizeQueryCaptureTx{capture: capture},
		lane:  lane,
		plan: outboxBatchLanePlan{
			lane: lane, rows: []outboxWindowAttemptRow{row},
			itemIDs: []int64{row.itemID}, outcomes: []string{"applied"},
		},
		streamIDs: []int64{lane.streamID}, itemIDs: []int64{row.itemID},
		fences: []int64{int64(lane.leaseFence)}, outcomes: []string{"applied"},
		successors: map[int64]outboxBatchSuccessor{
			lane.streamID: {itemID: 2002, sequence: sequence + 1, found: true},
		},
	}
}

func (f *outboxFinalizeQueryFixture) invoke(name string) error {
	ctx := context.Background()
	f.tx.reset()
	switch name {
	case "lock_lanes":
		_, err := f.state.loadLockedLaneGroup(ctx, f.tx, f.streamIDs)
		return err
	case "load_window":
		_, err := f.state.loadCurrentWindowGroup(ctx, f.tx, []outboxLaneRow{f.lane})
		return err
	case "apply_terminal":
		_, err := f.state.applyTerminalLaneGroup(ctx, f.tx, f.itemIDs, f.streamIDs, f.fences, f.outcomes, nil, []outboxBatchLanePlan{f.plan})
		return err
	case "load_successor":
		_, err := f.state.loadSuccessorGroup(ctx, f.tx, f.streamIDs)
		return err
	case "release_successor":
		return f.state.releaseLaneGroupToSuccessors(ctx, f.tx, map[int64]outboxLaneRow{f.lane.streamID: f.lane}, f.successors)
	case "delete_empty":
		return f.state.deleteEmptyLaneGroup(ctx, f.tx, map[int64]outboxLaneRow{f.lane.streamID: f.lane}, f.streamIDs)
	default:
		return fmt.Errorf("unknown finalize query %q", name)
	}
}

func captureOutboxFinalizeQueryCorpus(t *testing.T, cfg outboxStateConfig) []outboxQueryFingerprint {
	t.Helper()
	names := []string{"lock_lanes", "load_window", "apply_terminal", "load_successor", "release_successor", "delete_empty"}
	fixture := newOutboxFinalizeQueryFixture(cfg, true)
	fingerprints := make([]outboxQueryFingerprint, 0, len(names))
	for _, name := range names {
		if err := fixture.invoke(name); !errors.Is(err, errOutboxFinalizeQueryCaptured) {
			t.Fatalf("%s: error=%v", name, err)
		}
		if fixture.tx.query == "" {
			t.Fatalf("%s: no SQL captured", name)
		}
		fingerprint, err := outboxAllocationFingerprint(name, fixture.tx.query, fixture.tx.args)
		if err != nil {
			t.Fatalf("%s: fingerprint: %v", name, err)
		}
		fingerprints = append(fingerprints, fingerprint)
	}
	return fingerprints
}

type outboxFinalizeQueryGolden struct {
	sqlSHA256  string
	sqlBytes   int
	argsSHA256 string
}

var outboxFinalizeQueryGoldens = map[string]outboxFinalizeQueryGolden{
	"dispatch/lock_lanes":        {"8c71d4eb0a252c5d16e052f8b6ec08bb2d78fa93be02d941e89c221c4305059a", 233, "a89412695c82b7a31056df2c874f1e5d809865bded5c3c0148bade8bd2f6f7ab"},
	"dispatch/load_window":       {"af9c6e05653a97657482a764963c3dcc0c5a5952bcfc7501c5833f2ff133445f", 957, "7c99a708cba76dc2233733675805c2211881f16cbca4e487a23f9df9d92929fc"},
	"dispatch/apply_terminal":    {"d213d5dc98026bfab22170378fd326f79468502971f88e98ca7b8149f3d41378", 3192, "9251fa6bf43268525793ddcaf09d5529766d426fbc019136d3aad315146277e9"},
	"dispatch/load_successor":    {"9de3b909ecd9bc033c874355b9432dcbd6a7630d7a147a1d0d01f1e1506c82ee", 391, "8d41924ff8392cdf4dfa8a983312be63a88d24882e5aefa660bfd66e876c1aa5"},
	"dispatch/release_successor": {"c4f83ca1f0a09bcc623c65c2c999f0f05b4e729108096a88ee98ea0d34ce53ca", 549, "7cc2875f95e42aae70500c323279ddd5b0699bcc97ef9b8f97ffb89bb78111f2"},
	"dispatch/delete_empty":      {"8772c9c904c8b9b522c277ae2c1f8a327a737e16a46ecc55759275f07842d288", 220, "64562cf427f93524c5c9c25e9fb0dde565caca8b5828892f295823fabbfd10e4"},
	"delivery/lock_lanes":        {"36a773f0757989772406fde27d5b9961b3781a6e203615728fc716c20ab65b9d", 238, "a89412695c82b7a31056df2c874f1e5d809865bded5c3c0148bade8bd2f6f7ab"},
	"delivery/load_window":       {"c4e8f7fd16775a68ece2bf539b29d770a5d5eedfa8e553c57bec3477a30480ee", 875, "908d39b8950a0d46889f2f25b33b8236b25090aee9fe34dad02b4e20a689f728"},
	"delivery/apply_terminal":    {"57bd8456c310beb752acf49030d122c25b40456cacf373d3231da582e00240b3", 3197, "79b015959a2fc06bf0a22b135f4748aa69f5e13dc3fc891d3f9e572c7a0644b7"},
	"delivery/load_successor":    {"fe5a79d8cf4491fdfc96566d0cc6f402fc4c9129b1696c6a93031b3a033a3aca", 381, "9598674ff8581532c1bab9dd397c9f1b8ceaadc7ef317263f779a7e25d357710"},
	"delivery/release_successor": {"c8b2e746a9d5c406ce70584793d7c2caea4805442ffffb34bbb5ef18ef6c0925", 554, "50d5a7f25f090791a926c3a2950f9869ceff01bbb65c1954c4d2b376fc4b3cb4"},
	"delivery/delete_empty":      {"9d1c3fa974e20ce09de521e019f6713d88c0bfb60ecf7ab0d25527b6b8c0491d", 225, "64562cf427f93524c5c9c25e9fb0dde565caca8b5828892f295823fabbfd10e4"},
}

// The golden corpus was captured before changing production query
// preparation. It freezes both SQL bytes and the ordered typed arguments.
func TestOutboxFinalizeQueryCorpus(t *testing.T) {
	seen := make(map[string]bool, len(outboxFinalizeQueryGoldens))
	for _, cfg := range outboxAllocationConfigs() {
		queue := outboxAllocationQueueName(cfg)
		t.Run(queue, func(t *testing.T) {
			first := captureOutboxFinalizeQueryCorpus(t, cfg)
			second := captureOutboxFinalizeQueryCorpus(t, cfg)
			if !reflect.DeepEqual(first, second) {
				t.Fatal("finalize SQL or typed arguments were not reproducible")
			}
			for _, fingerprint := range first {
				key := queue + "/" + fingerprint.Name
				golden, ok := outboxFinalizeQueryGoldens[key]
				if !ok {
					t.Fatalf("missing golden for %s", key)
				}
				if fingerprint.SQLSHA256 != golden.sqlSHA256 || fingerprint.SQLBytes != golden.sqlBytes || fingerprint.ArgsSHA256 != golden.argsSHA256 {
					t.Fatalf("%s changed: SQL=%s/%d args=%s, want SQL=%s/%d args=%s", key,
						fingerprint.SQLSHA256, fingerprint.SQLBytes, fingerprint.ArgsSHA256,
						golden.sqlSHA256, golden.sqlBytes, golden.argsSHA256)
				}
				seen[key] = true
				encoded, err := json.Marshal(fingerprint)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("OUTBOX_FINALIZE_QUERY queue=%s %s", queue, encoded)
			}
		})
	}
	if len(seen) != len(outboxFinalizeQueryGoldens) {
		t.Fatalf("checked %d finalize query goldens, want %d", len(seen), len(outboxFinalizeQueryGoldens))
	}
}

func BenchmarkOutboxFinalizeQueryPreparation(b *testing.B) {
	for _, cfg := range outboxAllocationConfigs() {
		cfg := cfg
		b.Run(outboxAllocationQueueName(cfg), func(b *testing.B) {
			fixture := newOutboxFinalizeQueryFixture(cfg, false)
			names := []string{"lock_lanes", "load_window", "apply_terminal", "load_successor", "delete_empty"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, name := range names {
					if err := fixture.invoke(name); !errors.Is(err, errOutboxFinalizeQueryCaptured) {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

var outboxFinalizeQuerySelectionSink int

func BenchmarkOutboxFinalizePrecomputedQuerySelection(b *testing.B) {
	for _, cfg := range outboxAllocationConfigs() {
		cfg := cfg
		b.Run(outboxAllocationQueueName(cfg), func(b *testing.B) {
			queries := [...]string{
				cfg.finalizeLockLaneGroupSQL,
				cfg.finalizeLoadCurrentWindowSQL,
				cfg.finalizeApplyTerminalGroupSQL,
				cfg.finalizeLoadSuccessorGroupSQL,
				cfg.finalizeReleaseSuccessorGroupSQL,
				cfg.finalizeDeleteEmptyLaneGroupSQL,
			}
			b.ReportAllocs()
			b.ResetTimer()
			total := 0
			for i := 0; i < b.N; i++ {
				for _, query := range queries {
					total += len(query)
				}
			}
			outboxFinalizeQuerySelectionSink = total
		})
	}
}
