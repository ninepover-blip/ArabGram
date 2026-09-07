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

	"telesrv/internal/store"
)

// These fakes expose the DBTX/transaction boundaries, not an alternate store.
// Unexpected operations panic rather than silently emulating production work.
type idlePollDB struct {
	pgx.Tx
	rows                                idlePollRows
	row                                 idlePollRow
	beginErr, queryErr, commitErr       error
	begins, commits, rollbacks, queries int
	sql                                 string
	args                                []any
	capture                             bool
}

func (d *idlePollDB) Begin(context.Context) (pgx.Tx, error) {
	d.begins++
	d.rows.index, d.rows.closed = 0, false
	return d, d.beginErr
}

func (d *idlePollDB) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	d.queries++
	if d.capture {
		d.sql, d.args = query, append([]any(nil), args...)
	}
	return &d.rows, d.queryErr
}

func (d *idlePollDB) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	d.queries++
	if d.capture {
		d.sql, d.args = query, append([]any(nil), args...)
	}
	return &d.row
}

func (*idlePollDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec")
}

func (d *idlePollDB) Commit(context.Context) error   { d.commits++; return d.commitErr }
func (d *idlePollDB) Rollback(context.Context) error { d.rollbacks++; return nil }

type idlePollRows struct {
	pgx.Rows
	count, index, failScanAt int
	closed                   bool
	scanErr, err             error
}

func (r *idlePollRows) Next() bool { r.index++; return r.index <= r.count }
func (r *idlePollRows) Close()     { r.closed = true }
func (r *idlePollRows) Err() error { return r.err }
func (r *idlePollRows) Scan(dest ...any) error {
	if r.scanErr != nil && (r.failScanAt == 0 || r.index == r.failScanAt) {
		return r.scanErr
	}
	*dest[0].(*string) = "dialog_light"
	*dest[1].(*int64) = int64(r.index)
	*dest[2].(*string) = "user"
	*dest[3].(*int64) = int64(r.index + 100)
	*dest[4].(*int64) = 7
	*dest[5].(*int64) = 987
	return nil
}

type idlePollRow struct {
	err             error
	observed, ready time.Time
	recover         bool
}

func (r *idlePollRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*time.Time), *dest[1].(*time.Time), *dest[2].(*bool) = r.observed, r.ready, r.recover
	return nil
}

func TestIdleRelayPreservesTransactionAndRows(t *testing.T) {
	for _, count := range []int{0, 1, 256} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db := &idlePollDB{rows: idlePollRows{count: count}, capture: true}
			r := &ReadModelInvalidationRelay{db: db, beginner: db, owner: "allocation-test"}
			items, err := r.claim(context.Background())
			if err != nil || len(items) != count || db.begins != 1 || db.commits != 1 || db.rollbacks != 1 || !db.rows.closed {
				t.Fatalf("claim count=%d err=%v transaction=%d/%d/%d closed=%v", len(items), err, db.begins, db.commits, db.rollbacks, db.rows.closed)
			}
			if !reflect.DeepEqual(db.args, []any{256, "allocation-test", int64(2000)}) {
				t.Fatalf("claim arguments changed: %#v", db.args)
			}
			for i, item := range items {
				if item.Key.Model != "dialog_light" || item.Key.OwnerUserID != int64(i+1) || item.Key.PeerType != "user" || item.Key.PeerID != int64(i+101) || item.Version != 7 || item.Hash != 987 {
					t.Fatalf("claim row %d corrupted: %+v", i, item)
				}
			}
		})
	}
}

func TestIdleRelayFailureNeverReturnsPartialBatch(t *testing.T) {
	sentinel := errors.New("injected database failure")
	for _, stage := range []string{"begin", "query", "scan", "iteration", "commit"} {
		t.Run(stage, func(t *testing.T) {
			db := &idlePollDB{rows: idlePollRows{count: 2}}
			switch stage {
			case "begin":
				db.beginErr = sentinel
			case "query":
				db.queryErr = sentinel
			case "scan":
				db.rows.scanErr, db.rows.failScanAt = sentinel, 2
			case "iteration":
				db.rows.err = sentinel
			case "commit":
				db.commitErr = sentinel
			}
			r := &ReadModelInvalidationRelay{db: db, beginner: db, owner: "allocation-test"}
			items, err := r.claim(context.Background())
			if !errors.Is(err, sentinel) || items != nil {
				t.Fatalf("partial/silent result items=%v err=%v", items, err)
			}
			wantCommit, wantRollback := 0, 1
			if stage == "commit" {
				wantCommit = 1
			}
			if stage == "begin" {
				wantRollback = 0
			}
			if db.commits != wantCommit || db.rollbacks != wantRollback || (stage != "begin" && stage != "query" && !db.rows.closed) {
				t.Fatalf("failure cleanup commit=%d rollback=%d closed=%v", db.commits, db.rollbacks, db.rows.closed)
			}
		})
	}
}

func TestIdleOutboxReadinessBoundaries(t *testing.T) {
	for _, cfg := range []outboxStateConfig{dispatchOutboxStateConfig, deliveryOutboxStateConfig} {
		t.Run(cfg.itemsTable, func(t *testing.T) {
			db := &idlePollDB{capture: true}
			s := newDurableOutboxState(db, cfg, time.Minute)
			for _, tc := range []struct {
				name         string
				count        int
				ids          []int
				query, valid bool
			}{
				{"all", 0, nil, true, true}, {"scoped", 256, []int{8, 1, 8, -1, 256}, true, true},
				{"empty", 256, nil, false, true}, {"filtered_empty", 256, []int{-1, 256}, false, true},
				{"bad_count", 3, []int{1}, false, false}, {"missing_count", 0, []int{1}, false, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db.queries = 0
					db.row.err = pgx.ErrNoRows
					_, ok, err := s.NextReadyAt(context.Background(), cfg.kind, tc.count, tc.ids)
					if ok || (err == nil) != tc.valid || (db.queries == 1) != tc.query {
						t.Fatalf("boundary ok=%v err=%v queries=%d", ok, err, db.queries)
					}
					if tc.name == "scoped" && !reflect.DeepEqual(db.args, []any{[]int16{1, 8}}) {
						t.Fatalf("shards = %#v", db.args)
					}
					if tc.name == "all" && len(db.args) != 0 {
						t.Fatalf("all-shards arguments = %#v", db.args)
					}
				})
			}
			db.queries = 0
			if _, ok, err := s.NextReadyAt(context.Background(), store.OutboxQueueChannelPTS, 0, nil); err == nil || ok || db.queries != 0 {
				t.Fatal("wrong queue reached database")
			}
			for _, failure := range []error{context.Canceled, errors.New("query failure")} {
				db.row.err = failure
				if _, ok, err := s.NextReadyAt(context.Background(), cfg.kind, 0, nil); ok || !errors.Is(err, failure) {
					t.Fatalf("query failure lost: %v", err)
				}
			}
			db.row = idlePollRow{observed: time.Unix(300, 0), ready: time.Unix(350, 0)}
			for _, recover := range []bool{false, true} {
				db.row.recover = recover
				value, ok, err := s.NextReadyAt(context.Background(), cfg.kind, 0, nil)
				want := store.OutboxReadyClaim
				if recover {
					want = store.OutboxReadyRecoverLease
				}
				if err != nil || !ok || value.ObservedAt != db.row.observed || value.ReadyAt != db.row.ready || value.Kind != want {
					t.Fatalf("deadline result changed: %+v %v", value, err)
				}
			}
		})
	}
}

// Verbose output is captured once for each implementation and compared as raw
// query/argument data. No expected SQL is generated by the implementation helper.
func TestIdlePollQueryCorpus(t *testing.T) {
	for _, cfg := range []outboxStateConfig{dispatchOutboxStateConfig, deliveryOutboxStateConfig} {
		for _, scoped := range []bool{false, true} {
			db := &idlePollDB{capture: true, row: idlePollRow{err: pgx.ErrNoRows}}
			s := newDurableOutboxState(db, cfg, time.Minute)
			count := 0
			var ids []int
			if scoped {
				count, ids = 256, []int{7, 2, 7, -1, 256}
			}
			if _, ok, err := s.NextReadyAt(context.Background(), cfg.kind, count, ids); err != nil || ok {
				t.Fatal(err)
			}
			data, err := json.Marshal(struct {
				Name, SQL string
				Args      []any
			}{fmt.Sprintf("%s/scoped_%v", cfg.itemsTable, scoped), db.sql, db.args})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("IDLE_QUERY %s", data)
		}
	}
	db := &idlePollDB{capture: true}
	r := &ReadModelInvalidationRelay{db: db, beginner: db, owner: "corpus-owner"}
	if _, err := r.claim(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Name, SQL string
		Args      []any
	}{"relay", db.sql, db.args})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("IDLE_QUERY %s", data)
}

func BenchmarkIdleRelayClaim(b *testing.B) {
	for _, count := range []int{0, 1, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db := &idlePollDB{rows: idlePollRows{count: count}}
			r := &ReadModelInvalidationRelay{db: db, beginner: db, owner: "allocation-bench"}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				items, err := r.claim(ctx)
				if err != nil || len(items) != count {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkIdleOutboxNextReady(b *testing.B) {
	for _, cfg := range []outboxStateConfig{dispatchOutboxStateConfig, deliveryOutboxStateConfig} {
		for _, scoped := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/scoped_%v", cfg.itemsTable, scoped), func(b *testing.B) {
				db := &idlePollDB{row: idlePollRow{err: pgx.ErrNoRows}}
				s := newDurableOutboxState(db, cfg, time.Minute)
				count := 0
				var ids []int
				if scoped {
					count, ids = 256, []int{7, 2, 7}
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, ok, err := s.NextReadyAt(ctx, cfg.kind, count, ids); err != nil || ok {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
