package postgres

import (
	"context"
	"testing"
)

func TestDeliveryFenceSequencesRemainGloballyOrderedAcrossBackendsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	connA, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Release()
	connB, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Release()

	var pidA, pidB int
	if err := connA.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pidA); err != nil {
		t.Fatal(err)
	}
	if err := connB.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pidB); err != nil {
		t.Fatal(err)
	}
	if pidA == pidB {
		t.Fatalf("expected distinct PostgreSQL backends, both used pid %d", pidA)
	}

	for _, sequence := range []string{
		"dispatch_outbox_lease_fence_seq",
		"edge_delivery_outbox_lease_fence_seq",
		"channel_delivery_lease_fence_seq",
	} {
		t.Run(sequence, func(t *testing.T) {
			var cacheSize int64
			if err := connA.QueryRow(ctx, `
SELECT cache_size
FROM pg_sequences
WHERE schemaname = 'public' AND sequencename = $1`, sequence).Scan(&cacheSize); err != nil {
				t.Fatal(err)
			}
			if cacheSize != 1 {
				t.Fatalf("%s cache_size=%d, want 1", sequence, cacheSize)
			}

			var firstA, firstB, secondA int64
			if err := connA.QueryRow(ctx, `SELECT nextval($1::regclass)`, "public."+sequence).Scan(&firstA); err != nil {
				t.Fatal(err)
			}
			if err := connB.QueryRow(ctx, `SELECT nextval($1::regclass)`, "public."+sequence).Scan(&firstB); err != nil {
				t.Fatal(err)
			}
			if err := connA.QueryRow(ctx, `SELECT nextval($1::regclass)`, "public."+sequence).Scan(&secondA); err != nil {
				t.Fatal(err)
			}
			if !(firstA < firstB && firstB < secondA) {
				t.Fatalf("%s returned out-of-order fences across backends: A=%d B=%d A=%d", sequence, firstA, firstB, secondA)
			}
		})
	}
}
