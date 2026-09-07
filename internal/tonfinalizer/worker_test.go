package tonfinalizer

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

func TestTickAppliesOnlyCoreFinalizationJobs(t *testing.T) {
	for _, kind := range []domain.StarGiftTONJobKind{domain.StarGiftTONJobFinalize, domain.StarGiftTONJobSyncOwner} {
		t.Run(string(kind), func(t *testing.T) {
			job := finalizerTestJob(kind)
			store := &fakeFinalizerStore{jobs: []domain.StarGiftTONJob{job}}
			var changed domain.StarGiftTONFinalizationResult
			worker := newTestFinalizer(t, store, func(result domain.StarGiftTONFinalizationResult) { changed = result })
			if err := worker.Tick(context.Background()); err != nil {
				t.Fatalf("Tick: %v", err)
			}
			if kind == domain.StarGiftTONJobFinalize && len(store.finalizations) != 1 {
				t.Fatalf("finalizations = %d, want 1", len(store.finalizations))
			}
			if kind == domain.StarGiftTONJobSyncOwner && len(store.ownerSyncs) != 1 {
				t.Fatalf("owner syncs = %d, want 1", len(store.ownerSyncs))
			}
			if len(store.failures) != 0 || changed.Export.ID != job.ExportID {
				t.Fatalf("failures=%+v changed=%+v", store.failures, changed)
			}
		})
	}
}

func TestTickRetriesFailedAndRejectsUnexpectedJobs(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  domain.StarGiftTONJob
		err  error
	}{
		{name: "finalize conflict", job: finalizerTestJob(domain.StarGiftTONJobFinalize), err: domain.ErrStarGiftTONExportStateConflict},
		{name: "chain job", job: finalizerTestJob(domain.StarGiftTONJobMint)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeFinalizerStore{jobs: []domain.StarGiftTONJob{tc.job}, finalizeErr: tc.err}
			worker := newTestFinalizer(t, store, nil)
			if err := worker.Tick(context.Background()); err != nil {
				t.Fatalf("Tick: %v", err)
			}
			if len(store.failures) != 1 || store.failures[0].JobID != tc.job.ID || store.failures[0].RetryAt != 1_700_000_005 {
				t.Fatalf("failures = %+v", store.failures)
			}
		})
	}
}

func newTestFinalizer(t *testing.T, finalizerStore *fakeFinalizerStore, changed func(domain.StarGiftTONFinalizationResult)) *Worker {
	t.Helper()
	worker, err := New(finalizerStore, Config{
		WorkerID: "core-a:ton-finalizer", Batch: 10, PollInterval: time.Second,
		LeaseTimeout: 30 * time.Second, RequestTimeout: 10 * time.Second, RetryDelay: 5 * time.Second,
		OnChanged: changed,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	worker.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return worker
}

func finalizerTestJob(kind domain.StarGiftTONJobKind) domain.StarGiftTONJob {
	return domain.StarGiftTONJob{
		ID: 11, ExportID: 7, Kind: kind, Status: domain.StarGiftTONJobLeased,
		LeaseOwner: "core-a:ton-finalizer", LeaseExpiresAt: 1_700_000_030, FencingToken: 3,
	}
}

type fakeFinalizerStore struct {
	jobs          []domain.StarGiftTONJob
	finalizations []domain.StarGiftTONFinalization
	ownerSyncs    []domain.StarGiftTONFinalization
	failures      []domain.StarGiftTONJobFailure
	finalizeErr   error
}

func (s *fakeFinalizerStore) ClaimTONCoreJobs(context.Context, string, int, int, int) ([]domain.StarGiftTONJob, error) {
	return s.jobs, nil
}

func (s *fakeFinalizerStore) RenewTONCoreJob(context.Context, int64, string, int64, int, int) error {
	return nil
}

func (s *fakeFinalizerStore) FinalizeTONExport(_ context.Context, finalization domain.StarGiftTONFinalization) (domain.StarGiftTONFinalizationResult, error) {
	s.finalizations = append(s.finalizations, finalization)
	if s.finalizeErr != nil {
		return domain.StarGiftTONFinalizationResult{}, s.finalizeErr
	}
	return domain.StarGiftTONFinalizationResult{Export: domain.StarGiftTONExport{ID: 7}}, nil
}

func (s *fakeFinalizerStore) SyncTONOwner(_ context.Context, finalization domain.StarGiftTONFinalization) (domain.StarGiftTONFinalizationResult, error) {
	s.ownerSyncs = append(s.ownerSyncs, finalization)
	return domain.StarGiftTONFinalizationResult{Export: domain.StarGiftTONExport{ID: 7}}, nil
}

func (s *fakeFinalizerStore) FailTONCoreJob(_ context.Context, failure domain.StarGiftTONJobFailure) error {
	s.failures = append(s.failures, failure)
	return nil
}
