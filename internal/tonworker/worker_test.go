package tonworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
)

const testCollectionAddress = "0:2222222222222222222222222222222222222222222222222222222222222222"

func TestTickMintDisabledDoesNotClaimMintJobs(t *testing.T) {
	store := &fakeStore{}
	chain := &fakeChain{}
	worker := newTestWorker(t, store, chain, false)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if containsKind(store.claimKinds, domain.StarGiftTONJobMint) {
		t.Fatalf("claimed kinds = %v, must exclude mint while kill switch is disabled", store.claimKinds)
	}
	if chain.submitCalls != 0 {
		t.Fatalf("SubmitMint calls = %d, want 0", chain.submitCalls)
	}
}

func TestTickPersistsSubmissionEvidenceWhenSendOutcomeIsUncertain(t *testing.T) {
	job := testTONJob(domain.StarGiftTONJobMint)
	store := &fakeStore{jobs: []domain.StarGiftTONJob{job}}
	chain := &fakeChain{
		preparation: MintPreparation{SubmissionHash: bytesOf(32, 1), SubmissionBOC: []byte{0xb5, 0xee, 0x9c, 0x72}, ValidUntil: 1_700_000_120},
		dispatch:    MintDispatch{Dispatched: true},
		sendErr:     context.DeadlineExceeded,
	}
	worker := newTestWorker(t, store, chain, true)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(store.preparations) != 1 || len(store.submissions) != 1 {
		t.Fatalf("preparations/submissions = %d/%d, want 1/1", len(store.preparations), len(store.submissions))
	}
	if len(store.failures) != 0 {
		t.Fatalf("failures = %d, durable uncertain send must move to confirmation", len(store.failures))
	}
}

func TestRestartedWorkerReusesPreparedAssetWithoutBuildingNewBOC(t *testing.T) {
	job := testTONJob(domain.StarGiftTONJobMint)
	job.Asset = &domain.StarGiftTONAsset{
		ExportID: job.ExportID, SubmissionHash: bytesOf(32, 7), SubmissionBOC: []byte{0xb5, 0xee, 0x9c, 0x72},
		PreparedAt: 1_699_999_900, ValidUntil: 1_700_000_120,
	}
	store := &fakeStore{jobs: []domain.StarGiftTONJob{job}}
	chain := &fakeChain{dispatch: MintDispatch{Dispatched: true}}
	worker := newTestWorker(t, store, chain, true)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if chain.submitCalls != 0 {
		t.Fatalf("BuildMint calls after restart = %d, want 0", chain.submitCalls)
	}
	if len(store.preparations) != 0 || len(store.submissions) != 1 {
		t.Fatalf("prepared/submitted writes after restart = %d/%d, want 0/1", len(store.preparations), len(store.submissions))
	}
}

func TestTickRecordsFinalConfirmation(t *testing.T) {
	job := testTONJob(domain.StarGiftTONJobConfirm)
	job.Asset = &domain.StarGiftTONAsset{ExportID: job.ExportID, SubmissionHash: bytesOf(32, 1), SubmissionBOC: []byte{1}}
	store := &fakeStore{jobs: []domain.StarGiftTONJob{job}}
	chain := &fakeChain{confirmed: true, confirmation: MintConfirmation{
		OwnerAddress: "EQOwner", TxHash: bytesOf(32, 2), TxLT: "123", BlockSeqno: 456, FinalityDepth: 12,
	}}
	worker := newTestWorker(t, store, chain, true)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(store.confirmations) != 1 || store.confirmations[0].OwnerAddress != "EQOwner" {
		t.Fatalf("confirmations = %+v", store.confirmations)
	}
}

func TestTickPersistsFirstFinalityAnchorBeforeRetry(t *testing.T) {
	job := testTONJob(domain.StarGiftTONJobConfirm)
	job.Asset = &domain.StarGiftTONAsset{
		ExportID: job.ExportID, SubmissionHash: bytesOf(32, 1), SubmissionBOC: []byte{1},
		SubmittedAt: 1_699_999_990,
	}
	store := &fakeStore{jobs: []domain.StarGiftTONJob{job}}
	chain := &fakeChain{confirmation: MintConfirmation{
		TxHash: bytesOf(32, 2), TxLT: "123", BlockSeqno: 456,
	}}
	worker := newTestWorker(t, store, chain, true)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(store.mintObservations) != 1 || store.mintObservations[0].BlockSeqno != 456 ||
		store.mintObservations[0].ObservedAt != 1_700_000_000 {
		t.Fatalf("mint observations = %+v", store.mintObservations)
	}
	if len(store.confirmations) != 0 || len(store.failures) != 1 || store.failures[0].Terminal {
		t.Fatalf("confirmations/failures = %+v/%+v", store.confirmations, store.failures)
	}
}

func TestTickMarksPermanentChainFailureTerminal(t *testing.T) {
	job := testTONJob(domain.StarGiftTONJobMint)
	store := &fakeStore{jobs: []domain.StarGiftTONJob{job}}
	chain := &fakeChain{buildErr: Permanent(errors.New("collection code hash mismatch"))}
	worker := newTestWorker(t, store, chain, true)

	if err := worker.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(store.failures) != 1 || !store.failures[0].Terminal {
		t.Fatalf("failures = %+v, want terminal", store.failures)
	}
}

func TestRunSignalsReadyOnlyAfterDurableAdmission(t *testing.T) {
	store := &fakeStore{}
	chain := &fakeChain{}
	ctx, cancel := context.WithCancel(context.Background())
	ready := false
	worker, err := New(store, chain, Config{
		WorkerID: "ton-ready-a", InitialItemIndex: "17", MintABI: domain.StarGiftTONMintABIBasicCollectionV1, MintEnabled: true, Batch: 1,
		PollInterval: time.Second, LeaseTimeout: 30 * time.Second, RequestTimeout: 10 * time.Second,
		OnReady: func(domain.StarGiftTONChainState) {
			if len(store.admissions) != 1 {
				t.Fatalf("ready before durable admission: %+v", store.admissions)
			}
			ready = true
			cancel()
		},
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	worker.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !ready {
		t.Fatal("worker did not signal ready")
	}

	store = &fakeStore{admissionErr: errors.New("admission rejected")}
	ready = false
	worker, err = New(store, chain, Config{
		WorkerID: "ton-not-ready-a", InitialItemIndex: "17", MintABI: domain.StarGiftTONMintABIBasicCollectionV1, MintEnabled: true, Batch: 1,
		PollInterval: time.Second, LeaseTimeout: 30 * time.Second, RequestTimeout: 10 * time.Second,
		OnReady: func(domain.StarGiftTONChainState) { ready = true },
	}, nil)
	if err != nil {
		t.Fatalf("New rejected worker: %v", err)
	}
	worker.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	if err := worker.Run(context.Background()); err == nil || ready {
		t.Fatalf("Run admission failure err=%v ready=%v", err, ready)
	}
}

func newTestWorker(t *testing.T, store Store, chain Chain, mintEnabled bool) *Worker {
	t.Helper()
	worker, err := New(store, chain, Config{
		WorkerID: "ton-test-a", InitialItemIndex: "17", MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		MintEnabled: mintEnabled, Batch: 10,
		PollInterval: time.Second, LeaseTimeout: 30 * time.Second,
		RequestTimeout: 10 * time.Second, RetryDelay: time.Second, OwnerCheckInterval: time.Minute,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	worker.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	return worker
}

func testTONJob(kind domain.StarGiftTONJobKind) domain.StarGiftTONJob {
	return domain.StarGiftTONJob{
		ID: 11, ExportID: 7, Kind: kind, Status: domain.StarGiftTONJobLeased,
		LeaseOwner: "ton-test-a", LeaseExpiresAt: 1_700_000_030, FencingToken: 3,
		Export: domain.StarGiftTONExport{
			ID: 7, Network: domain.TONNetworkMainnet, MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
			WalletAddress: "EQOwner",
		},
	}
}

func containsKind(kinds []domain.StarGiftTONJobKind, want domain.StarGiftTONJobKind) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}

func bytesOf(size int, value byte) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = value
	}
	return out
}

type fakeStore struct {
	jobs             []domain.StarGiftTONJob
	claimKinds       []domain.StarGiftTONJobKind
	preparations     []domain.StarGiftTONMintPreparation
	submissions      []domain.StarGiftTONMintSubmission
	mintObservations []domain.StarGiftTONMintObservation
	confirmations    []domain.StarGiftTONMintConfirmation
	observations     []domain.StarGiftTONOwnerObservation
	failures         []domain.StarGiftTONJobFailure
	admissions       []domain.StarGiftTONAdmissionRequest
	admissionErr     error
}

func (s *fakeStore) RegisterTONAdmission(_ context.Context, req domain.StarGiftTONAdmissionRequest) error {
	s.admissions = append(s.admissions, req)
	return s.admissionErr
}

func (s *fakeStore) ClaimTONJobs(_ context.Context, _ string, kinds []domain.StarGiftTONJobKind, _, _, _ int) ([]domain.StarGiftTONJob, error) {
	s.claimKinds = append([]domain.StarGiftTONJobKind(nil), kinds...)
	return s.jobs, nil
}

func (s *fakeStore) RenewTONJob(context.Context, int64, string, int64, int, int) error { return nil }

func (s *fakeStore) RecordTONItemResolved(_ context.Context, resolution domain.StarGiftTONItemResolution) (domain.StarGiftTONExport, error) {
	return domain.StarGiftTONExport{ID: resolution.JobID, ExpectedItemAddress: resolution.ItemAddress}, nil
}

func (s *fakeStore) RecordTONMintPrepared(_ context.Context, preparation domain.StarGiftTONMintPreparation) (domain.StarGiftTONAsset, error) {
	s.preparations = append(s.preparations, preparation)
	return domain.StarGiftTONAsset{
		ExportID: preparation.JobID, SubmissionHash: preparation.SubmissionHash, SubmissionBOC: preparation.SubmissionBOC,
		PreparedAt: preparation.PreparedAt, ValidUntil: preparation.ValidUntil,
	}, nil
}

func (s *fakeStore) RecordTONMintSubmitted(_ context.Context, submission domain.StarGiftTONMintSubmission) (domain.StarGiftTONAsset, error) {
	s.submissions = append(s.submissions, submission)
	return domain.StarGiftTONAsset{}, nil
}

func (s *fakeStore) RecordTONMintObserved(_ context.Context, observation domain.StarGiftTONMintObservation) (domain.StarGiftTONAsset, error) {
	s.mintObservations = append(s.mintObservations, observation)
	return domain.StarGiftTONAsset{MintTxHash: observation.TxHash, MintTxLT: observation.TxLT, MintBlockSeqno: observation.BlockSeqno}, nil
}

func (s *fakeStore) RecordTONMintConfirmed(_ context.Context, confirmation domain.StarGiftTONMintConfirmation) (domain.StarGiftTONAsset, error) {
	s.confirmations = append(s.confirmations, confirmation)
	return domain.StarGiftTONAsset{}, nil
}

func (s *fakeStore) RecordTONOwnerObserved(_ context.Context, observation domain.StarGiftTONOwnerObservation) (domain.StarGiftTONAsset, error) {
	s.observations = append(s.observations, observation)
	return domain.StarGiftTONAsset{}, nil
}

func (s *fakeStore) FailTONJob(_ context.Context, failure domain.StarGiftTONJobFailure) error {
	s.failures = append(s.failures, failure)
	return nil
}

type fakeChain struct {
	preparation  MintPreparation
	buildErr     error
	dispatch     MintDispatch
	sendErr      error
	confirmation MintConfirmation
	confirmed    bool
	confirmErr   error
	observation  OwnerObservation
	observeErr   error
	submitCalls  int
}

func (c *fakeChain) Preflight(context.Context, bool) (domain.StarGiftTONChainState, error) {
	return domain.StarGiftTONChainState{
		Network: domain.TONNetworkMainnet, CollectionAddress: testCollectionAddress,
		CollectionCodeHash: bytesOf(32, 3), ObservedNextItemIndex: "17",
	}, nil
}

func (c *fakeChain) ResolveItemAddress(context.Context, domain.StarGiftTONExport) (string, error) {
	return "0:1111111111111111111111111111111111111111111111111111111111111111", nil
}

func (c *fakeChain) BuildMint(context.Context, domain.StarGiftTONExport) (MintPreparation, error) {
	c.submitCalls++
	return c.preparation, c.buildErr
}

func (c *fakeChain) SendMint(context.Context, domain.StarGiftTONExport, domain.StarGiftTONAsset) (MintDispatch, error) {
	return c.dispatch, c.sendErr
}

func (c *fakeChain) ConfirmMint(context.Context, domain.StarGiftTONExport, domain.StarGiftTONAsset) (MintConfirmation, bool, error) {
	return c.confirmation, c.confirmed, c.confirmErr
}

func (c *fakeChain) ObserveOwner(context.Context, domain.StarGiftTONExport, domain.StarGiftTONAsset) (OwnerObservation, error) {
	return c.observation, c.observeErr
}
