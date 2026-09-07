// Package tonworker executes durable TON chain jobs without owning Telegram
// gift lifecycle or PTS state.
package tonworker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

type Store interface {
	RegisterTONAdmission(ctx context.Context, req domain.StarGiftTONAdmissionRequest) error
	ClaimTONJobs(ctx context.Context, workerID string, kinds []domain.StarGiftTONJobKind, now, leaseSeconds, limit int) ([]domain.StarGiftTONJob, error)
	RenewTONJob(ctx context.Context, jobID int64, workerID string, fencingToken int64, now, leaseSeconds int) error
	RecordTONItemResolved(ctx context.Context, resolution domain.StarGiftTONItemResolution) (domain.StarGiftTONExport, error)
	RecordTONMintPrepared(ctx context.Context, preparation domain.StarGiftTONMintPreparation) (domain.StarGiftTONAsset, error)
	RecordTONMintSubmitted(ctx context.Context, submission domain.StarGiftTONMintSubmission) (domain.StarGiftTONAsset, error)
	RecordTONMintObserved(ctx context.Context, observation domain.StarGiftTONMintObservation) (domain.StarGiftTONAsset, error)
	RecordTONMintConfirmed(ctx context.Context, confirmation domain.StarGiftTONMintConfirmation) (domain.StarGiftTONAsset, error)
	RecordTONOwnerObserved(ctx context.Context, observation domain.StarGiftTONOwnerObservation) (domain.StarGiftTONAsset, error)
	FailTONJob(ctx context.Context, failure domain.StarGiftTONJobFailure) error
}

type Chain interface {
	Preflight(ctx context.Context, mintEnabled bool) (domain.StarGiftTONChainState, error)
	ResolveItemAddress(ctx context.Context, export domain.StarGiftTONExport) (string, error)
	BuildMint(ctx context.Context, export domain.StarGiftTONExport) (MintPreparation, error)
	SendMint(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (MintDispatch, error)
	ConfirmMint(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (MintConfirmation, bool, error)
	ObserveOwner(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (OwnerObservation, error)
}

type MintPreparation struct {
	SubmissionHash []byte
	SubmissionBOC  []byte
	ValidUntil     int
}

func (p MintPreparation) Valid(now int) bool {
	return len(p.SubmissionHash) == 32 && len(p.SubmissionBOC) > 0 && len(p.SubmissionBOC) <= 65536 && p.ValidUntil > now
}

type MintDispatch struct {
	Dispatched bool
	TxHash     []byte
	TxLT       string
	BlockSeqno int64
}

func (d MintDispatch) Valid() bool {
	return (len(d.TxHash) == 0 && d.TxLT == "" && d.BlockSeqno == 0) ||
		(len(d.TxHash) == 32 && validLogicalTime(d.TxLT) && d.BlockSeqno >= 0)
}

type MintConfirmation struct {
	OwnerAddress  string
	TxHash        []byte
	TxLT          string
	BlockSeqno    int64
	FinalityDepth int
}

func (c MintConfirmation) Valid() bool {
	return strings.TrimSpace(c.OwnerAddress) != "" && len(c.TxHash) == 32 &&
		validLogicalTime(c.TxLT) && c.BlockSeqno >= 0 && c.FinalityDepth > 0
}

func (c MintConfirmation) HasObservation() bool {
	return len(c.TxHash) != 0 || c.TxLT != "" || c.BlockSeqno != 0
}

func (c MintConfirmation) ObservationValid() bool {
	return len(c.TxHash) == 32 && validLogicalTime(c.TxLT) && c.BlockSeqno > 0
}

type OwnerObservation struct {
	OwnerAddress string
	ObservedLT   string
}

func (o OwnerObservation) Valid() bool {
	return strings.TrimSpace(o.OwnerAddress) != "" && validLogicalTime(o.ObservedLT)
}

type Config struct {
	WorkerID           string
	InitialItemIndex   string
	MintABI            string
	MintEnabled        bool
	Batch              int
	PollInterval       time.Duration
	LeaseTimeout       time.Duration
	RequestTimeout     time.Duration
	RetryDelay         time.Duration
	OwnerCheckInterval time.Duration
	OnReady            func(domain.StarGiftTONChainState)
}

type Worker struct {
	store  Store
	chain  Chain
	cfg    Config
	logger *zap.Logger
	now    func() time.Time
}

func New(store Store, chain Chain, cfg Config, logger *zap.Logger) (*Worker, error) {
	if store == nil || chain == nil {
		return nil, fmt.Errorf("TON worker store and chain are required")
	}
	cfg.WorkerID = strings.TrimSpace(cfg.WorkerID)
	cfg.MintABI = strings.TrimSpace(cfg.MintABI)
	initialItemIndex, err := strconv.ParseUint(strings.TrimSpace(cfg.InitialItemIndex), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid TON worker initial item index")
	}
	cfg.InitialItemIndex = strconv.FormatUint(initialItemIndex, 10)
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 5 * time.Second
	}
	if cfg.OwnerCheckInterval == 0 {
		cfg.OwnerCheckInterval = time.Minute
	}
	if cfg.WorkerID == "" || len(cfg.WorkerID) > 128 || !domain.ValidStarGiftTONMintABI(cfg.MintABI) || cfg.Batch < 1 || cfg.Batch > 100 ||
		cfg.PollInterval <= 0 || cfg.LeaseTimeout < 10*time.Second || cfg.LeaseTimeout > time.Hour ||
		cfg.RequestTimeout < time.Second || cfg.RequestTimeout >= cfg.LeaseTimeout ||
		cfg.RetryDelay <= 0 || cfg.OwnerCheckInterval <= 0 {
		return nil, fmt.Errorf("invalid TON worker config")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Worker{store: store, chain: chain, cfg: cfg, logger: logger, now: time.Now}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	state, err := w.refreshAdmission(ctx)
	if err != nil {
		return err
	}
	if w.cfg.OnReady != nil {
		w.cfg.OnReady(state)
	}
	for {
		if err := w.tickJobs(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("TON worker tick failed", zap.Error(err))
		}
		timer := time.NewTimer(w.cfg.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		if _, err := w.refreshAdmission(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("TON worker admission refresh failed", zap.Error(err))
		}
	}
}

func (w *Worker) Tick(ctx context.Context) error {
	if _, err := w.refreshAdmission(ctx); err != nil {
		return err
	}
	return w.tickJobs(ctx)
}

func (w *Worker) refreshAdmission(ctx context.Context) (domain.StarGiftTONChainState, error) {
	preflightCtx, cancel := context.WithTimeout(ctx, w.cfg.RequestTimeout)
	state, err := w.chain.Preflight(preflightCtx, w.cfg.MintEnabled)
	cancel()
	if err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("TON preflight: %w", err)
	}
	if !state.Valid() {
		return domain.StarGiftTONChainState{}, fmt.Errorf("TON preflight returned invalid collection state")
	}
	now := int(w.now().Unix())
	if err := w.store.RegisterTONAdmission(ctx, domain.StarGiftTONAdmissionRequest{
		StarGiftTONChainState: state, InitialItemIndex: w.cfg.InitialItemIndex,
		MintABI:          w.cfg.MintABI,
		WorkerInstanceID: w.cfg.WorkerID, MintEnabled: w.cfg.MintEnabled,
		ObservedAt: now, LeaseExpiresAt: now + durationSeconds(w.cfg.LeaseTimeout),
	}); err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("register TON worker admission: %w", err)
	}
	return state, nil
}

func (w *Worker) tickJobs(ctx context.Context) error {
	kinds := []domain.StarGiftTONJobKind{domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobConfirm, domain.StarGiftTONJobReconcileOwner}
	if w.cfg.MintEnabled {
		kinds = append([]domain.StarGiftTONJobKind{domain.StarGiftTONJobMint}, kinds...)
	}
	now := int(w.now().Unix())
	jobs, err := w.store.ClaimTONJobs(ctx, w.cfg.WorkerID, kinds, now, durationSeconds(w.cfg.LeaseTimeout), w.cfg.Batch)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := w.processLeasedJob(ctx, job); err != nil && !errors.Is(err, domain.ErrStarGiftTONLeaseLost) {
			w.logger.Warn("TON job processing failed", zap.Int64("job_id", job.ID), zap.String("kind", string(job.Kind)), zap.Error(err))
		}
	}
	return nil
}

func (w *Worker) processLeasedJob(ctx context.Context, job domain.StarGiftTONJob) error {
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	requestCtx, cancelRequest := context.WithTimeout(jobCtx, w.cfg.RequestTimeout)
	defer cancelRequest()

	renewDone := make(chan struct{})
	renewErr := make(chan error, 1)
	go w.renewLease(jobCtx, cancelJob, job, renewDone, renewErr)
	err := w.processJob(requestCtx, jobCtx, job)
	cancelJob()
	<-renewDone
	select {
	case leaseErr := <-renewErr:
		if leaseErr != nil {
			return leaseErr
		}
	default:
	}
	return err
}

func (w *Worker) renewLease(ctx context.Context, cancel context.CancelFunc, job domain.StarGiftTONJob, done chan<- struct{}, result chan<- error) {
	defer close(done)
	interval := w.cfg.LeaseTimeout / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := w.store.RenewTONJob(ctx, job.ID, w.cfg.WorkerID, job.FencingToken,
				int(w.now().Unix()), durationSeconds(w.cfg.LeaseTimeout))
			if err != nil {
				select {
				case result <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (w *Worker) processJob(chainCtx, storeCtx context.Context, job domain.StarGiftTONJob) error {
	switch job.Kind {
	case domain.StarGiftTONJobResolveItem:
		return w.processResolveItem(chainCtx, storeCtx, job)
	case domain.StarGiftTONJobMint:
		return w.processMint(chainCtx, storeCtx, job)
	case domain.StarGiftTONJobConfirm:
		return w.processConfirm(chainCtx, storeCtx, job)
	case domain.StarGiftTONJobReconcileOwner:
		return w.processOwner(chainCtx, storeCtx, job)
	default:
		return w.fail(storeCtx, job, fmt.Errorf("unsupported TON worker job kind %q", job.Kind), true)
	}
}

func (w *Worker) processResolveItem(chainCtx, storeCtx context.Context, job domain.StarGiftTONJob) error {
	itemAddress, err := w.chain.ResolveItemAddress(chainCtx, job.Export)
	if err != nil {
		return w.fail(storeCtx, job, err, IsPermanent(err))
	}
	if strings.TrimSpace(itemAddress) == "" {
		return w.fail(storeCtx, job, fmt.Errorf("TON collection returned an empty NFT item address"), true)
	}
	_, err = w.store.RecordTONItemResolved(storeCtx, domain.StarGiftTONItemResolution{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
		ItemAddress: strings.TrimSpace(itemAddress), ResolvedAt: int(w.now().Unix()),
	})
	return err
}

func (w *Worker) processMint(chainCtx, storeCtx context.Context, job domain.StarGiftTONJob) error {
	if !w.cfg.MintEnabled {
		return w.fail(storeCtx, job, fmt.Errorf("TON mint kill switch is disabled"), false)
	}
	asset := job.Asset
	if asset == nil {
		now := int(w.now().Unix())
		prepared, err := w.chain.BuildMint(chainCtx, job.Export)
		if err != nil {
			return w.fail(storeCtx, job, err, IsPermanent(err))
		}
		if !prepared.Valid(now) {
			return w.fail(storeCtx, job, fmt.Errorf("TON chain returned invalid prepared mint evidence"), true)
		}
		stored, err := w.store.RecordTONMintPrepared(storeCtx, domain.StarGiftTONMintPreparation{
			JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
			SubmissionHash: prepared.SubmissionHash, SubmissionBOC: prepared.SubmissionBOC,
			PreparedAt: now, ValidUntil: prepared.ValidUntil,
		})
		if err != nil {
			return err
		}
		asset = &stored
	}
	dispatch, chainErr := w.chain.SendMint(chainCtx, job.Export, *asset)
	if !dispatch.Valid() {
		return w.fail(storeCtx, job, fmt.Errorf("TON chain returned invalid dispatch evidence"), true)
	}
	if !dispatch.Dispatched {
		if chainErr == nil {
			chainErr = fmt.Errorf("TON mint was not dispatched")
		}
		return w.fail(storeCtx, job, chainErr, IsPermanent(chainErr))
	}
	_, err := w.store.RecordTONMintSubmitted(storeCtx, domain.StarGiftTONMintSubmission{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
		TxHash: dispatch.TxHash, TxLT: dispatch.TxLT, BlockSeqno: dispatch.BlockSeqno,
		SubmittedAt: int(w.now().Unix()),
	})
	if err != nil {
		return err
	}
	if chainErr != nil {
		w.logger.Warn("TON mint send outcome is uncertain; durable message will be reconciled",
			zap.Int64("export_id", job.ExportID), zap.Error(chainErr))
	}
	return nil
}

func (w *Worker) processConfirm(chainCtx, storeCtx context.Context, job domain.StarGiftTONJob) error {
	if job.Asset == nil {
		return w.fail(storeCtx, job, fmt.Errorf("submitted TON export has no asset evidence"), true)
	}
	result, confirmed, err := w.chain.ConfirmMint(chainCtx, job.Export, *job.Asset)
	if err != nil {
		return w.fail(storeCtx, job, err, IsPermanent(err))
	}
	if !confirmed {
		if result.HasObservation() {
			if !result.ObservationValid() {
				return w.fail(storeCtx, job, fmt.Errorf("TON chain returned invalid mint observation evidence"), true)
			}
			if _, err := w.store.RecordTONMintObserved(storeCtx, domain.StarGiftTONMintObservation{
				JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
				TxHash: result.TxHash, TxLT: result.TxLT, BlockSeqno: result.BlockSeqno,
				ObservedAt: int(w.now().Unix()),
			}); err != nil {
				return err
			}
		}
		return w.fail(storeCtx, job, fmt.Errorf("TON mint is not final yet"), false)
	}
	if !result.Valid() {
		return w.fail(storeCtx, job, fmt.Errorf("TON chain returned invalid confirmation evidence"), true)
	}
	_, err = w.store.RecordTONMintConfirmed(storeCtx, domain.StarGiftTONMintConfirmation{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
		OwnerAddress: result.OwnerAddress, TxHash: result.TxHash, TxLT: result.TxLT,
		BlockSeqno: result.BlockSeqno, FinalityDepth: result.FinalityDepth,
		ConfirmedAt: int(w.now().Unix()),
	})
	return err
}

func (w *Worker) processOwner(chainCtx, storeCtx context.Context, job domain.StarGiftTONJob) error {
	if job.Asset == nil {
		return w.fail(storeCtx, job, fmt.Errorf("confirmed TON export has no asset evidence"), true)
	}
	result, err := w.chain.ObserveOwner(chainCtx, job.Export, *job.Asset)
	if err != nil {
		return w.fail(storeCtx, job, err, IsPermanent(err))
	}
	if !result.Valid() {
		return w.fail(storeCtx, job, fmt.Errorf("TON chain returned invalid owner evidence"), true)
	}
	now := int(w.now().Unix())
	_, err = w.store.RecordTONOwnerObserved(storeCtx, domain.StarGiftTONOwnerObservation{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
		OwnerAddress: result.OwnerAddress, ObservedLT: result.ObservedLT,
		ObservedAt: now, NextCheckAt: now + durationSeconds(w.cfg.OwnerCheckInterval),
	})
	return err
}

func (w *Worker) fail(ctx context.Context, job domain.StarGiftTONJob, err error, terminal bool) error {
	failedAt := w.now()
	retryAt := int(failedAt.Add(w.cfg.RetryDelay).Unix())
	failureErr := w.store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
		Error: err.Error(), FailedAt: int(failedAt.Unix()), RetryAt: retryAt, Terminal: terminal,
	})
	if failureErr != nil {
		return failureErr
	}
	return err
}

type permanentError struct{ error }

func (permanentError) Permanent() bool { return true }

func Permanent(err error) error {
	if err == nil || IsPermanent(err) {
		return err
	}
	return permanentError{error: err}
}

func IsPermanent(err error) bool {
	var marker interface{ Permanent() bool }
	return errors.As(err, &marker) && marker.Permanent()
}

func durationSeconds(duration time.Duration) int {
	return int(math.Ceil(duration.Seconds()))
}

func validLogicalTime(value string) bool {
	if len(value) == 0 || len(value) > 20 {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
