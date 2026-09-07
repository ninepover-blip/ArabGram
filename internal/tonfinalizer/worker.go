// Package tonfinalizer applies confirmed chain facts to Core-owned Telegram
// gift and profile state. It never submits or reads TON transactions itself.
package tonfinalizer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type Config struct {
	WorkerID       string
	Batch          int
	PollInterval   time.Duration
	LeaseTimeout   time.Duration
	RequestTimeout time.Duration
	RetryDelay     time.Duration
	OnChanged      func(domain.StarGiftTONFinalizationResult)
}

type Worker struct {
	store  store.StarGiftTONFinalizerStore
	cfg    Config
	logger *zap.Logger
	now    func() time.Time
}

func New(finalizerStore store.StarGiftTONFinalizerStore, cfg Config, logger *zap.Logger) (*Worker, error) {
	cfg.WorkerID = strings.TrimSpace(cfg.WorkerID)
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 5 * time.Second
	}
	if finalizerStore == nil || cfg.WorkerID == "" || len(cfg.WorkerID) > 128 || cfg.Batch < 1 || cfg.Batch > 100 ||
		cfg.PollInterval <= 0 || cfg.LeaseTimeout < 10*time.Second || cfg.LeaseTimeout > time.Hour ||
		cfg.RequestTimeout < time.Second || cfg.RequestTimeout >= cfg.LeaseTimeout || cfg.RetryDelay <= 0 {
		return nil, fmt.Errorf("invalid TON finalizer config")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Worker{store: finalizerStore, cfg: cfg, logger: logger, now: time.Now}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := w.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("TON finalizer tick failed", zap.Error(err))
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
	}
}

func (w *Worker) Tick(ctx context.Context) error {
	now := int(w.now().Unix())
	jobs, err := w.store.ClaimTONCoreJobs(ctx, w.cfg.WorkerID, now, durationSeconds(w.cfg.LeaseTimeout), w.cfg.Batch)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := w.process(ctx, job); err != nil && !errors.Is(err, domain.ErrStarGiftTONLeaseLost) {
			w.logger.Warn("TON Core job failed", zap.Int64("job_id", job.ID), zap.String("kind", string(job.Kind)), zap.Error(err))
		}
	}
	return nil
}

func (w *Worker) process(ctx context.Context, job domain.StarGiftTONJob) error {
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	requestCtx, cancelRequest := context.WithTimeout(jobCtx, w.cfg.RequestTimeout)
	defer cancelRequest()
	renewDone := make(chan struct{})
	renewErr := make(chan error, 1)
	go w.renew(jobCtx, cancelJob, job, renewDone, renewErr)

	finalization := domain.StarGiftTONFinalization{
		JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken, FinalizedAt: int(w.now().Unix()),
	}
	var result domain.StarGiftTONFinalizationResult
	var err error
	switch job.Kind {
	case domain.StarGiftTONJobFinalize:
		result, err = w.store.FinalizeTONExport(requestCtx, finalization)
	case domain.StarGiftTONJobSyncOwner:
		result, err = w.store.SyncTONOwner(requestCtx, finalization)
	default:
		err = fmt.Errorf("unsupported TON Core job kind %q", job.Kind)
	}
	cancelJob()
	<-renewDone
	select {
	case leaseErr := <-renewErr:
		if leaseErr != nil {
			return leaseErr
		}
	default:
	}
	if err != nil {
		failedAt := w.now()
		failureErr := w.store.FailTONCoreJob(ctx, domain.StarGiftTONJobFailure{
			JobID: job.ID, WorkerID: w.cfg.WorkerID, FencingToken: job.FencingToken,
			Error: err.Error(), FailedAt: int(failedAt.Unix()), RetryAt: int(failedAt.Add(w.cfg.RetryDelay).Unix()),
		})
		if failureErr != nil {
			return failureErr
		}
		return err
	}
	if w.cfg.OnChanged != nil {
		w.cfg.OnChanged(result)
	}
	return nil
}

func (w *Worker) renew(ctx context.Context, cancel context.CancelFunc, job domain.StarGiftTONJob, done chan<- struct{}, result chan<- error) {
	defer close(done)
	ticker := time.NewTicker(w.cfg.LeaseTimeout / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.store.RenewTONCoreJob(ctx, job.ID, w.cfg.WorkerID, job.FencingToken,
				int(w.now().Unix()), durationSeconds(w.cfg.LeaseTimeout)); err != nil {
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

func durationSeconds(value time.Duration) int {
	return int(math.Ceil(value.Seconds()))
}
