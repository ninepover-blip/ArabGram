package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// DispatchOutboxLogicalShards is the stable account-stream hash space. Worker
// count changes ownership of shards, never the stream-to-shard mapping.
const DispatchOutboxLogicalShards = 256

// OutboxQueueKind names a durable ordering domain explicitly. Sequence values
// are queue-specific and must never be used to infer the queue kind.
type OutboxQueueKind uint8

const (
	OutboxQueueDispatchPTS OutboxQueueKind = iota + 1
	OutboxQueueAbsoluteDelivery
	OutboxQueueChannelPTS
)

// OutboxRecoveryPolicy is persisted with an item. It describes the durable
// recovery path after realtime delivery is no longer possible.
type OutboxRecoveryPolicy uint8

const (
	OutboxRecoveryDifference OutboxRecoveryPolicy = iota + 1
	OutboxRecoveryAbsoluteReload
)

type OutboxReadyKind uint8

const (
	OutboxReadyClaim OutboxReadyKind = iota + 1
	OutboxReadyRecoverLease
)

// OutboxNextReady is a database-clock observation of the next claim or lease
// recovery deadline. Kind prevents a ready lane from paying the evidence-expiry
// scans required only for an expired leased lane.
type OutboxNextReady struct {
	ObservedAt time.Time
	ReadyAt    time.Time
	Kind       OutboxReadyKind
}

func (n OutboxNextReady) Delay() time.Duration { return n.ReadyAt.Sub(n.ObservedAt) }

// OutboxAttemptRef is the exact cross-process fencing identity of one physical
// delivery attempt. LeaseFence is a lane ownership epoch; Attempt is the
// independent per-item attempt number.
type OutboxAttemptRef struct {
	QueueKind  OutboxQueueKind
	StreamID   int64
	ItemID     int64
	Sequence   int64
	LeaseFence uint64
	Attempt    int
}

type OutboxClaimRequest struct {
	QueueKind          OutboxQueueKind
	LogicalShardCount  int
	LogicalShardIDs    []int
	LaneLimit          int
	WindowSize         int
	WindowByteLimit    int
	LeaseDuration      time.Duration
	PhysicalDuration   time.Duration
	ClockSkewAllowance time.Duration
	Owner              string
}

// OutboxClaimPayload is a closed union. QueueKind selects the matching
// concrete payload; callers must reject a mismatched payload type.
type OutboxClaimPayload interface{ outboxClaimPayload() }

type DispatchOutboxPayload struct {
	EventType     domain.UpdateEventType
	ReadModelPeer domain.Peer
}

func (DispatchOutboxPayload) outboxClaimPayload() {}

type AbsoluteDeliveryPayload struct {
	TL []byte
}

func (AbsoluteDeliveryPayload) outboxClaimPayload() {}

type OutboxClaimedItem struct {
	Ref              OutboxAttemptRef
	RecoveryPolicy   OutboxRecoveryPolicy
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Payload          OutboxClaimPayload
	CommandNotAfter  time.Time
	EvidenceDeadline time.Time
	SourceInstanceID string
	Targets          []OutboxAttemptTarget
}

type OutboxClaimWindow struct {
	QueueKind  OutboxQueueKind
	StreamID   int64
	Owner      string
	LeaseFence uint64
	LeaseUntil time.Time
	Items      []OutboxClaimedItem
}

type OutboxBoundRecoveryMode uint8

const (
	// OutboxBoundRecoveryExactOwnerLive is a one-shot process-start recovery.
	// It rehydrates only a still-live lease owned by the same executor.
	OutboxBoundRecoveryExactOwnerLive OutboxBoundRecoveryMode = iota + 1
)

type OutboxRecoverBoundRequest struct {
	QueueKind         OutboxQueueKind
	Mode              OutboxBoundRecoveryMode
	Owner             string
	LogicalShardCount int
	LogicalShardIDs   []int
	LaneLimit         int
	LeaseDuration     time.Duration
}

// OutboxRecoverFinalizableRequest scopes the durable completion scan used on
// process startup and finalize notifications. LaneLimit bounds ordering
// domains. AttemptLimit independently bounds returned attempts and must fit the
// downstream FinalizeAttempts batch contract.
type OutboxRecoverFinalizableRequest struct {
	QueueKind         OutboxQueueKind
	LogicalShardCount int
	LogicalShardIDs   []int
	LaneLimit         int
	AttemptLimit      int
}

// OutboxEvidenceExpiryRequest drives the durable physical-receipt deadline
// sweep. MaxAttempts selects terminal recovery only after repeated timeouts.
type OutboxEvidenceExpiryRequest struct {
	QueueKind         OutboxQueueKind
	LogicalShardCount int
	LogicalShardIDs   []int
	LaneLimit         int
	MaxAttempts       int
}

// OutboxAttemptTarget is one frozen Edge command. BatchID identifies the
// fanout batch while CommandID uniquely identifies this target's command.
type OutboxAttemptTarget struct {
	TargetInstanceID string
	TargetUserID     int64
	BatchID          [16]byte
	CommandID        [16]byte
}

// OutboxAttemptTargetSet freezes the authoritative complete target set. An
// empty Targets slice is a bound authoritative empty set, not "unbound".
type OutboxAttemptTargetSet struct {
	Ref              OutboxAttemptRef
	SourceInstanceID string
	Targets          []OutboxAttemptTarget
}

type OutboxBindTargetOutcome uint8

const (
	OutboxBindTargetBound OutboxBindTargetOutcome = iota + 1
	OutboxBindTargetDuplicate
	OutboxBindTargetFenced
	OutboxBindTargetRejected
)

type OutboxBindTargetResult struct {
	Ref     OutboxAttemptRef
	Outcome OutboxBindTargetOutcome
}

type OutboxEvidenceKind uint8

const (
	OutboxEvidenceEdgeWritten OutboxEvidenceKind = iota + 1
	OutboxEvidenceEdgeNoEligible
	OutboxEvidenceAuthoritativeNoTargets
)

type OutboxAttemptEvidence struct {
	Ref              OutboxAttemptRef
	Kind             OutboxEvidenceKind
	SourceInstanceID string
	TargetInstanceID string
	TargetUserID     int64
	BatchID          [16]byte
	CommandID        [16]byte
	EligibleSessions int
	WrittenSessions  int
	ServerMsgID      int64
	ObservedAt       time.Time
}

type OutboxEvidenceOutcome uint8

const (
	OutboxEvidenceRecorded OutboxEvidenceOutcome = iota + 1
	OutboxEvidenceDuplicate
	OutboxEvidenceFenced
	OutboxEvidenceRejected
)

type OutboxEvidenceResult struct {
	Ref     OutboxAttemptRef
	Outcome OutboxEvidenceOutcome
}

type OutboxResolutionKind uint8

const (
	OutboxResolutionRetry OutboxResolutionKind = iota + 1
	OutboxResolutionAbandoned
	OutboxResolutionTerminalResync
)

// OutboxTargetAttemptResolution is an Edge-originated failure for one exact
// command from the attempt's frozen target ledger. Every identity component is
// mandatory; a DeliveryRef alone never authorizes an Edge to resolve an
// attempt.
type OutboxTargetAttemptResolution struct {
	Ref              OutboxAttemptRef
	SourceInstanceID string
	TargetInstanceID string
	TargetUserID     int64
	BatchID          [16]byte
	CommandID        [16]byte
	Kind             OutboxResolutionKind
	RetryDelay       time.Duration
	LastError        string
}

// OutboxOwnedAttemptResolution is an Egress-internal projection or admission
// failure. Owner must still hold the exact live lane/attempt lease.
type OutboxOwnedAttemptResolution struct {
	Ref        OutboxAttemptRef
	Owner      string
	Kind       OutboxResolutionKind
	RetryDelay time.Duration
	LastError  string
}

type OutboxResolutionOutcome uint8

const (
	OutboxResolutionRecorded OutboxResolutionOutcome = iota + 1
	OutboxResolutionDuplicate
	OutboxResolutionFenced
	OutboxResolutionRejected
)

type OutboxResolutionResult struct {
	Ref     OutboxAttemptRef
	Outcome OutboxResolutionOutcome
}

type OutboxFinalizeRequest struct {
	Ref             OutboxAttemptRef
	Owner           string
	RetainLease     bool
	WindowSize      int
	WindowByteLimit int
	LeaseDuration   time.Duration
}

type OutboxFinalizeOutcome uint8

const (
	OutboxFinalizeApplied OutboxFinalizeOutcome = iota + 1
	OutboxFinalizeScheduledRetry
	OutboxFinalizeAbandoned
	OutboxFinalizeTerminalResync
	OutboxFinalizeWaitingForPredecessor
	OutboxFinalizeFenced
	OutboxFinalizeRejected
)

type OutboxFinalizeResult struct {
	Ref     OutboxAttemptRef
	Outcome OutboxFinalizeOutcome
}

type OutboxFinalizeBatch struct {
	Results []OutboxFinalizeResult
	Next    []OutboxClaimWindow
}

// DurableOutboxStateStore is the common durable attempt state machine. Both
// account PTS and absolute delivery stores accept only their configured kind.
type DurableOutboxStateStore interface {
	ClaimWindows(context.Context, OutboxClaimRequest) ([]OutboxClaimWindow, error)
	RecoverBoundWindows(context.Context, OutboxRecoverBoundRequest) ([]OutboxClaimWindow, error)
	ExpireEvidenceDeadlines(context.Context, OutboxEvidenceExpiryRequest) ([]OutboxFinalizeRequest, error)
	RecoverFinalizableAttempts(context.Context, OutboxRecoverFinalizableRequest) ([]OutboxFinalizeRequest, error)
	BindAttemptTargets(context.Context, []OutboxAttemptTargetSet) ([]OutboxBindTargetResult, error)
	RecordAttemptEvidenceBatch(context.Context, []OutboxAttemptEvidence) ([]OutboxEvidenceResult, error)
	ResolveTargetAttemptBatch(context.Context, []OutboxTargetAttemptResolution) ([]OutboxResolutionResult, error)
	ResolveOwnedAttemptBatch(context.Context, []OutboxOwnedAttemptResolution) ([]OutboxResolutionResult, error)
	FinalizeAttempts(context.Context, []OutboxFinalizeRequest) (OutboxFinalizeBatch, error)
	NextReadyAt(context.Context, OutboxQueueKind, int, []int) (OutboxNextReady, bool, error)
}
