package edgecontrol

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"

	"telesrv/internal/deliverycontract"
)

var ErrDeliveryIndeterminate = errors.New("edgecontrol: delivery indeterminate")

var ErrDeliveryTargetUnavailable = errors.New("edgecontrol: delivery target instance unavailable")

var ErrDeliveryOverloaded = errors.New("edgecontrol: delivery executor overloaded")

var ErrLocationLeaseHeld = errors.New("edgecontrol: location lease held by another process")

var ErrLocationLeaseLost = errors.New("edgecontrol: location lease lost")

// BatchID identifies one frozen multi-Edge delivery fan-out. CommandID identifies
// one target Edge within that fan-out. Both are fixed-width protocol identities;
// neither is a human-readable or compatibility string.
type BatchID [16]byte
type CommandID [16]byte

func (id BatchID) Empty() bool   { return id == BatchID{} }
func (id CommandID) Empty() bool { return id == CommandID{} }

type QueueKind uint8

const (
	QueueAccountPTS QueueKind = iota + 1
	QueueAccountNonPTS
	QueueChannelPTS
)

func (d DetailCode) Valid() bool { return d <= DetailAttemptConflict }

type OrderingDomain struct {
	Kind     QueueKind
	StreamID int64
}

func OrderingDomainHash(domain OrderingDomain) uint64 {
	x := uint64(domain.StreamID) ^ (uint64(domain.Kind) * 0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// DeliveryRef is the complete durable identity carried through command
// admission, physical write and late client ACK. LeaseFence is independent from
// the human-readable attempt counter and prevents a reclaimed lane from being
// completed by an older process.
type DeliveryRef struct {
	Domain       OrderingDomain
	OutboxID     int64
	TargetUserID int64
	PTS          int
	LeaseFence   uint64
	Attempt      uint32
}

// DeliveryTracking is the immutable ledger identity attached to an outbound
// frame. BatchID and CommandID are intentionally retained through late client
// ACK observation; DeliveryRef alone is not an exact target-ledger key.
type DeliveryTracking struct {
	BatchID          BatchID
	CommandID        CommandID
	SourceInstanceID string
	Ref              DeliveryRef
}

func (t DeliveryTracking) Empty() bool {
	return t.BatchID.Empty() && t.CommandID.Empty() && t.Ref.Empty()
}

func (t DeliveryTracking) Valid() bool {
	return !t.BatchID.Empty() && !t.CommandID.Empty() && ValidDeliveryInstanceID(t.SourceInstanceID) && t.Ref.Valid()
}

func (r DeliveryRef) Empty() bool { return r == DeliveryRef{} }

func (r DeliveryRef) Valid() bool {
	if r.OutboxID <= 0 || r.TargetUserID < 0 || r.Domain.StreamID <= 0 || r.LeaseFence == 0 || r.Attempt == 0 {
		return false
	}
	switch r.Domain.Kind {
	case QueueAccountPTS:
		return r.TargetUserID > 0 && r.Domain.StreamID == r.TargetUserID && r.PTS > 0 && int64(r.PTS) <= int64(^uint32(0)>>1)
	case QueueAccountNonPTS:
		return r.TargetUserID > 0 && r.Domain.StreamID == r.TargetUserID && r.PTS == 0
	case QueueChannelPTS:
		return r.TargetUserID == 0 && r.PTS > 0 && int64(r.PTS) <= int64(^uint32(0)>>1)
	default:
		return false
	}
}

type ChannelDeliveryAudience uint8

const (
	ChannelAudienceMembers ChannelDeliveryAudience = iota + 1
	ChannelAudienceMessageBox
	ChannelAudienceMonoforumAdmins
)

const MaxChannelDeliveryExplicitUsers = 1000

// ChannelDeliveryRoute is frozen with the durable channel event. Membership
// is expanded from Edge-local indexes; explicit audience and affected users
// are bounded identities needed for message-box/admin ACLs and leave/kick
// events whose target is no longer present in the membership index.
type ChannelDeliveryRoute struct {
	ChannelID     int64
	Audience      ChannelDeliveryAudience
	AudienceUsers []int64
	AffectedUsers []int64
}

func (r ChannelDeliveryRoute) Empty() bool {
	return r.ChannelID == 0 && r.Audience == 0 && len(r.AudienceUsers) == 0 && len(r.AffectedUsers) == 0
}

func (r ChannelDeliveryRoute) ValidFor(domain OrderingDomain) bool {
	if domain.Kind != QueueChannelPTS || r.ChannelID <= 0 || r.ChannelID != domain.StreamID {
		return false
	}
	switch r.Audience {
	case ChannelAudienceMembers:
	case ChannelAudienceMessageBox, ChannelAudienceMonoforumAdmins:
		if len(r.AudienceUsers) == 0 {
			return false
		}
	default:
		return false
	}
	return canonicalPositiveIDs(r.AudienceUsers, MaxChannelDeliveryExplicitUsers) &&
		canonicalPositiveIDs(r.AffectedUsers, MaxChannelDeliveryExplicitUsers)
}

func canonicalPositiveIDs(ids []int64, limit int) bool {
	if len(ids) > limit {
		return false
	}
	for i, id := range ids {
		if id <= 0 || (i > 0 && ids[i-1] >= id) {
			return false
		}
	}
	return true
}

type DeliveryItem struct {
	Ref         DeliveryRef
	MessageType proto.MessageType
	PayloadHash [16]byte
	UpdateBytes []byte
	Channel     ChannelDeliveryRoute
}

func DeliveryPayloadHash(payload []byte) [16]byte {
	full := sha256.Sum256(payload)
	var out [16]byte
	copy(out[:], full[:len(out)])
	return out
}

type DeliveryBatch struct {
	BatchID          BatchID
	CommandID        CommandID
	SourceInstanceID string
	TargetInstanceID string
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	NotAfter         time.Time
	EvidenceDeadline time.Time
	Items            []DeliveryItem
}

const (
	MaxDeliveryBatchItems = deliverycontract.MaxBatchItems
	// Target count and identity length are protocol limits, not deployment
	// suggestions. They keep registry corruption from turning one durable lane
	// into an unbounded target ledger and ensure every plan accepted before bind
	// is encodable by the Redis v3 transport.
	MaxDeliveryTargets         = deliverycontract.MaxTargets
	MaxDeliveryInstanceIDBytes = deliverycontract.MaxInstanceIDBytes
	// MaxDeliveryBatchBytes bounds the immutable TL payload plus explicit
	// channel route identities carried on the Redis wire. Treating route IDs as
	// free would let Egress bind an attempt that Redis must deterministically
	// reject after the durable target set has already been frozen.
	MaxDeliveryBatchBytes = deliverycontract.MaxBatchBytes
	// Includes the configured one-minute physical bound plus at most ten
	// seconds of DB/Edge clock skew. Longer identities are rejected before they
	// can occupy Edge fence/terminal retention budgets.
	MaxDeliveryNotAfterHorizon = deliverycontract.MaxNotAfterHorizon
)

// DeliveryRequest is the Egress-facing, not-yet-routed batch. DeliveryFabric
// freezes registry targets and assigns one fixed-width CommandID per target.
type DeliveryRequest struct {
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	NotAfter         time.Time
	EvidenceDeadline time.Time
	Items            []DeliveryItem
}

// AccountDeliveryRouteRequest is the payload-free account routing boundary.
// Egress uses it before an expensive PTS projection so an authoritative empty
// online target set can be committed without loading or encoding the update.
type AccountDeliveryRouteRequest struct {
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	NotAfter         time.Time
	EvidenceDeadline time.Time
}

// ValidateDeliveryBatchEnvelope validates bounded v3 identities and ownership
// without re-hashing payload bytes. Redis send/parse use it to avoid hashing
// the same immutable batch once per target Edge; the accepting Edge calls the
// full ValidateDeliveryBatch exactly once before admission.
func ValidateDeliveryBatchEnvelope(batch DeliveryBatch) error {
	if batch.BatchID.Empty() || batch.CommandID.Empty() || !ValidDeliveryInstanceID(batch.SourceInstanceID) || !ValidDeliveryInstanceID(batch.TargetInstanceID) {
		return fmt.Errorf("edgecontrol: delivery batch identity is required")
	}
	if batch.TargetUserID < 0 || len(batch.Items) == 0 || len(batch.Items) > MaxDeliveryBatchItems {
		return fmt.Errorf("edgecontrol: delivery batch target and items are required")
	}
	if (batch.ExcludeAuthKeyID != ([8]byte{})) != (batch.ExcludeSessionID != 0) {
		return fmt.Errorf("edgecontrol: invalid delivery exclusion pair")
	}
	if batch.NotAfter.IsZero() || batch.NotAfter.UnixNano() <= 0 ||
		!batch.EvidenceDeadline.After(batch.NotAfter) {
		return fmt.Errorf("edgecontrol: ordered command and evidence deadlines are required")
	}
	if batch.EvidenceDeadline.After(time.Now().Add(MaxDeliveryNotAfterHorizon)) {
		return fmt.Errorf("edgecontrol: delivery deadline exceeds maximum horizon")
	}
	domain := batch.Items[0].Ref.Domain
	channel := domain.Kind == QueueChannelPTS
	if channel != (batch.TargetUserID == 0) || (channel && (batch.ExcludeAuthKeyID != ([8]byte{}) || batch.ExcludeSessionID != 0)) {
		return fmt.Errorf("edgecontrol: invalid delivery target for ordering domain")
	}
	seenRefs := make(map[DeliveryRef]struct{}, len(batch.Items))
	seenItems := make(map[int64]struct{}, len(batch.Items))
	totalBytes := 0
	for i, item := range batch.Items {
		if !item.Ref.Valid() || item.Ref.TargetUserID != batch.TargetUserID || item.Ref.Domain != domain ||
			(channel && !item.Channel.ValidFor(domain)) || (!channel && !item.Channel.Empty()) {
			return fmt.Errorf("edgecontrol: invalid delivery ref at index %d", i)
		}
		if item.MessageType != proto.MessageFromServer || len(item.UpdateBytes) == 0 {
			return fmt.Errorf("edgecontrol: invalid delivery payload at index %d", i)
		}
		if _, exists := seenRefs[item.Ref]; exists {
			return fmt.Errorf("edgecontrol: duplicate delivery ref at index %d", i)
		}
		if _, exists := seenItems[item.Ref.OutboxID]; exists {
			return fmt.Errorf("edgecontrol: duplicate delivery item at index %d", i)
		}
		seenRefs[item.Ref] = struct{}{}
		seenItems[item.Ref.OutboxID] = struct{}{}
		totalBytes += deliveryItemEnvelopeBytes(item)
		if totalBytes > MaxDeliveryBatchBytes {
			return fmt.Errorf("edgecontrol: delivery payload and route metadata exceed batch byte limit")
		}
	}
	return nil
}

func deliveryItemEnvelopeBytes(item DeliveryItem) int {
	return len(item.UpdateBytes) + 8*(len(item.Channel.AudienceUsers)+len(item.Channel.AffectedUsers))
}

// ValidateDeliveryBatch performs the receiver-side payload integrity check in
// addition to the bounded envelope validation.
func ValidateDeliveryBatch(batch DeliveryBatch) error {
	if err := ValidateDeliveryBatchEnvelope(batch); err != nil {
		return err
	}
	for i, item := range batch.Items {
		if item.PayloadHash != DeliveryPayloadHash(item.UpdateBytes) {
			return fmt.Errorf("edgecontrol: invalid delivery payload hash at index %d", i)
		}
	}
	return nil
}

type DetailCode uint16

const (
	DetailNone DetailCode = iota
	DetailInvalidIdentity
	DetailInvalidPayload
	DetailCapacity
	DetailTargetUnavailable
	DetailWriteFailed
	DetailDeadline
	DetailAttemptConflict
)

type AdmissionOutcome uint8

const (
	AdmissionAccepted AdmissionOutcome = iota + 1
	AdmissionDuplicateInFlight
	AdmissionDuplicateTerminal
	AdmissionOverloaded
	AdmissionRejected
)

type DeliveryAdmission struct {
	BatchID          BatchID
	CommandID        CommandID
	SourceInstanceID string
	TargetInstanceID string
	Outcome          AdmissionOutcome
	Detail           DetailCode
}

func (a DeliveryAdmission) Valid() bool {
	if a.BatchID.Empty() || a.CommandID.Empty() || !ValidDeliveryInstanceID(a.SourceInstanceID) || !ValidDeliveryInstanceID(a.TargetInstanceID) || !a.Detail.Valid() {
		return false
	}
	switch a.Outcome {
	case AdmissionAccepted, AdmissionDuplicateInFlight, AdmissionDuplicateTerminal:
		return a.Detail == DetailNone
	case AdmissionOverloaded:
		return a.Detail == DetailCapacity || a.Detail == DetailDeadline
	case AdmissionRejected:
		return a.Detail != DetailNone
	default:
		return false
	}
}

type PhysicalOutcome uint8

const (
	PhysicalWritten PhysicalOutcome = iota + 1
	PhysicalNoEligibleSessions
	PhysicalIndeterminate
	PhysicalRejected
)

type PhysicalReceipt struct {
	BatchID          BatchID
	CommandID        CommandID
	SourceInstanceID string
	TargetInstanceID string
	Ref              DeliveryRef
	Outcome          PhysicalOutcome
	Detail           DetailCode
	EligibleSessions int
	WrittenSessions  int
	FirstServerMsgID int64
	// ObservedAt is captured by the Conn outbound actor immediately after the
	// underlying writer returns nil. Reporter receive time is not equivalent.
	ObservedAt time.Time
}

type PhysicalReceiptResultOutcome uint8

const (
	PhysicalReceiptApplied PhysicalReceiptResultOutcome = iota + 1
	PhysicalReceiptStale
	PhysicalReceiptRejected
	PhysicalReceiptRetryable
)

type PhysicalReceiptResult struct {
	Outcome PhysicalReceiptResultOutcome
	Detail  DetailCode
}

type PhysicalReceiptReporter interface {
	ReportPhysicalReceipts(context.Context, []PhysicalReceipt) ([]PhysicalReceiptResult, error)
}

// ClientAckObservation is late, non-authoritative evidence. It preserves the
// exact batch/command target-ledger identity and never infers queue kind from a
// zero sequence.
type ClientAckObservation struct {
	Tracking         DeliveryTracking
	TargetInstanceID string
	AuthKeyID        [8]byte
	SessionID        int64
	ServerMsgID      int64
	ObservedAt       time.Time
}

func (o ClientAckObservation) Valid() bool {
	return o.Tracking.Valid() && ValidDeliveryInstanceID(o.TargetInstanceID) &&
		o.AuthKeyID != ([8]byte{}) && o.SessionID != 0 &&
		o.ServerMsgID > 0 && !o.ObservedAt.IsZero()
}

type ClientAckObservationOutcome uint8

const (
	ClientAckObservationApplied ClientAckObservationOutcome = iota + 1
	ClientAckObservationStale
	ClientAckObservationRejected
	ClientAckObservationRetryable
)

type ClientAckObservationResult struct {
	Outcome ClientAckObservationOutcome
	Detail  DetailCode
}

type ClientAckObservationReporter interface {
	ReportClientAcks(context.Context, []ClientAckObservation) ([]ClientAckObservationResult, error)
}

type TargetAdmission struct {
	TargetInstanceID string
	CommandID        CommandID
	Admission        DeliveryAdmission
	Err              error
}

// FrozenDelivery always retains every registry-selected target, including
// targets whose Redis command admission was indeterminate.
type FrozenDelivery struct {
	BatchID BatchID
	Targets []TargetAdmission
}

type PreparedDeliveryTarget struct {
	TargetInstanceID string
	CommandID        CommandID
}

// FrozenAccountDeliveryRoute owns one exact Redis registry snapshot and all
// protocol identities allocated from it. BindDelivery attaches immutable
// payloads without consulting the registry a second time.
type FrozenAccountDeliveryRoute struct {
	batchID          BatchID
	sourceInstanceID string
	targetUserID     int64
	excludeAuthKeyID [8]byte
	excludeSessionID int64
	notAfter         time.Time
	evidenceDeadline time.Time
	targets          []PreparedDeliveryTarget
}

func (r FrozenAccountDeliveryRoute) BatchID() BatchID { return r.batchID }

func (r FrozenAccountDeliveryRoute) SourceInstanceID() string { return r.sourceInstanceID }

func (r FrozenAccountDeliveryRoute) TargetUserID() int64 { return r.targetUserID }

func (r FrozenAccountDeliveryRoute) Targets() []PreparedDeliveryTarget {
	return append([]PreparedDeliveryTarget(nil), r.targets...)
}

func (r FrozenAccountDeliveryRoute) BindDelivery(request DeliveryRequest) (FrozenDeliveryPlan, error) {
	if request.TargetUserID != r.targetUserID || request.ExcludeAuthKeyID != r.excludeAuthKeyID ||
		request.ExcludeSessionID != r.excludeSessionID || !request.NotAfter.Equal(r.notAfter) ||
		!request.EvidenceDeadline.Equal(r.evidenceDeadline) {
		return FrozenDeliveryPlan{}, fmt.Errorf("edgecontrol: delivery request differs from frozen account route")
	}
	if err := validateDeliveryRequest(request); err != nil {
		return FrozenDeliveryPlan{}, err
	}
	if request.Items[0].Ref.Domain.Kind == QueueChannelPTS {
		return FrozenDeliveryPlan{}, fmt.Errorf("edgecontrol: channel delivery cannot bind an account route")
	}
	if err := validateFrozenAccountDeliveryRoute(r); err != nil {
		return FrozenDeliveryPlan{}, err
	}
	plan := buildFrozenDeliveryPlan(r.sourceInstanceID, r.batchID, request, r.targets)
	return plan, validateFrozenDeliveryPlanEnvelope(plan)
}

// FrozenDeliveryPlan is deliberately opaque. PrepareDelivery freezes the
// registry snapshot and allocates every wire identity without publishing.
// Callers bind a copy of Targets() in their durable ledger, then pass this same
// value to AdmitPreparedDelivery.
type FrozenDeliveryPlan struct {
	batchID          BatchID
	sourceInstanceID string
	targetUserID     int64
	excludeAuthKeyID [8]byte
	excludeSessionID int64
	notAfter         time.Time
	evidenceDeadline time.Time
	items            []DeliveryItem
	targets          []PreparedDeliveryTarget
}

func (p FrozenDeliveryPlan) BatchID() BatchID { return p.batchID }

func (p FrozenDeliveryPlan) SourceInstanceID() string { return p.sourceInstanceID }

func (p FrozenDeliveryPlan) Targets() []PreparedDeliveryTarget {
	return append([]PreparedDeliveryTarget(nil), p.targets...)
}

func (p FrozenDeliveryPlan) Validate() error { return validateFrozenDeliveryPlan(p) }

// RehydrateDeliveryPlan reconstructs the immutable plan after an Egress crash
// from the request and exact target ledger already committed before admission.
// It performs no registry lookup and no publish.
func RehydrateDeliveryPlan(sourceInstanceID string, batchID BatchID, request DeliveryRequest, targets []PreparedDeliveryTarget) (FrozenDeliveryPlan, error) {
	plan := buildFrozenDeliveryPlan(sourceInstanceID, batchID, request, targets)
	return plan, plan.Validate()
}

func buildFrozenDeliveryPlan(sourceInstanceID string, batchID BatchID, request DeliveryRequest, targets []PreparedDeliveryTarget) FrozenDeliveryPlan {
	items := make([]DeliveryItem, len(request.Items))
	for i := range request.Items {
		items[i] = request.Items[i]
		items[i].UpdateBytes = append([]byte(nil), request.Items[i].UpdateBytes...)
		items[i].Channel.AudienceUsers = append([]int64(nil), request.Items[i].Channel.AudienceUsers...)
		items[i].Channel.AffectedUsers = append([]int64(nil), request.Items[i].Channel.AffectedUsers...)
	}
	return FrozenDeliveryPlan{
		batchID: batchID, sourceInstanceID: sourceInstanceID,
		targetUserID: request.TargetUserID, excludeAuthKeyID: request.ExcludeAuthKeyID,
		excludeSessionID: request.ExcludeSessionID, notAfter: request.NotAfter.UTC(),
		evidenceDeadline: request.EvidenceDeadline.UTC(),
		items:            items, targets: append([]PreparedDeliveryTarget(nil), targets...),
	}
}

type DeliveryPlanner interface {
	PrepareAccountDeliveryRoute(context.Context, AccountDeliveryRouteRequest) (FrozenAccountDeliveryRoute, error)
	PrepareDelivery(context.Context, DeliveryRequest) (FrozenDeliveryPlan, error)
	AdmitPreparedDelivery(context.Context, FrozenDeliveryPlan) (FrozenDelivery, error)
}

type SessionControlKind string

// MaxChannelMembershipSyncPage bounds one Core-to-Edge staging command. The
// complete account membership remains unbounded and is streamed over pages.
const MaxChannelMembershipSyncPage = 1000

// ChannelMembershipSyncDisposition is the Edge-owned singleflight decision
// for one session membership bootstrap. Only the acquired caller may append
// pages and commit. In-progress and prepared callers must not scan PostgreSQL.
type ChannelMembershipSyncDisposition string

const (
	ChannelMembershipSyncAcquired   ChannelMembershipSyncDisposition = "acquired"
	ChannelMembershipSyncInProgress ChannelMembershipSyncDisposition = "in_progress"
	ChannelMembershipSyncPrepared   ChannelMembershipSyncDisposition = "prepared"
)

const (
	SessionControlCloseBusinessAuthKey                 SessionControlKind = "close_business_auth_key"
	SessionControlCloseRawAuthKey                      SessionControlKind = "close_raw_auth_key"
	SessionControlBindAuthKeySession                   SessionControlKind = "bind_auth_key_session"
	SessionControlBindRawAuthKey                       SessionControlKind = "bind_raw_auth_key"
	SessionControlBindUser                             SessionControlKind = "bind_user"
	SessionControlUnbindAuthKey                        SessionControlKind = "unbind_auth_key"
	SessionControlSetReceivesUpdates                   SessionControlKind = "set_receives_updates"
	SessionControlBeginUpdatesActivation               SessionControlKind = "begin_updates_activation"
	SessionControlEndUpdatesActivation                 SessionControlKind = "end_updates_activation"
	SessionControlBeginBootstrapProbe                  SessionControlKind = "begin_bootstrap_probe"
	SessionControlEndBootstrapProbe                    SessionControlKind = "end_bootstrap_probe"
	SessionControlSetClientLayer                       SessionControlKind = "set_client_layer"
	SessionControlSeedRawLayer                         SessionControlKind = "seed_raw_layer"
	SessionControlSeedBusinessLayer                    SessionControlKind = "seed_business_layer"
	SessionControlRefreshRawLayer                      SessionControlKind = "refresh_raw_layer"
	SessionControlClearRawLayer                        SessionControlKind = "clear_raw_layer"
	SessionControlPushSession                          SessionControlKind = "push_session"
	SessionControlPushSessionImmediate                 SessionControlKind = "push_session_immediate"
	SessionControlPushUser                             SessionControlKind = "push_user"
	SessionControlPushUserBatch                        SessionControlKind = "push_user_batch"
	SessionControlPushUserBounded                      SessionControlKind = "push_user_bounded"
	SessionControlPushUserTransient                    SessionControlKind = "push_user_transient"
	SessionControlPushUserAuthKey                      SessionControlKind = "push_user_auth_key"
	SessionControlPushUserAuthKeyTransient             SessionControlKind = "push_user_auth_key_transient"
	SessionControlPushUserExceptBusinessAuthKey        SessionControlKind = "push_user_except_business_auth_key"
	SessionControlPushUserTransientAtLeastLayer        SessionControlKind = "push_user_transient_at_least_layer"
	SessionControlPushUserAuthKeyTransientAtLeastLayer SessionControlKind = "push_user_auth_key_transient_at_least_layer"
	SessionControlTrackChannelInterest                 SessionControlKind = "track_channel_interest"
	SessionControlClearChannelInterest                 SessionControlKind = "clear_channel_interest"
	SessionControlRefreshChannelSubscription           SessionControlKind = "refresh_channel_subscription"
	SessionControlBeginChannelMembershipSync           SessionControlKind = "begin_channel_membership_sync"
	SessionControlAppendChannelMembershipSync          SessionControlKind = "append_channel_membership_sync"
	SessionControlCommitChannelMembershipSync          SessionControlKind = "commit_channel_membership_sync"
	SessionControlAbortChannelMembershipSync           SessionControlKind = "abort_channel_membership_sync"
	SessionControlAddUserChannelMembership             SessionControlKind = "add_user_channel_membership"
	SessionControlRemoveUserChannelMembership          SessionControlKind = "remove_user_channel_membership"
)

type SessionControlUserPush struct {
	TargetUserID    int64
	RawAuthKeyID    [8]byte
	ExceptSessionID int64
	MessageType     proto.MessageType
	UpdateBytes     []byte
	ChannelDelivery ChannelDeliveryWatermark
}

type ChannelDeliveryKind string

const (
	ChannelDeliveryPayload ChannelDeliveryKind = "payload"
	ChannelDeliveryNudge   ChannelDeliveryKind = "nudge"
)

type ChannelDeliveryWatermark struct {
	Kind      ChannelDeliveryKind
	ChannelID int64
	MinPts    int
	MaxPts    int
}

func (w ChannelDeliveryWatermark) Present() bool {
	return w.Kind != "" || w.ChannelID != 0 || w.MinPts != 0 || w.MaxPts != 0
}

func (w ChannelDeliveryWatermark) Valid() bool {
	if w.ChannelID <= 0 || w.MinPts <= 0 || w.MaxPts < w.MinPts {
		return false
	}
	return w.Kind == ChannelDeliveryPayload || w.Kind == ChannelDeliveryNudge
}

type SessionControlCommand struct {
	CommandID         string
	SourceInstanceID  string
	TargetInstanceID  string
	Kind              SessionControlKind
	AuthKeyID         [8]byte
	RawAuthKeyID      [8]byte
	BusinessAuthKeyID [8]byte
	ExceptSessionID   int64
	SessionID         int64
	UserID            int64
	TargetUserID      int64
	ReceivesUpdates   bool
	Layer             int
	Semantic          tlprofile.SemanticID
	MessageType       proto.MessageType
	UpdateBytes       []byte
	UserPushes        []SessionControlUserPush
	DeliveryTimeout   time.Duration
	ChannelID         int64
	ChannelIDs        []int64
	SubscriptionTTL   time.Duration
	MembershipSyncID  int64
	LifecycleToken    uint64
	LifecycleSuccess  bool
}

type SessionControlAck struct {
	CommandID                 string
	SourceInstanceID          string
	TargetInstanceID          string
	Affected                  int
	MembershipSyncID          int64
	MembershipSyncDisposition ChannelMembershipSyncDisposition
	LifecycleToken            uint64
	Error                     string
}

// DeliveryBatchAcceptor is the Edge-local command-admission boundary. A returned
// Accepted result means the fixed user executor and every future physical
// receipt slot are reserved. It never means that a socket write completed.
type DeliveryBatchAcceptor interface {
	AdmitDeliveryBatch(context.Context, DeliveryBatch) DeliveryAdmission
}

type DeliveryBatchHandler func(context.Context, DeliveryBatch) DeliveryAdmission

// DeliveryCommandBus carries only command admission through Redis. Physical
// receipts have a separate Edge -> Egress reporter boundary and must never be
// published through Redis as a fallback.
type DeliveryCommandBus interface {
	SendDeliveryBatch(context.Context, string, DeliveryBatch) (DeliveryAdmission, error)
	SubscribeDeliveryBatches(context.Context, string, DeliveryBatchHandler) error
}

// PhysicalReceiptReservation is obtained before Edge acknowledges command
// admission. Each index is resolved exactly once by Commit or Release. Commit
// must be O(1), non-blocking and safe from a Conn outbound actor.
type PhysicalReceiptReservation interface {
	Commit(index int, receipt PhysicalReceipt)
	Release(index int)
}

type PhysicalReceiptSink interface {
	ReservePhysicalReceipts(
		batchID BatchID,
		sourceInstanceID string,
		targetInstanceID string,
		commandID CommandID,
		notAfter time.Time,
		refs []DeliveryRef,
	) (PhysicalReceiptReservation, error)
}

type SessionControlCommandHandler func(context.Context, SessionControlCommand) SessionControlAck

type SessionControlCommandBus interface {
	SendSessionControl(ctx context.Context, targetInstanceID string, cmd SessionControlCommand) (SessionControlAck, error)
	SubscribeSessionControls(ctx context.Context, instanceID string, handle SessionControlCommandHandler) error
}

type LocationRecord struct {
	InstanceID           string
	UserID               int64
	RawAuthKeyID         [8]byte
	BusinessAuthKeyID    [8]byte
	SessionID            int64
	ReceivesUpdates      bool
	Layer                int
	ActiveChannelIDs     []int64
	ChannelSubscriptions []ChannelSubscriptionLocation
	UpdatedAtUnix        int64
	// LocationRevision is allocated atomically by the shared registry and is
	// the authoritative reconnect ownership order across Edge instances.
	LocationRevision int64
	// UpdatedAtUnixNano is diagnostic time only. A record without a positive
	// LocationRevision is invalid and is never eligible for routing.
	UpdatedAtUnixNano int64
}

// ChannelMembershipRecord is the instance-local online membership projection
// for one user. It is deliberately independent of session LocationRecord so a
// layer/ready/session mutation never rewrites the user's complete channel set,
// and multiple sessions on the same Edge share one Redis membership ref.
type ChannelMembershipRecord struct {
	InstanceID    string
	UserID        int64
	ChannelIDs    []int64
	UpdatedAtUnix int64
}

type ChannelSubscriptionLocation struct {
	ChannelID         int64
	ExpiresAtUnixNano int64
}

type LocationMutation struct {
	Record  LocationRecord
	Deleted bool
}

type ChannelMembershipMutation struct {
	Record  ChannelMembershipRecord
	Deleted bool
}

type LocationRegistry interface {
	ListUser(ctx context.Context, userID int64) ([]LocationRecord, error)
	ListBusinessAuthKey(ctx context.Context, authKeyID [8]byte) ([]LocationRecord, error)
	ListInstance(ctx context.Context, instanceID string) ([]LocationRecord, error)
}

// MutableLocationRegistry is the Edge-owned write boundary. Instance liveness
// is one fenced lease, independent of the number of active sessions. Session
// records and their secondary indexes are updated only when that session
// changes; callers must never renew liveness by rewriting a full snapshot.
type MutableLocationRegistry interface {
	LocationRegistry
	AcquireInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error
	RenewInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error
	ApplyLocationMutations(ctx context.Context, instanceID, leaseID string, mutations []LocationMutation) error
	ApplyChannelMembershipMutations(ctx context.Context, instanceID, leaseID string, mutations []ChannelMembershipMutation) error
	ReleaseInstanceLease(ctx context.Context, instanceID, leaseID string) error
}

type BatchUserLocationRegistry interface {
	ListUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error)
}

type RawAuthKeyLocationRegistry interface {
	ListRawAuthKey(ctx context.Context, authKeyID [8]byte) ([]LocationRecord, error)
}

type ActiveRawAuthKeyRegistry interface {
	ListActiveRawAuthKeyIDs(ctx context.Context) ([][8]byte, error)
}

type ChannelLocationRegistry interface {
	ListChannelInterest(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListChannelMember(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListChannelSubscription(ctx context.Context, channelID int64) ([]LocationRecord, error)
	ListOnlineChannelIDsSnapshot(ctx context.Context) ([]int64, error)
}

// ChannelDeliveryTargetRegistry returns owning Edge instances, never users.
// Implementations must use per-instance presence indexes and may return a
// bounded superset; the accepting Edge performs the frozen local-session
// expansion and reports NoEligibleSessions for a target without a match.
type ChannelDeliveryTargetRegistry interface {
	ListChannelDeliveryTargets(context.Context, []ChannelDeliveryRoute) ([]string, error)
}

// Controller is the Core-facing control-plane boundary for active MTProto
// sessions owned by Edge.
//
// MTProto session identity is raw auth_key_id + session_id. Every method that
// targets one logical session must carry both values; session_id alone is not
// globally unique across auth keys.
type Controller interface {
	BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte)
	AuthKeyIDForSession(rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool)
	// BindUserForAuthKey carries the session that observed the authorization,
	// but the resulting user identity is raw-auth-key scoped. A Telegram client
	// can open upload/download sessions under the same raw key before they send
	// a business RPC through CoreExec.
	BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64)
	UserIDResolvedForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (userID int64, resolved bool)
	UnbindAuthKey(authKeyID [8]byte) int
	SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool)
	PushToSessionForAuthKey(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error
	// excludeAuthKeyID/excludeSessionID must both be zero or both be non-zero.
	PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error)
}

type FullController interface {
	Controller
	RawAuthKeyIdentityBinder
	RawAuthKeyMetadataProvider
	ImmediateSessionPusher
	SessionUpdatesStateProvider
	SessionUpdatesActivationProvider
	SessionBootstrapProbeProvider
	ClientLayerBinder
	AuthKeyLayerBinder
	BusinessAuthKeyLayerBinder
	AuthKeyLayerRefresher
	AuthKeyInheritedLayerClearer
	ActiveSessionLayerEvidenceProvider
	SessionTerminator
	RawSessionTerminator
	BoundedSessionPusher
	TransientSessionPusher
	AuthKeyTargetedSessionPusher
	LayerAwareTransientPusher
	OnlineUserProvider
	ChannelSubscriptionProvider
	ChannelNudgeProvider
	ChannelFanoutRecoverySessionProvider
}

// NewLocal names the Edge-local control adapter used by a dedicated Edge
// process. It intentionally returns the same dynamic implementation so optional
// capabilities remain visible through type assertions at the edge boundary.
func NewLocal(controller Controller) Controller {
	return controller
}

// RawAuthKeyIdentityBinder switches all live sessions for a raw temporary key to
// the canonical permanent identity after auth.bindTempAuthKey succeeds.
type RawAuthKeyIdentityBinder interface {
	BindAuthKeyForRawAuthKey(rawAuthKeyID [8]byte, authKeyID [8]byte) int
}

// RawAuthKeyMetadataProvider exposes raw-key protocol expiry observed by Edge.
type RawAuthKeyMetadataProvider interface {
	AuthKeyExpiresAtForSession(rawAuthKeyID [8]byte, sessionID int64) (expiresAt int, found bool)
}

// ImmediateSessionPusher sends login-unblocking updates before the session has
// established its durable updates baseline.
type ImmediateSessionPusher interface {
	PushToSessionForAuthKeyImmediate(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error
}

// SessionUpdatesStateProvider exposes whether a live session is ready to receive updates.
type SessionUpdatesStateProvider interface {
	ReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64) bool
}

// SessionUpdatesActivationProvider fences one expensive readiness transition
// to the physical connection generation that owns the raw session on Edge.
type SessionUpdatesActivationProvider interface {
	BeginSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64) (token uint64, ok bool)
	EndSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64, token uint64)
}

// SessionBootstrapProbeProvider records the durable bootstrap readiness probe
// as a retryable one-shot owned by the physical Edge connection generation.
type SessionBootstrapProbeProvider interface {
	BeginSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64) (token uint64, ok bool)
	EndSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64, token uint64, success bool)
}

// ClientLayerBinder records explicit per-session layer evidence observed at Edge.
type ClientLayerBinder interface {
	SetClientLayerForAuthKey(rawAuthKeyID [8]byte, sessionID int64, layer int)
}

// AuthKeyLayerBinder seeds a mutable inherited layer for live sessions that
// have not yet produced explicit invokeWithLayer evidence.
type AuthKeyLayerBinder interface {
	SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int
}

// BusinessAuthKeyLayerBinder seeds unknown live sessions normalized to a
// permanent/business auth key.
type BusinessAuthKeyLayerBinder interface {
	SeedInheritedLayerForBusinessAuthKey(authKeyID [8]byte, layer int) int
}

// AuthKeyLayerRefresher refreshes inherited layer defaults after identity normalization.
type AuthKeyLayerRefresher interface {
	RefreshInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int
}

// AuthKeyInheritedLayerClearer removes mutable inherited defaults while
// preserving explicit per-session wire evidence.
type AuthKeyInheritedLayerClearer interface {
	ClearInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte) int
}

// ActiveSessionLayerEvidenceProvider exposes explicit live-session layer evidence.
type ActiveSessionLayerEvidenceProvider interface {
	ExplicitLayerEvidenceForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (layer int, msgID int64, ok bool)
}

// SessionTerminator closes sessions associated with a permanent business auth key.
type SessionTerminator interface {
	CloseSessionsForBusinessAuthKey(authKeyID [8]byte) int
}

// BoundedSessionTerminator closes sessions through the distributed Edge
// control fabric and reports an indeterminate remote delivery instead of
// silently treating it as an offline auth key.
type BoundedSessionTerminator interface {
	CloseSessionsForBusinessAuthKeyBounded(ctx context.Context, authKeyID [8]byte) (int, error)
}

// RawSessionTerminator closes sessions associated with a physical/raw auth key.
type RawSessionTerminator interface {
	CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int
}

// BoundedRawSessionTerminator is the context-aware form used when raw temp-key
// aliases must be retired as part of a confirmed authorization revocation.
type BoundedRawSessionTerminator interface {
	CloseSessionsForRawAuthKeyExceptBounded(ctx context.Context, authKeyID [8]byte, exceptSessionID int64) (int, error)
}

// BoundedSessionPusher provides online push with a caller supplied delivery
// deadline. Durable callers must rely on queue ACK/fencing for correctness, not
// on this deadline alone.
type BoundedSessionPusher interface {
	PushToUserExceptAuthKeySessionBounded(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

type ChannelDeliverySessionPusher interface {
	PushChannelUpdateToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, delivery ChannelDeliveryWatermark) (int, error)
}

// TransientSessionPusher sends short-lived updates that must not be queued for
// later durable delivery.
type TransientSessionPusher interface {
	PushToUserTransientExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// AuthKeyTargetedSessionPusher targets a specific business auth key for
// device-level updates such as secret chats.
type AuthKeyTargetedSessionPusher interface {
	PushToUserAuthKey(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass) (int, error)
	PushToUserAuthKeyTransient(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserExceptBusinessAuthKey(ctx context.Context, userID int64, excludeBusinessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// LayerAwareTransientPusher filters transient updates by exact live layer/profile before encoding.
type LayerAwareTransientPusher interface {
	PushToUserTransientAtLeastLayer(ctx context.Context, userID int64, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserAuthKeyTransientAtLeastLayer(ctx context.Context, userID int64, businessAuthKeyID [8]byte, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// SemanticTransientPusher filters transient updates through generated exact-profile
// metadata. It avoids hard-coding the first layer that introduced a constructor.
type SemanticTransientPusher interface {
	PushToUserTransientCompatible(ctx context.Context, userID int64, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
	PushToUserAuthKeyTransientCompatible(ctx context.Context, userID int64, businessAuthKeyID [8]byte, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error)
}

// OnlineUserProvider exposes a bounded runtime snapshot for transient online fanout.
type OnlineUserProvider interface {
	IsUserOnline(userID int64) bool
	OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64
	TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64)
	ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64)
	OnlineChannelUserIDs(channelID int64, limit int) []int64
	BeginSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID int64) (syncID int64, disposition ChannelMembershipSyncDisposition, err error)
	AppendSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64, channelIDs []int64) error
	CommitSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) (synced bool, err error)
	AbortSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64)
	AddUserChannelMembership(userID, channelID int64)
	RemoveUserChannelMembership(userID, channelID int64)
	OnlineChannelMemberUserIDs(channelID int64, limit int) []int64
}

// ChannelSubscriptionProvider tracks short-lived public-channel subscriptions.
type ChannelSubscriptionProvider interface {
	RefreshChannelSubscription(rawAuthKeyID [8]byte, sessionID, userID, channelID int64, ttl time.Duration)
	OnlineChannelSubscriberUserIDs(channelID int64, limit int) []int64
	OnlineChannelSubscriberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64
}

// ChannelNudgeProvider returns online channel members excluding already delivered users.
type ChannelNudgeProvider interface {
	OnlineChannelMemberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64
}

// ChannelFanoutRecoverySessionProvider snapshots channel IDs with online members.
type ChannelFanoutRecoverySessionProvider interface {
	OnlineChannelIDsSnapshot() []int64
}

type UserLocationBatchProvider interface {
	UserLocationRecordsForUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error)
}

type LocationTargetedUserPush struct {
	TargetUserID     int64
	Locations        []LocationRecord
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	MessageType      proto.MessageType
	Update           tg.UpdatesClass
	ChannelDelivery  ChannelDeliveryWatermark
}

type BatchLocationTargetedSessionPusher interface {
	PushToUserLocationBatches(ctx context.Context, pushes []LocationTargetedUserPush) (int, error)
}
