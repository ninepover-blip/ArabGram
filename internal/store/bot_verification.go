package store

import (
	"context"

	"telesrv/internal/domain"
)

// BotVerificationStore owns third-party verification: the icon catalogue, verifier
// status, the granted marks and the application queue in front of them.
//
// Reads on the projection path (PeerVerification / PeerVerificationBatch) are on
// every peer serialisation, so they must be cheap. The wire model carries one
// BotVerification, and the store therefore permits exactly one mark per peer.
type BotVerificationStore interface {
	// --- icon catalogue ---

	// UpsertVerificationIcon adds or updates a catalogue entry by document id.
	UpsertVerificationIcon(ctx context.Context, icon domain.VerificationIcon) (domain.VerificationIcon, error)
	// SetVerificationIconActive retires or restores an entry. Marks already granted
	// with it keep rendering: the icon id is denormalised onto the mark.
	SetVerificationIconActive(ctx context.Context, iconID int64, active bool) (domain.VerificationIcon, error)
	// VerificationIcon reads one entry by id.
	VerificationIcon(ctx context.Context, iconID int64) (domain.VerificationIcon, error)
	// VerificationIconByDocument reads one entry by its custom emoji document id.
	VerificationIconByDocument(ctx context.Context, documentID int64) (domain.VerificationIcon, error)
	// ListVerificationIcons lists the catalogue, newest first.
	ListVerificationIcons(ctx context.Context, activeOnly bool, limit int) ([]domain.VerificationIcon, error)

	// --- verifier status ---

	// UpsertBotVerifierSettingsWithDelivery grants or updates verifier status and
	// atomically appends the bounded updateUser effects for the verifier and every
	// user peer carrying one of its marks. Channel peers are returned separately as
	// transient authoritative-reload invalidations; they never become account PTS
	// or one durable account row per channel member.
	UpsertBotVerifierSettingsWithDelivery(ctx context.Context, settings domain.BotVerifierSettings, effects DeliveryEffectsBuilder[UserAudienceDeliverySnapshot]) (BotVerifierSettingsMutation, error)
	// SetBotVerifierEnabledWithDelivery is the matching kill-switch aggregate.
	SetBotVerifierEnabledWithDelivery(ctx context.Context, botID int64, enabled bool, effects DeliveryEffectsBuilder[UserAudienceDeliverySnapshot]) (BotVerifierSettingsMutation, error)
	// DeleteBotVerifierSettingsWithDelivery removes verifier status, cascades its
	// marks, and commits all reliable user effects in the same transaction.
	DeleteBotVerifierSettingsWithDelivery(ctx context.Context, botID int64, effects DeliveryEffectsBuilder[UserAudienceDeliverySnapshot]) (BotVerifierSettingsMutation, error)
	// BotVerifierSettings reads one verifier's status, enabled or not.
	BotVerifierSettings(ctx context.Context, botID int64) (domain.BotVerifierSettings, error)
	// BotVerifierSettingsBatch resolves several bots in one round trip for the
	// botInfo projection; bots without verifier status are absent from the map.
	BotVerifierSettingsBatch(ctx context.Context, botIDs []int64) (map[int64]domain.BotVerifierSettings, error)
	// ListBotVerifiers lists verifier bots for the admin panel.
	ListBotVerifiers(ctx context.Context, enabledOnly bool, limit int) ([]domain.BotVerifierSettings, error)

	// --- granted marks ---

	// GrantCustomVerification creates or updates the peer's mark. A different
	// verifier replaces the current mark rather than leaving hidden fallback state.
	// The icon is taken from the verifier's settings at grant time and the caller
	// has already resolved the description through
	// domain.BotVerifierSettings.DescriptionFor.
	GrantCustomVerification(ctx context.Context, mark domain.CustomVerification) (domain.CustomVerification, bool, error)
	// GrantCustomVerificationWithDelivery is the user-peer write boundary. The
	// mark and the frozen audience's absolute updateUser effects commit as one
	// unit; channel peers keep using GrantCustomVerification because their
	// aggregate owns a different notification/fanout boundary.
	GrantCustomVerificationWithDelivery(ctx context.Context, mark domain.CustomVerification, effects DeliveryEffectsBuilder[UserAudienceDeliverySnapshot]) (domain.CustomVerification, bool, error)
	// RevokeCustomVerification removes this verifier's mark from the peer and
	// reports whether anything was removed, so a repeated revoke is a no-op.
	RevokeCustomVerification(ctx context.Context, verifierBotID int64, peer domain.Peer) (bool, error)
	// RevokeCustomVerificationWithDelivery is the matching user-peer revoke
	// boundary. A missing mark is a true no-op and emits no duplicate effect.
	RevokeCustomVerificationWithDelivery(ctx context.Context, verifierBotID int64, peer domain.Peer, effects DeliveryEffectsBuilder[UserAudienceDeliverySnapshot]) (bool, error)
	// CustomVerification reads one verifier's mark on a peer.
	CustomVerification(ctx context.Context, verifierBotID int64, peer domain.Peer) (domain.CustomVerification, error)
	// PeerVerification returns the peer's single mark. Missing marks report
	// domain.ErrCustomVerificationNotFound.
	PeerVerification(ctx context.Context, peer domain.Peer) (domain.CustomVerification, error)
	// PeerVerificationBatch resolves the projection for many peers at once. This is
	// the call on the hot serialisation path, so peers without a mark are simply
	// absent instead of erroring.
	PeerVerificationBatch(ctx context.Context, peers []domain.Peer) (map[domain.Peer]domain.CustomVerification, error)
	// CountCustomVerifications reports how many peers a verifier has marked, for the
	// per-verifier bound.
	CountCustomVerifications(ctx context.Context, verifierBotID int64) (int, error)
	// ListCustomVerifications is the admin listing query with keyset paging.
	ListCustomVerifications(ctx context.Context, filter domain.CustomVerificationFilter) ([]domain.CustomVerification, error)

	// --- application queue ---

	// CreateCustomVerificationRequest files an application. A pending application on
	// the same (verifier, peer) reports domain.ErrCustomVerificationRequestExists.
	CreateCustomVerificationRequest(ctx context.Context, req domain.CustomVerificationRequest) (domain.CustomVerificationRequest, error)
	// DecideCustomVerificationRequest moves an application through its status
	// machine. approve=true grants the mark in the same transaction through the
	// supplied callback, so an approved application can never exist without its
	// mark; revoke removes it the same way.
	DecideCustomVerificationRequest(ctx context.Context, requestID int64, version int64, status domain.CustomVerificationRequestStatus, decidedBy, reason, note string, apply func(ctx context.Context, req domain.CustomVerificationRequest) error) (domain.CustomVerificationRequest, bool, error)
	// CustomVerificationRequest reads one application.
	CustomVerificationRequest(ctx context.Context, requestID int64) (domain.CustomVerificationRequest, error)
	// PendingCustomVerificationRequest returns the live application for a
	// (verifier, peer) pair, if any.
	PendingCustomVerificationRequest(ctx context.Context, verifierBotID int64, peer domain.Peer) (domain.CustomVerificationRequest, error)
	// ListCustomVerificationRequests is the review-queue query with keyset paging.
	ListCustomVerificationRequests(ctx context.Context, filter domain.CustomVerificationRequestFilter) ([]domain.CustomVerificationRequest, error)
	// CustomVerificationRequestsForApplicant returns an applicant's own history for
	// the verifier bot's /status command.
	CustomVerificationRequestsForApplicant(ctx context.Context, applicantUserID int64, limit int) ([]domain.CustomVerificationRequest, error)
	// CustomVerificationRequestCounts is the queue summary by status.
	CustomVerificationRequestCounts(ctx context.Context) (map[domain.CustomVerificationRequestStatus]int64, error)
}

// BotVerifierSettingsMutation is the committed verifier aggregate result.
// AffectedChannelIDs contains only channel/supergroup projection invalidations;
// callers may emit bounded transient updateChannel hints after commit, but must
// never treat those hints as reliable delivery.
type BotVerifierSettingsMutation struct {
	Settings           domain.BotVerifierSettings
	Changed            bool
	AffectedChannelIDs []int64
}

const (
	// MaxBotVerificationUserAudience bounds one user mark mutation. The frozen
	// set is intentionally identical to moderation updateUser fanout: self,
	// contacts and existing private-dialog counterparts, deduplicated before
	// persistence.
	MaxBotVerificationUserAudience = 4096
	// MaxBotVerifierSettingsDeliveryEffects bounds the complete reliable append
	// set of one verifier configuration transaction, across the verifier account
	// and every affected user mark.
	MaxBotVerifierSettingsDeliveryEffects = 4096
	// MaxBotVerifierSettingsChannelInvalidations bounds post-commit transient
	// cache invalidation. Exceeding it fails the owning mutation closed instead of
	// silently refreshing only a prefix.
	MaxBotVerifierSettingsChannelInvalidations = 4096
)
