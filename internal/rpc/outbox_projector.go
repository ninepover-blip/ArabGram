package rpc

import (
	"context"
	"errors"

	"github.com/iamxvbaba/td/clock"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/egress"
	"telesrv/internal/store"
)

var ErrOutboxProjectorUnavailable = errors.New("outbox projector is unavailable")
var ErrOutboxPresenceBatchUnavailable = errors.New("outbox batch presence provider is unavailable")

// OutboxUsersService makes sparse viewer projection a construction-time
// requirement for the production Egress projector instead of an optional
// runtime capability that could leave every claimed row retrying forever.
type OutboxUsersService interface {
	UsersService
	SparseBatchViewerUsersResolver
}

// OutboxProjectionDeps is the narrow set of read-model services needed to build
// receiver-specific online updates for Durable Egress.
type OutboxProjectionDeps struct {
	Users            OutboxUsersService
	Presence         UserLocationBatchProvider
	Usernames        UsernameRegistryService
	Dialogs          DialogsService
	Messages         MessagesService
	Stories          StoriesService
	Channels         ChannelsService
	Bots             BotsService
	AccountRatings   AccountRatingService
	BotVerifications BotVerificationService
}

// OutboxProjector exposes only the durable outbox projection boundary. It
// reuses Router's protocol projection implementation without exposing RPC
// dispatch or any write-path business methods to Egress.
type OutboxProjector struct {
	router *Router
}

// NewOutboxProjector creates a projection-only adapter for Egress. It is a
// separate constructor so Egress wiring does not need a CoreExec-capable router.
func NewOutboxProjector(cfg Config, deps OutboxProjectionDeps, log *zap.Logger, clk clock.Clock) (*OutboxProjector, error) {
	if deps.Users == nil || deps.Presence == nil {
		return nil, ErrOutboxPresenceBatchUnavailable
	}
	if log == nil {
		log = zap.NewNop()
	}
	if clk == nil {
		clk = clock.System
	}
	router := New(cfg, Deps{
		Users:            deps.Users,
		Usernames:        deps.Usernames,
		Dialogs:          deps.Dialogs,
		Messages:         deps.Messages,
		Stories:          deps.Stories,
		Channels:         deps.Channels,
		Bots:             deps.Bots,
		AccountRatings:   deps.AccountRatings,
		BotVerifications: deps.BotVerifications,
	}, log, clk)
	router.outboxPresence = deps.Presence
	return &OutboxProjector{router: router}, nil
}

// BuildOutboxUpdateBytes builds receiver-specific TL updates and returns their
// encoded wire bytes for claimed outbox rows. It satisfies OutboxUpdateBuilder.
func (p *OutboxProjector) BuildOutboxUpdateBytes(ctx context.Context, requests []egress.OutboxUpdateRequest) ([][]byte, error) {
	if p == nil || p.router == nil {
		return nil, ErrOutboxProjectorUnavailable
	}
	return p.router.BuildOutboxUpdateBytes(ctx, requests)
}

func (p *OutboxProjector) BuildChannelUpdateBytes(ctx context.Context, requests []egress.ChannelUpdateRequest) ([][]byte, error) {
	if p == nil || p.router == nil {
		return nil, ErrOutboxProjectorUnavailable
	}
	return p.router.BuildChannelUpdateBytes(ctx, requests)
}

// NewWelcomeDeliveryDispatcher wires the projection-only Egress router to the
// remote Edge control boundary. It does not expose Core RPC dispatch or local
// session ownership to the Egress process.
func (p *OutboxProjector) NewWelcomeDeliveryDispatcher(sessions EdgeController, deliveries store.WelcomeMessageDeliveryStore, log *zap.Logger) *WelcomeDeliveryDispatcher {
	if p == nil || p.router == nil {
		return NewWelcomeDeliveryDispatcher(nil, deliveries, log)
	}
	p.router.deps.Sessions = sessions
	return NewWelcomeDeliveryDispatcher(p.router, deliveries, log)
}

func (p *OutboxProjector) InvalidateStoryReadModelViewers(viewerUserIDs ...int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateStoryReadModelViewers(viewerUserIDs...)
	}
}

func (p *OutboxProjector) InvalidateStoryReadModelPeer(peer domain.Peer) {
	if p != nil && p.router != nil {
		p.router.InvalidateStoryReadModelPeer(peer)
	}
}

func (p *OutboxProjector) FlushStoryReadModelCache() {
	if p != nil && p.router != nil {
		p.router.FlushStoryReadModelCache()
	}
}

func (p *OutboxProjector) InvalidateChannelFullBotInfoReadModel(channelID int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateChannelFullBotInfoReadModel(channelID)
	}
}

func (p *OutboxProjector) FlushChannelFullBotInfoReadModel() {
	if p != nil && p.router != nil {
		p.router.FlushChannelFullBotInfoReadModel()
	}
}

func (p *OutboxProjector) InvalidateRPCProjectionReadModelForViewer(viewerUserID int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateRPCProjectionReadModelForViewer(viewerUserID)
	}
}

func (p *OutboxProjector) InvalidateRPCProjectionReadModelForContactOwner(ownerUserID int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateRPCProjectionReadModelForContactOwner(ownerUserID)
	}
}

func (p *OutboxProjector) InvalidateRPCProjectionReadModelForUser(userID int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateRPCProjectionReadModelForUser(userID)
	}
}

func (p *OutboxProjector) InvalidateRPCProjectionReadModelForPeer(ownerUserID int64, peer domain.Peer) {
	if p != nil && p.router != nil {
		p.router.InvalidateRPCProjectionReadModelForPeer(ownerUserID, peer)
	}
}

func (p *OutboxProjector) InvalidateRPCProjectionReadModelForChannel(channelID int64) {
	if p != nil && p.router != nil {
		p.router.InvalidateRPCProjectionReadModelForChannel(channelID)
	}
}

func (p *OutboxProjector) FlushRPCProjectionReadModel() {
	if p != nil && p.router != nil {
		p.router.FlushRPCProjectionReadModel()
	}
}
