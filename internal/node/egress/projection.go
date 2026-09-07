package egress

import (
	"github.com/iamxvbaba/td/clock"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	adminapp "telesrv/internal/admin"
	botsapp "telesrv/internal/app/bots"
	botverificationapp "telesrv/internal/app/botverification"
	channelapp "telesrv/internal/app/channels"
	"telesrv/internal/app/dialogs"
	messageapp "telesrv/internal/app/messages"
	ratingapp "telesrv/internal/app/rating"
	storiesapp "telesrv/internal/app/stories"
	usernamesapp "telesrv/internal/app/usernames"
	"telesrv/internal/app/users"
	"telesrv/internal/config"
	"telesrv/internal/edgecontrol"
	nodeprojection "telesrv/internal/node/projection"
	"telesrv/internal/officialgifts"
	"telesrv/internal/rpc"
	"telesrv/internal/store/postgres"
)

type egressProjectionRuntime struct {
	projector *rpc.OutboxProjector
	caches    postgres.ReadModelCacheSet
}

func newEgressProjectionRuntime(pool *pgxpool.Pool, rdb *redis.Client, cfg config.EgressConfig, instanceID string, presence edgecontrol.UserLocationBatchProvider, logger *zap.Logger) (egressProjectionRuntime, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	projectionStores := nodeprojection.NewStores(pool, rdb, nodeprojection.StoreConfig{
		ChannelRowCacheMaxEntries:    cfg.ChannelRowCacheMaxEntries,
		ChannelMemberCacheMaxEntries: cfg.ChannelMemberCacheMaxEntries,
		ChannelDialogCacheMaxEntries: cfg.ChannelDialogCacheMaxEntries,
		ChannelBoostCacheMaxEntries:  cfg.ChannelBoostCacheMaxEntries,
		ChannelBoostCacheTTL:         cfg.ChannelBoostCacheTTL,
	}, nodeprojection.StoreOptions{}, logger)
	userStore := projectionStores.UserStore
	contactStore := projectionStores.ContactStore
	collectiblePhoneStore := projectionStores.CollectiblePhoneStore
	readModelVersions := projectionStores.ReadModelVersions
	channelStore := projectionStores.ChannelStore
	dialogStore := projectionStores.DialogStore
	messageStore := projectionStores.MessageStore
	cachedPhotos := projectionStores.Photos
	userCache := projectionStores.UserCache
	adminStore := postgres.NewAdminStore(pool)
	adminService := adminapp.NewService(adminapp.Dependencies{
		Commands:      adminStore,
		Restrictions:  adminStore,
		OfficialGifts: officialgifts.New(cfg.OfficialGiftsDir),
	})
	privacyService := projectionStores.NewPrivacyService()
	usersService := users.NewService(userStore,
		users.WithBaseUserCache(userCache),
		users.WithContactStore(contactStore),
		users.WithPhotoProvider(cachedPhotos),
		users.WithPrivacyEvaluator(privacyService),
		users.WithAccountFreezeProvider(adminService),
		users.WithCollectiblePhoneStore(collectiblePhoneStore),
	)
	botStore := postgres.NewBotStore(pool)
	botsService := botsapp.NewService(userStore, botStore, messageStore,
		botsapp.WithLogger(logger.Named("app").Named("bots")),
		botsapp.WithBlockChecker(contactStore),
		botsapp.WithPublicChannelUsernameResolver(channelStore),
		botsapp.WithUserCache(userCache),
		botsapp.WithPublicBaseURL(cfg.PublicBaseURL),
	)
	channelsService := channelapp.NewService(channelStore,
		channelapp.WithBotProfileResolver(botsService),
		channelapp.WithReadModelVersions(readModelVersions),
		channelapp.WithSendPermissionChecker(adminService),
	)
	privacyService.ConfigureReadModels(usersService, channelStore)
	dialogsService := dialogs.NewService(dialogStore, channelStore).Configure(
		dialogs.WithContactStore(contactStore),
		dialogs.WithPhotoProvider(cachedPhotos),
		dialogs.WithPrivacyEvaluator(privacyService),
		dialogs.WithAccountFreezeProvider(adminService),
		dialogs.WithCollectiblePhoneProvider(collectiblePhoneStore),
		dialogs.WithReadModelVersions(readModelVersions),
	)
	messagesService := messageapp.NewService(messageStore, dialogStore,
		messageapp.WithContactStore(contactStore),
		messageapp.WithPhotoProvider(cachedPhotos),
		messageapp.WithPrivacyEvaluator(privacyService),
		messageapp.WithAccountFreezeProvider(adminService),
		messageapp.WithCollectiblePhoneProvider(collectiblePhoneStore),
		messageapp.WithReadModelVersions(readModelVersions),
		messageapp.WithSendPermissionChecker(adminService),
	)
	storiesService := storiesapp.NewService(postgres.NewStoryStore(pool), storiesapp.WithChannelStoryAccess(channelsService))
	collectibleUsernameStore := postgres.NewCollectibleUsernameStore(pool)
	usernamesService := usernamesapp.NewService(
		usernamesapp.WithRegistryStore(collectibleUsernameStore),
		usernamesapp.WithCollectibleStore(collectibleUsernameStore),
		usernamesapp.WithURLTemplate(cfg.CollectibleUsernameURLTemplate),
		usernamesapp.WithPublicBaseURL(cfg.PublicBaseURL),
		usernamesapp.WithLogger(logger.Named("app").Named("usernames")),
	)
	ratingService := ratingapp.NewService(
		ratingapp.WithStore(postgres.NewAccountRatingStore(pool)),
		ratingapp.WithEnabled(cfg.RatingEnabled),
		ratingapp.WithWeights(cfg.AccountRatingWeights()),
		ratingapp.WithPendingDelay(cfg.RatingPendingDelay),
		ratingapp.WithStaleAfter(cfg.RatingStaleAfter),
		ratingapp.WithLogger(logger.Named("app").Named("rating")),
	)
	botVerificationService := botverificationapp.NewService(
		botverificationapp.WithStore(postgres.NewBotVerificationStore(pool)),
		botverificationapp.WithUserDirectory(usersService),
		botverificationapp.WithBotDirectory(botsService),
		botverificationapp.WithChannelDirectory(channelsService),
		botverificationapp.WithLogger(logger.Named("app").Named("botverification")),
	)
	botsService.SetCustomVerification(botVerificationService)

	projector, err := rpc.NewOutboxProjector(rpc.Config{
		DC:                            cfg.DC,
		DefaultCountryCode:            cfg.DefaultCountryCode,
		IP:                            cfg.AdvertiseIP,
		Port:                          cfg.AdvertisePort,
		InstanceID:                    instanceID,
		OutboundPushTimeout:           cfg.OutboundPushTimeout,
		PublicBaseURL:                 cfg.PublicBaseURL,
		UpdatePublicURL:               cfg.UpdatePublicURL,
		PublicAppScheme:               cfg.PublicAppScheme,
		PublicAppLinkBase:             cfg.PublicAppLinkBase,
		TempKeyResolveCacheTTL:        cfg.TempKeyResolveCacheTTL,
		TempKeyResolveCacheMaxEntries: cfg.TempKeyResolveCacheMaxEntries,
		AuthUserCacheTTL:              cfg.AuthUserCacheTTL,
	}, rpc.OutboxProjectionDeps{
		Users:            usersService,
		Presence:         presence,
		Usernames:        usernamesService,
		Dialogs:          dialogsService,
		Messages:         messagesService,
		Stories:          storiesService,
		Channels:         channelsService,
		Bots:             botsService,
		AccountRatings:   ratingService,
		BotVerifications: botVerificationService,
	}, logger.Named("rpc").Named("outbox-projector"), clock.System)
	if err != nil {
		return egressProjectionRuntime{}, err
	}

	return egressProjectionRuntime{
		projector: projector,
		caches: projectionStores.ReadModelCacheSet(nodeprojection.CacheSetDeps{
			Dialogs:            dialogsService,
			Privacy:            privacyService,
			Stories:            projector,
			ChannelFullBots:    projector,
			ChannelBotMembers:  channelsService,
			ChannelMediaCounts: channelsService,
			PrivateMediaCounts: messagesService,
			RPCProjections:     projector,
			BotProfiles:        botsService,
		}),
	}, nil
}
