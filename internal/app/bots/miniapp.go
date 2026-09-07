package bots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const (
	defaultMainAppShortName     = "main"
	requestedWebViewButtonTTL   = 10 * time.Minute
	webViewCustomMethodQueryTTL = 5 * time.Minute
)

func (s *Service) UpsertBotApp(ctx context.Context, botUserID int64, app domain.BotApp) (domain.BotApp, int, error) {
	clean, err := s.normalizeBotApp(ctx, botUserID, app)
	if err != nil {
		return domain.BotApp{}, 0, err
	}
	out, version, err := s.bots.UpsertBotApp(ctx, clean)
	if err != nil {
		return domain.BotApp{}, 0, err
	}
	s.invalidateBotReadCaches(ctx, botUserID)
	return out, version, nil
}

func (s *Service) EnsureMenuBotApp(ctx context.Context, botUserID int64, button domain.BotMenuButton) (domain.BotApp, int, error) {
	if button.Type != domain.BotMenuButtonWebView {
		return domain.BotApp{}, 0, nil
	}
	desiredApp, err := s.normalizeBotApp(ctx, botUserID, domain.BotApp{
		BotUserID:          botUserID,
		ShortName:          defaultMainAppShortName,
		Title:              button.Text,
		URL:                button.URL,
		Main:               true,
		RequestWriteAccess: true,
	})
	if err != nil {
		return domain.BotApp{}, 0, err
	}
	app, found, err := s.GetBotAppByShortName(ctx, botUserID, desiredApp.ShortName)
	if err != nil {
		return domain.BotApp{}, 0, err
	}
	version := 0
	if !found || app != desiredApp {
		app, version, err = s.UpsertBotApp(ctx, botUserID, desiredApp)
		if err != nil {
			return domain.BotApp{}, 0, err
		}
	}
	desiredAttach := domain.BotAttachMenuBot{
		BotUserID:          botUserID,
		AppID:              app.ID,
		ShortName:          app.ShortName,
		RequestWriteAccess: app.RequestWriteAccess,
		ShowInAttachMenu:   true,
		ShowInSideMenu:     true,
		PeerTypes:          []string{"pm", "chat", "megagroup", "broadcast"},
	}
	attach, found, err := s.GetAttachMenuBot(ctx, botUserID)
	if err != nil {
		return domain.BotApp{}, 0, err
	}
	if !found || !equalAttachMenuBot(attach, desiredAttach) {
		attachVersion, err := s.UpsertAttachMenuBot(ctx, botUserID, desiredAttach)
		if err != nil {
			return domain.BotApp{}, 0, err
		}
		if version == 0 {
			version = attachVersion
		}
	}
	return app, version, nil
}

func equalAttachMenuBot(left, right domain.BotAttachMenuBot) bool {
	if len(left.PeerTypes) == 0 {
		left.PeerTypes = nil
	}
	if len(right.PeerTypes) == 0 {
		right.PeerTypes = nil
	}
	if len(left.Icons) == 0 {
		left.Icons = nil
	}
	if len(right.Icons) == 0 {
		right.Icons = nil
	}
	return reflect.DeepEqual(left, right)
}

func (s *Service) normalizeBotApp(ctx context.Context, botUserID int64, app domain.BotApp) (domain.BotApp, error) {
	if s == nil || s.bots == nil || botUserID == 0 {
		return domain.BotApp{}, domain.ErrBotAppInvalid
	}
	app.BotUserID = botUserID
	app.ShortName = strings.ToLower(strings.TrimSpace(app.ShortName))
	app.Title = strings.TrimSpace(app.Title)
	app.Description = strings.TrimSpace(app.Description)
	app.URL = strings.TrimSpace(app.URL)
	if !validBotAppShortName(app.ShortName) {
		return domain.BotApp{}, domain.ErrBotAppShortNameInvalid
	}
	if app.Title == "" || utf8.RuneCountInString(app.Title) > domain.MaxBotAppTitleLen ||
		utf8.RuneCountInString(app.Description) > domain.MaxBotAppDescriptionLen ||
		len(app.URL) > domain.MaxBotAppURLLen || !validHTTPSURL(app.URL) {
		return domain.BotApp{}, domain.ErrBotAppInvalid
	}
	if app.ID == 0 {
		app.ID = stableBotAppInt64("bot-app-id", fmt.Sprint(botUserID), app.ShortName)
	}
	if app.AccessHash == 0 {
		if existing, found, err := s.bots.GetBotAppByShortName(ctx, botUserID, app.ShortName); err == nil && found {
			app.AccessHash = existing.AccessHash
			if app.ID == 0 {
				app.ID = existing.ID
			}
		}
	}
	if app.AccessHash == 0 {
		app.AccessHash = stableBotAppInt64("bot-app-access", fmt.Sprint(botUserID), app.ShortName, app.URL)
	}
	app.Hash = botAppHash(app)
	return app, nil
}

func (s *Service) GetBotAppByID(ctx context.Context, appID, accessHash int64) (domain.BotApp, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotApp{}, false, nil
	}
	return s.bots.GetBotAppByID(ctx, appID, accessHash)
}

func (s *Service) GetBotAppByShortName(ctx context.Context, botUserID int64, shortName string) (domain.BotApp, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotApp{}, false, nil
	}
	return s.bots.GetBotAppByShortName(ctx, botUserID, strings.ToLower(strings.TrimSpace(shortName)))
}

func (s *Service) GetMainBotApp(ctx context.Context, botUserID int64) (domain.BotApp, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotApp{}, false, nil
	}
	return s.bots.GetMainBotApp(ctx, botUserID)
}

func (s *Service) ListBotApps(ctx context.Context, botUserID int64) ([]domain.BotApp, error) {
	if s == nil || s.bots == nil {
		return nil, nil
	}
	return s.bots.ListBotApps(ctx, botUserID)
}

func (s *Service) GetBotAppSettings(ctx context.Context, botUserID int64) (domain.BotAppSettings, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotAppSettings{}, false, nil
	}
	return s.bots.GetBotAppSettings(ctx, botUserID)
}

func (s *Service) UpsertBotAppSettings(ctx context.Context, botUserID int64, settings domain.BotAppSettings) (int, error) {
	if s == nil || s.bots == nil || botUserID == 0 {
		return 0, domain.ErrBotAppInvalid
	}
	version, err := s.bots.UpsertBotAppSettings(ctx, botUserID, settings)
	if err != nil {
		return 0, err
	}
	s.invalidateBotReadCaches(ctx, botUserID)
	return version, nil
}

func (s *Service) ListBotAppPreviewMedia(ctx context.Context, botUserID, appID int64) ([]domain.BotAppPreviewMedia, error) {
	if s == nil || s.bots == nil {
		return nil, nil
	}
	return s.bots.ListBotAppPreviewMedia(ctx, botUserID, appID)
}

func (s *Service) UpsertBotAppPreviewMedia(ctx context.Context, media domain.BotAppPreviewMedia) (domain.BotAppPreviewMedia, int, error) {
	if s == nil || s.bots == nil {
		return domain.BotAppPreviewMedia{}, 0, domain.ErrBotAppInvalid
	}
	if media.ID == 0 {
		items, err := s.bots.ListBotAppPreviewMedia(ctx, media.BotUserID, media.AppID)
		if err != nil {
			return domain.BotAppPreviewMedia{}, 0, err
		}
		if len(items) >= domain.MaxBotPreviewMedia {
			return domain.BotAppPreviewMedia{}, 0, domain.ErrBotAppInvalid
		}
	}
	out, version, err := s.bots.UpsertBotAppPreviewMedia(ctx, media)
	if err != nil {
		return domain.BotAppPreviewMedia{}, 0, err
	}
	s.invalidateBotReadCaches(ctx, media.BotUserID)
	return out, version, nil
}

func (s *Service) DeleteBotAppPreviewMedia(ctx context.Context, botUserID, appID, mediaID int64) (int, error) {
	version, err := s.bots.DeleteBotAppPreviewMedia(ctx, botUserID, appID, mediaID)
	if err != nil {
		return 0, err
	}
	s.invalidateBotReadCaches(ctx, botUserID)
	return version, nil
}

func (s *Service) ReorderBotAppPreviewMedia(ctx context.Context, botUserID, appID int64, mediaIDs []int64) (int, error) {
	version, err := s.bots.ReorderBotAppPreviewMedia(ctx, botUserID, appID, mediaIDs)
	if err != nil {
		return 0, err
	}
	s.invalidateBotReadCaches(ctx, botUserID)
	return version, nil
}

func (s *Service) UpsertAttachMenuBot(ctx context.Context, botUserID int64, bot domain.BotAttachMenuBot) (int, error) {
	if s == nil || s.bots == nil || botUserID == 0 {
		return 0, domain.ErrBotAttachMenuInvalid
	}
	bot.BotUserID = botUserID
	bot.ShortName = strings.ToLower(strings.TrimSpace(bot.ShortName))
	if bot.ShortName == "" {
		if app, found, err := s.GetMainBotApp(ctx, botUserID); err == nil && found {
			bot.AppID = app.ID
			bot.ShortName = app.ShortName
			bot.HasSettings = app.HasSettings
			bot.RequestWriteAccess = app.RequestWriteAccess
		}
	}
	if !validBotAppShortName(bot.ShortName) {
		return 0, domain.ErrBotAttachMenuInvalid
	}
	if len(bot.PeerTypes) == 0 {
		bot.PeerTypes = []string{"pm", "chat", "megagroup", "broadcast"}
	}
	if len(bot.PeerTypes) > domain.MaxBotAttachMenuPeerTypes || len(bot.Icons) > domain.MaxBotAttachMenuIcons {
		return 0, domain.ErrBotAttachMenuInvalid
	}
	if !bot.ShowInAttachMenu && !bot.ShowInSideMenu {
		bot.ShowInAttachMenu = true
		bot.ShowInSideMenu = true
	}
	version, err := s.bots.UpsertAttachMenuBot(ctx, bot)
	if err != nil {
		return 0, err
	}
	s.invalidateBotReadCaches(ctx, botUserID)
	return version, nil
}

func (s *Service) GetAttachMenuBot(ctx context.Context, botUserID int64) (domain.BotAttachMenuBot, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotAttachMenuBot{}, false, nil
	}
	return s.bots.GetAttachMenuBot(ctx, botUserID)
}

func (s *Service) ListAttachMenuBots(ctx context.Context) ([]domain.BotAttachMenuBot, error) {
	if s == nil || s.bots == nil {
		return nil, nil
	}
	return s.bots.ListAttachMenuBots(ctx)
}

func (s *Service) GetAttachMenuState(ctx context.Context, userID, botUserID int64) (domain.BotAttachMenuState, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotAttachMenuState{}, false, nil
	}
	return s.bots.GetAttachMenuState(ctx, userID, botUserID)
}

func (s *Service) SetAttachMenuState(ctx context.Context, state domain.BotAttachMenuState, effects store.DeliveryEffectsBuilder[domain.BotAttachMenuState]) (domain.BotAttachMenuState, error) {
	if s == nil || s.bots == nil {
		return domain.BotAttachMenuState{}, domain.ErrBotAttachMenuInvalid
	}
	return s.bots.SetAttachMenuState(ctx, state, effects)
}

func (s *Service) SaveRequestedWebViewButton(ctx context.Context, button domain.BotRequestedWebViewButton) (domain.BotRequestedWebViewButton, error) {
	if s == nil || s.bots == nil || button.BotUserID == 0 || button.UserID == 0 || button.ButtonID == 0 {
		return domain.BotRequestedWebViewButton{}, domain.ErrBotRequestedButtonInvalid
	}
	if button.WebAppReqID == "" {
		rnd, err := randomInt64()
		if err != nil {
			return domain.BotRequestedWebViewButton{}, err
		}
		button.WebAppReqID = hex.EncodeToString([]byte(fmt.Sprintf("%d:%d:%d", button.BotUserID, button.UserID, rnd)))
	}
	if button.MaxQuantity <= 0 {
		button.MaxQuantity = 1
	}
	if button.MaxQuantity > domain.MaxBotRequestedPeerQuantity {
		return domain.BotRequestedWebViewButton{}, domain.ErrBotRequestedButtonInvalid
	}
	now := s.now()
	if button.CreatedAt.IsZero() {
		button.CreatedAt = now
	}
	if button.ExpiresAt.IsZero() {
		button.ExpiresAt = now.Add(requestedWebViewButtonTTL)
	}
	if err := s.bots.SaveRequestedWebViewButton(ctx, button); err != nil {
		return domain.BotRequestedWebViewButton{}, err
	}
	return button, nil
}

func (s *Service) GetRequestedWebViewButton(ctx context.Context, botUserID, userID int64, reqID string) (domain.BotRequestedWebViewButton, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotRequestedWebViewButton{}, false, nil
	}
	return s.bots.GetRequestedWebViewButton(ctx, botUserID, userID, reqID)
}

func (s *Service) DeleteRequestedWebViewButton(ctx context.Context, botUserID, userID int64, reqID string) error {
	if s == nil || s.bots == nil {
		return nil
	}
	return s.bots.DeleteRequestedWebViewButton(ctx, botUserID, userID, reqID)
}

func (s *Service) SetBotEmojiStatusPermission(ctx context.Context, botUserID, userID int64, allowed bool) error {
	if s == nil || s.bots == nil {
		return domain.ErrBotNotFound
	}
	return s.bots.SetBotEmojiStatusPermission(ctx, botUserID, userID, allowed)
}

func (s *Service) BotEmojiStatusPermission(ctx context.Context, botUserID, userID int64) (bool, error) {
	if s == nil || s.bots == nil {
		return false, nil
	}
	return s.bots.BotEmojiStatusPermission(ctx, botUserID, userID)
}

func (s *Service) PutWebViewCustomMethodQuery(ctx context.Context, botUserID, userID int64, method, paramsJSON string) (domain.BotWebViewCustomMethodQuery, error) {
	method = strings.TrimSpace(method)
	if s == nil || s.bots == nil || botUserID == 0 || userID == 0 || method == "" || len(method) > domain.MaxBotCustomMethodLen || len(paramsJSON) > domain.MaxBotCustomMethodPayloadLen {
		return domain.BotWebViewCustomMethodQuery{}, domain.ErrBotCustomMethodUnavailable
	}
	rnd, err := randomInt64()
	if err != nil {
		return domain.BotWebViewCustomMethodQuery{}, err
	}
	now := s.now()
	query := domain.BotWebViewCustomMethodQuery{
		ID:           fmt.Sprintf("%d:%d:%d:%d", botUserID, userID, now.UnixNano(), rnd),
		BotUserID:    botUserID,
		UserID:       userID,
		CustomMethod: method,
		ParamsJSON:   paramsJSON,
		CreatedAt:    now,
		ExpiresAt:    now.Add(webViewCustomMethodQueryTTL),
	}
	if err := s.bots.PutWebViewCustomMethodQuery(ctx, query); err != nil {
		return domain.BotWebViewCustomMethodQuery{}, err
	}
	return query, nil
}

func validHTTPSURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != ""
}

func validBotAppShortName(shortName string) bool {
	if shortName == "" || len(shortName) > domain.MaxBotAppShortNameLen {
		return false
	}
	for _, r := range shortName {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}
	return true
}

func botAppHash(app domain.BotApp) int64 {
	return stableBotAppInt64("bot-app-hash", fmt.Sprint(app.BotUserID), app.ShortName, app.Title, app.Description, app.URL, fmt.Sprint(app.PhotoID), fmt.Sprint(app.DocumentID), fmt.Sprint(app.Inactive), fmt.Sprint(app.RequestWriteAccess), fmt.Sprint(app.HasSettings), fmt.Sprint(app.Main))
}

func stableBotAppInt64(parts ...string) int64 {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	value := int64(binary.BigEndian.Uint64(sum[:8]) & 0x7fffffffffffffff)
	if value == 0 {
		return 1
	}
	return value
}
