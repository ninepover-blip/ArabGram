package config

import "telesrv/internal/branding"

type ProcessGlobals struct {
	Branding           branding.Config
	PremiumBotUsername string
	PremiumBotUserID   int64
}

func processGlobalsFromConfig(c Config) ProcessGlobals {
	return ProcessGlobals{
		Branding:           c.Branding,
		PremiumBotUsername: c.PremiumBotUsername,
		PremiumBotUserID:   c.PremiumBotUserID,
	}
}

func (c Config) ProcessGlobals() ProcessGlobals {
	return processGlobalsFromConfig(c)
}

func (c Config) DebugAddress() string {
	return c.DebugAddr
}
