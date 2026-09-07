package store

import (
	"context"

	"telesrv/internal/domain"
)

// AppConfigStore 持久化 help.getAppConfig 数据。
type AppConfigStore interface {
	GetAppConfig(ctx context.Context, client string) (domain.AppConfig, bool, error)
	UpsertAppConfig(ctx context.Context, cfg domain.AppConfig) error
}

// CountryStore 持久化 help.getCountriesList 数据。
type CountryStore interface {
	ListCountries(ctx context.Context, langCode string) (domain.CountriesList, error)
	UpsertCountries(ctx context.Context, countries []domain.Country) error
}
