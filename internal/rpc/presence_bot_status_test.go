package rpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
)

func TestUserBotStatusUsesBaseFactProviderAndCachesOnlyKnownResults(t *testing.T) {
	users := &captureBaseBotStatusUsers{bot: true, found: true}
	r := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	for i := 0; i < 2; i++ {
		bot, known := r.userBotStatus(context.Background(), 42)
		if !known || !bot {
			t.Fatalf("bot status = %v, known=%v", bot, known)
		}
	}
	if users.statusCalls.Load() != 1 || users.byIDCalls.Load() != 0 {
		t.Fatalf("calls = status:%d projected:%d", users.statusCalls.Load(), users.byIDCalls.Load())
	}

	unknown := &captureBaseBotStatusUsers{err: errors.New("redis and postgres unavailable")}
	r = New(Config{}, Deps{Users: unknown}, zaptest.NewLogger(t), clock.System)
	for i := 0; i < 2; i++ {
		if bot, known := r.userBotStatus(context.Background(), 43); known || bot {
			t.Fatalf("unknown status = %v, known=%v", bot, known)
		}
	}
	if unknown.statusCalls.Load() != 2 {
		t.Fatalf("unknown result was cached; calls = %d", unknown.statusCalls.Load())
	}
}

type captureBaseBotStatusUsers struct {
	UsersService
	bot         bool
	found       bool
	err         error
	statusCalls atomic.Int64
	byIDCalls   atomic.Int64
}

func (u *captureBaseBotStatusUsers) BotStatus(context.Context, int64) (bool, bool, error) {
	u.statusCalls.Add(1)
	return u.bot, u.found, u.err
}

func (u *captureBaseBotStatusUsers) ByID(context.Context, int64, int64) (domain.User, bool, error) {
	u.byIDCalls.Add(1)
	return domain.User{ID: 42, Bot: u.bot}, u.found, u.err
}
