package postgres

import (
	"context"

	"go.uber.org/zap"
)

const BootstrapUpdateReadyNotifyChannel = "telesrv_bootstrap_update_ready"

type BootstrapUpdateReadyListener struct {
	dsn string
	log *zap.Logger
}

func NewBootstrapUpdateReadyListener(dsn string, log *zap.Logger) *BootstrapUpdateReadyListener {
	if log == nil {
		log = zap.NewNop()
	}
	return &BootstrapUpdateReadyListener{dsn: dsn, log: log}
}

func (l *BootstrapUpdateReadyListener) Run(ctx context.Context, wake func()) {
	if l == nil || wake == nil {
		return
	}
	backoff := channelListenerInitialBackoff
	for ctx.Err() == nil {
		err := l.listenAndConsume(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("bootstrap update ready listener disconnected; reconnecting",
			zap.Error(err), zap.Duration("backoff", backoff))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > channelListenerMaxBackoff {
			backoff = channelListenerMaxBackoff
		}
	}
}

func (l *BootstrapUpdateReadyListener) listenAndConsume(ctx context.Context, wake func()) error {
	conn, err := connectPostgresAdmitted(ctx, l.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN "+BootstrapUpdateReadyNotifyChannel); err != nil {
		return err
	}
	wake()
	l.log.Info("bootstrap update ready listener active", zap.String("notify_channel", BootstrapUpdateReadyNotifyChannel))
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification != nil && notification.Channel == BootstrapUpdateReadyNotifyChannel {
			wake()
		}
	}
}
