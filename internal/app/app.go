package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"event-driven-notification-system/internal/config"
	"event-driven-notification-system/internal/observe"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/repository/postgres"
	"event-driven-notification-system/pkg/logger"
)

type Services struct {
	Config config.Config
	Store  *postgres.Store
	Broker *broker.Broker
}

func Main(run func(context.Context, *Services) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := start(ctx, run); e != nil && !errors.Is(e, context.Canceled) {
		slog.Error("service stopped", "error", e)
		os.Exit(1)
	}
}
func start(ctx context.Context, run func(context.Context, *Services) error) error {
	c, e := config.Load()
	if e != nil {
		return e
	}
	slog.SetDefault(logger.New(os.Stdout, c.Level))
	shutdown, e := observe.Tracing(ctx, filepath.Base(os.Args[0]))
	if e != nil {
		return e
	}
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = shutdown(flush)
	}()
	s, e := postgres.Open(ctx, c)
	if e != nil {
		return e
	}
	defer s.DB.Close()
	b := broker.New(c.RedisAddr, c.RedisPassword, 0)
	defer b.Client.Close()
	return run(ctx, &Services{c, s, b})
}
func Wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (s *Services) Relay(ctx context.Context) {
	for ctx.Err() == nil {
		n, e := s.Store.Flush(ctx, s.Broker.Publish)
		switch {
		case e != nil:
			if ctx.Err() == nil {
				slog.Error("dispatch outbox", "error", e)
			}
			Wait(ctx, time.Second)
		case n == 0:
			Wait(ctx, 50*time.Millisecond)
		}
	}
}
