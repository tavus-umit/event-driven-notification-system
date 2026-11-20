package main

import (
	"context"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/observe"
	"event-driven-notification-system/internal/provider"
	"event-driven-notification-system/internal/ratelimit"
	"event-driven-notification-system/internal/worker"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

func main() { app.Main(run) }
func run(ctx context.Context, s *app.Services) error {
	var wg sync.WaitGroup
	start := func(f func()) { wg.Add(1); go func() { defer wg.Done(); f() }() }
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	metricsErr := make(chan error, 1)
	m := http.NewServeMux()
	m.Handle("/metrics", observe.Metrics(s.Broker))
	start(func() {
		e := observe.Serve(ctx, &http.Server{Addr: s.Config.MetricsAddr, Handler: m, ReadHeaderTimeout: 5 * time.Second})
		metricsErr <- e
		cancel()
	})
	start(func() { s.Relay(ctx) })
	start(func() {
		for ctx.Err() == nil {
			if e := s.Store.Recover(ctx); e != nil && ctx.Err() == nil {
				slog.Error("recover expired claims", "error", e)
			}
			app.Wait(ctx, 5*time.Second)
		}
	})
	channels := domain.Channels
	if s.Config.Channel != "" {
		channels = []domain.Channel{s.Config.Channel}
	}
	for _, ch := range channels {
		p := &worker.Pool{Service: s, Channel: ch, Sender: provider.New(s.Config.ProviderURL, s.Config.ProviderTimeout, ch, s.Config.BreakerThreshold), Limiter: ratelimit.Limiter{Client: s.Broker.Client}}
		start(func() { p.Run(ctx, s.Config.Workers) })
		start(func() { p.Retries(ctx) })
	}
	wg.Wait()
	return <-metricsErr
}
