package main

import (
	"context"
	"event-driven-notification-system/internal/api"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/observe"
	"net/http"
	"time"
)

func main() { app.Main(run) }
func run(ctx context.Context, s *app.Services) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.Relay(ctx) }()
	defer func() { cancel(); <-done }()
	a := api.API{S: s}
	server := &http.Server{Addr: s.Config.Addr, Handler: a.Router(ctx), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: s.Config.ReadTimeout, WriteTimeout: s.Config.WriteTimeout}
	return observe.Serve(ctx, server)
}
