package worker

import (
	"context"
	"errors"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/observe"
	"event-driven-notification-system/internal/provider"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/ratelimit"
	"event-driven-notification-system/pkg/logger"
	"fmt"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

type Sender interface {
	Deliver(context.Context, domain.Notification) provider.Result
}
type Pool struct {
	Service *app.Services
	Channel domain.Channel
	Sender  Sender
	Limiter ratelimit.Limiter
}

func (p *Pool) Run(ctx context.Context, workers int) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				l, e := p.Service.Broker.Pop(ctx, p.Channel)
				if l != nil {
					p.process(ctx, l, e)
					continue
				}
				if e != nil && ctx.Err() == nil {
					slog.Error("dequeue", "channel", p.Channel, "error", e)
					app.Wait(ctx, time.Second)
				} else {
					app.Wait(ctx, 50*time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
}
func (p *Pool) process(parent context.Context, l *broker.Lease, parseErr error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 45*time.Second)
	defer cancel()
	owner := uuid.NewString()
	defer func() {
		if v := recover(); v != nil {
			slog.Error("worker panic", "notification_id", l.Job.ID, "channel", p.Channel, "stack", string(debug.Stack()))
			p.quarantine(ctx, l, owner, fmt.Sprintf("processing panic: %v", v))
		}
	}()
	if parseErr != nil {
		p.quarantine(ctx, l, owner, parseErr.Error())
		return
	}
	if e := p.handle(ctx, l, owner); e != nil {
		slog.Error("delivery lease retained", "channel", p.Channel, "notification_id", l.Job.ID, "error", e)
		return
	}
	if e := p.Service.Broker.Ack(ctx, p.Channel, l); e != nil {
		slog.Error("acknowledge lease", "notification_id", l.Job.ID, "error", e)
	}
}
func (p *Pool) quarantine(ctx context.Context, l *broker.Lease, owner, reason string) {
	if _, e := uuid.Parse(l.Job.ID); e == nil {
		if e = p.Service.Store.Poison(ctx, l.Job.ID, owner, p.Channel, reason); e != nil {
			slog.Error("persist poison failure", "error", e)
			return
		}
	}
	if e := p.Service.Broker.Quarantine(ctx, p.Channel, l); e != nil {
		slog.Error("quarantine lease", "error", e)
	}
}
func (p *Pool) handle(ctx context.Context, l *broker.Lease, owner string) error {
	permit, e := p.Limiter.Wait(ctx, p.Channel)
	if e != nil {
		return e
	}
	defer func() {
		if e := permit.Done(ctx); e != nil {
			slog.Warn("rate reservation retained", "error", e)
		}
	}()
	n, e := p.Service.Store.Claim(ctx, l.Job.ID, owner, p.Channel, l.Job.Retry)
	if errors.Is(e, domain.ErrNotFound) {
		return nil
	}
	if e != nil {
		return e
	}
	ctx = logger.WithID(ctx, n.CorrelationID)
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": n.Traceparent})
	ctx, span := otel.Tracer("notification/worker").Start(ctx, "process notification")
	defer span.End()
	started := time.Now()
	result := p.Sender.Deliver(ctx, n)
	if result.Deferred {
		return p.Service.Store.DeferProbe(ctx, n.ID, owner)
	}
	a, status, due := outcome(n, result, started)
	e = p.Service.Store.Finish(ctx, n.ID, owner, a, status, result.MessageID, due)
	if e == nil {
		ch := string(n.Channel)
		observe.Latency.WithLabelValues(ch).Observe(float64(a.DurationMS) / 1000)
		observe.Deliveries.WithLabelValues(ch, string(status)).Inc()
		if status == domain.Failed {
			observe.DLQ.WithLabelValues(ch).Inc()
		}
		if status == domain.RetryPending {
			observe.Retries.WithLabelValues(ch).Inc()
		}
	}
	logger.From(ctx).Info("delivery attempt", "notification_id", n.ID, "channel", n.Channel, "attempt", a.Number, "status", status, "duration_ms", a.DurationMS, "error", e)
	return e
}
func outcome(n domain.Notification, r provider.Result, started time.Time) (domain.DeliveryAttempt, domain.Status, *time.Time) {
	a := domain.DeliveryAttempt{Number: n.RetryCount + 1, Status: "FAILURE", HTTPStatus: r.Code, Body: r.Body, Error: r.Reason, DurationMS: time.Since(started).Milliseconds(), At: started.UTC()}
	if r.MessageID != "" {
		a.Status = "SUCCESS"
		return a, domain.Delivered, nil
	}
	if r.Retryable && n.RetryCount < n.MaxRetries {
		due := time.Now().Add(provider.Backoff(n.RetryCount))
		return a, domain.RetryPending, &due
	}
	return a, domain.Failed, nil
}
func (p *Pool) Retries(ctx context.Context) {
	key := broker.Key(p.Channel, "delayed")
	for ctx.Err() == nil {
		ids, e := p.Service.Broker.Due(ctx, key)
		if e == nil {
			for _, id := range ids {
				if e = p.Service.Store.Promote(ctx, id, domain.RetryPending, -1); e != nil {
					break
				}
			}
		}
		if e != nil && ctx.Err() == nil {
			slog.Error("promote retries", "channel", p.Channel, "error", e)
		}
		app.Wait(ctx, 100*time.Millisecond)
	}
}
