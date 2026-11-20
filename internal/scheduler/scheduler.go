package scheduler

import (
	"context"
	"event-driven-notification-system/internal/app"
	"log/slog"
	"time"
)

// Tick reads the Redis due index. The guarded SQL transition writes the outbox;
// publishing that transition atomically removes the index member and adds the ready job.
func Tick(ctx context.Context, s *app.Services) (int, error) {
	ids, e := s.Broker.Due(ctx, "queue:scheduled")
	if e != nil || len(ids) == 0 {
		return 0, e
	}
	res, e := s.Store.DB.ExecContext(ctx, `UPDATE notifications SET status='QUEUED',updated_at=now() WHERE id=ANY($1::uuid[]) AND status='SCHEDULED' AND scheduled_at<=now()`, ids)
	if e != nil {
		return 0, e
	}
	n, e := res.RowsAffected()
	return int(n), e
}
func Run(ctx context.Context, s *app.Services) error {
	done := make(chan struct{})
	go func() { defer close(done); s.Relay(ctx) }()
	defer func() { <-done }()
	for ctx.Err() == nil {
		n, e := Tick(ctx, s)
		if e != nil && ctx.Err() == nil {
			slog.Error("schedule due notifications", "error", e)
		}
		delay := time.Second
		if n >= 100 {
			delay = 50 * time.Millisecond
		}
		app.Wait(ctx, delay)
	}
	return nil
}
