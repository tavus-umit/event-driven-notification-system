package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/provider"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/ratelimit"
	"event-driven-notification-system/internal/repository/postgres"
	"event-driven-notification-system/internal/testutil"
)

type panicSender struct{}

func (panicSender) Deliver(context.Context, domain.Notification) provider.Result {
	panic("invalid job")
}
func TestRecoveryAndQuarantine(t *testing.T) {
	s := testutil.Services(t, 9)
	ctx := context.Background()
	n := domain.Notification{Summary: domain.Summary{ID: uuid.NewString(), Channel: domain.Email, Recipient: "test@example.com", Priority: domain.High, Status: domain.Queued, CreatedAt: time.Now()}, Content: "test", MaxRetries: 3}
	if e := s.Store.Ingest(ctx, []domain.Notification{n}, nil); e != nil {
		t.Fatal(e)
	}
	outage := errors.New("commit acknowledgement lost")
	_, e := s.Store.Flush(ctx, func(ctx context.Context, events []postgres.Event) error {
		if e := s.Broker.Publish(ctx, events); e != nil {
			return e
		}
		return outage
	})
	if !errors.Is(e, outage) {
		t.Fatal(e)
	}
	if _, e = s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	if length := s.Broker.Client.LLen(ctx, broker.Key(n.Channel, "high")).Val(); length != 1 {
		t.Fatal("outbox duplicated job", length)
	}
	old, e := s.Broker.Pop(ctx, n.Channel)
	if e != nil || old == nil {
		t.Fatal("missing job", e)
	}
	owner := uuid.NewString()
	if _, e = s.Store.Claim(ctx, n.ID, owner, n.Channel, 0); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Store.DB.Exec("UPDATE notifications SET lease_until=now()-interval '1 second' WHERE id=$1", n.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Store.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	a := domain.DeliveryAttempt{Status: "SUCCESS", Number: 1, At: time.Now()}
	if e = s.Store.Finish(ctx, n.ID, owner, a, domain.Delivered, "stale", nil); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("stale worker could finish", e)
	}
	lease, e := s.Broker.Pop(ctx, n.Channel)
	if e != nil || lease == nil {
		t.Fatal(e)
	}
	p := Pool{Service: s, Channel: n.Channel, Sender: panicSender{}, Limiter: ratelimit.Limiter{Client: s.Broker.Client}}
	p.process(ctx, lease, nil)
	got, e := s.Store.Get(ctx, n.ID)
	if e != nil || got.Status != domain.Failed {
		t.Fatal(got, e)
	}
	if s.Broker.Client.LIndex(ctx, broker.Key(n.Channel, "quarantine"), -1).Val() != lease.Raw {
		t.Fatal("missing quarantine entry")
	}
	if e = s.Broker.Ack(ctx, n.Channel, old); e != nil {
		t.Fatal(e)
	}
	t.Log("replayed outbox without duplicate enqueue, fenced expired owner, quarantined panic")
}
