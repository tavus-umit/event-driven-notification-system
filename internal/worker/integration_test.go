package worker

import (
	"context"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/provider"
	"event-driven-notification-system/internal/ratelimit"
	"event-driven-notification-system/internal/testutil"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

type rejectingSender struct{}

func (rejectingSender) Deliver(context.Context, domain.Notification) provider.Result {
	return provider.Result{Code: 503, Reason: "unavailable", Retryable: true}
}
func TestRetryToDLQ(t *testing.T) {
	s := testutil.Services(t, 13)
	ctx, cancel := context.WithCancel(context.Background())
	p := Pool{Service: s, Sender: rejectingSender{}, Channel: domain.SMS, Limiter: ratelimit.Limiter{Client: s.Broker.Client}}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); p.Run(ctx, 2) }()
	go func() { defer wg.Done(); p.Retries(ctx) }()
	go func() { defer wg.Done(); s.Relay(ctx) }()
	defer func() { cancel(); wg.Wait() }()
	n := domain.Notification{Summary: domain.Summary{ID: uuid.NewString(), Recipient: "+123", Channel: domain.SMS, Priority: domain.High, Status: domain.Queued, CreatedAt: time.Now()}, Content: "test", MaxRetries: 3}
	before := s.Broker.Client.LLen(ctx, "queue:sms:dlq").Val()
	if e := s.Store.Ingest(ctx, []domain.Notification{n}, nil); e != nil {
		t.Fatal(e)
	}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		got, e := s.Store.Get(ctx, n.ID)
		if e != nil {
			t.Fatal(e)
		}
		if got.Status == domain.Failed && s.Broker.Client.LLen(ctx, "queue:sms:dlq").Val() > before {
			if len(got.Attempts) != 4 || got.RetryCount != 3 {
				t.Fatal(got)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for DLQ")
}
