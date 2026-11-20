package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"
)

func TestDLQReplay(t *testing.T) {
	s := testutil.Services(t, 7)
	ctx := context.Background()
	n := domain.Notification{Summary: domain.Summary{ID: uuid.NewString(), Channel: domain.Push, Priority: domain.Low, Status: domain.Queued, CreatedAt: time.Now()}, Content: "test", MaxRetries: 3}
	if e := s.Store.Ingest(ctx, []domain.Notification{n}, nil); e != nil {
		t.Fatal(e)
	}
	owner := uuid.NewString()
	if _, e := s.Store.Claim(ctx, n.ID, owner, n.Channel, 0); e != nil {
		t.Fatal(e)
	}
	a := domain.DeliveryAttempt{Number: 1, Status: "FAILURE", Error: "provider HTTP 422", HTTPStatus: 422, Body: "invalid token", At: time.Now()}
	if e := s.Store.Finish(ctx, n.ID, owner, a, domain.Failed, "", nil); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	var out bytes.Buffer
	if e := execute(ctx, s, []string{"dlq", "list", "--channel", "push"}, &out); e != nil || !strings.Contains(out.String(), "NON_RETRYABLE_ERROR") {
		t.Fatal(out.String(), e)
	}
	out.Reset()
	if e := execute(ctx, s, []string{"dlq", "replay", "--channel", "push", "--priority", "high"}, &out); e != nil || !strings.Contains(out.String(), n.ID) {
		t.Fatal(out.String(), e)
	}
	got, e := s.Store.Get(ctx, n.ID)
	if e != nil || got.Status != domain.Queued || got.Priority != domain.High || got.AttemptBase != 1 || len(got.Attempts) != 1 {
		t.Fatal(got, e)
	}
}
