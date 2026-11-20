package postgres_test

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"

	"testing"
	"time"
)

func TestBatchAndLease(t *testing.T) {
	s := testutil.Services(t, 15).Store
	ctx := context.Background()
	var e error
	id := uuid.NewString()
	ns := make([]domain.Notification, 1000)
	for i := range ns {
		ns[i] = domain.Notification{Summary: domain.Summary{ID: uuid.NewString(), Channel: domain.SMS, Recipient: "+123", Priority: domain.Normal, Status: domain.Queued, CreatedAt: time.Now().UTC()}, BatchID: id, Content: "test", MaxRetries: 3}
	}
	r := domain.Receipt{Key: id, Hash: "test", Code: 202, Body: []byte("{}")}
	if e = s.Ingest(ctx, ns, &r); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(s.Ingest(ctx, ns, &r), domain.ErrConflict) {
		t.Fatal("duplicate receipt accepted")
	}
	b, e := s.Batch(ctx, id)
	if e != nil || b.Breakdown["queued"] != 1000 {
		t.Fatal(b, e)
	}
	if _, e = s.Cancel(ctx, ns[1].ID); e != nil {
		t.Fatal(e)
	}
	owner := uuid.NewString()
	if _, e = s.Claim(ctx, ns[0].ID, owner, domain.SMS, 0); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Cancel(ctx, ns[0].ID); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("cancelled a claimed item", e)
	}
	a := domain.DeliveryAttempt{Number: 1, Status: "SUCCESS", HTTPStatus: 202, At: time.Now()}
	if e = s.Finish(ctx, ns[0].ID, owner, a, domain.Delivered, "mock", nil); e != nil {
		t.Fatal(e)
	}
	n, e := s.Get(ctx, ns[0].ID)
	if e != nil || len(n.Attempts) != 1 || n.Status != domain.Delivered {
		t.Fatal(n, e)
	}
	list, total, e := s.List(ctx, domain.Filter{Statuses: []string{"DELIVERED"}, Channel: domain.SMS, Page: 1, Limit: 1})
	if e != nil || total < 1 || len(list) != 1 {
		t.Fatal(total, e)
	}
	b, e = s.Batch(ctx, id)
	if e != nil || b.Progress != 0.2 {
		t.Fatal(b, e)
	}
	t.Log("1000 rows committed, duplicate rejected, 2 terminal rows, progress 0.2%")
}
