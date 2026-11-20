package redis

import (
	"context"
	"encoding/json"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/repository/postgres"
	"github.com/google/uuid"
	r "github.com/redis/go-redis/v9"
	"os"
	"testing"
	"time"
)

func TestFairnessAndAging(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR")
	}
	b := New(addr, "", 14)
	defer b.Client.Close()
	ctx := context.Background()
	c := domain.Channel(uuid.NewString())
	ev := []postgres.Event{}
	tiers := []domain.Priority{domain.High, domain.Normal, domain.Low}
	orig := map[string]domain.Priority{}
	for _, p := range tiers {
		for i := 0; i < 10; i++ {
			id := uuid.NewString()
			ev = append(ev, postgres.Event{ID: id, Channel: c, Priority: p, Current: domain.Queued})
			orig[id] = p
		}
	}
	if e := b.Publish(ctx, ev); e != nil {
		t.Fatal(e)
	}
	if e := b.Publish(ctx, ev); e != nil {
		t.Fatal(e)
	}
	for _, tier := range tiers {
		if b.Client.LLen(ctx, Key(c, string(tier))).Val() != 10 {
			t.Fatal("duplicate publish")
		}
	}
	counts := map[domain.Priority]int{}
	for i := 0; i < 10; i++ {
		l, e := b.Pop(ctx, c)
		if e != nil || l == nil {
			t.Fatal(e)
		}
		counts[orig[l.Job.ID]]++
		if e = b.Ack(ctx, c, l); e != nil {
			t.Fatal(e)
		}
	}
	if counts[domain.High] != 6 || counts[domain.Normal] != 3 || counts[domain.Low] != 1 {
		t.Fatal(counts)
	}
	aged, _ := json.Marshal(Job{ID: uuid.NewString(), Enqueued: time.Now().Add(-time.Minute).UnixMilli()})
	b.Client.LPush(ctx, Key(c, "low"), aged)
	_, e := b.Pop(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	if b.Client.LPos(ctx, Key(c, "normal"), string(aged), r.LPosArgs{}).Err() != nil {
		t.Fatal("low item was not promoted")
	}
	t.Log(counts)
}
