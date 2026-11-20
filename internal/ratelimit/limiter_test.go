package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	r "github.com/redis/go-redis/v9"

	"event-driven-notification-system/internal/domain"
)

func TestBucket(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR")
	}
	c := r.NewClient(&r.Options{Addr: addr, DB: 14})
	defer c.Close()
	ctx := context.Background()
	ch := domain.Channel(uuid.NewString())
	key := "ratelimit:" + string(ch)
	defer c.Del(ctx, key, key+":window")
	var first *Permit
	l := Limiter{c}
	for i := 0; i < 100; i++ {
		p, _, e := l.Try(ctx, ch)
		if e != nil || p == nil {
			t.Fatal(i, e)
		}
		if i == 0 {
			first = p
		}
	}
	if p, d, e := l.Try(ctx, ch); e != nil || p != nil || d <= 0 {
		t.Fatal("ceiling", d, e)
	}
	if e := first.Done(ctx); e != nil {
		t.Fatal(e)
	}
	time.Sleep(time.Second)
	if p, _, e := l.Try(ctx, ch); e != nil || p == nil {
		t.Fatal("no refill", e)
	}
}
