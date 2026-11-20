package ratelimit

import (
	"context"
	"event-driven-notification-system/internal/domain"
	r "github.com/redis/go-redis/v9"
	"os"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR")
	}
	c := r.NewClient(&r.Options{Addr: addr, DB: 14})
	defer c.Close()
	ctx := context.Background()
	ch := domain.Channel("bucket-test")
	c.Del(ctx, "ratelimit:bucket-test", "ratelimit:bucket-test:window")
	l := Limiter{c}
	for i := 0; i < 100; i++ {
		p, _, e := l.Try(ctx, ch)
		if e != nil || p == nil {
			t.Fatal(i, e)
		}
		if e = p.Done(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if p, d, e := l.Try(ctx, ch); e != nil || p != nil || d <= 0 {
		t.Fatal("ceiling", d, e)
	}
	time.Sleep(time.Second)
	if p, _, e := l.Try(ctx, ch); e != nil || p == nil {
		t.Fatal("no refill", e)
	}
}
