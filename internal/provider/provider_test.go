package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/pkg/logger"
)

func TestClassification(t *testing.T) {
	for _, code := range []int{202, 400, 422, 429, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var p map[string]string
				if json.NewDecoder(r.Body).Decode(&p) != nil || len(p) != 3 || p["channel"] != "sms" || p["to"] != "+123" || p["content"] != "hello" || r.Header.Get("X-Correlation-ID") != "trace" {
					t.Error("invalid outbound contract")
				}
				w.WriteHeader(code)
				json.NewEncoder(w).Encode(Accepted{"id", "accepted", time.Now()})
			}))
			defer h.Close()
			n := domain.Notification{Summary: domain.Summary{Channel: domain.SMS, Recipient: "+123"}, Content: "hello"}
			r := New(h.URL, time.Second, domain.SMS, 5).Deliver(logger.WithID(context.Background(), "trace"), n)
			if r.Code != code || (r.MessageID != "") != (code == 202) || r.Retryable != (code == 429 || code == 503) {
				t.Fatal(r)
			}
		})
	}
}
func TestBreakerAndBackoff(t *testing.T) {
	var calls atomic.Int32
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	defer h.Close()
	c := New(h.URL, time.Second, domain.Email, 2)
	for i := 0; i < 3; i++ {
		v := c.Deliver(context.Background(), domain.Notification{})
		if v.Deferred != (i == 2) {
			t.Fatal(i, v)
		}
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
	for i, cap := range []time.Duration{1, 2, 4, 8, 16, 32, 60, 60} {
		for j := 0; j < 100; j++ {
			d := Backoff(i)
			if d < 0 || d > cap*time.Second {
				t.Fatal(d)
			}
		}
	}
}
func TestInvalidAck(t *testing.T) {
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) }))
	defer h.Close()
	if r := New(h.URL, time.Second, domain.Push, 5).Deliver(context.Background(), domain.Notification{}); !r.Retryable || r.MessageID != "" {
		t.Fatal(r)
	}
}

func TestTimeoutAndHalfOpenRecovery(t *testing.T) {
	var slow atomic.Bool
	slow.Store(true)
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
			return
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(Accepted{"ok", "accepted", time.Now()})
	}))
	defer h.Close()
	c := New(h.URL, 40*time.Millisecond, domain.SMS, 1)
	settings := breakerSettings(domain.SMS, 1)
	settings.Timeout = time.Millisecond
	c.breaker = gobreaker.NewCircuitBreaker(settings)
	result := c.Deliver(context.Background(), domain.Notification{})
	if !result.Retryable || result.Deferred || result.Code != 0 {
		t.Fatal(result)
	}
	slow.Store(false)
	time.Sleep(3 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if result = c.Deliver(context.Background(), domain.Notification{}); result.MessageID != "ok" {
			t.Fatal(result)
		}
	}
	if c.breaker.State() != gobreaker.StateClosed {
		t.Fatal(c.breaker.State())
	}
}
