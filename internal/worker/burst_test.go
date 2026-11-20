package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/api"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/provider"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/ratelimit"
	"event-driven-notification-system/internal/testutil"
	"event-driven-notification-system/internal/worker"
)

func TestBurst500(t *testing.T) {
	s := testutil.Services(t, 10)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var mu sync.Mutex
	var arrivals []time.Time
	seen := map[string]bool{}
	correlation := uuid.NewString()
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		at := time.Now()
		id := r.Header.Get("X-Idempotency-Key")
		if r.Header.Get("X-Correlation-ID") != correlation {
			t.Error("lost correlation ID")
		}
		if !strings.Contains(r.Header.Get("traceparent"), "000000000000000000000000000000aa") {
			t.Error("lost traceparent")
		}
		mu.Lock()
		if seen[id] {
			t.Errorf("duplicate delivery %s", id)
		}
		seen[id] = true
		arrivals = append(arrivals, at)
		mu.Unlock()
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(provider.Accepted{MessageID: id, Status: "accepted", Timestamp: at.UTC()})
	}))
	defer h.Close()
	router := (&api.API{S: s}).Router(ctx)
	items := make([]api.CreateRequest, 500)
	for i := range items {
		items[i] = api.CreateRequest{Channel: domain.SMS, Recipient: "+123", Content: "burst"}
	}
	raw, e := json.Marshal(map[string]any{"notifications": items})
	if e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest("POST", "/api/v1/notifications/batch", bytes.NewReader(raw))
	req.Header.Set("X-Correlation-ID", correlation)
	req.Header.Set("traceparent", "00-000000000000000000000000000000aa-00000000000000bb-01")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != 202 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	var response struct {
		ID  string   `json:"batchId"`
		IDs []string `json:"notificationIds"`
	}
	if e = json.NewDecoder(rec.Body).Decode(&response); e != nil || len(response.IDs) != 500 {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Relay(ctx) }()
	// Replicas use separate Redis clients, admission objects and provider transports.
	for i := 0; i < 2; i++ {
		b := broker.New(s.Broker.Client.Options().Addr, "", 10)
		t.Cleanup(func() { b.Client.Close() })
		replica := &app.Services{Store: s.Store, Broker: b, Config: s.Config}
		p := worker.Pool{Service: replica, Channel: domain.SMS, Limiter: ratelimit.Limiter{Client: b.Client}, Sender: provider.New(h.URL, 2*time.Second, domain.SMS, 5)}
		wg.Add(1)
		go func() { defer wg.Done(); p.Run(ctx, 10) }()
	}
	defer func() { stop(); wg.Wait() }()
	for deadline := time.Now().Add(time.Minute); ; {
		batch, e := s.Store.Batch(ctx, response.ID)
		if e != nil {
			t.Fatal(e)
		}
		if batch.Breakdown["delivered"] == 500 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delivery timeout", batch)
		}
		time.Sleep(40 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(arrivals) != 500 {
		t.Fatal(len(arrivals))
	}
	sort.Slice(arrivals, func(i, j int) bool { return arrivals[i].Before(arrivals[j]) })
	peak, left := 0, 0
	for right, at := range arrivals {
		for !arrivals[left].After(at.Add(-time.Second)) {
			left++
		}
		peak = max(peak, right-left+1)
	}
	if peak > 100 {
		t.Fatalf("peak was %d", peak)
	}
	t.Logf("500 unique HTTP arrivals across two worker replicas in %s; busiest rolling second: %d", arrivals[499].Sub(arrivals[0]).Round(time.Millisecond), peak)
}
