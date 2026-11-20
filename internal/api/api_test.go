package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	ws "github.com/gorilla/websocket"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"
)

func TestAPIReplayAndStream(t *testing.T) {
	s := testutil.Services(t, 12)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := API{s}
	srv := httptest.NewServer(a.Router(ctx))
	defer srv.Close()
	const path = "/api/v1/notifications"
	key := uuid.NewString()
	correlation := uuid.NewString()
	post := func(body string) (string, int) {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Idempotency-Key", key)
		req.Header.Set("X-Correlation-ID", correlation)
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Error(e)
			return "", 0
		}
		defer resp.Body.Close()
		if resp.Header.Get("X-Correlation-ID") != correlation {
			t.Error("missing correlation")
		}
		var n domain.Summary
		_ = json.NewDecoder(resp.Body).Decode(&n)
		return n.ID, resp.StatusCode
	}
	conn, _, e := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/api/v1/ws/notifications?channel=sms", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	body := `{"recipient":"+123","content":"hello","channel":"sms"}`
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, code := post(body)
			if code != 202 {
				t.Errorf("code %d", code)
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	first := <-ids
	for id := range ids {
		if id != first {
			t.Fatal("duplicate notification")
		}
	}
	if _, code := post(strings.ReplaceAll(body, "hello", "changed")); code != 409 {
		t.Fatal(code)
	}
	if _, e = s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var event map[string]any
	if e = conn.ReadJSON(&event); e != nil || event["notificationId"] != first {
		t.Fatal(event, e)
	}
	for _, p := range []string{path + "/" + first, path + "?status=QUEUED&page=1&limit=5", "/health", "/ready", "/metrics"} {
		res, e := http.Get(srv.URL + p)
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal(p, res.Status)
		}
	}
	res, e := http.Post(srv.URL+path+"/"+first+"/cancel", "application/json", nil)
	if e != nil {
		t.Fatal(e)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.Status)
	}
	t.Log("8 concurrent creates replayed one UUID, payload conflict returned 409, WebSocket received QUEUED")
}
