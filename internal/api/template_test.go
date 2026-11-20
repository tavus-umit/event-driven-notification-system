package api

import (
	"context"
	"encoding/json"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/scheduler"
	"event-driven-notification-system/internal/testutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestScheduledTemplate(t *testing.T) {
	s := testutil.Services(t, 11)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := API{s}
	router := a.Router(ctx)
	do := func(method, path, body string, code int, v any) {
		t.Helper()
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
		if rec.Code != code {
			t.Fatal(path, rec.Code, rec.Body.String())
		}
		if v != nil {
			if e := json.Unmarshal(rec.Body.Bytes(), v); e != nil {
				t.Fatal(e)
			}
		}
	}
	const path = "/api/v1/templates"
	form := `{"name":"greeting","channel":"sms","bodyTemplate":"Hi {{name}}"}`
	var tmpl domain.NotificationTemplate
	do("POST", path, form, 201, &tmpl)
	do("GET", path+"/"+tmpl.ID, "", 200, nil)
	do("GET", path, "", 200, nil)
	do("PUT", path+"/"+tmpl.ID, form, 200, nil)
	at := time.Now().UTC().Add(450 * time.Millisecond)
	body := `{"recipient":"+123","templateId":"` + tmpl.ID + `","variables":{"name":"Ada"},"scheduledAt":"` + at.Format(time.RFC3339Nano) + `"}`
	var n domain.Summary
	do("POST", "/api/v1/notifications", body, 202, &n)
	if n.Status != domain.Scheduled {
		t.Fatal(n)
	}
	if _, e := s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	if count, e := scheduler.Tick(ctx, s); count != 0 || e != nil {
		t.Fatal("early dispatch", count, e)
	}
	time.Sleep(time.Until(at) + 10*time.Millisecond)
	if count, e := scheduler.Tick(ctx, s); count != 1 || e != nil {
		t.Fatal(count, e)
	}
	if _, e := s.Store.Flush(ctx, s.Broker.Publish); e != nil {
		t.Fatal(e)
	}
	got, e := s.Store.Get(ctx, n.ID)
	if e != nil || got.Status != domain.Queued || got.Content != "Hi Ada" {
		t.Fatal(got, e)
	}
	if s.Broker.Client.ZScore(ctx, "queue:scheduled", n.ID).Err() == nil {
		t.Fatal("scheduled member remains")
	}
	do("POST", "/api/v1/notifications", strings.Replace(body, "Ada", strings.Repeat("a", 1600), 1), 400, nil)
	do(http.MethodDelete, path+"/"+tmpl.ID, "", 204, nil)
	do("GET", path+"/"+tmpl.ID, "", 404, nil)
}
