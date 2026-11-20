package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"
)

func TestBatchBoundariesAndItemReceipt(t *testing.T) {
	s := testutil.Services(t, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	router := (&API{s}).Router(ctx)
	post := func(path, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/v1/notifications"+path, strings.NewReader(body))
		r.Header.Set("X-Idempotency-Key", key)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	item := `{"recipient":"+123","channel":"sms","content":"ok"}`
	makeBatch := func(n int) string {
		return `{"notifications":[` + strings.TrimSuffix(strings.Repeat(item+",", n), ",") + "]}"
	}
	for _, n := range []int{0, 1001} {
		if w := post("/batch", makeBatch(n), ""); w.Code != 400 {
			t.Fatal(n, w.Code)
		}
	}
	bad := `{"notifications":[` + item + `,{"recipient":"bad","channel":"sms","content":"bad"}]}`
	if w := post("/batch", bad, ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
	var count int
	if e := s.Store.DB.QueryRow("SELECT count(*) FROM notifications").Scan(&count); e != nil || count != 0 {
		t.Fatal("partial batch", count, e)
	}
	key := uuid.NewString()
	itemWithKey := strings.Replace(item, "{", `{"idempotencyKey":"`+key+`",`, 1)
	body := `{"notifications":[` + itemWithKey + "]}"
	header := uuid.NewString()
	w := post("/batch", body, header)
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	var batch struct {
		IDs []string `json:"notificationIds"`
	}
	json.Unmarshal(w.Body.Bytes(), &batch)
	if len(batch.IDs) != 1 {
		t.Fatal(batch)
	}
	w = post("", item, key)
	var one domain.Summary
	json.Unmarshal(w.Body.Bytes(), &one)
	if w.Code != 202 || one.ID != batch.IDs[0] {
		t.Fatal(w.Code, one)
	}
	if w = post("/batch", body, header); w.Code != 202 {
		t.Fatal(w.Code)
	}
	if w = post("/batch", body, uuid.NewString()); w.Code != 409 {
		t.Fatal("reused batch item", w.Code)
	}
	if w = post("/batch", makeBatch(1000), ""); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
}
