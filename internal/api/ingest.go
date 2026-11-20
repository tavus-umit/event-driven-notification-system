package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"

	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/pkg/logger"
)

type CreateRequest struct {
	TemplateID string                     `json:"templateId,omitempty"`
	Variables  map[string]string          `json:"variables,omitempty"`
	Recipient  string                     `json:"recipient"`
	Channel    domain.Channel             `json:"channel"`
	Content    string                     `json:"content"`
	Priority   domain.Priority            `json:"priority"`
	Key        string                     `json:"idempotencyKey,omitempty"`
	Scheduled  *time.Time                 `json:"scheduledAt,omitempty"`
	Metadata   map[string]json.RawMessage `json:"metadata,omitempty"`
}
type batchRequest struct {
	ID    string          `json:"batchId,omitempty"`
	Items []CreateRequest `json:"notifications"`
}

func decode(w http.ResponseWriter, r *http.Request, maxBytes int64, v any) error {
	body, e := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if e != nil {
		return e
	}
	if !utf8.Valid(body) {
		return errors.New("invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if e = d.Decode(v); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
func requestHash(path string, req batchRequest) string {
	if len(req.Items) == 1 && path == "/api/v1/notifications" {
		one := req.Items[0]
		one.Key = ""
		req.Items = []CreateRequest{one}
	}
	payload, _ := json.Marshal(req)
	return fmt.Sprintf("%x", sha256.Sum256(append([]byte(path+"\n"), payload...)))
}
func (a *API) create(w http.ResponseWriter, r *http.Request) {
	batch := strings.HasSuffix(r.URL.Path, "/batch")
	var req batchRequest
	key := r.Header.Get("X-Idempotency-Key")
	if batch {
		var e error
		req, e = readBatch(w, r)
		if e != nil {
			problem(w, r, 400, e.Error())
			return
		}
	} else {
		var one CreateRequest
		if e := decode(w, r, 4<<20, &one); e != nil {
			problem(w, r, 400, e.Error())
			return
		}
		req.Items = []CreateRequest{one}
		if key == "" {
			key = one.Key
		} else if one.Key != "" && key != one.Key {
			problem(w, r, 400, "header and payload idempotency keys differ")
			return
		}
	}
	if len(req.Items) < 1 || len(req.Items) > 1000 {
		problem(w, r, 400, "expected 1 to 1000 notifications")
		return
	}
	if req.ID != "" {
		if _, e := uuid.Parse(req.ID); e != nil {
			problem(w, r, 400, "batchId must be a UUID")
			return
		}
	}
	if len(key) > 256 || strings.ContainsRune(key, 0) {
		problem(w, r, 400, "invalid idempotency key")
		return
	}
	ctx := r.Context()
	var receipt *domain.Receipt
	if key != "" {
		receipt = &domain.Receipt{Key: key, Hash: requestHash(r.URL.Path, req), Code: 202}
		release, done := a.guard(w, r, receipt)
		if done {
			return
		}
		defer func() {
			unlock, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			if e := release(unlock); e != nil {
				logger.From(ctx).Warn("release idempotency lock", "error", e)
			}
		}()
	}
	now := time.Now().UTC()
	if batch && req.ID == "" {
		req.ID = uuid.NewString()
	}
	ns := make([]domain.Notification, len(req.Items))
	ids := make([]string, len(ns))
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	usedKeys := map[string]bool{key: true}
	templates := map[string]domain.NotificationTemplate{}
	for i, item := range req.Items {
		original := item
		if batch && item.Key != "" {
			if len(item.Key) > 256 || strings.ContainsRune(item.Key, 0) || usedKeys[item.Key] {
				problem(w, r, 400, "invalid or repeated batch item idempotency key")
				return
			}
			usedKeys[item.Key] = true
		}
		if e := a.render(ctx, &item, templates); e != nil {
			var invalid invalidTemplate
			if errors.As(e, &invalid) {
				problem(w, r, 400, e.Error())
			} else {
				failure(w, r, e)
			}
			return
		}
		p := item.Priority
		if p == "" {
			p = domain.Normal
		}
		st := domain.Queued
		if item.Scheduled != nil && item.Scheduled.After(now) {
			st = domain.Scheduled
		}
		id := uuid.NewString()
		ids[i] = id
		n := domain.Notification{
			Summary: domain.Summary{ID: id, Channel: item.Channel, Recipient: item.Recipient, Priority: p, Status: st, CreatedAt: now},
			BatchID: req.ID, TemplateID: item.TemplateID, Content: item.Content,
			MaxRetries: a.S.Config.MaxRetries, ScheduledAt: item.Scheduled, Metadata: item.Metadata,
			UpdatedAt: now, CorrelationID: logger.ID(ctx), Traceparent: carrier.Get("traceparent"), IdempotencyKey: item.Key,
		}
		if e := n.Validate(); e != nil {
			problem(w, r, 400, fmt.Sprintf("item %d: %s", i+1, e))
			return
		}
		if batch && item.Key != "" {
			body, _ := json.Marshal(n.Summary)
			n.Receipt = &domain.Receipt{Key: item.Key, Hash: requestHash("/api/v1/notifications", batchRequest{Items: []CreateRequest{original}}), Code: 202, Body: body}
		}
		if !batch {
			n.IdempotencyKey = key
		}
		ns[i] = n
	}
	var response any = ns[0].Summary
	if batch {
		response = map[string]any{"batchId": req.ID, "totalReceived": len(ns), "status": "PROCESSING", "notificationIds": ids, "createdAt": now}
	}
	data, _ := json.Marshal(response)
	if receipt != nil {
		receipt.Body = data
	}
	if e := a.S.Store.Ingest(ctx, ns, receipt); e != nil {
		if errors.Is(e, domain.ErrConflict) && receipt != nil && a.replay(w, r, receipt) {
			return
		}
		failure(w, r, e)
		return
	}
	jsonResponse(w, 202, json.RawMessage(data))
}
func (a *API) replay(w http.ResponseWriter, r *http.Request, want *domain.Receipt) bool {
	got, e := a.S.Store.Receipt(r.Context(), want.Key)
	if errors.Is(e, domain.ErrNotFound) {
		return false
	}
	if e != nil {
		failure(w, r, e)
		return true
	}
	if want.Hash != got.Hash {
		problem(w, r, 409, "idempotency key already used for a different payload")
		return true
	}
	jsonResponse(w, got.Code, got.Body)
	return true
}
func (a *API) guard(w http.ResponseWriter, r *http.Request, want *domain.Receipt) (func(context.Context) error, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	for {
		if a.replay(w, r, want) {
			return nil, true
		}
		release, ok, e := a.S.Broker.Lock(ctx, want.Key)
		if e != nil {
			failure(w, r, e)
			return nil, true
		}
		if ok {
			return release, false
		}
		if !app.Wait(ctx, 25*time.Millisecond) {
			problem(w, r, 409, "idempotent request in progress")
			return nil, true
		}
	}
}
