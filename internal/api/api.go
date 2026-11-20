package api

import (
	"context"
	"encoding/json"
	"errors"
	"event-driven-notification-system/internal/api/websocket"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/observe"
	"event-driven-notification-system/pkg/logger"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type API struct{ S *app.Services }

func (a *API) Router(ctx context.Context) http.Handler {
	r := chi.NewRouter()
	r.Use(requestContext)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) { problem(w, r, 404, "resource not found") })
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) { problem(w, r, 405, "method not allowed") })
	r.Get("/health", a.health)
	r.Get("/ready", a.health)
	r.Handle("/metrics", observe.Metrics(a.S.Broker))
	h := websocket.New(ctx, a.S.Broker)
	go h.Run()
	r.Get("/api/v1/ws/notifications", h.ServeHTTP)
	const base = "/api/v1/notifications"
	r.Post(base, a.create)
	r.Post(base+"/batch", a.create)
	r.Get(base, a.list)
	r.Get(base+"/{id}", a.detail)
	r.Get(base+"/batches/{id}", a.batch)
	r.Post(base+"/{id}/cancel", a.cancel)
	return r
}
func requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if _, e := uuid.Parse(id); e != nil {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-ID", id)
		ctx := logger.WithID(r.Context(), id)
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("notification/api").Start(ctx, r.Method+" request")
		defer span.End()
		r = r.WithContext(ctx)
		rw := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		defer func() {
			if v := recover(); v != nil {
				logger.From(ctx).Error("request panic", "stack", string(debug.Stack()))
				problem(rw, r, 500, "internal error")
			}
			status := rw.Status()
			if status == 0 && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				status = http.StatusSwitchingProtocols
			}
			logger.From(ctx).Info("HTTP request", "method", r.Method, "path", r.URL.Path, "status", status, "duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(rw, r)
	})
}
func jsonResponse(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
func problem(w http.ResponseWriter, r *http.Request, code int, detail string) {
	title := http.StatusText(code)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "https://api.notifications.internal/errors/" + strings.ToLower(strings.ReplaceAll(title, " ", "-")), "title": title, "status": code, "detail": detail, "instance": r.URL.Path, "correlationId": logger.ID(r.Context()), "timestamp": time.Now().UTC()})
}
func failure(w http.ResponseWriter, r *http.Request, e error) {
	switch {
	case errors.Is(e, domain.ErrNotFound):
		problem(w, r, 404, e.Error())
	case errors.Is(e, domain.ErrConflict):
		problem(w, r, 409, e.Error())
	default:
		logger.From(r.Context()).Error("service failure", "error", e)
		problem(w, r, 503, "service temporarily unavailable")
	}
}
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	db := a.S.Store.DB.PingContext(ctx) == nil
	redis := a.S.Broker.Client.Ping(ctx).Err() == nil
	code := 200
	if !db || !redis {
		code = 503
	}
	jsonResponse(w, code, map[string]bool{"postgres": db, "redis": redis, "healthy": db && redis})
}
func resourceID(w http.ResponseWriter, r *http.Request) string {
	id := chi.URLParam(r, "id")
	if _, e := uuid.Parse(id); e != nil {
		problem(w, r, 400, "invalid UUID")
		return ""
	}
	return id
}
func (a *API) detail(w http.ResponseWriter, r *http.Request) {
	if id := resourceID(w, r); id != "" {
		n, e := a.S.Store.Get(r.Context(), id)
		if e != nil {
			failure(w, r, e)
			return
		}
		jsonResponse(w, 200, n)
	}
}
func (a *API) batch(w http.ResponseWriter, r *http.Request) {
	if id := resourceID(w, r); id != "" {
		b, e := a.S.Store.Batch(r.Context(), id)
		if e != nil {
			failure(w, r, e)
			return
		}
		jsonResponse(w, 200, b)
	}
}
func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	if id := resourceID(w, r); id != "" {
		at, e := a.S.Store.Cancel(r.Context(), id)
		if e != nil {
			failure(w, r, e)
			return
		}
		jsonResponse(w, 200, map[string]any{"id": id, "status": domain.Cancelled, "cancelledAt": at})
	}
}
func filter(r *http.Request) (domain.Filter, error) {
	q := r.URL.Query()
	f := domain.Filter{Page: 1, Limit: 20, Channel: domain.Channel(q.Get("channel")), Priority: domain.Priority(q.Get("priority"))}
	if (f.Channel != "" && !f.Channel.Valid()) || (f.Priority != "" && !f.Priority.Valid()) {
		return f, errors.New("invalid channel or priority")
	}
	if v := q.Get("status"); v != "" {
		f.Statuses = strings.Split(v, ",")
		for _, s := range f.Statuses {
			if !domain.Status(s).Valid() {
				return f, errors.New("invalid status")
			}
		}
	}
	for _, p := range []struct {
		k      string
		target *int
		max    int
	}{{"page", &f.Page, 1<<31 - 1}, {"limit", &f.Limit, 100}} {
		if q.Has(p.k) {
			n, e := strconv.Atoi(q.Get(p.k))
			if e != nil || n < 1 || n > p.max {
				return f, fmt.Errorf("invalid %s", p.k)
			}
			*p.target = n
		}
	}
	for _, p := range []struct {
		k, alias string
		target   **time.Time
	}{{"date_from", "start_date", &f.From}, {"date_to", "end_date", &f.To}} {
		v := q.Get(p.k)
		if v == "" {
			v = q.Get(p.alias)
		}
		if v != "" {
			t, e := time.Parse(time.RFC3339Nano, v)
			if e != nil {
				return f, fmt.Errorf("invalid %s", p.k)
			}
			*p.target = &t
		}
	}
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		return f, errors.New("inverted date range")
	}
	return f, nil
}
func (a *API) list(w http.ResponseWriter, r *http.Request) {
	f, e := filter(r)
	if e != nil {
		problem(w, r, 400, e.Error())
		return
	}
	data, total, e := a.S.Store.List(r.Context(), f)
	if e != nil {
		failure(w, r, e)
		return
	}
	jsonResponse(w, 200, map[string]any{"data": data, "pagination": map[string]int{"page": f.Page, "limit": f.Limit, "totalCount": total, "totalPages": (total + f.Limit - 1) / f.Limit}})
}
