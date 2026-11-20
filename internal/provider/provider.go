package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/pkg/logger"
	"fmt"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

type Payload struct {
	To      string         `json:"to"`
	Channel domain.Channel `json:"channel"`
	Content string         `json:"content"`
}
type Accepted struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}
type Result struct {
	MessageID, Body, Reason string
	Code                    int
	Retryable               bool
	Deferred                bool
}
type Client struct {
	url     string
	http    *http.Client
	breaker *gobreaker.CircuitBreaker
}

func New(url string, timeout time.Duration, channel domain.Channel, threshold int) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 100
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{Name: string(channel), Interval: 10 * time.Second, Timeout: 30 * time.Second, MaxRequests: 3,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return int(c.ConsecutiveFailures) >= threshold || (c.Requests >= 10 && c.TotalFailures*2 >= c.Requests)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Info("provider circuit changed", "channel", name, "from", from.String(), "to", to.String())
		}})
	return &Client{url, &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, cb}
}
func (c *Client) Deliver(ctx context.Context, n domain.Notification) Result {
	ctx, span := otel.Tracer("notification/provider").Start(ctx, "provider POST")
	defer span.End()
	v, e := c.breaker.Execute(func() (any, error) {
		result := c.send(ctx, n)
		if result.Retryable {
			return result, errors.New(result.Reason)
		}
		return result, nil
	})
	if v != nil {
		return v.(Result)
	}
	return Result{Reason: e.Error(), Retryable: true, Deferred: true}
}
func (c *Client) send(ctx context.Context, n domain.Notification) Result {
	data, e := json.Marshal(Payload{n.Recipient, n.Channel, n.Content})
	if e != nil {
		return Result{Reason: e.Error()}
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(data))
	if e != nil {
		return Result{Reason: e.Error()}
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", logger.ID(ctx))
	req.Header.Set("X-Idempotency-Key", n.ID)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, e := c.http.Do(req)
	if e != nil {
		return Result{Reason: e.Error(), Retryable: true}
	}
	defer resp.Body.Close()
	const limit = 16 * 1024
	data, e = io.ReadAll(io.LimitReader(resp.Body, limit+1))
	r := Result{Code: resp.StatusCode, Body: strings.ReplaceAll(strings.ToValidUTF8(string(data[:min(len(data), limit)]), "�"), "\x00", "�")}
	if resp.StatusCode != http.StatusAccepted {
		r.Retryable = resp.StatusCode == 429 || resp.StatusCode == 408 || resp.StatusCode >= 500
		r.Reason = fmt.Sprintf("provider HTTP %d", resp.StatusCode)
		return r
	}
	var a Accepted
	if e != nil || len(data) > limit || json.Unmarshal(data, &a) != nil || a.Status != "accepted" || a.Timestamp.IsZero() || strings.TrimSpace(a.MessageID) == "" || len(a.MessageID) > 256 || strings.ContainsRune(a.MessageID, 0) {
		r.Reason = "invalid provider acknowledgement"
		r.Retryable = true
		return r
	}
	r.MessageID = a.MessageID
	return r
}
func Backoff(retry int) time.Duration {
	upper := min(time.Minute, time.Second<<min(6, max(0, retry)))
	return time.Duration(rand.Int64N(int64(upper) + 1))
}
