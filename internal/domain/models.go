package domain

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Channel string

const (
	SMS   Channel = "sms"
	Email Channel = "email"
	Push  Channel = "push"
)

var Channels = []Channel{SMS, Email, Push}

func (c Channel) Valid() bool { return c == SMS || c == Email || c == Push }

type Priority string

const (
	High   Priority = "high"
	Normal Priority = "normal"
	Low    Priority = "low"
)

func (p Priority) Valid() bool { return p == High || p == Normal || p == Low }

type Status string

const (
	Pending      Status = "PENDING"
	Scheduled    Status = "SCHEDULED"
	Queued       Status = "QUEUED"
	Processing   Status = "PROCESSING"
	Delivered    Status = "DELIVERED"
	Failed       Status = "FAILED"
	Cancelled    Status = "CANCELLED"
	RetryPending Status = "RETRY_PENDING"
)

func (s Status) Valid() bool {
	switch s {
	case Pending, Scheduled, Queued, Processing, Delivered, Failed, Cancelled, RetryPending:
		return true
	}
	return false
}

var ErrInvalid = errors.New("invalid request data")
var ErrConflict = errors.New("state or idempotency conflict")
var ErrNotFound = errors.New("resource not found")

type Summary struct {
	ID        string    `json:"id"`
	Channel   Channel   `json:"channel"`
	Recipient string    `json:"recipient"`
	Priority  Priority  `json:"priority"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}
type Notification struct {
	Receipt     *Receipt `json:"-"`
	AttemptBase int      `json:"-"`
	Summary
	BatchID           string                     `json:"batchId,omitempty"`
	Content           string                     `json:"content"`
	RetryCount        int                        `json:"retryCount"`
	MaxRetries        int                        `json:"maxRetries"`
	ProviderMessageID string                     `json:"providerMessageId,omitempty"`
	ErrorMessage      string                     `json:"errorMessage,omitempty"`
	UpdatedAt         time.Time                  `json:"updatedAt"`
	ScheduledAt       *time.Time                 `json:"scheduledAt,omitempty"`
	TemplateID        string                     `json:"templateId,omitempty"`
	Metadata          map[string]json.RawMessage `json:"metadata,omitempty"`
	Attempts          []DeliveryAttempt          `json:"deliveryAttempts"`
	CorrelationID     string                     `json:"-"`
	Traceparent       string                     `json:"-"`
	IdempotencyKey    string                     `json:"-"`
}
type NotificationBatch struct {
	ID        string         `json:"batchId"`
	Total     int            `json:"totalCount"`
	Status    string         `json:"status"`
	Breakdown map[string]int `json:"breakdown"`
	Progress  float64        `json:"progressPercentage"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}
type DeliveryAttempt struct {
	Number     int       `json:"attemptNumber"`
	Status     string    `json:"status"`
	HTTPStatus int       `json:"httpStatusCode,omitempty"`
	Body       string    `json:"responseBody,omitempty"`
	Error      string    `json:"errorReason,omitempty"`
	DurationMS int64     `json:"durationMs"`
	At         time.Time `json:"attemptedAt"`
}
type NotificationTemplate struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Channel Channel `json:"channel"`
	Subject string  `json:"subject,omitempty"`
	Body    string  `json:"bodyTemplate"`
}
type Filter struct {
	Statuses    []string
	Channel     Channel
	Priority    Priority
	From, To    *time.Time
	Page, Limit int
}
type Receipt struct {
	Key, Hash string
	Code      int
	Body      json.RawMessage
}
type NotificationRepository interface {
	Ingest(context.Context, []Notification, *Receipt) error
	Get(context.Context, string) (Notification, error)
	Cancel(context.Context, string) (time.Time, error)
	List(context.Context, Filter) ([]Summary, int, error)
}
type BatchRepository interface {
	Batch(context.Context, string) (NotificationBatch, error)
}
type IdempotencyRepository interface {
	Receipt(context.Context, string) (Receipt, error)
}
type DeliveryAttemptRepository interface {
	Attempts(context.Context, string) ([]DeliveryAttempt, error)
}
