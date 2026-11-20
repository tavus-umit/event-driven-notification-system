package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"event-driven-notification-system/internal/domain"
)

func (s *Store) Receipt(ctx context.Context, key string) (domain.Receipt, error) {
	var r domain.Receipt
	r.Key = key
	e := s.DB.QueryRowContext(ctx, "SELECT request_hash,response_status,response_body FROM idempotency_keys WHERE key=$1 AND expires_at>now()", key).Scan(&r.Hash, &r.Code, &r.Body)
	return r, dbError(e)
}

const columns = `id,channel,recipient,priority,status,created_at,coalesce(batch_id::text,''),content,retry_count,max_retries,coalesce(provider_message_id,''),coalesce(error_message,''),updated_at,scheduled_at,coalesce(template_id::text,''),metadata,correlation_id,traceparent,attempt_base`

type scanner interface{ Scan(...any) error }

func scanNotification(row scanner) (domain.Notification, error) {
	var n domain.Notification
	var meta []byte
	e := row.Scan(&n.ID, &n.Channel, &n.Recipient, &n.Priority, &n.Status, &n.CreatedAt, &n.BatchID, &n.Content, &n.RetryCount, &n.MaxRetries, &n.ProviderMessageID, &n.ErrorMessage, &n.UpdatedAt, &n.ScheduledAt, &n.TemplateID, &meta, &n.CorrelationID, &n.Traceparent, &n.AttemptBase)
	if e == nil && len(meta) > 0 {
		e = json.Unmarshal(meta, &n.Metadata)
	}
	return n, dbError(e)
}
func (s *Store) Get(ctx context.Context, id string) (domain.Notification, error) {
	tx, e := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if e != nil {
		return domain.Notification{}, e
	}
	defer tx.Rollback()
	n, e := scanNotification(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM notifications WHERE id=$1", id))
	if e == nil {
		n.Attempts, e = readAttempts(ctx, tx, id)
	}
	return n, e
}
func (s *Store) Attempts(ctx context.Context, id string) ([]domain.DeliveryAttempt, error) {
	return readAttempts(ctx, s.DB, id)
}

type attemptReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readAttempts(ctx context.Context, reader attemptReader, id string) ([]domain.DeliveryAttempt, error) {
	rows, e := reader.QueryContext(ctx, `SELECT attempt_number,status,coalesce(http_status_code,0),coalesce(response_body,''),coalesce(error_reason,''),duration_ms,attempted_at FROM delivery_attempts WHERE notification_id=$1 ORDER BY attempt_number,attempted_at`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.DeliveryAttempt{}
	for rows.Next() {
		var a domain.DeliveryAttempt
		if e = rows.Scan(&a.Number, &a.Status, &a.HTTPStatus, &a.Body, &a.Error, &a.DurationMS, &a.At); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *Store) Cancel(ctx context.Context, id string) (time.Time, error) {
	var at time.Time
	e := s.DB.QueryRowContext(ctx, `UPDATE notifications SET status='CANCELLED',updated_at=now() WHERE id=$1 AND status IN ('PENDING','SCHEDULED','QUEUED') RETURNING updated_at`, id).Scan(&at)
	if errors.Is(e, sql.ErrNoRows) {
		var exists bool
		e = s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM notifications WHERE id=$1)", id).Scan(&exists)
		if e == nil {
			if exists {
				e = domain.ErrConflict
			} else {
				e = domain.ErrNotFound
			}
		}
	}
	return at, e
}
func (s *Store) List(ctx context.Context, f domain.Filter) ([]domain.Summary, int, error) {
	where := " WHERE true"
	args := []any{}
	add := func(expr string, v any) { args = append(args, v); where += " AND " + fmt.Sprintf(expr, len(args)) }
	if len(f.Statuses) > 0 {
		add("status=ANY($%d::notification_status[])", f.Statuses)
	}
	if f.Channel != "" {
		add("channel=$%d", f.Channel)
	}
	if f.Priority != "" {
		add("priority=$%d", f.Priority)
	}
	if f.From != nil {
		add("created_at >= $%d", f.From)
	}
	if f.To != nil {
		add("created_at <= $%d", f.To)
	}
	tx, e := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return nil, 0, e
	}
	defer tx.Rollback()
	var total int
	if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM notifications"+where, args...).Scan(&total); e != nil {
		return nil, 0, e
	}
	rows, e := tx.QueryContext(ctx, "SELECT id,channel,recipient,priority,status,created_at FROM notifications"+where+fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2), append(args, f.Limit, int64(f.Page-1)*int64(f.Limit))...)
	if e != nil {
		return nil, 0, e
	}
	defer rows.Close()
	out := []domain.Summary{}
	for rows.Next() {
		var n domain.Summary
		if e = rows.Scan(&n.ID, &n.Channel, &n.Recipient, &n.Priority, &n.Status, &n.CreatedAt); e != nil {
			return nil, 0, e
		}
		out = append(out, n)
	}
	return out, total, rows.Err()
}
func (s *Store) Batch(ctx context.Context, id string) (domain.NotificationBatch, error) {
	b := domain.NotificationBatch{ID: id, Breakdown: map[string]int{}}
	for _, st := range []string{"pending", "scheduled", "queued", "processing", "retry_pending", "failed", "cancelled", "delivered"} {
		b.Breakdown[st] = 0
	}
	rows, e := s.DB.QueryContext(ctx, `SELECT n.status,count(*),b.total_count,b.created_at,max(n.updated_at) FROM notification_batches b JOIN notifications n ON n.batch_id=b.id WHERE b.id=$1 GROUP BY n.status,b.id`, id)
	if e != nil {
		return b, e
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var count int
		var at time.Time
		if e = rows.Scan(&st, &count, &b.Total, &b.CreatedAt, &at); e != nil {
			return b, e
		}
		b.Breakdown[strings.ToLower(st)] = count
		if at.After(b.UpdatedAt) {
			b.UpdatedAt = at
		}
	}
	if e = rows.Err(); e != nil {
		return b, e
	}
	if b.Total == 0 {
		return b, domain.ErrNotFound
	}
	done := b.Breakdown["delivered"] + b.Breakdown["failed"] + b.Breakdown["cancelled"]
	b.Progress = 100 * float64(done) / float64(b.Total)
	b.Status = "PROCESSING"
	if done == b.Total {
		b.Status = "COMPLETED"
		if b.Breakdown["failed"] > 0 {
			b.Status = "PARTIALLY_FAILED"
		}
	}
	return b, nil
}
