package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"event-driven-notification-system/internal/domain"
	"fmt"
	"strings"
	"time"
)

type insertRow struct {
	ID, Batch, Channel, Recipient, Content, Priority, Status, Key, Template, Correlation, Trace string
	Retries                                                                                     int
	Scheduled                                                                                   *time.Time
	Created                                                                                     time.Time
	Metadata                                                                                    map[string]json.RawMessage
}

func (s *Store) Ingest(ctx context.Context, ns []domain.Notification, receipt *domain.Receipt) error {
	if len(ns) == 0 || len(ns) > 1000 {
		return errors.New("expected 1..1000 notifications")
	}
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if receipt != nil {
		r := receipt
		res, e := tx.ExecContext(ctx, `INSERT INTO idempotency_keys(key,request_hash,response_status,response_body,expires_at) VALUES($1,$2,$3,$4,now()+interval '24 hours')
 ON CONFLICT(key) DO UPDATE SET request_hash=excluded.request_hash,response_status=excluded.response_status,response_body=excluded.response_body,created_at=now(),expires_at=excluded.expires_at WHERE idempotency_keys.expires_at<=now()`, r.Key, r.Hash, r.Code, r.Body)
		if e != nil {
			return dbError(e)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return domain.ErrConflict
		}
	}
	first := ns[0]
	if first.BatchID != "" {
		_, e = tx.ExecContext(ctx, "INSERT INTO notification_batches(id,total_count,created_at) VALUES($1,$2,$3)", first.BatchID, len(ns), first.CreatedAt)
		if e != nil {
			return dbError(e)
		}
	}
	rows := make([]insertRow, len(ns))
	for i, n := range ns {
		rows[i] = insertRow{n.ID, n.BatchID, string(n.Channel), n.Recipient, n.Content, string(n.Priority), string(n.Status), n.IdempotencyKey, n.TemplateID, n.CorrelationID, n.Traceparent, n.MaxRetries, n.ScheduledAt, n.CreatedAt, n.Metadata}
	}
	data, e := json.Marshal(rows)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO notifications(id,batch_id,channel,recipient,content,priority,status,idempotency_key,template_id,correlation_id,traceparent,max_retries,scheduled_at,created_at,updated_at,metadata)
 SELECT "ID",nullif("Batch",'')::uuid,"Channel","Recipient","Content","Priority","Status",nullif("Key",''),nullif("Template",'')::uuid,"Correlation","Trace","Retries","Scheduled","Created","Created",coalesce("Metadata",'{}')
 FROM jsonb_to_recordset($1::jsonb) AS x("ID" uuid,"Batch" text,"Channel" channel_type,"Recipient" text,"Content" text,"Priority" priority_type,"Status" notification_status,"Key" text,"Template" text,"Correlation" text,"Trace" text,"Retries" int,"Scheduled" timestamptz,"Created" timestamptz,"Metadata" jsonb)`, data)
	if e != nil {
		return dbError(e)
	}
	return tx.Commit()
}
func (s *Store) Receipt(ctx context.Context, key string) (domain.Receipt, error) {
	var r domain.Receipt
	r.Key = key
	e := s.DB.QueryRowContext(ctx, "SELECT request_hash,response_status,response_body FROM idempotency_keys WHERE key=$1 AND expires_at>now()", key).Scan(&r.Hash, &r.Code, &r.Body)
	return r, dbError(e)
}

const columns = `id,channel,recipient,priority,status,created_at,coalesce(batch_id::text,''),content,retry_count,max_retries,coalesce(provider_message_id,''),coalesce(error_message,''),updated_at,scheduled_at,coalesce(template_id::text,''),metadata,correlation_id,traceparent`

type scanner interface{ Scan(...any) error }

func scanNotification(row scanner) (domain.Notification, error) {
	var n domain.Notification
	var meta []byte
	e := row.Scan(&n.ID, &n.Channel, &n.Recipient, &n.Priority, &n.Status, &n.CreatedAt, &n.BatchID, &n.Content, &n.RetryCount, &n.MaxRetries, &n.ProviderMessageID, &n.ErrorMessage, &n.UpdatedAt, &n.ScheduledAt, &n.TemplateID, &meta, &n.CorrelationID, &n.Traceparent)
	if e == nil && len(meta) > 0 {
		e = json.Unmarshal(meta, &n.Metadata)
	}
	return n, dbError(e)
}
func (s *Store) Get(ctx context.Context, id string) (domain.Notification, error) {
	n, e := scanNotification(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM notifications WHERE id=$1", id))
	if e == nil {
		n.Attempts, e = s.Attempts(ctx, id)
	}
	return n, e
}
func (s *Store) Attempts(ctx context.Context, id string) ([]domain.DeliveryAttempt, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT attempt_number,status,coalesce(http_status_code,0),coalesce(response_body,''),coalesce(error_reason,''),duration_ms,attempted_at FROM delivery_attempts WHERE notification_id=$1 ORDER BY attempt_number,attempted_at`, id)
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
	}
	return b, nil
}
