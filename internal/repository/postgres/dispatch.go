package postgres

import (
	"context"
	"event-driven-notification-system/internal/domain"
	"time"
)

type Event struct {
	Seq         int64           `json:"sequence"`
	ID          string          `json:"notificationId"`
	BatchID     string          `json:"batchId,omitempty"`
	Channel     domain.Channel  `json:"channel"`
	Priority    domain.Priority `json:"priority"`
	Recipient   string          `json:"recipient"`
	Previous    domain.Status   `json:"previousStatus"`
	Current     domain.Status   `json:"currentStatus"`
	Due         *time.Time      `json:"dueAt,omitempty"`
	Retry       int             `json:"retryCount"`
	Reason      string          `json:"reason,omitempty"`
	Correlation string          `json:"correlationId"`
	Trace       string          `json:"traceparent,omitempty"`
	At          time.Time       `json:"timestamp"`
	Type        string          `json:"event"`
}

func (s *Store) Flush(ctx context.Context, publish func(context.Context, []Event) error) (int, error) {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	rows, e := tx.QueryContext(ctx, `SELECT seq,notification_id,coalesce(batch_id::text,''),channel,priority,recipient,previous_status,current_status,due_at,retry_count,reason,correlation_id,traceparent,created_at FROM notification_outbox ORDER BY seq LIMIT 1000 FOR UPDATE SKIP LOCKED`)
	if e != nil {
		return 0, e
	}
	defer rows.Close()
	var events []Event
	var ids []int64
	for rows.Next() {
		var v Event
		v.Type = "NOTIFICATION_STATUS_CHANGED"
		if e = rows.Scan(&v.Seq, &v.ID, &v.BatchID, &v.Channel, &v.Priority, &v.Recipient, &v.Previous, &v.Current, &v.Due, &v.Retry, &v.Reason, &v.Correlation, &v.Trace, &v.At); e != nil {
			return 0, e
		}
		events = append(events, v)
		ids = append(ids, v.Seq)
	}
	if e = rows.Err(); e != nil || len(events) == 0 {
		return 0, e
	}
	if e = publish(ctx, events); e != nil {
		return 0, e
	}
	if _, e = tx.ExecContext(ctx, "DELETE FROM notification_outbox WHERE seq=ANY($1)", ids); e != nil {
		return 0, e
	}
	return len(events), tx.Commit()
}
func (s *Store) Claim(ctx context.Context, id, owner string, channel domain.Channel, retry int) (domain.Notification, error) {
	return scanNotification(s.DB.QueryRowContext(ctx, `UPDATE notifications SET status='PROCESSING',claim_id=$2,lease_until=now()+interval '90 seconds',updated_at=now() WHERE id=$1 AND channel=$3 AND status='QUEUED' AND retry_count=$4 RETURNING `+columns, id, owner, channel, retry))
}
func (s *Store) Finish(ctx context.Context, id, owner string, a domain.DeliveryAttempt, status domain.Status, message string, due *time.Time) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE notifications SET status=$3::notification_status,provider_message_id=nullif($4,''),error_message=nullif($5,''),retry_count=retry_count+CASE WHEN $3::notification_status='RETRY_PENDING' THEN 1 ELSE 0 END,due_at=$6,claim_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_id=$2 AND status='PROCESSING'`, id, owner, status, message, a.Error, due)
	if e != nil {
		return e
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return domain.ErrConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO delivery_attempts(notification_id,attempt_number,status,http_status_code,response_body,error_reason,duration_ms,attempted_at) VALUES($1,$2,$3,nullif($4,0),$5,$6,$7,$8)`, id, a.Number, a.Status, a.HTTPStatus, a.Body, a.Error, a.DurationMS, a.At)
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Promote(ctx context.Context, id string, from domain.Status, retry int) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE notifications SET status='QUEUED',due_at=NULL,updated_at=now() WHERE id=$1 AND status=$2 AND retry_count=$3 AND coalesce(due_at,scheduled_at)<=now()`, id, from, retry)
	return e
}
func (s *Store) Recover(ctx context.Context) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE notifications SET status='QUEUED',claim_id=NULL,lease_until=NULL,updated_at=now() WHERE status='PROCESSING' AND lease_until<=now()`)
	return e
}
