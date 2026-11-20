package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"

	"event-driven-notification-system/internal/domain"
)

var notificationColumns = []string{"id", "batch_id", "channel", "recipient", "content", "priority", "status", "idempotency_key", "template_id", "correlation_id", "traceparent", "max_retries", "scheduled_at", "created_at", "updated_at", "metadata"}

// Ingest borrows one connection from the bounded SQL pool. Native COPY streams
// the notifications and the statement trigger stages their dispatch in bulk.
func (s *Store) Ingest(ctx context.Context, items []domain.Notification, receipt *domain.Receipt) error {
	ctx, span := otel.Tracer("notification/postgres").Start(ctx, "persist notifications")
	defer span.End()
	if len(items) < 1 || len(items) > 1000 {
		return errors.New("expected 1..1000 notifications")
	}
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		pg := driverConn.(*stdlib.Conn).Conn()
		if _, registered := pg.TypeMap().TypeForName("channel_type"); !registered {
			types, err := pg.LoadTypes(ctx, []string{"channel_type", "priority_type", "notification_status"})
			if err != nil {
				return err
			}
			pg.TypeMap().RegisterTypes(types)
		}
		return dbError(pgx.BeginFunc(ctx, pg, func(tx pgx.Tx) error {
			if err := saveReceipts(ctx, tx, items, receipt); err != nil {
				return err
			}
			first := items[0]
			if first.BatchID != "" {
				if _, err := tx.Exec(ctx, "INSERT INTO notification_batches(id,total_count,created_at) VALUES($1,$2,$3)", first.BatchID, len(items), first.CreatedAt); err != nil {
					return err
				}
			}
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"notifications"}, notificationColumns, pgx.CopyFromSlice(len(items), func(i int) ([]any, error) { return copyValues(items[i]) }))
			return err
		}))
	})
}
func copyValues(n domain.Notification) ([]any, error) {
	var id, batch, template pgtype.UUID
	if err := id.Scan(n.ID); err != nil {
		return nil, err
	}
	if n.BatchID != "" {
		if err := batch.Scan(n.BatchID); err != nil {
			return nil, err
		}
	}
	if n.TemplateID != "" {
		if err := template.Scan(n.TemplateID); err != nil {
			return nil, err
		}
	}
	var key any
	if n.IdempotencyKey != "" {
		key = n.IdempotencyKey
	}
	metadata := []byte("{}")
	if n.Metadata != nil {
		var err error
		metadata, err = json.Marshal(n.Metadata)
		if err != nil {
			return nil, err
		}
	}
	return []any{id, batch, string(n.Channel), n.Recipient, n.Content, string(n.Priority), string(n.Status), key, template, n.CorrelationID, n.Traceparent, n.MaxRetries, n.ScheduledAt, n.CreatedAt, n.CreatedAt, metadata}, nil
}
