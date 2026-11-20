package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"event-driven-notification-system/internal/domain"
)

func saveReceipts(ctx context.Context, tx pgx.Tx, items []domain.Notification, batch *domain.Receipt) error {
	var receipts []domain.Receipt
	if batch != nil {
		receipts = append(receipts, *batch)
	}
	for _, item := range items {
		if item.Receipt != nil {
			receipts = append(receipts, *item.Receipt)
		}
	}
	if len(receipts) == 0 {
		return nil
	}
	data, e := json.Marshal(receipts)
	if e != nil {
		return e
	}
	res, e := tx.Exec(ctx, `INSERT INTO idempotency_keys(key,request_hash,response_status,response_body,expires_at)
 SELECT "Key","Hash","Code","Body",now()+interval '24 hours' FROM jsonb_to_recordset($1) AS x("Key" text,"Hash" text,"Code" int,"Body" jsonb) ORDER BY "Key"
 ON CONFLICT(key) DO UPDATE SET request_hash=excluded.request_hash,response_status=excluded.response_status,response_body=excluded.response_body,created_at=now(),expires_at=excluded.expires_at WHERE idempotency_keys.expires_at<=now()`, data)
	if e != nil {
		return dbError(e)
	}
	if res.RowsAffected() != int64(len(receipts)) {
		return domain.ErrConflict
	}
	return nil
}
