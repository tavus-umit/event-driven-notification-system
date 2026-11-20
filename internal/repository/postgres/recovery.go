package postgres

import (
	"context"

	"event-driven-notification-system/internal/domain"
)

func (s *Store) DeferProbe(ctx context.Context, id, owner string) error {
	r, e := s.DB.ExecContext(ctx, `UPDATE notifications SET status='RETRY_PENDING',due_at=now()+interval '30 seconds',claim_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_id=$2 AND status='PROCESSING'`, id, owner)
	if e != nil {
		return e
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return domain.ErrConflict
	}
	return nil
}
func (s *Store) Poison(ctx context.Context, id, owner string, channel domain.Channel, reason string) error {
	_, e := s.DB.ExecContext(ctx, `UPDATE notifications SET status='FAILED',error_message=$4,claim_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND channel=$3 AND (status='QUEUED' OR (status='PROCESSING' AND claim_id=$2))`, id, owner, channel, reason)
	return e
}
