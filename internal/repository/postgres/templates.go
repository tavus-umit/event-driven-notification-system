package postgres

import (
	"context"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
)

func (s *Store) SaveTemplate(ctx context.Context, t domain.NotificationTemplate, create bool) (domain.NotificationTemplate, error) {
	var e error
	if create {
		t.ID = uuid.NewString()
		_, e = s.DB.ExecContext(ctx, `INSERT INTO notification_templates(id,name,channel,subject,body_template) VALUES($1,$2,$3,$4,$5)`, t.ID, t.Name, t.Channel, t.Subject, t.Body)
	} else {
		e = s.DB.QueryRowContext(ctx, `UPDATE notification_templates SET name=$2,channel=$3,subject=$4,body_template=$5,updated_at=now() WHERE id=$1 RETURNING id`, t.ID, t.Name, t.Channel, t.Subject, t.Body).Scan(&t.ID)
	}
	return t, dbError(e)
}
func (s *Store) Template(ctx context.Context, id string) (domain.NotificationTemplate, error) {
	var t domain.NotificationTemplate
	e := s.DB.QueryRowContext(ctx, `SELECT id,name,channel,coalesce(subject,''),body_template FROM notification_templates WHERE id=$1`, id).Scan(&t.ID, &t.Name, &t.Channel, &t.Subject, &t.Body)
	return t, dbError(e)
}
func (s *Store) Templates(ctx context.Context) ([]domain.NotificationTemplate, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT id,name,channel,coalesce(subject,''),body_template FROM notification_templates ORDER BY name`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.NotificationTemplate{}
	for rows.Next() {
		var t domain.NotificationTemplate
		if e = rows.Scan(&t.ID, &t.Name, &t.Channel, &t.Subject, &t.Body); e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *Store) DeleteTemplate(ctx context.Context, id string) error {
	var deleted string
	return dbError(s.DB.QueryRowContext(ctx, "DELETE FROM notification_templates WHERE id=$1 RETURNING id", id).Scan(&deleted))
}
