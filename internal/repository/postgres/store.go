package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"event-driven-notification-system/internal/config"
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/migrations"
)

type Store struct{ DB *sql.DB }

func Open(ctx context.Context, c config.Config) (*Store, error) {
	src, e := iofs.New(migrations.Files, ".")
	if e != nil {
		return nil, e
	}
	m, e := migrate.NewWithSourceInstance("iofs", src, c.DatabaseURL)
	if e != nil {
		return nil, e
	}
	e = m.Up()
	_, closeErr := m.Close()
	if e != nil && !errors.Is(e, migrate.ErrNoChange) {
		return nil, e
	}
	if closeErr != nil {
		return nil, closeErr
	}
	db, e := sql.Open("pgx", c.DatabaseURL)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(c.MaxOpen)
	db.SetMaxIdleConns(c.MaxIdle)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db}, nil
}
func dbError(e error) error {
	if errors.Is(e, sql.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pg *pgconn.PgError
	if errors.As(e, &pg) && (pg.Code == "23505" || pg.Code == "23503") {
		return domain.ErrConflict
	}
	if pg != nil {
		switch pg.Code {
		case "22021", "22P05", "22001", "22P02":
			return domain.ErrInvalid
		}
	}
	return e
}

var _ domain.NotificationRepository = (*Store)(nil)
var _ domain.BatchRepository = (*Store)(nil)
var _ domain.DeliveryAttemptRepository = (*Store)(nil)
var _ domain.IdempotencyRepository = (*Store)(nil)
