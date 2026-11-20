package postgres_test

import (
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"event-driven-notification-system/internal/testutil"
	"event-driven-notification-system/migrations"
)

func TestUpgradeFromOriginalSchema(t *testing.T) {
	s := testutil.Services(t, 6)
	src, e := iofs.New(migrations.Files, ".")
	if e != nil {
		t.Fatal(e)
	}
	m, e := migrate.NewWithSourceInstance("iofs", src, s.Config.DatabaseURL)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if e = m.Migrate(1); e != nil {
		t.Fatal(e)
	}
	_, e = s.Store.DB.Exec("INSERT INTO notifications(channel,recipient,content,status) VALUES('sms','+123','legacy','QUEUED'),('sms','+124','legacy','PENDING')")
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Up(); e != nil {
		t.Fatal(e)
	}
	var queued int
	if e = s.Store.DB.QueryRow("SELECT count(*) FROM notification_outbox WHERE current_status='QUEUED'").Scan(&queued); e != nil || queued != 2 {
		t.Fatal(queued, e)
	}
}
