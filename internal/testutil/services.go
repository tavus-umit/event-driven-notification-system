// Package testutil provisions an isolated database for service-backed tests.
package testutil

import (
	"context"
	"database/sql"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/config"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/repository/postgres"
	"github.com/google/uuid"
	"net/url"
	"os"
	"strings"
	"testing"
)

func Services(t *testing.T, redisDB int) *app.Services {
	t.Helper()
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_REDIS_ADDR for integration tests")
	}
	u, e := url.Parse(dsn)
	if e != nil {
		t.Fatal(e)
	}
	admin, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	name := "notify_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, e = admin.Exec("CREATE DATABASE " + name); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := admin.Exec("DROP DATABASE " + name); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	u.Path = "/" + name
	c := config.Config{DatabaseURL: u.String(), MaxOpen: 32, MaxIdle: 8, MaxRetries: 3}
	s, e := postgres.Open(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.DB.Close() })
	b := broker.New(addr, "", redisDB)
	t.Cleanup(func() { b.Client.Close() })
	return &app.Services{Config: c, Store: s, Broker: b}
}
