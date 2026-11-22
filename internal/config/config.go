package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"event-driven-notification-system/internal/domain"
)

type Config struct {
	Addr, MetricsAddr, DatabaseURL, RedisAddr, RedisPassword, ProviderURL string
	Channel                                                               domain.Channel
	Level                                                                 slog.Level
	MaxOpen, MaxIdle, Workers, MaxRetries, BreakerThreshold               int
	ReadTimeout, WriteTimeout, ProviderTimeout                            time.Duration
}

func env(k, fallback string) string {
	if s, ok := os.LookupEnv(k); ok {
		return s
	}
	return fallback
}
func Load() (Config, error) {
	var c Config
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return c, err
	}
	c.Addr = ":" + env("SERVER_PORT", "8080")
	c.MetricsAddr = ":" + env("METRICS_PORT", "9091")
	u := url.URL{Scheme: "postgres", User: url.UserPassword(env("DB_USER", "notification_user"), env("DB_PASSWORD", "notification_secret")), Host: net.JoinHostPort(env("DB_HOST", "localhost"), env("DB_PORT", "5432")), Path: "/" + env("DB_NAME", "notification_db")}
	q := u.Query()
	q.Set("sslmode", env("DB_SSLMODE", "disable"))
	u.RawQuery = q.Encode()
	c.DatabaseURL = env("DATABASE_URL", u.String())
	c.RedisAddr = net.JoinHostPort(env("REDIS_HOST", "localhost"), env("REDIS_PORT", "6379"))
	c.RedisPassword = env("REDIS_PASSWORD", "")
	c.ProviderURL = os.Getenv("PROVIDER_WEBHOOK_URL")
	if c.ProviderURL != "" {
		provider, err := url.Parse(c.ProviderURL)
		if err != nil || provider.Host == "" || (provider.Scheme != "http" && provider.Scheme != "https") {
			return c, errors.New("invalid PROVIDER_WEBHOOK_URL")
		}
	}
	c.Channel = domain.Channel(env("WORKER_CHANNEL", ""))
	if c.Channel != "" && !c.Channel.Valid() {
		return c, errors.New("invalid WORKER_CHANNEL")
	}
	if err := c.Level.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return c, err
	}
	for _, p := range []struct {
		k             string
		target        *int
		def, min, max int
	}{
		{"DB_MAX_OPEN_CONNS", &c.MaxOpen, 50, 1, 1000}, {"DB_MAX_IDLE_CONNS", &c.MaxIdle, 25, 0, 1000}, {"WORKERS_PER_CHANNEL", &c.Workers, 16, 1, 200}, {"MAX_RETRIES", &c.MaxRetries, 3, 0, 3}, {"CIRCUIT_BREAKER_FAIL_THRESHOLD", &c.BreakerThreshold, 5, 1, 10000},
	} {
		v, e := strconv.Atoi(env(p.k, strconv.Itoa(p.def)))
		if e != nil || v < p.min || v > p.max {
			return c, fmt.Errorf("invalid %s", p.k)
		}
		*p.target = v
	}
	if c.MaxIdle > c.MaxOpen {
		return c, errors.New("idle connections exceed open connections")
	}
	for _, k := range []string{"RATE_LIMIT_SMS", "RATE_LIMIT_EMAIL", "RATE_LIMIT_PUSH"} {
		if env(k, "100") != "100" {
			return c, fmt.Errorf("%s must be 100", k)
		}
	}
	for _, p := range []struct {
		k, def string
		target *time.Duration
	}{{"SERVER_READ_TIMEOUT", "10s", &c.ReadTimeout}, {"SERVER_WRITE_TIMEOUT", "10s", &c.WriteTimeout}} {
		d, e := time.ParseDuration(env(p.k, p.def))
		if e != nil || d <= 0 {
			return c, fmt.Errorf("invalid %s", p.k)
		}
		*p.target = d
	}
	sec, e := strconv.Atoi(env("PROVIDER_TIMEOUT_SECONDS", "5"))
	if e != nil || sec < 1 || sec > 30 {
		return c, errors.New("provider timeout must be 1..30 seconds")
	}
	c.ProviderTimeout = time.Duration(sec) * time.Second
	return c, nil
}
