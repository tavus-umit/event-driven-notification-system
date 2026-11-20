# Phase 5 — Channel workers and failure recovery

## 1. Executive summary of completed work

Ran a real Redis and PostgreSQL worker loop with a failing sender. The first three failures were delayed and retried. The fourth committed FAILED, left four immutable attempts, and produced a Redis DLQ record. A separate assertion covers immediate failure on HTTP 422.

### File and symbol inventory

- `internal/app/app.go` — `Services`, `Main`, `start`, `Wait`, `Services.Relay`
- `cmd/worker/main.go` — `main`, `run`
- `internal/worker/worker.go` — `Sender`, `Pool`, `Pool.Run`, `Pool.process`, `Pool.quarantine`, `Pool.handle`, `outcome`, `Pool.Retries`
- `internal/worker/worker_test.go` — `TestRetryBudget`
- `internal/worker/integration_test.go` — `rejectingSender`, `rejectingSender.Deliver`, `TestRetryToDLQ`
- `internal/testutil/services.go` — `Services`
- `internal/repository/postgres/dispatch.go` — `Event`, `Store.Flush`, `Store.Claim`, `Store.Finish`, `Store.Promote`, `Store.Recover`
- `internal/repository/postgres/recovery.go` — `Store.DeferProbe`, `Store.Poison`
- `docs/reports/PHASE_5_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

Each process can run all three independent pools or select one WORKER_CHANNEL. The goroutine count bounds outstanding leases. SIGINT and SIGTERM stop new pops, while context.WithoutCancel and a 45-second deadline drain active work. A 90-second visibility timeout bounds recovery from crashes. SQL and Redis both retain recoverable ownership until completion. A circuit-open deferral spends no provider attempt. At-least-once delivery still permits duplicate external effects if a provider accepts a request before the worker can record its result.

## 3. Source code walkthrough and annotated snippets

Acquire admission before claiming. If cancellation won, the conditional update finds no row and the obsolete queue entry is acknowledged.

From `internal/worker/worker.go`

```go
	permit, e := p.Limiter.Wait(ctx, p.Channel)
	if e != nil {
		return e
	}
	defer func() {
		if e := permit.Done(ctx); e != nil {
			slog.Warn("rate reservation retained", "error", e)
		}
	}()
	n, e := p.Service.Store.Claim(ctx, l.Job.ID, owner, p.Channel, l.Job.Retry)
	if errors.Is(e, domain.ErrNotFound) {
		return nil
	}
	if e != nil {
		return e
```

A retry stores a due time in the same transaction as the failed attempt. The outbox relay routes it to the delayed sorted set.

From `internal/worker/worker.go`

```go
	if r.Retryable && n.RetryCount < n.MaxRetries {
		due := time.Now().Add(provider.Backoff(n.RetryCount))
		return a, domain.RetryPending, &due
	}
	return a, domain.Failed, nil
}
```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
gofmt -w internal/app internal/testutil internal/worker internal/repository/postgres cmd/worker && TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test -v -race ./internal/worker ./cmd/worker
```

Observed output (selected lines)

```text
=== RUN   TestRetryToDLQ
2026/11/20 17:08:28 INFO delivery attempt correlation_id="" notification_id=bd2ffab2-f9f2-4bb8-b3be-e1eda754c3a7 channel=sms attempt=1 status=RETRY_PENDING duration_ms=0 error=<nil>
2026/11/20 17:08:28 INFO delivery attempt correlation_id="" notification_id=bd2ffab2-f9f2-4bb8-b3be-e1eda754c3a7 channel=sms attempt=2 status=RETRY_PENDING duration_ms=0 error=<nil>
2026/11/20 17:08:29 INFO delivery attempt correlation_id="" notification_id=bd2ffab2-f9f2-4bb8-b3be-e1eda754c3a7 channel=sms attempt=3 status=RETRY_PENDING duration_ms=0 error=<nil>
2026/11/20 17:08:31 INFO delivery attempt correlation_id="" notification_id=bd2ffab2-f9f2-4bb8-b3be-e1eda754c3a7 channel=sms attempt=4 status=FAILED duration_ms=0 error=<nil>
--- PASS: TestRetryToDLQ (4.10s)
=== RUN   TestRetryBudget
--- PASS: TestRetryBudget (0.00s)
PASS
ok  	event-driven-notification-system/internal/worker	5.122s
?   	event-driven-notification-system/cmd/worker	[no test files]
```

## 5. Prerequisites and handoff

Use app.Main for service lifecycle, and Services.Relay for durable asynchronous publish. testutil.Services provisions a test-owned PostgreSQL database and selects a dedicated Redis logical database.
