# Phase 8 — Deployment, end-to-end tests and measured performance

## 1. Executive summary of completed work

Built and ran the API, three channel workers, scheduler, PostgreSQL 16, Redis 7 and Prometheus. Verified embedded migration version 6 and upgrades from a populated version 1. The full uncached race suite passed with the database tests enabled. Two independently connected worker pools delivered 500 unique HTTP requests with a peak of 100 in a rolling second. The separate Compose burst produced 500 distinct IDs and 500 provider acknowledgements with the same peak.

- The Dockerfile builds non-root images for the server, worker, scheduler, and CLI. Compose configures durable Redis AOF, independent pools and bounded shutdown. The CI workflow runs lint, service-backed race tests, container startup and the three-channel smoke test.
- The integration suite covers atomic rejection, keyed batch-item receipts, stale worker fencing, panic quarantine, replay after an interrupted Redis acknowledgement, DLQ replay, breaker recovery, template scheduling and migration rollback/upgrade.
- The API exposes global provider counts and latency histograms. PostgreSQL maintains bounded counters on audit insertion. Batch counters handle terminal transitions and manual replay. The OpenAPI description includes templates, WebSockets and service probes.
- The final inventory includes the Go sources formatted and verified in this phase. The smoke script defines `request` and `main`. SQL adds `attempt_bucket`, `record_attempt_stats`, `maintain_batch_counts` and `dispatch_inserted_notifications`. The Makefile adds `test-integration` and `run-scheduler` and includes the CLI in `build`.

### File and symbol inventory

- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `prometheus.yml`
- `.github/workflows/ci.yml`
- `.golangci.yml`
- `Makefile`
- `README.md`
- `docs/openapi.yaml`
- `go.mod`
- `go.sum`
- `scripts/smoke.py`
- `migrations/000002_dispatch_state.up.sql`
- `migrations/000004_attempt_metrics.up.sql`
- `migrations/000004_attempt_metrics.down.sql`
- `migrations/000005_batch_counters.up.sql`
- `migrations/000005_batch_counters.down.sql`
- `migrations/000006_bulk_dispatch.up.sql`
- `migrations/000006_bulk_dispatch.down.sql`
- `cmd/cli/main_test.go` — `TestDLQReplay`
- `cmd/cli/main.go` — `main`, `run`, `execute`
- `cmd/scheduler/main.go` — `main`
- `cmd/server/main.go` — `main`, `run`
- `cmd/worker/main.go` — `main`, `run`
- `internal/api/api_test.go` — `TestAPIReplayAndStream`
- `internal/api/api.go` — `API`, `API.Router`, `requestContext`, `jsonResponse`, `problem`, `failure`, `API.health`, `resourceID`, `API.detail`, `API.batch`, `API.cancel`, `filter`, `API.list`
- `internal/api/batch_test.go` — `TestBatchBoundariesAndItemReceipt`
- `internal/api/batch.go` — `readBatch`
- `internal/api/benchmark_test.go` — `BenchmarkBatchIngestion`
- `internal/api/ingest.go` — `CreateRequest`, `batchRequest`, `decode`, `requestHash`, `API.create`, `API.replay`, `API.guard`
- `internal/api/template_test.go` — `TestScheduledTemplate`
- `internal/api/templates.go` — `API.templateRoutes`, `API.saveTemplate`, `invalidTemplate`, `API.render`
- `internal/api/websocket/hub.go` — `subscriber`, `Hub`, `New`, `Hub.Run`, `Hub.ServeHTTP`
- `internal/app/app.go` — `Services`, `Main`, `start`, `Wait`, `Services.Relay`
- `internal/config/config_test.go` — `TestConfiguration`
- `internal/config/config.go` — `Config`, `env`, `Load`
- `internal/domain/models.go` — `Channel`, `Channel.Valid`, `Priority`, `Priority.Valid`, `Status`, `Status.Valid`, `Summary`, `Notification`, `NotificationBatch`, `DeliveryAttempt`, `NotificationTemplate`, `Filter`, `Receipt`, `NotificationRepository`, `BatchRepository`, `IdempotencyRepository`, `DeliveryAttemptRepository`
- `internal/domain/validate_test.go` — `TestValidation`
- `internal/domain/validate.go` — `Notification.Validate`
- `internal/observe/aggregate.go` — `Totals`, `Totals.Describe`, `Totals.Collect`
- `internal/observe/observe.go` — `Metrics`, `Tracing`, `Serve`
- `internal/provider/provider_test.go` — `TestClassification`, `TestBreakerAndBackoff`, `TestInvalidAck`, `TestTimeoutAndHalfOpenRecovery`
- `internal/provider/provider.go` — `Payload`, `Accepted`, `Result`, `Client`, `New`, `breakerSettings`, `Client.Deliver`, `Client.send`, `Backoff`
- `internal/queue/redis/broker_test.go` — `TestFairnessAndAging`
- `internal/queue/redis/broker.go` — `Broker`, `New`, `Key`, `Job`, `Lease`, `Broker.Publish`, `Broker.Pop`, `Broker.Ack`, `Broker.Quarantine`, `Broker.Due`, `Broker.Lock`
- `internal/ratelimit/limiter_test.go` — `TestBucket`
- `internal/ratelimit/limiter.go` — `Limiter`, `Permit`, `Limiter.Try`, `Limiter.Wait`, `Permit.Done`
- `internal/repository/postgres/benchmark_test.go` — `BenchmarkBulkInsert`
- `internal/repository/postgres/dispatch.go` — `Event`, `Store.Flush`, `Store.Claim`, `Store.Finish`, `Store.Promote`, `Store.Recover`
- `internal/repository/postgres/migrations_test.go` — `TestUpgradeFromOriginalSchema`
- `internal/repository/postgres/notifications.go` — `Store.Receipt`, `scanner`, `scanNotification`, `Store.Get`, `Store.Attempts`, `attemptReader`, `readAttempts`, `Store.Cancel`, `Store.List`, `Store.Batch`
- `internal/repository/postgres/receipts.go` — `saveReceipts`
- `internal/repository/postgres/recovery.go` — `Store.DeferProbe`, `Store.Poison`
- `internal/repository/postgres/store_test.go` — `TestBatchAndLease`
- `internal/repository/postgres/store.go` — `Store`, `Open`, `dbError`
- `internal/repository/postgres/templates.go` — `Store.SaveTemplate`, `Store.Template`, `Store.Templates`, `Store.DeleteTemplate`
- `internal/scheduler/scheduler.go` — `Tick`, `Run`
- `internal/template/template_test.go` — `TestRender`
- `internal/template/template.go` — `Render`, `Validate`
- `internal/testutil/services.go` — `Services`
- `internal/worker/burst_test.go` — `TestBurst500`
- `internal/worker/integration_test.go` — `rejectingSender`, `rejectingSender.Deliver`, `TestRetryToDLQ`
- `internal/worker/recovery_test.go` — `panicSender`, `panicSender.Deliver`, `TestRecoveryAndQuarantine`
- `internal/worker/worker_test.go` — `TestRetryBudget`
- `internal/worker/worker.go` — `Sender`, `Pool`, `Pool.Run`, `Pool.process`, `Pool.quarantine`, `Pool.handle`, `outcome`, `Pool.Retries`
- `migrations/embed.go`
- `pkg/logger/logger.go` — `correlationKey`, `WithID`, `ID`, `New`, `From`
- `internal/repository/postgres/ingest.go` — `Store.Ingest`, `copyValues`
- `docs/DATABASE_SCHEMA.md`
- `docs/RATE_LIMITING_AND_PRIORITY.md`
- `docs/reports/PHASE_8_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

The release uses a shared build stage and separate Alpine entry points. Worker replication preserves the per-channel admission key. Redis fsyncs each accepted write in this Compose deployment. The SQL transaction and the outbox sequence fence interruptions at the database/broker boundary.

The initial JSON recordset path measured 73.05 ms p95 per repository transaction. The final pgx COPY path streams typed values on a borrowed pool connection. A statement-level transition table inserts the entire batch into the outbox at once. A small upfront enum registration cost applies to each new ingest connection. An immutable audit record also updates one fixed-size metrics bucket. Terminal batch counters introduce a row lock on the batch header at completion.

The HTTP benchmarks submit 20 sequential batches of 1,000 over loopback to the real handler and PostgreSQL. The database and Redis are live. The benchmark measures acceptance through the durable commit. WSL measured **87.44 ms p95 and 15,359 notifications/s**. On the Compose network, the same HTTP benchmark measured **75.51 ms p95 and 16,315 notifications/s**. Both exceed the 5,000 notifications/s goal and meet the 150 ms HTTP p95 target in this workload.

PostgreSQL statement logging over 51 COPY operations, including the benchmark calibration, measured **29.696 ms median, 48.984 ms p95 and 58.347 ms maximum**. A 50-transaction run including encoding, network, batch creation, outbox persistence and commit measured **78.07 ms p95**. The 50 ms target held at the measured SQL statement p95. Full transaction timings varied across runs. The 20-sample WSL run measured 66.13 ms p95 and a cold container benchmark measured 643.2 ms p95. These timings are local measurements and should be remeasured on the deployment storage.

During verification, a test that released each reservation immediately depended on making 100 Redis round trips within a second. It failed under disk contention. The final test holds 100 permits, checks that the next reservation waits, then releases one and verifies refill. The scheduling test separately verifies no early promotion and polls for promotion after the deadline to accommodate independent clocks.

## 3. Source code walkthrough and annotated snippets

One transaction includes receipt reservation, batch header creation, streamed COPY and the trigger-generated outbox. An error from any part rolls them back together.

From `internal/repository/postgres/ingest.go`

```go
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
```

The receiver supplies all timestamps. Sorting them and advancing the left boundary computes the maximum over every rolling second, including windows crossing wall-clock second boundaries.

From `internal/worker/burst_test.go`

```go
	peak, left := 0, 0
	for right, at := range arrivals {
		for !arrivals[left].After(at.Add(-time.Second)) {
			left++
		}
		peak = max(peak, right-left+1)
	}
	if peak > 100 {
		t.Fatalf("peak was %d", peak)
	}
	t.Logf("500 unique HTTP arrivals across two worker replicas in %s; busiest rolling second: %d", arrivals[499].Sub(arrivals[0]).Round(time.Millisecond), peak)
}
```

Each terminal delta is applied under the batch row lock. Replaying a failed notification reverses its terminal contribution until delivery finishes again.

From `migrations/000005_batch_counters.up.sql`

```sql
 IF done_delta<>0 OR failed_delta<>0 OR success_delta<>0 THEN
  UPDATE notification_batches SET
   completed_count=completed_count+done_delta,failure_count=failure_count+failed_delta,success_count=success_count+success_delta,
   updated_at=greatest(updated_at,NEW.updated_at),
   status=CASE WHEN completed_count+done_delta<total_count THEN 'PROCESSING'
               WHEN failure_count+failed_delta>0 THEN 'PARTIALLY_FAILED' ELSE 'COMPLETED' END
   WHERE id=NEW.batch_id;
 END IF;
 RETURN NEW;
END $$;
```

The worker and scheduler share a minimal runtime, and each image has its own executable entry point.

From `Dockerfile`

```Dockerfile
FROM runtime AS worker
COPY --from=build /out/worker /usr/local/bin/worker
EXPOSE 9091
ENTRYPOINT ["worker"]

FROM runtime AS scheduler
COPY --from=build /out/scheduler /usr/local/bin/scheduler
ENTRYPOINT ["scheduler"]

```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
docker compose up -d postgres redis
docker exec notification_postgres createdb -U notification_user notification_phase_tests
# The test role creates and removes separate databases for integration cases.
TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test -v -race -count=1 ./...
TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test ./internal/repository/postgres -run '^$' -bench '^BenchmarkBulkInsert$' -benchtime=20x -count=1
TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test ./internal/api -run '^$' -bench '^BenchmarkBatchIngestion$' -benchtime=20x -count=1
TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable&options=-c%20log_min_duration_statement%3D0&application_name=notify_bulk_profile' TEST_REDIS_ADDR=localhost:6379 go test ./internal/repository/postgres -run '^$' -bench '^BenchmarkBulkInsert$' -benchtime=50x -count=1
~/.local/notification-sdk/lint/golangci-lint run ./...
go vet ./...
GOTOOLCHAIN=go1.22.12 go build ./cmd/...
go build ./cmd/...
go mod verify
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7 -shellcheck= -pyflakes= .github/workflows/ci.yml
uvx --from openapi-spec-validator openapi-spec-validator docs/openapi.yaml
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 180
python scripts/smoke.py
CGO_ENABLED=0 go test -c -o /mnt/c/Users/holym/AppData/Local/Temp/notify-bench-01a1f93f/db-bench ./internal/repository/postgres
CGO_ENABLED=0 go test -c -o /mnt/c/Users/holym/AppData/Local/Temp/notify-bench-01a1f93f/http-bench ./internal/api
```

Additional Docker Desktop checks ran from PowerShell. The commands below are excerpts. The complete phase inventory, original burst commands and logs are preserved at `git show 445110e:docs/reports/PHASE_8_REPORT.md`.

```powershell
$mount="type=bind,source=$env:TEMP\notify-bench-01a1f93f,target=/bench,readonly"
$settings=@('--rm','--network','event-driven-notification-system_default','--mount',$mount,'-e','TEST_DATABASE_URL=postgres://notification_user:notification_secret@postgres:5432/notification_phase_tests?sslmode=disable','-e','TEST_REDIS_ADDR=redis:6379','alpine:3.22')
docker run @settings /bench/db-bench '-test.run=^$' '-test.bench=^BenchmarkBulkInsert$' '-test.benchtime=20x'
if($LASTEXITCODE -ne 0){exit 1}
docker run @settings /bench/http-bench '-test.run=^$' '-test.bench=^BenchmarkBatchIngestion$' '-test.benchtime=20x' | Select-Object -Last 3
exit $LASTEXITCODE
$samples=@(docker logs notification_postgres --since 180s 2>&1 | ForEach-Object { if ("$_" -match 'duration: ([0-9.]+) ms\s+statement: copy .*notifications') { [double]::Parse($Matches[1],[cultureinfo]::InvariantCulture) } } | Sort-Object)
if ($samples.Count -gt 0) { "COPY samples=$($samples.Count) median_ms=$($samples[[int][math]::Ceiling(0.5*$samples.Count)-1]) p95_ms=$($samples[[int][math]::Ceiling(0.95*$samples.Count)-1]) max_ms=$($samples[-1])" } else { docker logs notification_postgres --since 60s 2>&1 | Select-String -Pattern 'duration:' | Select-Object -Last 6 }
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
(Invoke-RestMethod 'http://localhost:9090/api/v1/targets').data.activeTargets | Select-Object scrapeUrl,health
```

Observed output (selected lines)

```text
--- PASS: TestDLQReplay (12.94s)
ok  	event-driven-notification-system/cmd/cli	13.995s
--- PASS: TestAPIReplayAndStream (13.05s)
--- PASS: TestBatchBoundariesAndItemReceipt (1.80s)
--- PASS: TestScheduledTemplate (7.78s)
ok  	event-driven-notification-system/internal/api	23.699s
--- PASS: TestConfiguration (0.00s)
ok  	event-driven-notification-system/internal/config	1.026s
--- PASS: TestValidation (0.00s)
ok  	event-driven-notification-system/internal/domain	1.029s
--- PASS: TestClassification (0.02s)
--- PASS: TestBreakerAndBackoff (0.00s)
--- PASS: TestInvalidAck (0.00s)
--- PASS: TestTimeoutAndHalfOpenRecovery (1.00s)
ok  	event-driven-notification-system/internal/provider	2.060s
--- PASS: TestFairnessAndAging (4.61s)
ok  	event-driven-notification-system/internal/queue/redis	5.620s
--- PASS: TestBucket (12.70s)
ok  	event-driven-notification-system/internal/ratelimit	13.737s
--- PASS: TestUpgradeFromOriginalSchema (12.22s)
--- PASS: TestBatchAndLease (1.76s)
ok  	event-driven-notification-system/internal/repository/postgres	15.021s
--- PASS: TestRender (0.00s)
ok  	event-driven-notification-system/internal/template	1.016s
--- PASS: TestRetryToDLQ (20.61s)
--- PASS: TestRecoveryAndQuarantine (1.49s)
--- PASS: TestRetryBudget (0.00s)
    burst_test.go:121: 500 unique HTTP arrivals across two worker replicas in 5.686s; busiest rolling second: 100
--- PASS: TestBurst500 (6.91s)
ok  	event-driven-notification-system/internal/worker	30.040s

Batch 56dc35ce-033f-4b11-b5c2-e0684402df46 delivered all 3 channels. Replay returned the original IDs.

batch=682724c6-397d-424b-93ab-11a53dd0a9d1 unique|acknowledgements|rolling_peak|elapsed_seconds=500|500|100|9.031

COPY samples=51 median_ms=29.696 p95_ms=48.984 max_ms=58.347

50 repository transactions: 56942100 ns/op, 78.07 ms/p95
20 HTTP batches from WSL: 87.44 ms/p95, 15359 notifications/s
20 HTTP batches on the Compose network: 75.51 ms/p95, 16315 notifications/s
0 issues.
Go 1.22.12 build and go vet passed.
docs/openapi.yaml: OK
all modules verified
Actionlint, vet and Go 1.22.12 build passed.
schema_migrations: version=6, dirty=false
readiness: {"healthy":true,"postgres":true,"redis":true}
Prometheus scrape targets: api:8080, worker-sms:9091, worker-email:9091, worker-push:9091 — all up
```

## 5. Prerequisites and handoff

The complete stack remains available through Docker Compose. API readiness is at http://localhost:8080/ready and Prometheus at http://localhost:9090. Run `make test-integration` with a POSIX shell to repeat the local service-backed suite. Use `python scripts/smoke.py --cancel-scheduled` to check the running stack locally, or `python scripts/smoke.py` to exercise the configured external webhook. The core tests need a development PostgreSQL role with CREATEDB and dedicated Redis logical databases. The benchmarks use logical database 6.

External delivery is at least once. A provider should honor the stable notification ID in its idempotency header to deduplicate a retry after an ambiguous acknowledgement. The workflow definition and its local equivalents were verified here. GitHub-hosted execution requires a push or pull request. OTLP export uses the collector endpoint supplied by the deployment.
