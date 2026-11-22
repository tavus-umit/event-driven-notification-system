# Phase 7 — Scheduling, templates and DLQ replay

## 1. Executive summary of completed work

Created, read, listed, updated and deleted a template through HTTP. A scheduled SMS rendered to “Hi Ada”, remained scheduled before its deadline, then moved to QUEUED and disappeared from the scheduled index. An expansion exceeding 1,600 characters returned 400. Both scheduler and replay CLI built.

### File and symbol inventory

- `cmd/scheduler/main.go` — `main`
- `cmd/cli/main.go` — `main`, `run`
- `internal/scheduler/scheduler.go` — `Tick`, `Run`
- `internal/template/template.go` — `Render`, `Validate`
- `internal/template/template_test.go` — `TestRender`
- `internal/repository/postgres/templates.go` — `Store.SaveTemplate`, `Store.Template`, `Store.Templates`, `Store.DeleteTemplate`
- `internal/repository/postgres/notifications.go` — `insertRow`, `Store.Ingest`, `Store.Receipt`, `scanner`, `scanNotification`, `Store.Get`, `Store.Attempts`, `Store.Cancel`, `Store.List`, `Store.Batch`
- `internal/api/templates.go` — `API.templateRoutes`, `API.saveTemplate`, `invalidTemplate`, `API.render`
- `internal/api/api.go` — `API`, `API.Router`, `requestContext`, `jsonResponse`, `problem`, `failure`, `API.health`, `resourceID`, `API.detail`, `API.batch`, `API.cancel`, `filter`, `API.list`
- `internal/api/ingest.go` — `CreateRequest`, `batchRequest`, `decode`, `API.create`, `API.replay`, `API.guard`
- `internal/api/template_test.go` — `TestScheduledTemplate`
- `internal/domain/models.go` — `Channel`, `Channel.Valid`, `Priority`, `Priority.Valid`, `Status`, `Status.Valid`, `Summary`, `Notification`, `NotificationBatch`, `DeliveryAttempt`, `NotificationTemplate`, `Filter`, `Receipt`, `NotificationRepository`, `BatchRepository`, `IdempotencyRepository`, `DeliveryAttemptRepository`
- `internal/worker/worker.go` — `Sender`, `Pool`, `Pool.Run`, `Pool.process`, `Pool.quarantine`, `Pool.handle`, `outcome`, `Pool.Retries`
- `migrations/000003_replay.up.sql`
- `migrations/000003_replay.down.sql`
- `docs/reports/PHASE_7_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

The scheduler checks Redis server time and PostgreSQL eligibility. Its guarded bulk SQL transition is safe for multiple schedulers. The transactional outbox delivers a Lua operation that removes the due member and adds the ready job atomically. Template syntax supports simple named values and bounded single-pass output. Each HTTP batch caches the template definition per ID, preserving a consistent rendering for repeated uses. DLQ replay starts a new retry budget and tracks an attempt offset to preserve the original audit sequence.

## 3. Source code walkthrough and annotated snippets

The renderer accepts only identifiers, requires every value and bounds output before appending. Substituted strings remain literal.

From `internal/template/template.go`

```go
		name, tail, closed := strings.Cut(rest, "}}")
		name = strings.TrimSpace(name)
		if !closed || !variable.MatchString(name) {
			return "", errors.New("invalid template placeholder")
		}
		if values != nil {
			v, ok := values[name]
			if !ok {
				return "", fmt.Errorf("missing variable %s", name)
			}
			if out.Len()+len(v) > maxBytes {
				return "", errors.New("rendered text exceeds limit")
			}
			out.WriteString(v)
		}
		body = tail
```

Only due rows still in SCHEDULED can transition. A racing cancellation or another scheduler removes eligibility before dispatch.

From `internal/scheduler/scheduler.go`

```go
	res, e := s.Store.DB.ExecContext(ctx, `UPDATE notifications SET status='QUEUED',updated_at=now() WHERE id=ANY($1::uuid[]) AND status='SCHEDULED' AND scheduled_at<=now()`, ids)
	if e != nil {
		return 0, e
	}
	n, e := res.RowsAffected()
	return int(n), e
}
```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
gofmt -w cmd internal && TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test -v -race ./internal/template ./internal/api -run 'TestRender|TestScheduledTemplate' && go build ./cmd/scheduler ./cmd/cli
```

Observed output (selected lines)

```text
=== RUN   TestRender
--- PASS: TestRender (0.00s)
PASS
ok  	event-driven-notification-system/internal/template	1.012s
=== RUN   TestScheduledTemplate
2026/11/20 17:13:52 INFO HTTP request correlation_id=42ad89ec-2017-4bf1-b74d-2a7fa8539b26 method=POST path=/api/v1/templates status=201 duration_ms=8
2026/11/20 17:13:52 INFO HTTP request correlation_id=dc429b75-c2b0-4bcb-8323-7c7abd16ba37 method=GET path=/api/v1/templates/03f569b2-57c9-471b-8faf-7105b1700b8c status=200 duration_ms=2
2026/11/20 17:13:52 INFO HTTP request correlation_id=1fbdb183-32a0-43f0-898c-3c2478a9d32c method=GET path=/api/v1/templates status=200 duration_ms=2
2026/11/20 17:13:52 INFO HTTP request correlation_id=03df97c7-af58-4a02-b305-55716021d385 method=PUT path=/api/v1/templates/03f569b2-57c9-471b-8faf-7105b1700b8c status=200 duration_ms=4
2026/11/20 17:13:52 INFO HTTP request correlation_id=a18b87cc-a1b3-4fa6-84ac-fa602a87589e method=POST path=/api/v1/notifications status=202 duration_ms=10
2026/11/20 17:13:53 INFO HTTP request correlation_id=5e748277-e6a2-4871-818f-8b7efe0137e8 method=POST path=/api/v1/notifications status=400 duration_ms=0
2026/11/20 17:13:53 INFO HTTP request correlation_id=ff512917-e00e-496a-9b08-83b3c9d5ea1e method=DELETE path=/api/v1/templates/03f569b2-57c9-471b-8faf-7105b1700b8c status=204 duration_ms=3
2026/11/20 17:13:53 INFO HTTP request correlation_id=830c02be-b564-4cf3-845f-9b3610121ceb method=GET path=/api/v1/templates/03f569b2-57c9-471b-8faf-7105b1700b8c status=404 duration_ms=0
--- PASS: TestScheduledTemplate (1.16s)
PASS
ok  	event-driven-notification-system/internal/api	2.198s
```

## 5. Prerequisites and handoff

The Compose stack needs one scheduler, an API, one worker service per channel and an external webhook URL. The CLI accepts dlq list or dlq replay with channel, limit and priority flags. Set OTEL_EXPORTER_OTLP_ENDPOINT to send distributed spans to an OTLP collector.
