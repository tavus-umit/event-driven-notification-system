# Phase 2 — Durable persistence and dispatch

## 1. Executive summary of completed work

Applied both migrations to PostgreSQL 16 and exercised the public repository operations with 1,000 notifications. The test inspected the aggregate, immutable attempt history, filtered list, duplicate receipt and cancellation conflict.

### File and symbol inventory

- `go.mod`
- `go.sum`
- `migrations/embed.go`
- `migrations/000002_dispatch_state.up.sql`
- `migrations/000002_dispatch_state.down.sql`
- `internal/repository/postgres/store.go` — `Store`, `Open`, `dbError`
- `internal/repository/postgres/notifications.go` — `insertRow`, `Store.Ingest`, `Store.Receipt`, `scanner`, `scanNotification`, `Store.Get`, `Store.Attempts`, `Store.Cancel`, `Store.List`, `Store.Batch`
- `internal/repository/postgres/dispatch.go` — `Event`, `Store.Flush`, `Store.Claim`, `Store.Finish`, `Store.Promote`, `Store.Recover`
- `internal/repository/postgres/store_test.go` — `TestBatchAndLease`
- `docs/reports/PHASE_2_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

The pgx database/sql driver exposes explicit upper bounds on idle and active connections. A JSON recordset provides one parameterized insert independent of batch size. A database trigger records each committed status transition in the outbox. A relay locks bounded groups of rows while publishing, trading one short-lived database transaction for replay after network failures. A claim owner fences stale workers. Aggregation reads indexed notification rows, avoiding contention on a shared batch counter. The 24-hour receipt controls key expiration. The old permanent unique index would prevent reuse after expiration.

## 3. Source code walkthrough and annotated snippets

PostgreSQL casts the batch in one statement. Any invalid row rolls back the batch header, receipt and every outbox row.

From `internal/repository/postgres/notifications.go`

```go
 FROM jsonb_to_recordset($1::jsonb) AS x("ID" uuid,"Batch" text,"Channel" channel_type,"Recipient" text,"Content" text,"Priority" priority_type,"Status" notification_status,"Key" text,"Template" text,"Correlation" text,"Trace" text,"Retries" int,"Scheduled" timestamptz,"Created" timestamptz,"Metadata" jsonb)`, data)
	if e != nil {
		return dbError(e)
	}
```

The owner predicate makes attempt recording and status changes conditional on the same lease. A stale finisher cannot create a second audit record.

From `internal/repository/postgres/dispatch.go`

```go
	res, e := tx.ExecContext(ctx, `UPDATE notifications SET status=$3::notification_status,provider_message_id=nullif($4,''),error_message=nullif($5,''),retry_count=retry_count+CASE WHEN $3::notification_status='RETRY_PENDING' THEN 1 ELSE 0 END,due_at=$6,claim_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_id=$2 AND status='PROCESSING'`, id, owner, status, message, a.Error, due)
	if e != nil {
		return e
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return domain.ErrConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO delivery_attempts(notification_id,attempt_number,status,http_status_code,response_body,error_reason,duration_ms,attempted_at) VALUES($1,$2,$3,nullif($4,0),$5,$6,$7,$8)`, id, a.Number, a.Status, a.HTTPStatus, a.Body, a.Error, a.DurationMS, a.At)
```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
gofmt -w internal/repository migrations/embed.go && TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' go test -v -race ./internal/repository/postgres
```

Observed output (selected lines)

```text
=== RUN   TestBatchAndLease
    store_test.go:67: 1000 rows committed, duplicate rejected, 2 terminal rows, progress 0.2%
--- PASS: TestBatchAndLease (0.16s)
PASS
ok  	event-driven-notification-system/internal/repository/postgres	1.182s
```

## 5. Prerequisites and handoff

The relay receives ordered Event values in a callback. Return nil after the whole broker batch succeeds. Failed callbacks leave the SQL outbox intact. The broker must tolerate repeated sequence numbers.
