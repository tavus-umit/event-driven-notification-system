# Phase 6 — REST API, WebSockets and telemetry

## 1. Executive summary of completed work

Exercised HTTP creation, replay, payload conflict, cancellation, status, listing, readiness, health, metrics and a filtered WebSocket subscription. Eight concurrent submissions received one notification ID. The same key with changed content returned 409.

### File and symbol inventory

- `go.mod`
- `go.sum`
- `internal/api/api.go` — `API`, `API.Router`, `requestContext`, `jsonResponse`, `problem`, `failure`, `API.health`, `resourceID`, `API.detail`, `API.batch`, `API.cancel`, `filter`, `API.list`
- `internal/api/ingest.go` — `CreateRequest`, `batchRequest`, `decode`, `API.create`, `API.replay`, `API.guard`
- `internal/api/websocket/hub.go` — `subscriber`, `Hub`, `New`, `Hub.Run`, `Hub.ServeHTTP`
- `internal/api/api_test.go` — `TestAPIReplayAndStream`
- `internal/observe/observe.go` — `Metrics`, `Tracing`, `Serve`
- `internal/app/app.go` — `Services`, `Main`, `start`, `Wait`, `Services.Relay`
- `internal/worker/worker.go` — `Sender`, `Pool`, `Pool.Run`, `Pool.process`, `Pool.quarantine`, `Pool.handle`, `outcome`, `Pool.Retries`
- `internal/ratelimit/limiter.go` — `Limiter`, `Permit`, `Limiter.Try`, `Limiter.Wait`, `Permit.Done`
- `cmd/worker/main.go` — `main`, `run`
- `cmd/server/main.go` — `main`, `run`
- `docs/reports/PHASE_6_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

Chi preserves net/http interoperability and WebSocket hijacking. Admission hashes the endpoint and typed request and persists the 202 response with the batch. A Redis ownership token reduces duplicate work, while the SQL receipt resolves the race if the lock expires. Bounded WebSocket subscriber queues disconnect lagging clients. The API and worker expose process-local Prometheus metrics. PostgreSQL and Redis probes have bounded deadlines. TraceContext propagates W3C parent spans and an optional OTLP/gRPC exporter batches spans.

## 3. Source code walkthrough and annotated snippets

Replay reads the original committed response and checks the request hash before returning success.

From `internal/api/ingest.go`

```go
func (a *API) replay(w http.ResponseWriter, r *http.Request, want *domain.Receipt) bool {
	got, e := a.S.Store.Receipt(r.Context(), want.Key)
	if errors.Is(e, domain.ErrNotFound) {
		return false
	}
	if e != nil {
		failure(w, r, e)
		return true
	}
	if want.Hash != got.Hash {
		problem(w, r, 409, "idempotency key already used for a different payload")
		return true
	}
	jsonResponse(w, got.Code, got.Body)
	return true
```

A full subscriber buffer disconnects that client, preserving delivery to the other subscribers.

From `internal/api/websocket/hub.go`

```go
					case c.send <- data:
					default:
						delete(h.clients, c)
						close(c.send)
					}
				}
			}
			h.mu.Unlock()
```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
gofmt -w cmd internal && TEST_DATABASE_URL='postgres://notification_user:notification_secret@localhost:5432/notification_phase_tests?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test -v -race ./internal/api/... ./cmd/server ./cmd/worker
```

Observed output (selected lines)

```text
=== RUN   TestAPIReplayAndStream
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=84
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=86
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=85
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=91
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=88
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=86
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=94
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=202 duration_ms=93
2026/11/20 17:11:56 INFO HTTP request correlation_id=8b612e86-a37c-43b5-8dc4-95fc8d9c45cc method=POST path=/api/v1/notifications status=409 duration_ms=0
2026/11/20 17:11:56 INFO HTTP request correlation_id=b692de5b-be1c-414a-b1e5-e30c6b59485b method=GET path=/api/v1/notifications/71fa63a5-ee24-4975-8876-9365568fea7c status=200 duration_ms=4
2026/11/20 17:11:56 INFO HTTP request correlation_id=b2f52eee-8d9e-4360-a46c-fc3f37e6932d method=GET path=/api/v1/notifications status=200 duration_ms=4
2026/11/20 17:11:56 INFO HTTP request correlation_id=5e42076a-8e4a-431a-8e3f-2f123c822bf9 method=GET path=/health status=200 duration_ms=0
2026/11/20 17:11:56 INFO HTTP request correlation_id=c9384cf1-392c-48bc-9c77-a02d05c39c00 method=GET path=/ready status=200 duration_ms=0
2026/11/20 17:11:56 INFO HTTP request correlation_id=cf17b140-098f-4a04-8f2d-92404da42652 method=GET path=/metrics status=200 duration_ms=9
2026/11/20 17:11:56 INFO HTTP request correlation_id=c0e8a16a-59fc-4f89-9e45-4fe08b135548 method=POST path=/api/v1/notifications/71fa63a5-ee24-4975-8876-9365568fea7c/cancel status=200 duration_ms=4
    api_test.go:103: 8 concurrent creates replayed one UUID, payload conflict returned 409, WebSocket received QUEUED
2026/11/20 17:11:56 INFO HTTP request correlation_id=8e53c1a0-128f-4918-ae12-0a15b8f4cfc0 method=GET path=/api/v1/ws/notifications status=101 duration_ms=152
--- PASS: TestAPIReplayAndStream (0.72s)
PASS
ok  	event-driven-notification-system/internal/api	1.768s
?   	event-driven-notification-system/internal/api/websocket	[no test files]
?   	event-driven-notification-system/cmd/server	[no test files]
?   	event-driven-notification-system/cmd/worker	[no test files]
```

## 5. Prerequisites and handoff

Templates can resolve a CreateRequest before domain validation. The scheduler can reuse Services and the transactional relay. Metrics require scraping each worker in addition to the API. WebSocket streams report live changes and the REST endpoint supplies current state after reconnect.
