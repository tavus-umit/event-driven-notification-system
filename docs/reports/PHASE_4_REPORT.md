# Phase 4 — Webhook transport and failure policy

## 1. Executive summary of completed work

An HTTP test server verified the three-field request, correlation header and classification of 202, 400, 422, 429 and 503. Tests also exercised empty acknowledgements, breaker opening and every backoff cap through 60 seconds.

### File and symbol inventory

- `go.mod`
- `go.sum`
- `internal/provider/provider.go` — `Payload`, `Accepted`, `Result`, `Client`, `New`, `Client.Deliver`, `Client.send`, `Backoff`
- `internal/provider/provider_test.go` — `TestClassification`, `TestBreakerAndBackoff`, `TestInvalidAck`
- `docs/reports/PHASE_4_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

Each pool owns a sony/gobreaker instance and a reusable HTTP transport. A permanent client error is an unsuccessful delivery but evidence the provider is responding. Redirects are disabled to preserve the configured destination and count each dispatch. A bounded response body protects memory and the audit table. The fixed notification ID is forwarded as an idempotency key for provider-side deduplication after ambiguous timeouts.

## 3. Source code walkthrough and annotated snippets

The breaker opens after consecutive failures or a sufficient failure fraction. The configuration allows three half-open probes after cooldown.

From `internal/provider/provider.go`

```go
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return int(c.ConsecutiveFailures) >= threshold || (c.Requests >= 10 && c.TotalFailures*2 >= c.Requests)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Info("provider circuit changed", "channel", name, "from", from.String(), "to", to.String())
		}})
	return &Client{url, &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, cb}
```

The zero-based retry index gives first, second and third retry caps of one, two and four seconds. The shared random generator supports concurrent callers.

From `internal/provider/provider.go`

```go
func Backoff(retry int) time.Duration {
	upper := min(time.Minute, time.Second<<min(6, max(0, retry)))
	return time.Duration(rand.Int64N(int64(upper) + 1))
}
```

## 4. Verification and testing results

The following results were recorded in WSL 2. The complete original inventory, commands, and output are preserved at `git show ba10a09:docs/reports/PHASE_4_REPORT.md`. To verify the current transport, run `go test -race -v ./internal/provider`.

Observed output (selected lines)

```text
=== RUN   TestClassification
=== RUN   TestClassification/Accepted
=== RUN   TestClassification/Bad_Request
=== RUN   TestClassification/Unprocessable_Entity
=== RUN   TestClassification/Too_Many_Requests
=== RUN   TestClassification/Service_Unavailable
--- PASS: TestClassification (0.01s)
    --- PASS: TestClassification/Accepted (0.00s)
    --- PASS: TestClassification/Bad_Request (0.00s)
    --- PASS: TestClassification/Unprocessable_Entity (0.00s)
    --- PASS: TestClassification/Too_Many_Requests (0.00s)
    --- PASS: TestClassification/Service_Unavailable (0.00s)
=== RUN   TestBreakerAndBackoff
2026/11/20 17:06:47 INFO provider circuit changed channel=email from=closed to=open
--- PASS: TestBreakerAndBackoff (0.00s)
=== RUN   TestInvalidAck
--- PASS: TestInvalidAck (0.00s)
PASS
ok  	event-driven-notification-system/internal/provider	1.021s
```

## 5. Prerequisites and handoff

The worker receives Result with provider response, retryability and breaker deferral. Acknowledged deliveries have a nonempty MessageID. Retry exhaustion is decided from the persisted count.
