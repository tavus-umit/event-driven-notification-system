# Comprehensive Testing Strategy

## 1. Testing Philosophy & Test Pyramid

The testing strategy guarantees that high-volume message ingestion, rate-limiting constraints, priority ordering, and intelligent retries function flawlessly under burst traffic.

```
                   / \
                  /   \     E2E / Load Tests (k6, Vegetta, Burst Simulation)
                 /-----\
                /       \    Integration Tests (Postgres, Redis, Mock Provider)
               /---------\
              /           \  Unit Tests (Validation, Backoff Math, Token Bucket, Models)
             /-------------\
```

---

## 2. Test Execution Commands

The entire test suite can be executed with a single command:

```bash
# Execute full test suite with race detector enabled
go test -v -race ./...

# Run tests with code coverage analysis
go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -html=coverage.out -o coverage.html
```

---

## 3. Test Suites Breakdown

### 3.1. Unit Tests
- **Location**: `*_test.go` placed adjacent to source code.
- **Coverage Focus**:
  - **Payload Validators**: E.164 phone formats (`+905551234567`), email formats, push tokens, GSM-7/Unicode character limits.
  - **Backoff Math**: Validates that calculated backoff sleep matches exponential progression:
    $$t_{\text{sleep}} \le \min(t_{\text{max}}, t_{\text{base}} \times 2^{\text{attempt}})$$
  - **Token Bucket Limiter**: Verifies token depletion and token replenishment over time.
  - **Template Renderer**: Verifies `{{var}}` replacement and handles missing variables.
  - **Error Classifier**: Correctly categorizes HTTP status codes into `Transient` vs `Permanent`.

### 3.2. Integration Tests
- **Database Operations**:
  - Tests atomic bulk ingestion of 1,000 notifications in a single transaction.
  - Tests pagination, status filtering, and date range query performance.
  - Tests idempotency key conflict handling (duplicate key returns cached result).
- **Queue Operations**:
  - Tests push and pop across `high`, `normal`, and `low` Redis priority queues.
  - Verifies that high priority notifications are dequeued ahead of lower priority items.
  - Tests scheduled notification promotion using Redis Sorted Sets.

### 3.3. End-to-End (E2E) & Behavioral Verification Tests

#### Test Scenario A: Rate Limit Enforcement (100 msg/sec per channel)
- **Objective**: Verify that regardless of queue size, no channel exceeds 100 HTTP calls per second to the provider.
- **Methodology**:
  1. Enqueue 500 SMS notifications into Redis.
  2. Start SMS channel worker pool with 10 concurrent goroutines.
  3. Record timestamps of incoming HTTP requests at the mock provider.
  4. Group request timestamps into 1-second rolling buckets.
  5. **Pass Criteria**: Every 1-second window contains $\le 100$ requests.

#### Test Scenario B: Priority Queue Dispatching
- **Objective**: Verify that high priority messages overtake existing normal and low priority backlogs.
- **Methodology**:
  1. Enqueue 100 `low` priority messages into `queue:sms:low`.
  2. Enqueue 5 `high` priority messages into `queue:sms:high`.
  3. Start worker.
  4. Inspect the execution order recorded in `delivery_attempts`.
  5. **Pass Criteria**: All 5 `high` priority notifications are processed and delivered before the remaining low priority messages.

#### Test Scenario C: Transient Failure, Exponential Backoff & DLQ
- **Objective**: Verify automatic retries upon receiving HTTP 429 or 500, followed by DLQ transfer upon exhaustion.
- **Methodology**:
  1. Configure mock provider to return HTTP 500 for a specific recipient.
  2. Enqueue notification with `max_retries = 3`.
  3. Assert worker receives HTTP 500, schedules retry with exponential backoff delay.
  4. Allow backoff timer to expire for all 3 retries.
  5. **Pass Criteria**:
     - Exactly 4 total attempts (1 initial + 3 retries) are logged in `delivery_attempts`.
     - Final notification status in PostgreSQL is `FAILED`.
     - Message is present in `queue:{channel}:dlq`.

#### Test Scenario D: Circuit Breaker Activation
- **Objective**: Protect system and downstream provider during an outage.
- **Methodology**:
  1. Configure mock provider to drop connections / return 503 for all requests.
  2. Submit 15 notifications.
  3. **Pass Criteria**:
     - After 5 consecutive failures, circuit breaker transitions to `OPEN`.
     - Subsequent requests fail fast or queue into delayed storage without invoking outbound HTTP requests.

---

## 4. Performance & Load Benchmark Script

A load-testing script using Go benchmarks or k6 simulates production burst conditions:

```go
// Sample Load Test Benchmark
func BenchmarkBatchIngestion(b *testing.B) {
    client := NewAPIClient("http://localhost:8080")
    batch := GenerateTestBatch(1000) // 1000 valid notification items

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        resp, err := client.CreateBatch(batch)
        if err != nil || resp.StatusCode != http.StatusAccepted {
            b.Fatalf("Batch ingestion failed: %v", err)
        }
    }
}
```
