# Phase 3 — Redis queues and distributed admission

## 1. Executive summary of completed work

Verified a 6/3/1 dequeue cycle, aging of a waiting low-priority job, duplicate outbox replay suppression, exhaustion at 100 grants and refill against Redis 7.

### File and symbol inventory

- `go.mod`
- `go.sum`
- `internal/queue/redis/broker.go` — `Broker`, `New`, `Key`, `Job`, `Lease`, `Broker.Publish`, `Broker.Pop`, `Broker.Ack`, `Broker.Quarantine`, `Broker.Due`, `Broker.Lock`
- `internal/queue/redis/broker_test.go` — `TestFairnessAndAging`
- `internal/ratelimit/limiter.go` — `Limiter`, `Permit`, `Limiter.Try`, `Limiter.Wait`, `Permit.Done`
- `internal/ratelimit/limiter_test.go` — `TestBucket`
- `internal/repository/postgres/dispatch.go` — `Event`, `Store.Flush`, `Store.Claim`, `Store.Finish`, `Store.Promote`, `Store.Recover`
- `docs/reports/PHASE_3_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

Redis TIME supplies a common clock. A capacity-100 bucket alone can grant another token 10 ms after the first 100. A second, bounded sorted set retains each reservation through one second after provider completion, accommodating the delay between admission and HTTP arrival. Long provider latency can reduce throughput. Crashed callers retain a slot for 90 seconds. A per-channel Redis cursor coordinates replicas. Atomic pop and a visibility lease protect the gap before the PostgreSQL claim. An advisory transaction lock serializes relay batches so later transitions cannot overtake earlier committed events.

## 3. Source code walkthrough and annotated snippets

Each successful pop advances the shared cursor. Empty tiers donate their slots and a leased record remains recoverable.

From `internal/queue/redis/broker.go`

```go
local slots={1,1,1,1,1,1,2,2,2,3}
local cursor=tonumber(redis.call('GET',KEYS[4]) or '0')
for step=0,9 do
 local index=(cursor+step)%10
 local tier=slots[index+1]
 local raw=redis.call('LPOP',KEYS[tier])
 if raw then
  redis.call('SET',KEYS[4],(index+1)%10)
  local member=cjson.encode({raw,tier,ARGV[1]})
  redis.call('ZADD',KEYS[5],now+90000,member)
  return {raw,member}
 end
end
return {}`)
```

Admission removes expired reservations, checks outstanding capacity, then consumes a refilled token within the same Lua invocation.

From `internal/ratelimit/limiter.go`

```go
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
if redis.call('ZCARD',KEYS[2])>=100 then
 local first=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES')
 return math.min(10000,math.max(1,tonumber(first[2])-now))
end
if tokens<1 then return math.ceil((1-tokens)*10000) end
redis.call('HSET',KEYS[1],'tokens',tokens-1,'time',string.format('%.0f',now))
redis.call('EXPIRE',KEYS[1],120)
redis.call('ZADD',KEYS[2],now+90000000,ARGV[1])
redis.call('EXPIRE',KEYS[2],120)
return 0`)
var release = r.NewScript(`
```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
gofmt -w internal/queue internal/ratelimit internal/repository/postgres/dispatch.go && TEST_REDIS_ADDR=localhost:6379 go test -v -race ./internal/queue/... ./internal/ratelimit
```

Observed output (selected lines)

```text
=== RUN   TestFairnessAndAging
    broker_test.go:68: map[high:6 low:1 normal:3]
--- PASS: TestFairnessAndAging (0.02s)
PASS
ok  	event-driven-notification-system/internal/queue/redis	1.035s
=== RUN   TestBucket
--- PASS: TestBucket (1.07s)
PASS
ok  	event-driven-notification-system/internal/ratelimit	2.082s
```

## 5. Prerequisites and handoff

The worker can Pop, Wait, dispatch, release the rate reservation and Ack. Use Quarantine for invalid payloads. The scheduler can inspect Due sorted sets while SQL remains authoritative about eligibility.
