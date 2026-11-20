# Event-Driven Notification System: Architecture & Design

## 1. System Overview

The **Event-Driven Notification System** is a high-throughput, distributed notification engine engineered in Go (1.22+) to reliably process, orchestrate, and deliver millions of notifications across multiple communication channels (**SMS**, **Email**, and **Push**).

The system is designed to absorb sudden high-velocity traffic spikes (e.g., flash sales, breaking notifications, system alerts), enforce per-channel throughput constraints (100 messages/second per channel), provide intelligent retry mechanisms with exponential backoff and circuit breakers, ensure strict idempotency, and provide real-time observability and delivery tracking.

---

## 2. High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Clients["Clients & External Consumers"]
        API_Client["REST API Consumers"]
        WS_Client["WebSocket Clients"]
    end

    subgraph Ingestion["Ingestion & API Layer"]
        API_GW["API Gateway / Router (Go Chi/Gin)"]
        Auth_MW["Correlation ID & Auth Middleware"]
        Idemp_MW["Idempotency Filter (Redis Key Check)"]
        Val_MW["Content & Schema Validator"]
        Batch_Proc["Batch Ingestion Processor (up to 1000 items)"]
    end

    subgraph Storage["Persistence & Caching"]
        PG[(PostgreSQL Primary DB)]
        PG_Read[(Read Replica - Optional)]
        Redis[(Redis Cluster / Instance)]
    end

    subgraph Queue_Layer["Distributed Message Broker (Redis)"]
        PQ_SMS["SMS Queues (High / Normal / Low)"]
        PQ_EMAIL["Email Queues (High / Normal / Low)"]
        PQ_PUSH["Push Queues (High / Normal / Low)"]
        DLQ["Dead Letter Queues (DLQ)"]
        SCHED_ZSET["Delayed/Scheduled ZSET"]
    end

    subgraph Worker_Engine["Processing & Delivery Engine"]
        Scheduler["Scheduler Cron / Worker"]
        SMS_Workers["SMS Channel Workers"]
        Email_Workers["Email Channel Workers"]
        Push_Workers["Push Channel Workers"]
        
        Rate_Limiter["Distributed Token Bucket (100 msg/s per channel)"]
        Circuit_Breaker["Circuit Breaker (Failure Rate & Open/Half-Open)"]
        Retry_Manager["Retry Manager (Exponential Backoff + Full Jitter)"]
    end

    subgraph External_Integration["External Provider"]
        Webhook["External Notification Provider (Webhook)"]
    end

    subgraph Observability["Observability & Real-Time Tracking"]
        Prometheus["Prometheus Metrics (/metrics)"]
        Grafana["Grafana Dashboards"]
        Jaeger["OpenTelemetry Tracing / Jaeger"]
        WS_Hub["WebSocket Hub (Real-Time Status Broadcast)"]
    end

    %% Client Interactions
    API_Client -->|HTTP Requests| API_GW
    WS_Client <-->|WebSocket Connection| WS_Hub

    %% Ingestion Pipeline
    API_GW --> Auth_MW --> Idemp_MW --> Val_MW
    Val_MW -->|Single Request| Batch_Proc
    Val_MW -->|Batch Request (<=1000)| Batch_Proc
    
    %% Storage & Enqueue
    Batch_Proc -->|Persist Notification State| PG
    Batch_Proc -->|Enqueue by Channel & Priority| Redis
    Batch_Proc -->|Scheduled Notifications| SCHED_ZSET
    Idemp_MW <-->|Check & Set Lock/Cache| Redis

    %% Scheduler
    Scheduler -->|Poll Due Tasks| SCHED_ZSET
    Scheduler -->|Push Due Items| Redis

    %% Worker Execution
    Redis --> PQ_SMS & PQ_EMAIL & PQ_PUSH
    PQ_SMS --> SMS_Workers
    PQ_EMAIL --> Email_Workers
    PQ_PUSH --> Push_Workers

    SMS_Workers & Email_Workers & Push_Workers --> Rate_Limiter
    Rate_Limiter --> Circuit_Breaker
    Circuit_Breaker -->|HTTP POST Request| Webhook

    %% Retry & DLQ
    Circuit_Breaker -.->|Transient Failure| Retry_Manager
    Retry_Manager -.->|Re-enqueue with Backoff| Redis
    Retry_Manager -.->|Max Retries Exceeded| DLQ
    DLQ --> PG

    %% Status Updates & Events
    SMS_Workers & Email_Workers & Push_Workers -->|Update Final Status| PG
    SMS_Workers & Email_Workers & Push_Workers -->|Publish Status Event| Redis
    Redis -->|Pub/Sub Notification Event| WS_Hub
    WS_Hub -->|Push Real-time Event| WS_Client

    %% Observability Flows
    API_GW & SMS_Workers & Email_Workers & Push_Workers --> Prometheus
    API_GW & SMS_Workers & Email_Workers & Push_Workers --> Jaeger
```

---

## 3. Core Architectural Components

### 3.1. API & Ingestion Service (`cmd/server`)
- **HTTP Engine**: Lightweight, high-throughput Go HTTP router (`net/http` with `chi` or `gin`) with graceful shutdown handling.
- **Batch Processing**: Deconstructs incoming bulk requests (up to 1,000 items) into optimized database multi-row batch inserts (`COPY` or bulk `INSERT INTO ... VALUES (...)`) and pipelined Redis push operations to keep latency under 100ms.
- **Idempotency Guard**: Intercepts requests carrying an `X-Idempotency-Key` or payload key. Uses Redis distributed locks (`SETNX`) with TTL to guard against concurrent duplicate submissions and stores the response hash.
- **Validation Engine**: Channel-specific payload validator verifying recipient formats (E.164 phone numbers for SMS, RFC 5322 emails, FCM/APNS tokens for Push), payload sizes, and priority levels.

### 3.2. Message Broker & Priority Queue Architecture (`internal/queue`)
To satisfy strict priority handling and channel isolation, the queuing topology uses isolated per-channel and per-priority queues in Redis:
- **Queue Taxonomy**:
  - `queue:{channel}:high`
  - `queue:{channel}:normal`
  - `queue:{channel}:low`
  - `queue:{channel}:delayed` (Sorted Set for exponential backoff retries and scheduled deliveries)
  - `queue:{channel}:dlq` (Dead Letter Queue for exhausted or unrecoverable failures)
- **Starvation-Free Consumption**:
  Instead of strict priority polling which could starve `low` priority during burst periods, workers apply a **Weighted Fair Queue (WFQ)** pattern (e.g., 60% High, 30% Normal, 10% Low) or strict multi-pop with priority aging.

### 3.3. Processing Engine & Channel Workers (`cmd/worker`)
- **Dedicated Channel Pools**: Independent worker pools execute SMS, Email, and Push notifications. This ensures an outage or backlog in one channel (e.g., slow email SMTP/webhook) never degrades SMS or Push delivery.
- **Worker Concurrency**: Controlled goroutine pools managed with worker queues and cancellation contexts.

### 3.4. Rate Limiting Engine (`internal/ratelimit`)
- **Constraint**: Strict maximum of **100 messages per second per channel**.
- **Algorithm**: **Distributed Token Bucket** implemented via Redis Lua script (or in-memory rate limiter synchronized across instances).
- **Execution Flow**:
  1. Worker fetches a notification candidate from the priority queue.
  2. Worker queries the channel rate limiter token bucket: `TakeToken(channel, 1)`.
  3. If tokens are available, the message is dispatched immediately.
  4. If bucket is empty, the worker pauses for the exact token replenishment interval or re-schedules the message with minimal delay, guaranteeing no channel exceeds 100 calls/sec.

### 3.5. Delivery, Retry & Circuit Breaker Engine (`internal/provider`)
- **HTTP Client**: Tuned HTTP client with connection pooling, keep-alive, idle connection timeouts, and per-request timeouts (5 seconds).
- **Circuit Breaker**: Implements the Martin Fowler Circuit Breaker pattern with three states:
  - `CLOSED`: Normal operation; all requests pass through.
  - `OPEN`: Failures exceed threshold (e.g., >50% failure rate over a 10s window); requests immediately fail fast or pause without hitting provider.
  - `HALF-OPEN`: After a cooldown period (e.g., 30s), a probe request tests provider availability.
- **Exponential Backoff with Full Jitter**:
  $$t_{\text{sleep}} = \text{random}(0, \min(t_{\text{max}}, t_{\text{base}} \times 2^{\text{attempt}}))$$
- **Dead Letter Queue (DLQ)**: Captures messages exceeding maximum retry attempts (e.g., 5 attempts) or non-retryable errors (e.g., 400 Bad Request, invalid recipient address) for auditing and manual replay.

### 3.6. Bonus Systems
- **Scheduler Service (`cmd/scheduler`)**: Scans a Redis Sorted Set (`ZREVRANGEBYSCORE` with timestamp $\le$ now) and moves scheduled notifications to active priority queues.
- **Template Engine (`internal/template`)**: Supports parameterized text/HTML templates with variable interpolation (e.g., `Hello {{name}}, your code is {{code}}`).
- **WebSocket Gateway (`internal/websocket`)**: Hub managing client connections, subscribing to Redis Pub/Sub events for notification state transitions (`QUEUED`, `PROCESSING`, `DELIVERED`, `FAILED`, `CANCELLED`).
- **Distributed Tracing**: Context propagation using OpenTelemetry (W3C Trace Context) from API request across Redis message payload into worker execution and outbound HTTP client calls.

---

## 4. Architectural Decisions Records (ADRs)

### ADR 1: Redis for Priority Queuing and Rate Limiting
- **Context**: Need high-throughput queuing with strict priority (high/normal/low), sub-millisecond latency, delayed task scheduling, and distributed 100 msg/s rate limiting.
- **Decision**: Use Redis (Redis Lists, Sorted Sets, and Lua scripts) as the core queuing and coordination layer, backed by PostgreSQL as the source of truth.
- **Rationale**: Redis primitives natively support atomic atomic pops (`BLPOP`, `ZPOPMIN`), TTL-based idempotency locks, token-bucket Lua scripts, and lightweight Pub/Sub for WebSockets without the operational overhead of Kafka or RabbitMQ clusters.

### ADR 2: PostgreSQL as System of Record with Versioned Migrations
- **Context**: Need durable persistence, audit logs, filtering by status/channel/date, and pagination for millions of notifications.
- **Decision**: Store all notification states, batches, delivery attempt logs, and templates in PostgreSQL with indexed timestamp columns, composite indexes on `(status, channel, created_at)`, and `golang-migrate` for versioned migrations.
- **Rationale**: Relational integrity prevents orphaned delivery records; JSONB columns allow flexible metadata without schema sprawl; transactional guarantees ensure batch consistency.

### ADR 3: Channel-Separated Worker Pools
- **Context**: Notification channels possess fundamentally different latency profiles (Push is sub-second; Email may have slower response times; SMS can experience carrier delays).
- **Decision**: Deploy independent worker goroutine pools per channel (`worker-sms`, `worker-email`, `worker-push`).
- **Rationale**: Eliminates cross-channel head-of-line blocking. An outage in Email provider webhooks will not affect urgent SMS deliveries.

---

## 5. Technology Stack Summary

| Layer | Technology | Justification |
| :--- | :--- | :--- |
| **Language** | Go 1.22+ | Native concurrency (goroutines/channels), high throughput, low memory footprint |
| **HTTP Framework** | `go-chi/chi/v5` or `gin-gonic/gin` | Idiomatic, standard `net/http` compatibility, zero-allocation routing |
| **Database** | PostgreSQL 16 | Relational consistency, rich indexing, JSONB support, robust ACID transactions |
| **Database Migrations**| `golang-migrate/migrate` | Industry standard, versioned SQL files, forward/rollback reliability |
| **Cache & Queue Broker** | Redis 7 (Alpine) | In-memory latency, sorted sets for scheduling, Lua scripting for rate limiting |
| **Rate Limiter** | Redis Token Bucket / `golang.org/x/time/rate` | Strict 100 msg/sec per channel enforcement |
| **Circuit Breaker** | `sony/gobreaker` | Battle-tested circuit breaker implementation for Go |
| **Observability** | Prometheus (`client_golang`) | Real-time queue metrics, latency histograms, error counters |
| **Distributed Tracing** | OpenTelemetry (`go.opentelemetry.io/otel`) | End-to-end W3C trace propagation through queues and HTTP calls |
| **Containerization** | Docker & Docker Compose | Single-command local environment orchestration |
