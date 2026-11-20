# Developer Guide & Setup Instructions

## 1. Prerequisites

Ensure you have the following installed on your machine:
- **Go 1.22+** (Verify with `go version`)
- **Docker & Docker Compose** (Verify with `docker compose version`)
- **Make** (or run equivalent commands directly in terminal/PowerShell)
- **Curl** or **Postman / Bruno** for API testing

---

## 2. Standard Project Directory Structure

The project follows the standard enterprise Go architecture:

```
event-driven-notification-system/
├── cmd/
│   ├── server/                # REST API & WebSocket server entrypoint
│   │   └── main.go
│   ├── worker/                # Background channel worker daemon entrypoint
│   │   └── main.go
│   ├── scheduler/             # Scheduled notification scanner
│   │   └── main.go
│   └── mock-provider/         # Local webhook.site provider simulator
│       └── main.go
├── internal/
│   ├── api/                   # HTTP handlers, routing, middleware, DTOs
│   │   ├── handler/           # Notification, batch, template, cancel handlers
│   │   ├── middleware/        # Correlation ID, idempotency, logging, recovery
│   │   └── websocket/         # Real-time WebSocket hub & client connection pool
│   ├── config/                # Environment variables & configuration loading
│   ├── domain/                # Entities, Enums, Value Objects & Interfaces
│   │   ├── notification.go
│   │   ├── batch.go
│   │   ├── attempt.go
│   │   └── template.go
│   ├── metrics/               # Prometheus metrics instrumentation
│   ├── provider/              # HTTP webhook client, circuit breaker, retry logic
│   ├── queue/                 # Redis priority queues & delayed tasks
│   │   ├── publisher.go
│   │   └── consumer.go
│   ├── ratelimit/             # Distributed token bucket (100 msg/s/channel)
│   ├── repository/            # PostgreSQL database access layer
│   │   └── postgres/
│   ├── service/               # Application business logic
│   └── template/              # Template parsing & variable substitution
├── migrations/                # Versioned SQL migrations (000001_init_schema.up.sql)
├── pkg/
│   ├── logger/                # Structured slog JSON logger
│   └── validator/             # Channel format validators (E.164, RFC 5322, etc.)
├── test/                      # Integration and E2E test scripts
├── docs/                      # Architectural and technical documentation
├── .env.example               # Example environment variable file
├── Dockerfile                 # Multi-stage production container build
├── docker-compose.yml         # One-command local setup for all services
├── Makefile                   # Developer productivity commands
└── README.md                  # Project root README
```

---

## 3. Environment Configuration

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

### Key Environment Variables:
```env
# Server Configuration
SERVER_PORT=8080
SERVER_READ_TIMEOUT=10s
SERVER_WRITE_TIMEOUT=10s
ENVIRONMENT=development
LOG_LEVEL=info

# Database Configuration (PostgreSQL)
DB_HOST=localhost
DB_PORT=5432
DB_USER=notification_user
DB_PASSWORD=notification_secret
DB_NAME=notification_db
DB_SSLMODE=disable
DB_MAX_OPEN_CONNS=50
DB_MAX_IDLE_CONNS=25

# Message Broker & Cache (Redis)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# Channel Rate Limits (Messages per second)
RATE_LIMIT_SMS=100
RATE_LIMIT_EMAIL=100
RATE_LIMIT_PUSH=100

# Provider Webhook Configuration
# Point to your webhook.site URL or use the local mock provider:
PROVIDER_WEBHOOK_URL=http://localhost:8081/webhook
PROVIDER_TIMEOUT_SECONDS=5
MAX_RETRIES=3
CIRCUIT_BREAKER_FAIL_THRESHOLD=5
```

---

## 4. Quick Start: Local Development

### Option A: One-Command Setup with Docker Compose (Recommended)
Launch the entire system (PostgreSQL, Redis, API Server, Worker Pool, Mock Webhook Provider, and Prometheus) with a single command:

```bash
docker-compose up --build
```

- **REST API**: Accessible at `http://localhost:8080`
- **Mock Provider**: Running at `http://localhost:8081`
- **Prometheus Metrics**: `http://localhost:8080/metrics`
- **Health Check**: `http://localhost:8080/health`

---

### Option B: Running Bare-Metal / Native Go

1. **Start Core Dependencies (Postgres & Redis)**:
   ```bash
   docker-compose up -d postgres redis
   ```

2. **Run Database Migrations**:
   ```bash
   make migrate-up
   # Or using golang-migrate CLI directly:
   migrate -path migrations -database "postgres://notification_user:notification_secret@localhost:5432/notification_db?sslmode=disable" up
   ```

3. **Start the Mock External Provider (Terminal 1)**:
   ```bash
   go run ./cmd/mock-provider
   ```

4. **Start the API Server (Terminal 2)**:
   ```bash
   go run ./cmd/server
   ```

5. **Start Channel Workers (Terminal 3)**:
   ```bash
   go run ./cmd/worker
   ```

---

## 5. Helpful Makefile Targets

```makefile
# Run all tests (unit + integration)
make test

# Run tests with race detection and code coverage
make test-coverage

# Execute code formatting and linter
make lint

# Run database migrations up
make migrate-up

# Rollback last migration
make migrate-down

# Build Docker images
make docker-build

# Start all services via docker-compose
make up

# Stop all services
make down
```

---

## 6. Testing the API Interactively

### 1. Send Single SMS Notification
```bash
curl -i -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: test-sms-001" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "content": "Hello! Your verification code is 123456.",
    "priority": "high"
  }'
```

### 2. Send Batch of Notifications (up to 1,000)
```bash
curl -i -X POST http://localhost:8080/api/v1/notifications/batch \
  -H "Content-Type: application/json" \
  -d '{
    "notifications": [
      {
        "recipient": "+905551234567",
        "channel": "sms",
        "content": "Flash sale starts now!",
        "priority": "high"
      },
      {
        "recipient": "user@example.com",
        "channel": "email",
        "content": "Check out today special deals",
        "priority": "normal"
      }
    ]
  }'
```

### 3. Check Notification Status
```bash
curl -i -X GET http://localhost:8080/api/v1/notifications/{NOTIFICATION_ID}
```

### 4. Query Real-Time Metrics
```bash
curl -i http://localhost:8080/metrics
```
