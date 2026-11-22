# API Specification & Provider Integration Guide

## 1. Overview & Conventions

The Event-Driven Notification System exposes a RESTful JSON API following OpenAPI 3.0 standards.

### 1.1. Base URL & Versioning
- **Base URL**: `http://localhost:8080/api/v1`
- **Health / Metrics**: `http://localhost:8080/health`, `http://localhost:8080/metrics`

### 1.2. Standard Request Headers
- `Content-Type: application/json`
- `X-Correlation-ID: <uuid>` *(Optional: generated automatically if omitted)*
- `X-Idempotency-Key: <unique-key>` *(Optional: prevents duplicate execution across network retries)*

### 1.3. Standard Error Format (RFC 7807)
All 4xx and 5xx responses return a standardized error object:
```json
{
  "type": "https://api.notifications.internal/errors/validation-failed",
  "title": "Validation Failed",
  "status": 400,
  "detail": "Recipient phone number is invalid for SMS channel",
  "instance": "/api/v1/notifications",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-11-20T14:30:00Z"
}
```

---

## 2. API Endpoints

### 2.1. Create Single Notification
- **Method & Path**: `POST /api/v1/notifications`
- **Description**: Submits an individual notification for asynchronous delivery.
- **Request Body**:
```json
{
  "recipient": "+905551234567",
  "channel": "sms",
  "content": "Your verification code is 849201.",
  "priority": "high",
  "idempotencyKey": "order-10042-sms-code",
  "scheduledAt": null,
  "metadata": {
    "userId": "usr_99182",
    "eventType": "AUTH_2FA"
  }
}
```
- **Responses**:
  - `202 Accepted`:
```json
{
  "id": "c1f7a04a-81a1-432d-8628-912f71694b8e",
  "status": "QUEUED",
  "channel": "sms",
  "recipient": "+905551234567",
  "priority": "high",
  "createdAt": "2026-11-20T14:30:00Z"
}
```
  - `400 Bad Request`: Validation failure (e.g., recipient format, content length limit exceeded).
  - `409 Conflict`: Idempotency key already processed with a conflicting payload.

---

### 2.2. Create Batch Notifications
- **Method & Path**: `POST /api/v1/notifications/batch`
- **Description**: Accepts up to **1,000 notifications** in a single atomic/bulk ingestion request.
- **Constraints**: Maximum 1,000 items per batch payload.
- **Request Body**:
```json
{
  "batchId": "optional-custom-batch-key-123",
  "notifications": [
    {
      "recipient": "+905551234567",
      "channel": "sms",
      "content": "Flash Sale starts in 10 minutes!",
      "priority": "high"
    },
    {
      "recipient": "customer@example.com",
      "channel": "email",
      "content": "Your exclusive 20% discount coupon is READY2026",
      "priority": "normal"
    },
    {
      "recipient": "fcm_token_abc123xyz789",
      "channel": "push",
      "content": "Flash Sale is LIVE!",
      "priority": "high"
    }
  ]
}
```
- **Responses**:
  - `202 Accepted`:
```json
{
  "batchId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "totalReceived": 3,
  "status": "PROCESSING",
  "notificationIds": [
    "c1f7a04a-81a1-432d-8628-912f71694b8e",
    "8e44122d-6211-4a57-b088-7245bf94a112",
    "d018a38c-84d9-482f-8703-9b2f6381ab92"
  ],
  "createdAt": "2026-11-20T14:30:00Z"
}
```
  - `400 Bad Request`: Payload contains 0 or $> 1,000$ notifications or invalid schema.

---

### 2.3. Query Notification Status
- **Method & Path**: `GET /api/v1/notifications/{id}`
- **Description**: Returns detailed real-time state and delivery attempt history for a specific notification.
- **Responses**:
  - `200 OK`:
```json
{
  "id": "c1f7a04a-81a1-432d-8628-912f71694b8e",
  "batchId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "channel": "sms",
  "recipient": "+905551234567",
  "content": "Your verification code is 849201.",
  "priority": "high",
  "status": "DELIVERED",
  "retryCount": 1,
  "maxRetries": 3,
  "providerMessageId": "prov_msg_7781a",
  "createdAt": "2026-11-20T14:30:00Z",
  "updatedAt": "2026-11-20T14:30:02Z",
  "deliveryAttempts": [
    {
      "attemptNumber": 1,
      "status": "FAILURE",
      "httpStatusCode": 429,
      "errorReason": "Provider rate limit reached",
      "durationMs": 42,
      "attemptedAt": "2026-11-20T14:30:01Z"
    },
    {
      "attemptNumber": 2,
      "status": "SUCCESS",
      "httpStatusCode": 202,
      "responseBody": "{\"messageId\": \"prov_msg_7781a\", \"status\": \"accepted\"}",
      "durationMs": 55,
      "attemptedAt": "2026-11-20T14:30:02Z"
    }
  ]
}
```
  - `404 Not Found`: Notification ID does not exist.

---

### 2.4. Query Batch Status
- **Method & Path**: `GET /api/v1/notifications/batches/{batch_id}`
- **Description**: Aggregates delivery metrics for a batch.
- **Responses**:
  - `200 OK`:
```json
{
  "batchId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "totalCount": 1000,
  "status": "PROCESSING",
  "breakdown": {
    "pending": 0,
    "queued": 120,
    "processing": 50,
    "delivered": 810,
    "failed": 20,
    "cancelled": 0
  },
  "progressPercentage": 83.0,
  "createdAt": "2026-11-20T14:30:00Z",
  "updatedAt": "2026-11-20T14:30:15Z"
}
```
  - `404 Not Found`: Batch ID not found.

---

### 2.5. Cancel Pending Notification
- **Method & Path**: `POST /api/v1/notifications/{id}/cancel`
- **Description**: Cancels a notification that is currently in `PENDING`, `SCHEDULED`, or `QUEUED` state.
- **Responses**:
  - `200 OK`:
```json
{
  "id": "c1f7a04a-81a1-432d-8628-912f71694b8e",
  "status": "CANCELLED",
  "cancelledAt": "2026-11-20T14:30:01Z"
}
```
  - `409 Conflict`: Cannot cancel; notification is already `PROCESSING` or has reached terminal state (`DELIVERED` or `FAILED`).

---

### 2.6. List Notifications with Filtering & Pagination
- **Method & Path**: `GET /api/v1/notifications`
- **Query Parameters**:
  - `status`: comma-separated string (e.g. `DELIVERED,FAILED`)
  - `channel`: string (`sms`, `email`, `push`)
  - `priority`: string (`high`, `normal`, `low`)
  - `date_from`: ISO-8601 string (e.g. `2026-11-20T00:00:00Z`)
  - `date_to`: ISO-8601 string (e.g. `2026-11-20T23:59:59Z`)
  - `page`: integer (default `1`)
  - `limit`: integer (default `20`, max `100`)
- **Responses**:
  - `200 OK`:
```json
{
  "data": [
    {
      "id": "c1f7a04a-81a1-432d-8628-912f71694b8e",
      "channel": "sms",
      "recipient": "+905551234567",
      "priority": "high",
      "status": "DELIVERED",
      "createdAt": "2026-11-20T14:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "totalCount": 1845,
    "totalPages": 93
  }
}
```

---

### 2.7. Bonus: Template Management Endpoints
- `POST /api/v1/templates`: Create message template with variables (`{{name}}`, `{{code}}`).
- `GET /api/v1/templates`: List available templates.

---

### 2.8. Bonus: Real-Time WebSocket Updates
- **Path**: `GET /api/v1/ws/notifications`
- **Protocol**: WebSocket (RFC 6455)
- **Query Parameters**:
  - `batch_id`: Optional filter to stream updates for a specific batch.
  - `channel`: Optional filter by channel.
- **Stream Message Format**:
```json
{
  "event": "NOTIFICATION_STATUS_CHANGED",
  "notificationId": "c1f7a04a-81a1-432d-8628-912f71694b8e",
  "batchId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "channel": "sms",
  "recipient": "+905551234567",
  "previousStatus": "PROCESSING",
  "currentStatus": "DELIVERED",
  "timestamp": "2026-11-20T14:30:02Z"
}
```

---

## 3. External Provider Webhook Integration

Workers send notification payloads to the external `PROVIDER_WEBHOOK_URL` specified in `.env`. For the assessment, configure Webhook.site to return HTTP 202 with the acknowledgement below.

### 3.1. Outbound Request Format
- **Method & URL**: `POST https://webhook.site/{your-uuid}`
- **Headers**: `Content-Type: application/json`
- **Payload**:
```json
{
  "to": "+905551234567",
  "channel": "sms",
  "content": "Your message"
}
```

### 3.2. Expected Provider Response
- **Status Code**: `202 Accepted`
- **Response Body**:
```json
{
  "messageId": "uuid-here",
  "status": "accepted",
  "timestamp": "ISO8601"
}
```

### 3.3. Provider Response Mapping Table
| HTTP Status | Interpretation | System Action |
| :--- | :--- | :--- |
| `202 Accepted` | Delivery accepted by provider | Transition status to `DELIVERED`, record attempt. |
| `429 Too Many Requests` | Provider rate limit exceeded | Record attempt, trigger exponential backoff retry. |
| `500 / 502 / 503 / 504` | Provider transient server error | Record attempt, trigger exponential backoff retry; update circuit breaker. |
| `400 Bad Request` | Malformed payload / invalid address | Terminal failure: transition to `FAILED`, move to DLQ. |
| `Timeout / Connection Refused` | Network partition / failure | Record attempt, trigger exponential backoff retry. |
