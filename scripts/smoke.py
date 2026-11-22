"""Check delivery through Compose, or verify scheduled cancellation without sending."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import time
import uuid
from urllib.request import Request, urlopen

API = "http://localhost:8080"


def request(path, payload=None, key=None, method=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Idempotency-Key"] = key
    body = json.dumps(payload).encode() if payload is not None else None
    with urlopen(Request(API + path, data=body, headers=headers, method=method), timeout=10) as response:
        return response.status, json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cancel-scheduled", action="store_true", help="schedule and cancel the batch before delivery")
    args = parser.parse_args()
    _, ready = request("/ready")
    assert ready["postgres"] and ready["redis"], ready
    items = [
        {"channel": "sms", "recipient": "+12025550123", "content": "Compose smoke SMS"},
        {"channel": "email", "recipient": "test@example.com", "content": "Compose smoke email"},
        {"channel": "push", "recipient": "a" * 32, "content": "Compose smoke push"},
    ]
    key = str(uuid.uuid4())
    if args.cancel_scheduled:
        due = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        for item in items:
            item["scheduledAt"] = due
    path = "/api/v1/notifications/batch"
    status, receipt = request(path, {"notifications": items}, key)
    assert status == 202 and len(receipt["notificationIds"]) == 3, receipt
    _, replay = request(path, {"notifications": items}, key)
    assert replay == receipt, replay
    if args.cancel_scheduled:
        for notification_id in receipt["notificationIds"]:
            item_path = "/api/v1/notifications/" + notification_id
            _, scheduled = request(item_path)
            assert scheduled["status"] == "SCHEDULED", scheduled
            request(item_path + "/cancel", method="POST")
            _, cancelled = request(item_path)
            assert cancelled["status"] == "CANCELLED" and not cancelled["deliveryAttempts"], cancelled
        _, batch = request("/api/v1/notifications/batches/" + receipt["batchId"])
        assert batch["breakdown"]["cancelled"] == 3 and batch["progressPercentage"] == 100, batch
        print("All 3 scheduled notifications were cancelled before delivery. Replay returned the original IDs.")
        return
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        _, batch = request("/api/v1/notifications/batches/" + receipt["batchId"])
        if batch["status"] == "COMPLETED":
            break
        time.sleep(0.2)
    assert batch["breakdown"]["delivered"] == 3 and batch["progressPercentage"] == 100, batch
    for notification_id in receipt["notificationIds"]:
        _, notification = request("/api/v1/notifications/" + notification_id)
        assert notification["providerMessageId"], notification
        assert [a["httpStatusCode"] for a in notification["deliveryAttempts"]] == [202], notification
    print(f"Batch {receipt['batchId']} delivered all 3 channels. Replay returned the original IDs.")


if __name__ == "__main__":
    main()
