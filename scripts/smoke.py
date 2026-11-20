"""Check delivery through the local Compose stack and its mock provider."""
import json
import time
import uuid
from urllib.request import Request, urlopen

API = "http://localhost:8080"


def request(path, payload=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Idempotency-Key"] = key
    body = json.dumps(payload).encode() if payload is not None else None
    with urlopen(Request(API + path, data=body, headers=headers), timeout=10) as response:
        return response.status, json.load(response)


def main():
    _, ready = request("/ready")
    assert ready["postgres"] and ready["redis"], ready
    items = [
        {"channel": "sms", "recipient": "+12025550123", "content": "Compose smoke SMS"},
        {"channel": "email", "recipient": "test@example.com", "content": "Compose smoke email"},
        {"channel": "push", "recipient": "a" * 32, "content": "Compose smoke push"},
    ]
    key = str(uuid.uuid4())
    path = "/api/v1/notifications/batch"
    status, receipt = request(path, {"notifications": items}, key)
    assert status == 202 and len(receipt["notificationIds"]) == 3, receipt
    _, replay = request(path, {"notifications": items}, key)
    assert replay == receipt, replay
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
