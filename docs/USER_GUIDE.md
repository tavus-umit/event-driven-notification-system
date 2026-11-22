# How to use the notification system on Windows

Start with **1–3** to see a message go from your keyboard to the local provider. Use **4–8** to try the other ways to send and track messages. **9–12** cover monitoring, recovery, testing, and connecting your own provider. **13** stops the system.

Use **PowerShell**. Copy one block at a time into the **same window** so the next block can use the IDs from the last one. The examples send demo data to the Webhook.site capture URL in your local `.env`. Configure its HTTP `202` response as shown in [step 12](#use-webhooksite-as-the-provider-in-the-assessment) before sending. Webhook.site records the POST and returns an acknowledgement. Your phone and inbox will stay quiet.

| I want to… | Go here |
| --- | --- |
| Start the app and send my first message | [Start Docker](#1-start-the-system-and-check-that-it-is-ready) · [Check all channels](#2-run-one-automatic-check-across-all-three-channels) · [Send one SMS](#3-send-one-sms-and-see-what-happened-to-it) |
| Avoid duplicates, send in bulk, or look up an ID | [Repeat safely](#4-repeat-a-request-without-sending-a-second-message) · [Send a batch](#5-send-a-batch-with-sms-email-and-push) · [Search messages](#6-find-messages-by-status-channel-priority-or-date) |
| Send later or reuse wording | [Schedule and cancel](#7-schedule-a-message-then-practice-cancelling-another-one) · [Use templates](#8-create-use-edit-and-delete-a-template) |
| Watch delivery or investigate a failure | [Live updates and metrics](#9-watch-live-updates-logs-and-metrics) · [Recover a failure](#10-practice-a-failed-delivery-and-recover-it) |
| Verify the limits, connect a provider, or shut down | [Run the full tests](#11-prove-that-retries-priority-and-the-rate-limit-work) · [Connect a provider](#12-connect-a-real-provider-or-tracing-collector-optional) · [Stop](#13-stop-now-and-come-back-later) |

## 1. Start the system and check that it is ready

Open Docker Desktop and wait until its engine is running. Then open PowerShell in the repository folder. On this PC you can get there with the first line below. On another PC, use the folder where you cloned the project.

```powershell
Set-Location "$env:USERPROFILE\Desktop\event-driven-notification-system"
docker compose up -d --build --wait
docker compose ps --all
```

**OK if** you see eight running services. PostgreSQL, Redis, the API, and the three channel workers should say `healthy`. The scheduler and Prometheus only need to say `Up`.

**If a service exits or keeps restarting**, run `docker compose logs --tail=50` followed by the service name, for example `docker compose logs --tail=50 api`. If Docker says a port is already allocated, check which app or other Compose project is using the port it names.

```powershell
$root = 'http://localhost:8080'
$api = "$root/api/v1"
Invoke-RestMethod "$root/ready"
docker compose exec -T worker-sms printenv PROVIDER_WEBHOOK_URL
```

**OK if** `healthy`, `postgres`, and `redis` are all `True`, and the SMS worker prints the Webhook.site capture URL from your `.env`. All three workers use this address. If you see a different URL, follow step 12B.

**If you get a connection error**, check that `api` is running. For HTTP `503`, check `docker compose logs --tail=50 api postgres redis`. The same dependency check is available at `/health`.

## 2. Run one automatic check across all three channels

```powershell
python scripts/smoke.py
```

**OK if** the final line reads `Batch <id> delivered all 3 channels. Replay returned the original IDs.` The script submits SMS, email, and push, repeats the request, waits for delivery, and checks for exactly one HTTP `202` provider attempt per message.

**If Python is missing**, try `py -3 scripts/smoke.py`. You can also do the checks by hand in steps 3–5. If the script times out, check `docker compose logs --tail=50 worker-sms worker-email worker-push`.

## 3. Send one SMS and see what happened to it

Paste these two helpers once. The first sends JSON. The second checks the status once a second until the message finishes. Defining them prints nothing.

```powershell
function Send-Json($path, $body, $headers = @{}, $method = 'Post') {
    $json = $body | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Method $method -Uri "$api/$path" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body $json
}

function Get-FinalNotification([string]$id, [int]$seconds = 60) {
    $deadline = [datetime]::UtcNow.AddSeconds($seconds)
    do {
        $item = Invoke-RestMethod "$api/notifications/$id"
        if ($item.status -in @('DELIVERED', 'FAILED', 'CANCELLED')) { return $item }
        Start-Sleep -Seconds 1
    } while ([datetime]::UtcNow -lt $deadline)
    throw "Still $($item.status) after $seconds seconds. Notification ID is $id."
}
```

Send a test message. Keep both IDs. The idempotency key lets you safely repeat this request. The correlation ID lets you find its logs.

```powershell
$sms = @{ channel = 'sms'; recipient = '+12025550123'; content = 'My first test message'; priority = 'high' }
$correlation = [guid]::NewGuid().ToString()
$headers = @{ 'X-Idempotency-Key' = [guid]::NewGuid().ToString(); 'X-Correlation-ID' = $correlation }
$single = Send-Json 'notifications' $sms $headers
$single | Format-List id, status, channel, priority
```

**OK if** you get an `id` and `QUEUED`. That receipt means the API accepted the work. To check whether the provider accepted the message, run this next.

```powershell
$detail = Get-FinalNotification $single.id
$detail | Format-List id, status, providerMessageId, retryCount
$detail.deliveryAttempts | Format-Table attemptNumber, status, httpStatusCode, durationMs, errorReason
```

**OK if** you see `DELIVERED`, a `providerMessageId`, and one `SUCCESS` attempt with code `202`. **If you see `FAILED`**, read `errorReason` and `httpStatusCode` in the table. **If it stays `QUEUED`**, check `docker compose logs --tail=50 worker-sms`.

## 4. Repeat a request without sending a second message

```powershell
$replay = Send-Json 'notifications' $sms $headers
$replay.id -eq $single.id
(Get-FinalNotification $replay.id).deliveryAttempts.Count
```

**OK if** you see `True` and `1`. Reusing the same key and payload within 24 hours returns the original receipt. Use the status lookup from step 3 for the current result. To deliberately send a new message, generate a fresh key.

To see what happens if you accidentally reuse a key for different content, run this once.

```powershell
$changed = $sms.Clone()
$changed.content = 'Different message with the old key'
try {
    Send-Json 'notifications' $changed $headers
} catch {
    [int]$_.Exception.Response.StatusCode
}
```

**OK if** the number is `409`. The API rejected the conflicting request. **If it creates a new ID**, check whether `$headers` still contains the key from step 3.

## 5. Send a batch with SMS, email, and push

```powershell
$items = @(
    @{ channel = 'sms'; recipient = '+12025550123'; content = 'Your order is ready'; priority = 'high' }
    @{ channel = 'email'; recipient = 'reader@example.com'; content = 'Your order receipt'; priority = 'normal'; metadata = @{ subject = 'Receipt' } }
    @{ channel = 'push'; recipient = ('a' * 32); content = 'View your order'; priority = 'low'; metadata = @{ title = 'Order update' } }
)
$batchHeaders = @{ 'X-Idempotency-Key' = [guid]::NewGuid().ToString() }
$batch = Send-Json 'notifications/batch' @{ notifications = $items } $batchHeaders
$batch | Format-List batchId, totalReceived, notificationIds
foreach ($id in $batch.notificationIds) {
    Get-FinalNotification $id | Format-List channel, status, providerMessageId
}
$progress = Invoke-RestMethod "$api/notifications/batches/$($batch.batchId)"
$progress | Format-List status, totalCount, progressPercentage
$progress.breakdown | Format-List
```

**OK if** you get three IDs, all three channels say `DELIVERED`, the batch says `COMPLETED`, progress is `100`, and `delivered` is `3`. Progress counts every finished message, including cancellations and failures. Check the breakdown whenever you need the delivery count.

**If the request returns `400`**, read its `detail` field. One invalid entry rejects the whole batch. Send 1–1,000 entries per batch. The same idempotency header works on the whole batch.

| Channel | What to put in `recipient` | Content limit |
| --- | --- | --- |
| `sms` | An international number beginning with `+` and country code, e.g. `+12025550123` | 1,600 characters |
| `email` | A plain address, e.g. `reader@example.com` | 500 KiB |
| `push` | A device token of 32–256 allowed characters. Use the repeated `a` above for this Webhook.site example. | 2,048 characters |

Use `high`, `normal`, or `low`. Omitting `priority` selects `normal`. Each channel has its own workers and a shared ceiling of 100 provider calls per second. With all tiers backlogged, workers select six high, three normal, and one low per ten queue slots. Older low and normal messages can move up. A three-message batch can finish in any order. Step 11 checks priority and rate limiting under load.

## 6. Find messages by status, channel, priority, or date

```powershell
$since = [datetime]::UtcNow.AddDays(-1).ToString('o')
$until = [datetime]::UtcNow.ToString('o')
$page = Invoke-RestMethod "$api/notifications?channel=sms&priority=high&status=DELIVERED,FAILED&date_from=$since&date_to=$until&page=1&limit=10"
$page.data | Format-Table id, recipient, status, priority, createdAt
$page.pagination | Format-List
```

**OK if** the SMS from step 3 appears on a fresh stack and the page shows up to ten matching rows. If you already have many messages, increase `page` to look through later pages. The maximum `limit` is 100. An empty `data` array and a total of zero mean the query matched nothing. To see the latest messages across all channels, run `(Invoke-RestMethod "$api/notifications").data`.

| Status | What it means for you |
| --- | --- |
| `PENDING` / `QUEUED` | The system has the message. A worker can pick it up. |
| `SCHEDULED` | The system is waiting for its due time. |
| `PROCESSING` | A worker has claimed this message. |
| `RETRY_PENDING` | A temporary failure occurred. Another attempt is due. |
| `DELIVERED` | The provider returned a valid acceptance. |
| `FAILED` | Automatic attempts have ended. Inspect the history and the dead-letter queue in step 10. |
| `CANCELLED` | Cancellation succeeded. The worker will skip this message. |

## 7. Schedule a message, then practice cancelling another one

Set `scheduledAt` to a future UTC time. Paste the whole block to create a message due in 15 seconds and wait for it.

```powershell
$later = $sms.Clone()
$later.scheduledAt = [datetime]::UtcNow.AddSeconds(15).ToString('o')
$scheduled = Send-Json 'notifications' $later
$scheduled.status
(Get-FinalNotification $scheduled.id).status
```

**OK if** you see `SCHEDULED` followed by `DELIVERED` after the due time. **If it stays `SCHEDULED`**, compare the saved `scheduledAt` with the current UTC time and check `docker compose logs --tail=50 scheduler`.

For cancellation, use a fresh message five minutes in the future so you have time to cancel it.

```powershell
$later.scheduledAt = [datetime]::UtcNow.AddMinutes(5).ToString('o')
$toCancel = Send-Json 'notifications' $later
Invoke-RestMethod -Method Post "$api/notifications/$($toCancel.id)/cancel"
$cancelled = Get-FinalNotification $toCancel.id
$cancelled.status
$cancelled.deliveryAttempts.Count
```

**OK if** you see `CANCELLED` and `0` attempts. **If you get `409`**, look up that ID. Cancellation is available only during `PENDING`, `SCHEDULED`, or `QUEUED`.

## 8. Create, use, edit, and delete a template

Create an SMS template once. You can fill in its `{{name}}` and `{{code}}` each time you send it.

```powershell
$templateBody = @{
    name = "welcome-$([guid]::NewGuid().ToString('N').Substring(0,8))"
    channel = 'sms'
    bodyTemplate = 'Hello {{name}}, your code is {{code}}.'
}
$tpl = Send-Json 'templates' $templateBody
Invoke-RestMethod "$api/templates/$($tpl.id)" | Format-List id, name, channel, bodyTemplate
(Invoke-RestMethod "$api/templates") | Format-Table id, name, channel
$fromTemplate = Send-Json 'notifications' @{
    recipient = '+12025550123'
    templateId = $tpl.id
    variables = @{ name = 'Sam'; code = '4821' }
}
Get-FinalNotification $fromTemplate.id | Format-List status, content
```

**OK if** you see your template and then `DELIVERED` with `Hello Sam, your code is 4821.` The template supplies the channel and content. **If you get `400`**, supply every named variable as a string, omit `content`, and check the channel's content limit after substitution.

To edit the template and then remove this demo template, run the following.

```powershell
$templateBody.bodyTemplate = 'Welcome back, {{name}}.'
Send-Json "templates/$($tpl.id)" $templateBody @{} 'Put' | Format-List id, bodyTemplate
(Invoke-WebRequest -UseBasicParsing -Method Delete -Uri "$api/templates/$($tpl.id)").StatusCode
```

**OK if** the updated body appears and the delete prints `204`. A subsequent lookup of that template ID should return `404`.

## 9. Watch live updates, logs, and metrics

**Live updates.** Open [the API readiness page](http://localhost:8080/ready) in your browser. Press **F12**, choose **Console**, and enter the JavaScript below on that page.

```javascript
window.feed = new WebSocket('ws://localhost:8080/api/v1/ws/notifications?channel=sms');
feed.onopen = () => console.log('Listening for SMS');
feed.onmessage = ({data}) => {
    const event = JSON.parse(data);
    console.log(event.notificationId, event.currentStatus);
};
```

After `Listening for SMS` appears, go back to PowerShell and send a fresh message.

```powershell
$live = Send-Json 'notifications' $sms
$live.id
```

**OK if** the browser shows that ID changing status through `DELIVERED`. **If the console is quiet**, check that it was open on port 8080 before you sent the SMS. The stream shows new events. You can also filter with `?batch_id=<batch UUID>` or omit the query to see every channel. Close the connection with `feed.close()` in the browser console.

**Logs.** To trace the message from step 3, run this in PowerShell.

```powershell
docker compose logs api worker-sms | Select-String -SimpleMatch $correlation
```

**OK if** entries from the API and SMS worker carry the same `correlation_id`. For live logs run `docker compose logs -f api worker-sms`. Press **Ctrl+C** to leave the log view.

**Metrics.** Open [Prometheus targets](http://localhost:9090/targets) and wait a few seconds. Then open [the query page](http://localhost:9090/query), enter `notification_provider_attempts_total{outcome="SUCCESS"}`, and press **Execute**. You can also open [the raw metrics](http://localhost:8080/metrics) directly.

**OK if** all four targets are `UP` and the API series has successful attempts for the channels you used. Query `notifications_queue_depth` to see the number ready to send and `notification_provider_latency_seconds_count` to see how many provider timings were recorded. A queue depth of zero is normal after delivery. **If a target is down**, look up the matching service with `docker compose ps --all`.

## 10. Practice a failed delivery and recover it

To practice retry exhaustion on an isolated endpoint, run the `TestRetryToDLQ` test in step 11. To inspect an actual failed SMS, use the following CLI command.

```powershell
docker build --target cli -t notification-guide-cli .
$apiContainer = docker compose ps -q api
function Invoke-Dlq($action) {
    docker run --rm --network "container:$apiContainer" -e DB_HOST=postgres -e REDIS_HOST=redis notification-guide-cli dlq $action --channel sms --limit 1
    if ($LASTEXITCODE -ne 0) { throw 'The dead-letter command failed.' }
}
Invoke-Dlq list
```

**OK if** an empty queue prints nothing. If it contains a failure, inspect the entry below. After you have fixed the response in Webhook.site, you can replay the first entry if it matches the notification you intend to resend.

```powershell
$entry = Invoke-Dlq list | ConvertFrom-Json
$entry | Format-List notificationId, totalAttempts, lastErrorCode, terminalReason
if ($entry -and $entry.notificationId -eq (Read-Host 'Enter the notification ID to resend')) {
    Invoke-Dlq replay
    $recovered = Get-FinalNotification $entry.notificationId
    $recovered | Format-List id, status, providerMessageId
    $recovered.deliveryAttempts | Format-Table attemptNumber, status, httpStatusCode
}
```

**OK if** the replayed notification reaches `DELIVERED`, and its history shows an additional successful `202` attempt. A temporary `503` can cause up to three retries. A permanent `422` sends the notification to the dead-letter queue after the first attempt.

## 11. Prove that retries, priority, and the rate limit work

The smoke test in step 2 takes seconds. For the full checks, keep the local Compose stack running. On this PC, Go and the race detector are installed in Ubuntu WSL. Run this in PowerShell from the repository folder.

```powershell
$linuxHome = (wsl -d Ubuntu-22.04 --exec printenv HOME).Trim()
$go = "$linuxHome/.local/notification-sdk/go/bin/go"
wsl -d Ubuntu-22.04 --exec $go version
wsl -d Ubuntu-22.04 --exec env 'TEST_DATABASE_URL=postgres://notification_user:notification_secret@localhost:5432/notification_db?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 $go test -count=1 -race -v ./...
$LASTEXITCODE
```

On another machine with Go 1.22+ and a working race detector in PowerShell, use this version instead. The database and Redis addresses below are for the default local stack.

```powershell
$env:TEST_DATABASE_URL = 'postgres://notification_user:notification_secret@localhost:5432/notification_db?sslmode=disable'
$env:TEST_REDIS_ADDR = 'localhost:6379'
go test -count=1 -race -v ./...
$LASTEXITCODE
```

**OK if** the exit code is `0`, `TestRetryToDLQ`, `TestFairnessAndAging`, and `TestBurst500` all say `PASS`, and the 500-message test reports a `busiest rolling second` of **100 or fewer** across two SMS worker replicas. A smaller number passes. `TestBreakerAndBackoff` and `TestTimeoutAndHalfOpenRecovery` exercise the circuit breaker.

**If you see `SKIP` on service-backed tests**, check both `TEST_` settings and rerun. **If you see `FAIL`, `DATA RACE`, or a peak above 100**, the verification failed. If the WSL command cannot find Go, use the path shown by `command -v go` inside your own Linux distribution, or use the native command above.

## 12. Connect a real provider or tracing collector (optional)

### Use Webhook.site as the provider in the assessment

You send one SMS request to the local API. Its SMS worker sends an HTTP POST to Webhook.site. You can see the outgoing message in your browser and see the acknowledgement saved against that same notification ID in the local API. Use demo data because this sends the recipient and message outside your computer.

**A. Set up the provider response.** Open [Webhook.site](https://webhook.site/) and keep that tab open. Copy **Your unique URL**, which looks like `https://webhook.site/<your-uuid>`. The browser address containing `#!/view/` opens the dashboard. Click **Edit** in the upper right. Set the default response status to `202`, set the content type to `application/json`, and paste this into the response body. Click **Save**. [Webhook.site FAQ](https://docs.webhook.site/) · [Default response settings](https://docs.webhook.site/api/tokens.html#create-token)

```json
{
  "messageId": "9fbf2bd0-673e-4b4f-b67c-3cc8205841c2",
  "status": "accepted",
  "timestamp": "2026-11-21T12:00:00Z"
}
```

**OK if** you reopen **Edit** and see the saved `202` and JSON. These fixed values make one demo easy to verify. This static response returns the same provider ID for every request. The timestamp in the PDF is a format placeholder. The worker needs an actual date and time like the one above.

**B. Check the workers' URL.** Open the `.env` file in the project root. Its `PROVIDER_WEBHOOK_URL` line should contain the capture URL from A. On a fresh clone, create a plain-text file named `.env` alongside `docker-compose.yml` with the line `PROVIDER_WEBHOOK_URL=https://webhook.site/<your-uuid>`, replacing the placeholder. Remove any older PowerShell override so Docker Compose reads the file.

```powershell
Set-Location "$env:USERPROFILE\Desktop\event-driven-notification-system"
Remove-Item Env:PROVIDER_WEBHOOK_URL -ErrorAction SilentlyContinue
docker compose up -d --build --wait
docker compose exec -T worker-sms printenv PROVIDER_WEBHOOK_URL
```

**OK if** the last line matches **Your unique URL**. Changes to `.env` take effect when you rerun `docker compose up -d --wait`.

**C. Send one notification through your system.** The local API field is `recipient`. The worker turns that into the provider's `to` field.

```powershell
$notifications = 'http://localhost:8080/api/v1/notifications'
$body = @{ recipient = '+905551234567'; channel = 'sms'; content = 'Your message' } | ConvertTo-Json
$sent = Invoke-RestMethod -Method Post -Uri $notifications -ContentType 'application/json' -Body $body
$sent | Format-List id, status
```

**OK if** PowerShell shows an `id` and `QUEUED`. Now look at the Webhook.site tab. Select the incoming **POST** and open its body. You should see the following fields. In the request headers, `X-Idempotency-Key` should match `$sent.id`.

```json
{"to":"+905551234567","channel":"sms","content":"Your message"}
```

**D. Confirm that the worker accepted the reply.** Run this block. If the status is still `QUEUED` or `PROCESSING`, wait a few seconds and run **this lookup only** again.

```powershell
$result = Invoke-RestMethod "$notifications/$($sent.id)"
$result | Format-List status, providerMessageId, retryCount
$result.deliveryAttempts | Format-Table attemptNumber, status, httpStatusCode, errorReason
```

**OK if** you see `DELIVERED`, provider ID `9fbf2bd0-673e-4b4f-b67c-3cc8205841c2`, and a `SUCCESS` attempt with HTTP `202`. For a clean first attempt, `retryCount` is `0`. Together with the captured POST this proves the round trip. Webhook.site simulates a provider, so your phone will stay quiet.

| What you see | What to do |
| --- | --- |
| HTTP `200` in the attempt | Set the Webhook.site response status to `202` and save. |
| HTTP `202` with `invalid provider acknowledgement` | Check the response body. It needs a nonempty `messageId`, lowercase `accepted`, and a timestamp with a time zone. |
| An empty Webhook.site inbox or HTTP `405` | Compare the worker URL from B with **Your unique URL**. If it contains `#!/view/`, copy the capture URL and repeat B. Check `docker compose logs --tail=30 worker-sms`. |
| The ID says `FAILED` after you fixed the response | Repeat C once to get a fresh notification ID, then use D to check that ID. |

### Connect a tracing collector

**Tracing.** If you run an OpenTelemetry collector, set `OTEL_EXPORTER_OTLP_ENDPOINT` to its OTLP/gRPC URL. For a collector on your Windows host listening on port 4317, use the command below. Send another local message, then open the trace viewer attached to your collector.

```powershell
$env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://host.docker.internal:4317'
docker compose up -d --wait api worker-sms worker-email worker-push scheduler
```

**OK if** you can follow the API request to a worker's `provider POST` span in the same trace. **If no spans appear**, check your collector's gRPC receiver and exporter. To undo this optional setting, remove the environment variable and repeat the preceding Compose command. For settings that should persist across PowerShell sessions, use the local `.env` file in the project root. In that case, restore the defaults there too.

## 13. Stop now and come back later

```powershell
docker compose stop
docker compose ps --all
```

**OK if** the containers say `Exited`. Your local database and queue volumes are retained. Next time, run `docker compose up -d --wait` and check `/ready` again. In a new PowerShell window, repeat the `$root` and `$api` assignments in step 1 and the helper definitions in step 3. You can retrieve any saved notification ID with `Invoke-RestMethod "$api/notifications/YOUR-ID"`.

For the full request and response schemas, see the [API specification](API_SPECIFICATION.md) and [OpenAPI file](openapi.yaml).
