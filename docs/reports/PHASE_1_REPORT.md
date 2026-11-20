# Phase 1 — Configuration and domain model

## 1. Executive summary of completed work

Initialized the Go module, application layout, domain contracts, limits, configuration loader and contextual JSON logger. Boundary tests cover Unicode SMS length and recipient validation across all three channels. Configuration tests reject attempts to increase the rate ceiling.

### File and symbol inventory

- `go.mod`
- `go.sum`
- `.gitignore`
- `internal/domain/models.go` — `Channel`, `Channel.Valid`, `Priority`, `Priority.Valid`, `Status`, `Status.Valid`, `Summary`, `Notification`, `NotificationBatch`, `DeliveryAttempt`, `NotificationTemplate`, `Filter`, `Receipt`, `NotificationRepository`, `BatchRepository`, `IdempotencyRepository`, `DeliveryAttemptRepository`
- `internal/domain/validate.go` — `Notification.Validate`
- `internal/domain/validate_test.go` — `TestValidation`
- `internal/config/config.go` — `Config`, `env`, `Load`
- `internal/config/config_test.go` — `TestConfiguration`
- `pkg/logger/logger.go` — `correlationKey`, `WithID`, `ID`, `New`, `From`
- `docs/reports/PHASE_1_REPORT.md` — phase acceptance record.

## 2. Design decisions and technical justifications

The payload contract permits email and push messages containing only content. Subject, title and custom data therefore receive their additional validation when supplied in metadata. SMS uses Unicode code points for the absolute 1,600-character limit. The domain includes RETRY_PENDING from the state diagram. The next migration extends the original enum. Configuration reads process variables before .env defaults and rejects pool and timeout combinations that would invalidate worker bounds.

## 3. Source code walkthrough and annotated snippets

Validation occurs after template rendering, so substitution cannot bypass the channel limits.

From `internal/domain/validate.go`

```go
	case SMS:
		if !phone.MatchString(n.Recipient) || utf8.RuneCountInString(n.Content) > 1600 {
			return errors.New("SMS requires an E.164 recipient and at most 1600 characters")
		}
	case Email:
		a, err := mail.ParseAddress(n.Recipient)
		if err != nil || a.Address != n.Recipient || len(n.Content) > 500*1024 {
			return errors.New("invalid email address or body exceeds 500 KiB")
		}
```

A private context key carries request identity without reserving a public string key.

From `pkg/logger/logger.go`

```go
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}
func ID(ctx context.Context) string { s, _ := ctx.Value(correlationKey{}).(string); return s }
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, AddSource: true}))
}
func From(ctx context.Context) *slog.Logger { return slog.Default().With("correlation_id", ID(ctx)) }


```

## 4. Verification and testing results

Commands ran from the repository root. Go commands used Ubuntu 22.04 under WSL 2.

```sh
go mod init event-driven-notification-system
go mod edit -go=1.22.0
go get github.com/google/uuid@v1.6.0 github.com/joho/godotenv@v1.5.1
mkdir -p cmd && gofmt -w internal pkg && go mod tidy && go test -v -race ./...
```

Observed output (selected lines)

```text
=== RUN   TestConfiguration
--- PASS: TestConfiguration (0.00s)
PASS
ok  	event-driven-notification-system/internal/config	1.013s
=== RUN   TestValidation
--- PASS: TestValidation (0.00s)
PASS
ok  	event-driven-notification-system/internal/domain	1.011s
?   	event-driven-notification-system/pkg/logger	[no test files]
```

## 5. Prerequisites and handoff

Repository interfaces accept context.Context for cancellation. Notification carries the persisted delivery fields, validated channel and priority, and correlation metadata. Concrete drivers belong in the next phase.
