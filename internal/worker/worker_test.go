package worker

import (
	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/provider"
	"testing"
	"time"
)

func TestRetryBudget(t *testing.T) {
	for i := 0; i < 4; i++ {
		n := domain.Notification{RetryCount: i, MaxRetries: 3}
		a, s, d := outcome(n, provider.Result{Code: 503, Retryable: true, Reason: "unavailable"}, time.Now())
		if a.Number != i+1 || ((d != nil) != (i < 3)) || ((s == domain.Failed) != (i == 3)) {
			t.Fatal(i, s, d)
		}
	}
	_, s, d := outcome(domain.Notification{MaxRetries: 3}, provider.Result{Code: 422, Reason: "invalid"}, time.Now())
	if s != domain.Failed || d != nil {
		t.Fatal(s, d)
	}
}
