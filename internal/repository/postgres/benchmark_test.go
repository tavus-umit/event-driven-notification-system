package postgres_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"
)

func BenchmarkBulkInsert(b *testing.B) {
	s := testutil.Services(b, 6).Store
	ctx := context.Background()
	items := make([]domain.Notification, 1000)
	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := uuid.NewString()
		at := time.Now()
		for j := range items {
			items[j] = domain.Notification{Summary: domain.Summary{ID: uuid.NewString(), Channel: domain.SMS, Recipient: "+123", Priority: domain.Normal, Status: domain.Queued, CreatedAt: at}, Content: "benchmark", BatchID: batch, MaxRetries: 3}
		}
		b.StartTimer()
		start := time.Now()
		if e := s.Ingest(ctx, items, nil); e != nil {
			b.Fatal(e)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	b.ReportMetric(float64(durations[(95*b.N+99)/100-1].Microseconds())/1000, "ms/p95")
}
