package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/testutil"
)

// BenchmarkBatchIngestion measures HTTP acceptance through the durable outbox commit.
func BenchmarkBatchIngestion(b *testing.B) {
	s := testutil.Services(b, 6)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer((&API{s}).Router(ctx))
	defer server.Close()
	items := make([]CreateRequest, 1000)
	for i := range items {
		items[i] = CreateRequest{Channel: domain.SMS, Content: "benchmark", Recipient: "+123"}
	}
	body, e := json.Marshal(batchRequest{Items: items})
	if e != nil {
		b.Fatal(e)
	}
	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		r, e := server.Client().Post(server.URL+"/api/v1/notifications/batch", "application/json", bytes.NewReader(body))
		if e != nil {
			b.Fatal(e)
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		if r.StatusCode != 202 {
			b.Fatal(r.Status)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	b.ReportMetric(float64(durations[(95*b.N+99)/100-1].Microseconds())/1000, "ms/p95")
	b.ReportMetric(float64(1000*b.N)/b.Elapsed().Seconds(), "notifications/s")
}
