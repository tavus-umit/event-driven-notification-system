package observe

import (
	"context"
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"event-driven-notification-system/internal/domain"
)

// Totals reads bounded counters maintained in the same transaction as the audit row.
type Totals struct{ DB *sql.DB }

var attemptsDesc = prometheus.NewDesc("notification_provider_attempts_total", "Provider attempts committed by all workers.", []string{"channel", "outcome"}, nil)
var durationDesc = prometheus.NewDesc("notification_provider_latency_seconds", "Provider round trip duration across all workers.", []string{"channel"}, nil)

func (t Totals) Describe(ch chan<- *prometheus.Desc) { ch <- attemptsDesc; ch <- durationDesc }
func (t Totals) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, e := t.DB.QueryContext(ctx, "SELECT channel,outcome,bucket_ms,samples,sum_ms FROM notification_attempt_stats")
	if e != nil {
		ch <- prometheus.NewInvalidMetric(attemptsDesc, e)
		return
	}
	defer rows.Close()
	type series struct {
		samples map[string]uint64
		bins    map[int]uint64
		sum     int64
	}
	all := map[domain.Channel]*series{}
	for _, c := range domain.Channels {
		all[c] = &series{samples: map[string]uint64{"SUCCESS": 0, "FAILURE": 0}, bins: map[int]uint64{}}
	}
	for rows.Next() {
		var c domain.Channel
		var outcome string
		var bucket int
		var count uint64
		var sum int64
		if e = rows.Scan(&c, &outcome, &bucket, &count, &sum); e != nil {
			ch <- prometheus.NewInvalidMetric(attemptsDesc, e)
			return
		}
		s := all[c]
		s.samples[outcome] += count
		s.bins[bucket] += count
		s.sum += sum
	}
	if e = rows.Err(); e != nil {
		ch <- prometheus.NewInvalidMetric(attemptsDesc, e)
		return
	}
	for c, s := range all {
		var n uint64
		for outcome, count := range s.samples {
			ch <- prometheus.MustNewConstMetric(attemptsDesc, prometheus.CounterValue, float64(count), string(c), outcome)
			n += count
		}
		bounds := []int{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}
		buckets := map[float64]uint64{}
		var total uint64
		for _, ms := range bounds {
			total += s.bins[ms]
			buckets[float64(ms)/1000] = total
		}
		ch <- prometheus.MustNewConstHistogram(durationDesc, n, float64(s.sum)/1000, buckets, string(c))
	}
}
