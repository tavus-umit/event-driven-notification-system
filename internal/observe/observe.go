package observe

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	r "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"

	"event-driven-notification-system/internal/domain"
	broker "event-driven-notification-system/internal/queue/redis"
)

var Depth = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "notifications_queue_depth", Help: "Ready notifications by channel and priority."}, []string{"channel", "priority"})
var Deliveries = promauto.NewCounterVec(prometheus.CounterOpts{Name: "notifications_delivered_total", Help: "Committed provider attempt outcomes."}, []string{"channel", "status"})
var Latency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_delivery_duration_seconds", Help: "Provider round trip time."}, []string{"channel"})
var Retries = promauto.NewCounterVec(prometheus.CounterOpts{Name: "notification_retries_total", Help: "Transient failures scheduled for retry."}, []string{"channel"})
var DLQ = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dlq_messages_total", Help: "Terminal provider failures."}, []string{"channel"})
var Throttled = promauto.NewCounterVec(prometheus.CounterOpts{Name: "ratelimit_throttled_total", Help: "Admission checks deferred for channel capacity."}, []string{"channel"})

func Metrics(b *broker.Broker, collectors ...prometheus.Collector) http.Handler {
	registry := prometheus.NewRegistry()
	for _, c := range collectors {
		registry.MustRegister(c)
	}
	h := promhttp.HandlerFor(prometheus.Gatherers{registry, prometheus.DefaultGatherer}, promhttp.HandlerOpts{})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		pipe := b.Client.Pipeline()
		counts := map[[2]string]*r.IntCmd{}
		for _, ch := range domain.Channels {
			for _, p := range []string{"high", "normal", "low"} {
				counts[[2]string{string(ch), p}] = pipe.LLen(ctx, broker.Key(ch, p))
			}
		}
		if _, e := pipe.Exec(ctx); e != nil {
			http.Error(w, "queue metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		for k, c := range counts {
			Depth.WithLabelValues(k[0], k[1]).Set(float64(c.Val()))
		}
		h.ServeHTTP(w, req)
	})
}
func Tracing(ctx context.Context, name string) (func(context.Context) error, error) {
	opts := []sdk.TracerProviderOption{sdk.WithResource(resource.NewSchemaless(attribute.String("service.name", name))), sdk.WithSampler(sdk.ParentBased(sdk.AlwaysSample()))}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, e := otlptracegrpc.New(ctx)
		if e != nil {
			return nil, e
		}
		opts = append(opts, sdk.WithBatcher(exp))
	}
	tp := sdk.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}
func Serve(ctx context.Context, s *http.Server) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		drain, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = s.Shutdown(drain)
	}()
	e := s.ListenAndServe()
	if errors.Is(e, http.ErrServerClosed) {
		<-done
		return nil
	}
	return e
}
