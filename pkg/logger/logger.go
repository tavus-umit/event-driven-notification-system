package logger

import (
	"context"
	"io"
	"log/slog"
)

type correlationKey struct{}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}
func ID(ctx context.Context) string { s, _ := ctx.Value(correlationKey{}).(string); return s }
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, AddSource: true}))
}
func From(ctx context.Context) *slog.Logger { return slog.Default().With("correlation_id", ID(ctx)) }
