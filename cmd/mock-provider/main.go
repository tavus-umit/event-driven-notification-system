package main

import (
	"encoding/json"
	"event-driven-notification-system/internal/provider"
	"event-driven-notification-system/pkg/logger"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	m.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		var p provider.Payload
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
		d.DisallowUnknownFields()
		if d.Decode(&p) != nil || !p.Channel.Valid() || p.To == "" || p.Content == "" {
			http.Error(w, "invalid payload", 400)
			return
		}
		id := uuid.NewString()
		if key := r.Header.Get("X-Idempotency-Key"); key != "" {
			id = uuid.NewSHA1(uuid.NameSpaceOID, []byte(key)).String()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(provider.Accepted{MessageID: id, Status: "accepted", Timestamp: time.Now().UTC()})
	})
	return m
}
func main() {
	slog.SetDefault(logger.New(os.Stdout, slog.LevelInfo))
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	s := &http.Server{Addr: ":" + port, Handler: handler(), ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	if e := s.ListenAndServe(); e != nil {
		slog.Error("mock provider stopped", "error", e)
		os.Exit(1)
	}
}
