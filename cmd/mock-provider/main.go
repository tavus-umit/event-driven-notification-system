package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"event-driven-notification-system/internal/provider"
	"event-driven-notification-system/pkg/logger"
)

func handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	m.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		var p provider.Payload
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
		d.DisallowUnknownFields()
		if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || !p.Channel.Valid() || p.To == "" || p.Content == "" {
			http.Error(w, "invalid payload", 400)
			return
		}
		if fail := os.Getenv("MOCK_FAILURE_STATUS"); fail != "" && (os.Getenv("MOCK_FAILURE_RECIPIENT") == "" || os.Getenv("MOCK_FAILURE_RECIPIENT") == p.To) {
			code, e := strconv.Atoi(fail)
			if e == nil && code >= 400 && code <= 599 {
				http.Error(w, "simulated provider failure", code)
				return
			}
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
