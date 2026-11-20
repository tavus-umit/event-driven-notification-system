package main

import (
	"context"
	"encoding/json"
	"errors"
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/domain"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/repository/postgres"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() { app.Main(run) }
func run(ctx context.Context, s *app.Services) error {
	if len(os.Args) < 3 || os.Args[1] != "dlq" {
		return errors.New("usage: cli dlq list|replay --channel sms --limit 100 --priority high")
	}
	action := os.Args[2]
	fs := flag.NewFlagSet("dlq", flag.ContinueOnError)
	channel := fs.String("channel", "sms", "notification channel")
	limit := fs.Int("limit", 100, "number of entries")
	priority := fs.String("priority", "high", "replay priority")
	if e := fs.Parse(os.Args[3:]); e != nil {
		return e
	}
	ch := domain.Channel(*channel)
	p := domain.Priority(*priority)
	if !ch.Valid() || !p.Valid() || *limit < 1 || *limit > 10000 || (action != "list" && action != "replay") {
		return errors.New("invalid DLQ operation or arguments")
	}
	key := broker.Key(ch, "dlq")
	entries, e := s.Broker.Client.LRange(ctx, key, 0, int64(*limit-1)).Result()
	if e != nil {
		return e
	}
	for _, raw := range entries {
		if action == "list" {
			fmt.Println(raw)
			continue
		}
		var event postgres.Event
		if e = json.Unmarshal([]byte(raw), &event); e != nil {
			return e
		}
		if event.At.IsZero() {
			return errors.New("missing failure timestamp")
		}
		res, e := s.Store.DB.ExecContext(ctx, `UPDATE notifications SET attempt_base=(SELECT coalesce(max(attempt_number),0) FROM delivery_attempts WHERE notification_id=$1),status='QUEUED',priority=$2,retry_count=0,error_message=NULL,due_at=NULL,updated_at=now() WHERE id=$1 AND channel=$3 AND status='FAILED' AND updated_at<=$4`, event.ID, p, ch, event.At.Add(time.Microsecond))
		if e != nil {
			return e
		}
		if e = s.Broker.Client.LRem(ctx, key, 1, raw).Err(); e != nil {
			return e
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			fmt.Println(event.ID)
		}
	}
	return nil
}
