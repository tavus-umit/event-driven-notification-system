package websocket

import (
	"context"
	"encoding/json"
	"event-driven-notification-system/internal/domain"
	broker "event-driven-notification-system/internal/queue/redis"
	"event-driven-notification-system/internal/repository/postgres"
	ws "github.com/gorilla/websocket"
	"net/http"
	"sync"
	"time"
)

type subscriber struct {
	batch   string
	channel domain.Channel
	send    chan []byte
}
type Hub struct {
	mu      sync.Mutex
	clients map[*subscriber]bool
	ctx     context.Context
	broker  *broker.Broker
}

func New(ctx context.Context, b *broker.Broker) *Hub {
	return &Hub{clients: map[*subscriber]bool{}, ctx: ctx, broker: b}
}
func (h *Hub) Run() {
	sub := h.broker.Client.Subscribe(h.ctx, broker.Topic)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-h.ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			data := []byte(m.Payload)
			var v postgres.Event
			if json.Unmarshal(data, &v) != nil {
				continue
			}
			h.mu.Lock()
			for c := range h.clients {
				if (c.batch == "" || c.batch == v.BatchID) && (c.channel == "" || c.channel == v.Channel) {
					select {
					case c.send <- data:
					default:
						delete(h.clients, c)
						close(c.send)
					}
				}
			}
			h.mu.Unlock()
		}
	}
}
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u := ws.Upgrader{HandshakeTimeout: 5 * time.Second}
	c, e := u.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
	s := &subscriber{r.URL.Query().Get("batch_id"), domain.Channel(r.URL.Query().Get("channel")), make(chan []byte, 64)}
	h.mu.Lock()
	h.clients[s] = true
	h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.clients, s); h.mu.Unlock() }()
	c.SetReadLimit(1024)
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.SetPongHandler(func(string) error { return c.SetReadDeadline(time.Now().Add(60 * time.Second)) })
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, e := c.ReadMessage(); e != nil {
				return
			}
		}
	}()
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-closed:
			return
		case <-ping.C:
			if c.WriteControl(ws.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
				return
			}
		case msg, ok := <-s.send:
			if !ok {
				return
			}
			_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if c.WriteMessage(ws.TextMessage, msg) != nil {
				return
			}
		}
	}
}
