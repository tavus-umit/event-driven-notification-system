package ratelimit

import (
	"context"
	"time"

	"github.com/google/uuid"
	r "github.com/redis/go-redis/v9"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/observe"
)

const Capacity = 100

type Limiter struct{ Client *r.Client }

var acquire = r.NewScript(`
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000000+tonumber(clock[2])
local prev=redis.call('HMGET',KEYS[1],'tokens','time')
local tokens=math.min(100,tonumber(prev[1] or '100')+math.max(0,now-tonumber(prev[2] or now))/10000)
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
if redis.call('ZCARD',KEYS[2])>=100 then
 local first=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES')
 return math.min(10000,math.max(1,tonumber(first[2])-now))
end
if tokens<1 then return math.ceil((1-tokens)*10000) end
redis.call('HSET',KEYS[1],'tokens',tokens-1,'time',string.format('%.0f',now))
redis.call('EXPIRE',KEYS[1],120)
redis.call('ZADD',KEYS[2],now+90000000,ARGV[1])
redis.call('EXPIRE',KEYS[2],120)
return 0`)
var release = r.NewScript(`
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000000+tonumber(clock[2])
return redis.call('ZADD',KEYS[1],'XX',now+1000000,ARGV[1])`)

type Permit struct {
	client  *r.Client
	key, id string
}

func (l Limiter) Try(ctx context.Context, c domain.Channel) (*Permit, time.Duration, error) {
	k := "ratelimit:" + string(c)
	id := uuid.NewString()
	wait, e := acquire.Run(ctx, l.Client, []string{k, k + ":window"}, id).Int64()
	if e != nil || wait != 0 {
		return nil, time.Duration(wait) * time.Microsecond, e
	}
	return &Permit{l.Client, k + ":window", id}, 0, nil
}
func (l Limiter) Wait(ctx context.Context, c domain.Channel) (*Permit, error) {
	for {
		p, delay, e := l.Try(ctx, c)
		if e != nil || p != nil {
			return p, e
		}
		observe.Throttled.WithLabelValues(string(c)).Inc()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Done holds the rolling-window reservation through provider completion.
// Lost workers leave a bounded reservation longer than the provider timeout.
func (p *Permit) Done(ctx context.Context) error {
	return release.Run(ctx, p.client, []string{p.key}, p.id).Err()
}
