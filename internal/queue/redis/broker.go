package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	r "github.com/redis/go-redis/v9"

	"event-driven-notification-system/internal/domain"
	"event-driven-notification-system/internal/repository/postgres"
)

const Topic = "events:notification_status"

type Broker struct{ Client *r.Client }

func New(addr, password string, db int) *Broker {
	return &Broker{r.NewClient(&r.Options{Addr: addr, Password: password, DB: db, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})}
}
func Key(c domain.Channel, tier string) string { return "queue:" + string(c) + ":" + tier }

type Job struct {
	Correlation string `json:"correlationId,omitempty"`
	Trace       string `json:"traceparent,omitempty"`
	ID          string `json:"id"`
	Retry       int    `json:"retry"`
	Enqueued    int64  `json:"enqueued"`
}
type Lease struct {
	Job         Job
	Raw, Member string
}

// The routing mutation and status broadcast share one idempotent Redis operation.
const publishLua = `
if not redis.call('SET',KEYS[1],'1','NX','EX',86400) then return 0 end
local status=ARGV[1]
if status=='QUEUED' then
 redis.call('ZREM',KEYS[3],ARGV[3])
 redis.call('ZREM',KEYS[4],ARGV[3])
 redis.call('RPUSH',KEYS[2],ARGV[2])
elseif status=='SCHEDULED' then redis.call('ZADD',KEYS[3],ARGV[4],ARGV[3])
elseif status=='RETRY_PENDING' then redis.call('ZADD',KEYS[4],ARGV[4],ARGV[3])
elseif status=='FAILED' then redis.call('RPUSH',KEYS[5],ARGV[5])
elseif status=='CANCELLED' then
 redis.call('ZREM',KEYS[3],ARGV[3])
 redis.call('ZREM',KEYS[4],ARGV[3])
end
redis.call('PUBLISH',KEYS[6],ARGV[5])
return 1`

func (b *Broker) Publish(ctx context.Context, events []postgres.Event) error {
	pipe := b.Client.Pipeline()
	for _, v := range events {
		body, e := json.Marshal(v)
		if e != nil {
			return e
		}
		job, e := json.Marshal(Job{ID: v.ID, Retry: v.Retry, Enqueued: time.Now().UnixMilli(), Correlation: v.Correlation, Trace: v.Trace})
		if e != nil {
			return e
		}
		var due int64
		if v.Due != nil {
			due = v.Due.UnixMilli()
		}
		keys := []string{fmt.Sprintf("dispatch:%s:%d", v.ID, v.Seq), Key(v.Channel, string(v.Priority)), "queue:scheduled", Key(v.Channel, "delayed"), Key(v.Channel, "dlq"), Topic}
		pipe.Eval(ctx, publishLua, keys, string(v.Current), job, v.ID, due, body)
	}
	_, e := pipe.Exec(ctx)
	return e
}

var pop = r.NewScript(`
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local expired=redis.call('ZRANGEBYSCORE',KEYS[5],'-inf',now,'LIMIT',0,8)
for _,entry in ipairs(expired) do
 local lease=cjson.decode(entry)
 redis.call('RPUSH',KEYS[lease[2]],lease[1])
 redis.call('ZREM',KEYS[5],entry)
end
for tier=3,2,-1 do
 local head=redis.call('LINDEX',KEYS[tier],0)
 if head then
  local ok,j=pcall(cjson.decode,head)
  local waited=tier==3 and 30000 or 60000
  if ok and type(j)=='table' and type(j.enqueued)=='number' and now-j.enqueued>waited then
   redis.call('RPUSH',KEYS[tier-1],redis.call('LPOP',KEYS[tier]))
  end
 end
end
local slots={1,1,1,1,1,1,2,2,2,3}
local cursor=tonumber(redis.call('GET',KEYS[4]) or '0')
for step=0,9 do
 local index=(cursor+step)%10
 local tier=slots[index+1]
 local raw=redis.call('LPOP',KEYS[tier])
 if raw then
  redis.call('SET',KEYS[4],(index+1)%10)
  local member=cjson.encode({raw,tier,ARGV[1]})
  redis.call('ZADD',KEYS[5],now+90000,member)
  return {raw,member}
 end
end
return {}`)

func (b *Broker) Pop(ctx context.Context, c domain.Channel) (*Lease, error) {
	vals, e := pop.Run(ctx, b.Client, []string{Key(c, "high"), Key(c, "normal"), Key(c, "low"), Key(c, "cursor"), Key(c, "inflight")}, uuid.NewString()).StringSlice()
	if e != nil || len(vals) == 0 {
		return nil, e
	}
	l := &Lease{Raw: vals[0], Member: vals[1]}
	if e = json.Unmarshal([]byte(l.Raw), &l.Job); e != nil {
		return l, e
	}
	if _, e = uuid.Parse(l.Job.ID); e != nil || l.Job.Retry < 0 {
		return l, errors.New("invalid queue job")
	}
	return l, nil
}
func (b *Broker) Ack(ctx context.Context, c domain.Channel, l *Lease) error {
	return b.Client.ZRem(ctx, Key(c, "inflight"), l.Member).Err()
}

var quarantine = r.NewScript(`if redis.call('ZREM',KEYS[1],ARGV[1])==1 then redis.call('RPUSH',KEYS[2],ARGV[2]) end return 1`)

func (b *Broker) Quarantine(ctx context.Context, c domain.Channel, l *Lease) error {
	return quarantine.Run(ctx, b.Client, []string{Key(c, "inflight"), Key(c, "quarantine")}, l.Member, l.Raw).Err()
}
func (b *Broker) Due(ctx context.Context, key string) ([]string, error) {
	now, e := b.Client.Time(ctx).Result()
	if e != nil {
		return nil, e
	}
	return b.Client.ZRangeByScore(ctx, key, &r.ZRangeBy{Min: "-inf", Max: fmt.Sprint(now.UnixMilli()), Count: 100}).Result()
}

var unlock = r.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`)

func (b *Broker) Lock(ctx context.Context, key string) (func(context.Context) error, bool, error) {
	k := "idempotency:lock:" + key
	owner := uuid.NewString()
	ok, e := b.Client.SetNX(ctx, k, owner, 15*time.Second).Result()
	return func(ctx context.Context) error { return unlock.Run(ctx, b.Client, []string{k}, owner).Err() }, ok, e
}
