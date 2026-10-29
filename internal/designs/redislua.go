package designs

import (
	"context"
	"fmt"
	"strconv"

	"hotledger/internal/model"
	"hotledger/internal/resp"
)

// ---------------------------------------------------------------------------
// Design 4: Redis Lua fast path + async durable log + reconciler (the common
// industry pattern for hot counters and wallets).
//
// One Lua script checks the idempotency key, checks the balance, moves the
// coins, records the result and appends the transfer to a Redis Stream, all
// atomically because Redis runs a script without interleaving. The client is
// answered immediately. A reconciler process tails the stream and applies it
// to Postgres, the durable system of record.
//
// The failure window is the point of including it: Redis acknowledges before
// the write is durable (appendfsync everysec loses up to about a second on
// kill -9), and the reconciler can copy an entry to Postgres before Redis
// loses it. The checker measures both effects instead of assuming them away.

const luaTransfer = `
local done = redis.call('HGET', 'xfer', ARGV[1])
if done then return done end
local b = tonumber(redis.call('HGET', 'bal', ARGV[2]) or '0')
local amt = tonumber(ARGV[4])
local res = '0'
if b >= amt then
  redis.call('HINCRBY', 'bal', ARGV[2], -amt)
  redis.call('HINCRBY', 'bal', ARGV[3], amt)
  res = '1'
end
redis.call('HSET', 'xfer', ARGV[1], res)
redis.call('XADD', 'log', '*', 'id', ARGV[1], 'src', ARGV[2], 'dst', ARGV[3], 'amt', ARGV[4], 'ok', res)
return res`

type RedisLua struct {
	p   *resp.Pool
	sha string
}

func NewRedisLua(addrs []string, _ model.Layout) (*RedisLua, error) {
	p := resp.NewPool(addrs[0], 64)
	v, err := p.Do(context.Background(), "SCRIPT", "LOAD", luaTransfer)
	if err != nil {
		return nil, fmt.Errorf("script load: %w", err)
	}
	return &RedisLua{p: p, sha: v.(string)}, nil
}

func (d *RedisLua) Close() error { d.p.Close(); return nil }

func (d *RedisLua) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	v, err := d.p.Do(ctx, "EVALSHA", d.sha, 0, t.ID, t.Src, t.Dst, t.Amount)
	if err != nil {
		if re, ok := err.(resp.Error); ok && len(re) >= 8 && re[:8] == "NOSCRIPT" {
			// Redis restarted and lost the script cache: reload and retry.
			if _, lerr := d.p.Do(ctx, "SCRIPT", "LOAD", luaTransfer); lerr != nil {
				return model.Unknown, lerr
			}
		}
		return model.Unknown, err
	}
	s, _ := v.(string)
	switch s {
	case "1":
		return model.Applied, nil
	case "0":
		return model.Rejected, nil
	}
	return model.Unknown, fmt.Errorf("unexpected reply %v", v)
}

// SeedRedis writes initial balances and creates the stream consumer group.
func SeedRedis(ctx context.Context, p *resp.Pool, l model.Layout) error {
	if _, err := p.Do(ctx, "FLUSHALL"); err != nil {
		return err
	}
	args := []any{"HSET", "bal"}
	flush := func() error {
		if len(args) > 2 {
			if _, err := p.Do(ctx, args...); err != nil {
				return err
			}
			args = args[:2]
		}
		return nil
	}
	for id := int64(1); id <= l.NumAccounts(); id++ {
		args = append(args, strconv.FormatInt(id, 10), l.InitialBalance(id))
		if len(args) >= 2002 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	_, err := p.Do(ctx, "XGROUP", "CREATE", "log", "reconciler", "0", "MKSTREAM")
	return err
}
