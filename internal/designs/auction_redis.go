package designs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"hotledger/internal/model"
	"hotledger/internal/resp"
)

// ---------------------------------------------------------------------------
// Design 4 for auctions: one Lua script decides the bid against the auction
// hash with the same rules as model.AuctionRules, stamps it with Redis TIME,
// records the decision for idempotency and appends it to the same stream the
// reconciler copies to Postgres. Numbers that do not fit Lua's %.14g string
// conversion (microsecond timestamps) are formatted with %.0f explicitly.

const luaBid = `
local done = redis.call('HGET', 'bid', ARGV[1])
if done then return done end
local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000000 + tonumber(t[2])
local key = 'auc:' .. ARGV[2]
local v = redis.call('HMGET', key, 'high', 'deadline', 'hard')
local high, dl, hard = tonumber(v[1]), tonumber(v[2]), tonumber(v[3])
local amt = tonumber(ARGV[4])
local reason = ''
if ts > dl then
  reason = 'closed'
elseif amt < math.max(high + tonumber(ARGV[5]), tonumber(ARGV[6])) then
  reason = 'low'
end
local after = high
local nd = dl
if reason == '' then
  after = amt
  local w = tonumber(ARGV[8])
  if ARGV[7] == '1' and dl - ts < w then
    nd = math.max(dl, math.min(ts + w, hard))
  end
  redis.call('HSET', key, 'high', ARGV[4], 'bidder', ARGV[3], 'deadline', string.format('%.0f', nd))
end
local acc = '0'
if reason == '' then acc = '1' end
local res = acc .. '|' .. reason .. '|' .. string.format('%.0f', ts) .. '|' .. string.format('%.0f', after)
redis.call('HSET', 'bid', ARGV[1], res)
redis.call('XADD', 'log', '*', 't', 'b', 'id', ARGV[1], 'auc', ARGV[2], 'bidder', ARGV[3], 'amt', ARGV[4],
  'res', res, 'dl', string.format('%.0f', nd))
return res`

type AuctionRedis struct {
	p     *resp.Pool
	sha   string
	rules model.AuctionRules
}

func NewAuctionRedis(addrs []string, rules model.AuctionRules) (*AuctionRedis, error) {
	p := resp.NewPool(addrs[0], 64)
	v, err := p.Do(context.Background(), "SCRIPT", "LOAD", luaBid)
	if err != nil {
		return nil, fmt.Errorf("script load: %w", err)
	}
	return &AuctionRedis{p: p, sha: v.(string), rules: rules}, nil
}

func (d *AuctionRedis) Close() error { d.p.Close(); return nil }

// ParseBidResult decodes "accepted|reason|ts|high".
func ParseBidResult(s string) (model.BidResult, error) {
	f := strings.Split(s, "|")
	if len(f) != 4 {
		return model.BidResult{}, fmt.Errorf("bad bid result %q", s)
	}
	ts, err1 := strconv.ParseInt(f[2], 10, 64)
	hi, err2 := strconv.ParseInt(f[3], 10, 64)
	if err1 != nil || err2 != nil {
		return model.BidResult{}, fmt.Errorf("bad bid result %q", s)
	}
	return model.BidResult{Accepted: f[0] == "1", Reason: f[1], TsUs: ts, High: hi}, nil
}

func (d *AuctionRedis) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	ext := "0"
	if d.rules.Extended {
		ext = "1"
	}
	v, err := d.p.Do(ctx, "EVALSHA", d.sha, 0, b.ID, b.Auction, b.Bidder, b.Amount, d.rules.MinInc, d.rules.Reserve, ext, d.rules.WindowUs)
	if err != nil {
		if re, ok := err.(resp.Error); ok && len(re) >= 8 && re[:8] == "NOSCRIPT" {
			if _, lerr := d.p.Do(ctx, "SCRIPT", "LOAD", luaBid); lerr != nil {
				return model.BidResult{}, lerr
			}
		}
		return model.BidResult{}, err
	}
	s, _ := v.(string)
	return ParseBidResult(s)
}

func (d *AuctionRedis) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	v, err := d.p.Do(ctx, "HMGET", "auc:"+strconv.FormatInt(a, 10), "high", "bidder", "deadline", "hard")
	if err != nil {
		return model.AuctionState{}, err
	}
	f, _ := v.([]any)
	if len(f) != 4 {
		return model.AuctionState{}, fmt.Errorf("bad auction reply %v", v)
	}
	n := func(x any) int64 { s, _ := x.(string); i, _ := strconv.ParseInt(s, 10, 64); return i }
	return model.AuctionState{ID: a, High: n(f[0]), Bidder: n(f[1]), DeadlineUs: n(f[2]), HardEndUs: n(f[3])}, nil
}

// SeedRedisAuctions writes the auctions' initial state.
func SeedRedisAuctions(ctx context.Context, p *resp.Pool, states []model.AuctionState) error {
	for _, s := range states {
		if _, err := p.Do(ctx, "HSET", "auc:"+strconv.FormatInt(s.ID, 10), "high", s.High, "bidder", s.Bidder,
			"deadline", s.DeadlineUs, "hard", s.HardEndUs); err != nil {
			return err
		}
	}
	return nil
}
