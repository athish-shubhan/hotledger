package load

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"hotledger/internal/designs"
	"hotledger/internal/history"
	"hotledger/internal/model"
	"hotledger/internal/store"
	"hotledger/internal/workload"
)

// AuctionConfig drives countdown auctions. Workers pick an auction by Zipf
// popularity (the hot LIVE auction is id 1), read it now and then, and bid a
// little above the highest bid they have seen. During the burst every
// request targets auctions 1 and 2: sniping in the last seconds.
type AuctionConfig struct {
	Auctions    int
	Bidders     int64
	Skew        float64
	ReadFrac    float64
	ClientID    int64
	Duration    time.Duration
	Concurrency int
	Rate        float64
	BurstSecs   int
	DupFrac     float64
	AttemptTO   time.Duration
	Grace       time.Duration
	Seed        uint64
}

// RunAuction executes the auction workload and returns the summary.
func RunAuction(ctx context.Context, cfg AuctionConfig, d designs.Auctioneer, hw *history.Writer) Summary {
	z := workload.NewZipf(cfg.Auctions, cfg.Skew)
	start := time.Now()
	end := start.Add(cfg.Duration)
	hard := end.Add(cfg.Grace)

	var (
		seq     atomic.Int64
		retries atomic.Int64
		mu      sync.Mutex
		lats    []time.Duration
		blats   []time.Duration
		rlats   []time.Duration
		results = map[string]int64{}
		ops     int64
		dups    int64
		inTime  int64
	)
	record := func(op history.Op, lat time.Duration, burst bool) {
		hw.Record(op)
		mu.Lock()
		results[op.Kind+":"+op.Result]++
		if op.Dup {
			dups++
		} else {
			ops++
			if op.Result != "unknown" {
				if op.Kind == "read" {
					rlats = append(rlats, lat)
				} else {
					lats = append(lats, lat)
					if burst {
						blats = append(blats, lat)
					}
				}
				if op.Complete <= end.UnixNano() {
					inTime++
				}
			}
		}
		mu.Unlock()
	}

	submit := func(b model.Bid) (model.BidResult, bool, int, string) {
		backoff := 20 * time.Millisecond
		dlBackoff := 2 * time.Millisecond
		var lastErr string
		for attempt := 1; ; attempt++ {
			actx, cancel := context.WithTimeout(ctx, cfg.AttemptTO)
			r, err := d.Bid(actx, b)
			cancel()
			if err == nil {
				return r, true, attempt, ""
			}
			lastErr = err.Error()
			if time.Now().After(hard) || ctx.Err() != nil {
				return r, false, attempt, lastErr
			}
			retries.Add(1)
			if !store.IsRetryable(err) {
				time.Sleep(backoff/2 + time.Duration(rand.Int64N(int64(backoff))))
				if backoff < time.Second {
					backoff *= 2
				}
			} else {
				// Deadlock or serialization failure: nothing committed, retry
				// soon, with full jitter so two victims do not collide again.
				time.Sleep(time.Duration(rand.Int64N(int64(dlBackoff))))
				if dlBackoff < 64*time.Millisecond {
					dlBackoff *= 2
				}
			}
		}
	}
	fill := func(op *history.Op, r model.BidResult, known bool) {
		switch {
		case !known:
			op.Result = "unknown"
		case r.Accepted:
			op.Result = "applied"
		default:
			op.Result = "rejected"
		}
		if known {
			op.Reason, op.TsUs, op.High = r.Reason, r.TsUs, r.High
		}
	}

	var total int64 = -1
	baseOps := int64(0)
	due := func(i int64) (time.Time, bool) { return time.Time{}, false }
	if cfg.Rate > 0 {
		baseSecs := cfg.Duration.Seconds() - float64(cfg.BurstSecs)
		baseOps = int64(cfg.Rate * baseSecs)
		burstOps := int64(cfg.Rate * 10 * float64(cfg.BurstSecs))
		total = baseOps + burstOps
		due = func(i int64) (time.Time, bool) {
			if i < baseOps {
				return start.Add(time.Duration(float64(i) / cfg.Rate * float64(time.Second))), false
			}
			j := i - baseOps
			return start.Add(time.Duration((baseSecs + float64(j)/(cfg.Rate*10)) * float64(time.Second))), true
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.Seed^uint64(cfg.ClientID)<<32^uint64(w), 0xa0761f62))
			known := map[int64]int64{} // highest bid this worker has seen per auction
			for {
				i := seq.Add(1) - 1
				invoke := time.Now()
				burst := false
				if total >= 0 {
					if i >= total {
						return
					}
					var at time.Time
					at, burst = due(i)
					if dl := time.Until(at); dl > 0 {
						time.Sleep(dl)
					}
					invoke = at
				} else if time.Now().After(end) || ctx.Err() != nil {
					return
				}
				var a int64
				if burst {
					a = 1 + rng.Int64N(2)
				} else {
					a = int64(z.Sample(rng)) + 1
				}
				id := cfg.ClientID<<40 | i
				if rng.Float64() < cfg.ReadFrac {
					actx, cancel := context.WithTimeout(ctx, cfg.AttemptTO)
					s, err := d.Read(actx, a)
					cancel()
					done := time.Now()
					op := history.Op{ID: id, Kind: "read", Auction: a, Invoke: invoke.UnixNano(), Complete: done.UnixNano(), Attempts: 1}
					if err != nil {
						op.Result, op.Err = "unknown", err.Error()
					} else {
						op.Result, op.High, op.Leader, op.Deadline = "ok", s.High, s.Bidder, s.DeadlineUs
						if s.High > known[a] {
							known[a] = s.High
						}
					}
					record(op, done.Sub(invoke), burst)
					continue
				}
				b := model.Bid{ID: id, Auction: a, Bidder: 1 + rng.Int64N(cfg.Bidders), Amount: max(known[a], 9) + 1 + rng.Int64N(5)}
				newOp := func() history.Op {
					return history.Op{ID: b.ID, Kind: "bid", Auction: b.Auction, Bidder: b.Bidder, Amount: b.Amount}
				}
				dup := rng.Float64() < cfg.DupFrac
				var dupWG sync.WaitGroup
				if dup && rng.Float64() < 0.5 {
					dupWG.Add(1)
					go func() {
						defer dupWG.Done()
						s := time.Now()
						r, ok, att, e := submit(b)
						op := newOp()
						op.Invoke, op.Complete, op.Attempts, op.Dup, op.Err = s.UnixNano(), time.Now().UnixNano(), att, true, e
						fill(&op, r, ok)
						record(op, 0, false)
					}()
					dup = false
				}
				r, ok, att, e := submit(b)
				done := time.Now()
				op := newOp()
				op.Invoke, op.Complete, op.Attempts, op.Err = invoke.UnixNano(), done.UnixNano(), att, e
				fill(&op, r, ok)
				record(op, done.Sub(invoke), burst)
				if ok && r.High > known[a] {
					known[a] = r.High
				}
				if dup {
					s := time.Now()
					r2, ok2, att2, e2 := submit(b)
					op2 := newOp()
					op2.Invoke, op2.Complete, op2.Attempts, op2.Dup, op2.Err = s.UnixNano(), time.Now().UnixNano(), att2, true, e2
					fill(&op2, r2, ok2)
					record(op2, 0, false)
				}
				dupWG.Wait()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	s := Summary{
		Ops: ops, DupOps: dups, Results: results, ElapsedSec: elapsed.Seconds(),
		ThroughputOps: float64(inTime) / cfg.Duration.Seconds(),
		LatencyMs:     percentiles(lats), Retries: retries.Load(),
	}
	if len(blats) > 0 {
		s.BurstLatency = percentiles(blats)
	}
	if len(rlats) > 0 {
		s.ReadLatency = percentiles(rlats)
	}
	return s
}
