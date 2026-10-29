// Package load drives a Ledger with the wallet workload, retries unknown
// outcomes with the same idempotency key, injects duplicate submissions, and
// records the client-side history for the checker.
package load

import (
	"context"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"hotledger/internal/designs"
	"hotledger/internal/history"
	"hotledger/internal/model"
	"hotledger/internal/store"
	"hotledger/internal/workload"
)

type Config struct {
	Workload    workload.Config
	ClientID    int64
	Duration    time.Duration
	Concurrency int
	Rate        float64 // ops/s; 0 = closed loop (each worker sends as fast as it gets answers)
	BurstSecs   int     // open loop only: final seconds at 10x rate aimed at the two battle creators
	DupFrac     float64 // share of ops that are submitted a second time with the same id
	AttemptTO   time.Duration
	Grace       time.Duration // how long retries may continue after the run ends
	Seed        uint64
}

type Summary struct {
	Ops           int64              `json:"ops"`
	DupOps        int64              `json:"dup_ops"`
	Results       map[string]int64   `json:"results"`
	ElapsedSec    float64            `json:"elapsed_sec"`
	ThroughputOps float64            `json:"throughput_ops_per_sec"` // ops with a known outcome / run duration
	LatencyMs     map[string]float64 `json:"latency_ms"`             // over non-duplicate ops with a known outcome
	BurstLatency  map[string]float64 `json:"burst_latency_ms,omitempty"`
	ReadLatency   map[string]float64 `json:"read_latency_ms,omitempty"` // auctions: plain reads
	Retries       int64              `json:"retries"`
	DesignStats   map[string]int64   `json:"design_stats,omitempty"`
}

type statser interface{ Stats() map[string]int64 }

// Run executes the workload and returns the summary.
func Run(ctx context.Context, cfg Config, led designs.Ledger, hw *history.Writer) Summary {
	z := workload.NewZipf(int(cfg.Workload.Layout.Creators), cfg.Workload.Skew)
	start := time.Now()
	end := start.Add(cfg.Duration)
	hard := end.Add(cfg.Grace)

	var (
		seq     atomic.Int64
		retries atomic.Int64
		mu      sync.Mutex
		lats    []time.Duration
		blats   []time.Duration
		results = map[string]int64{}
		ops     int64
		dups    int64
		inTime  int64
	)
	record := func(op history.Op, lat time.Duration, burst bool) {
		hw.Record(op)
		mu.Lock()
		results[op.Result]++
		if op.Dup {
			dups++
		} else {
			ops++
			if op.Result != "unknown" {
				lats = append(lats, lat)
				if burst {
					blats = append(blats, lat)
				}
				if op.Complete <= end.UnixNano() {
					inTime++
				}
			}
		}
		mu.Unlock()
	}

	// submit runs all attempts of one idempotency key.
	submit := func(t model.Transfer) (model.Outcome, int, string) {
		backoff := 20 * time.Millisecond
		dlBackoff := 2 * time.Millisecond
		var lastErr string
		for attempt := 1; ; attempt++ {
			actx, cancel := context.WithTimeout(ctx, cfg.AttemptTO)
			o, err := led.Transfer(actx, t)
			cancel()
			if err == nil {
				return o, attempt, ""
			}
			lastErr = err.Error()
			if time.Now().After(hard) || ctx.Err() != nil {
				return model.Unknown, attempt, lastErr
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

	// Open loop: op i is due at a fixed time regardless of how slow the system
	// is, and latency is measured from that due time (no coordinated omission).
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
			gen := workload.NewGen(cfg.Workload, z, cfg.Seed^uint64(cfg.ClientID)<<32^uint64(w))
			for {
				i := seq.Add(1) - 1
				var t model.Transfer
				invoke := time.Now()
				burst := false
				if total >= 0 {
					if i >= total {
						return
					}
					var at time.Time
					at, burst = due(i)
					if d := time.Until(at); d > 0 {
						time.Sleep(d)
					}
					invoke = at
				} else if time.Now().After(end) || ctx.Err() != nil {
					return
				}
				if burst {
					t = gen.NextBattle()
				} else {
					t = gen.Next()
				}
				t.ID = cfg.ClientID<<40 | i
				dup := gen.Float64() < cfg.DupFrac
				var dupWG sync.WaitGroup
				if dup && gen.Float64() < 0.5 {
					// concurrent duplicate: same key racing the original
					dupWG.Add(1)
					go func() {
						defer dupWG.Done()
						s := time.Now()
						o, att, e := submit(t)
						op := history.FromTransfer(t)
						op.Invoke, op.Complete, op.Result, op.Attempts, op.Dup, op.Err = s.UnixNano(), time.Now().UnixNano(), o.String(), att, true, e
						record(op, 0, false)
					}()
					dup = false
				}
				o, att, e := submit(t)
				done := time.Now()
				op := history.FromTransfer(t)
				op.Invoke, op.Complete, op.Result, op.Attempts, op.Err = invoke.UnixNano(), done.UnixNano(), o.String(), att, e
				record(op, done.Sub(invoke), burst)
				if dup {
					// late duplicate: the same key resubmitted after the answer
					s := time.Now()
					o2, att2, e2 := submit(t)
					op2 := history.FromTransfer(t)
					op2.Invoke, op2.Complete, op2.Result, op2.Attempts, op2.Dup, op2.Err = s.UnixNano(), time.Now().UnixNano(), o2.String(), att2, true, e2
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
	if st, ok := led.(statser); ok {
		s.DesignStats = st.Stats()
	}
	return s
}

func percentiles(d []time.Duration) map[string]float64 {
	if len(d) == 0 {
		return nil
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	q := func(p float64) float64 {
		i := int(p * float64(len(d)-1))
		return float64(d[i]) / 1e6
	}
	return map[string]float64{"p50": q(0.50), "p95": q(0.95), "p99": q(0.99), "p999": q(0.999), "max": q(1)}
}
