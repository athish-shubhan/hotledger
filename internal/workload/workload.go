// Package workload generates skewed gift/withdraw, flash-sale purchase and
// auction bid traffic.
package workload

import (
	"math"
	"math/rand/v2"
	"sort"

	"hotledger/internal/model"
)

// Zipf samples ranks 0..n-1 with P(r) proportional to 1/(r+1)^s. Unlike
// math/rand's Zipf it accepts s <= 1 (we need s = 0.99).
type Zipf struct{ cdf []float64 }

func NewZipf(n int, s float64) *Zipf {
	cdf := make([]float64, n)
	var sum float64
	for i := 0; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), s)
		cdf[i] = sum
	}
	for i := range cdf {
		cdf[i] /= sum
	}
	return &Zipf{cdf: cdf}
}

func (z *Zipf) Sample(r *rand.Rand) int {
	u := r.Float64()
	return sort.SearchFloat64s(z.cdf, u)
}

// TopShare is the probability mass of the k hottest ranks.
func (z *Zipf) TopShare(k int) float64 { return z.cdf[k-1] }

// Config for the wallet workload.
type Config struct {
	Layout       model.Layout
	Skew         float64 // Zipf s over creators
	WithdrawFrac float64 // share of operations that are creator withdrawals
}

// Gen produces transfers. One Gen per worker goroutine (not thread-safe).
type Gen struct {
	cfg  Config
	zipf *Zipf
	rng  *rand.Rand
}

func NewGen(cfg Config, z *Zipf, seed uint64) *Gen {
	return &Gen{cfg: cfg, zipf: z, rng: rand.New(rand.NewPCG(seed, seed^0x5bd1e995))}
}

var giftAmounts = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 5, 5, 5, 5, 10, 10, 10, 100}

// purchaseQty: most flash-sale orders are for one unit.
var purchaseQty = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 5}

// Next returns a transfer (ID left zero; the caller assigns it).
func (g *Gen) Next() model.Transfer {
	l := g.cfg.Layout
	creator := l.Creator(int64(g.zipf.Sample(g.rng)))
	if l.Inventory() {
		// creator = SKU chosen by Zipf popularity, viewer = buyer
		buyer := l.Viewer(g.rng.Int64N(l.Viewers))
		return model.Transfer{Kind: model.Purchase, Src: creator, Dst: buyer, Amount: purchaseQty[g.rng.IntN(len(purchaseQty))]}
	}
	if g.rng.Float64() < g.cfg.WithdrawFrac {
		return model.Transfer{Kind: model.Withdraw, Src: creator, Dst: l.Payout(), Amount: 50 + g.rng.Int64N(450)}
	}
	viewer := l.Viewer(g.rng.Int64N(l.Viewers))
	return model.Transfer{Kind: model.Gift, Src: viewer, Dst: creator, Amount: giftAmounts[g.rng.IntN(len(giftAmounts))]}
}

// NextBattle returns a gift to one of the two battle creators (ranks 0 and 1),
// the end-of-LIVE-Match surge.
// For inventory it is the flash sale: orders for SKUs 0 and 1.
func (g *Gen) NextBattle() model.Transfer {
	l := g.cfg.Layout
	viewer := l.Viewer(g.rng.Int64N(l.Viewers))
	if l.Inventory() {
		return model.Transfer{Kind: model.Purchase, Src: l.Creator(int64(g.rng.IntN(2))), Dst: viewer, Amount: purchaseQty[g.rng.IntN(len(purchaseQty))]}
	}
	return model.Transfer{Kind: model.Gift, Src: viewer, Dst: l.Creator(int64(g.rng.IntN(2))), Amount: giftAmounts[g.rng.IntN(len(giftAmounts))]}
}

func (g *Gen) Float64() float64 { return g.rng.Float64() }
