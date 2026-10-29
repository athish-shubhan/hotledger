package checker

import (
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"hotledger/internal/model"
)

// simulate builds a random concurrent history of one auction served by an
// implementation that applies each operation atomically at a point inside
// its interval. bug > 0 makes that fraction of operations compute their
// answer from a stale state (the state before the most recent accepted bid),
// which may or may not be explainable by some other linearization.
func simulate(r *rand.Rand, rules model.AuctionRules, init model.AuctionState, clients, perClient int, bug float64) []porcupine.Operation {
	type ev struct {
		call, apply, ret int64
		read             bool
		bidder, amount   int64
		ts               int64
	}
	var evs []ev
	for c := 0; c < clients; c++ {
		t := int64(r.IntN(5))
		for k := 0; k < perClient; k++ {
			call := t
			apply := call + 1 + int64(r.IntN(10))
			ret := apply + int64(r.IntN(10))
			e := ev{call: call, apply: apply, ret: ret, read: r.Float64() < 0.25, bidder: int64(c + 1)}
			e.ts = call + int64(r.IntN(int(apply-call)+1))
			evs = append(evs, e)
			t = ret + int64(r.IntN(3))
		}
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].apply < evs[j].apply })
	s, prev := init, init
	var ops []porcupine.Operation
	for i, e := range evs {
		view := s
		if r.Float64() < bug {
			view = prev
		}
		if e.read {
			ops = append(ops, porcupine.Operation{ClientId: i, Input: readIn{auction: 1},
				Output: readOut{high: view.High, leader: view.Bidder, deadline: view.DeadlineUs}, Call: e.call, Return: e.ret})
			continue
		}
		amt := max(s.High, rules.Reserve-1) + int64(r.IntN(5)) - 1
		res, ns := rules.Decide(view, model.Bid{Bidder: e.bidder, Amount: amt}, e.ts)
		if res.Accepted {
			if amt > s.High {
				prev, s = s, ns
			}
		}
		ops = append(ops, porcupine.Operation{ClientId: i, Input: bidIn{auction: 1, bidder: e.bidder, amount: amt},
			Output: bidOut{accepted: res.Accepted, reason: res.Reason, ts: res.TsUs, high: res.High}, Call: e.call, Return: e.ret})
	}
	return ops
}

// The exact chain checker must agree with Porcupine on every history.
func TestChainCheckerAgreesWithPorcupine(t *testing.T) {
	rules := model.AuctionRules{Extended: true, WindowUs: 6, MinInc: 1, Reserve: 3}
	init := model.AuctionState{ID: 1, DeadlineUs: 40, HardEndUs: 60}
	m := auctionModel(rules, map[int64]model.AuctionState{1: init})
	r := rand.New(rand.NewPCG(1, 2))
	counts := map[[2]bool]int{}
	for i := 0; i < 4000; i++ {
		bug := 0.0
		if i%2 == 1 {
			bug = 0.15
		}
		ops := simulate(r, rules, init, 2+r.IntN(4), 3+r.IntN(5), bug)
		chain, why := chainLinearizable(rules, init, ops)
		res := porcupine.CheckOperationsTimeout(m, ops, 10*time.Second)
		if res == porcupine.Unknown {
			t.Fatalf("porcupine timed out on %d ops", len(ops))
		}
		pc := res == porcupine.Ok
		counts[[2]bool{chain, pc}]++
		if chain != pc {
			t.Fatalf("history %d (%d ops): chain=%v (%s) porcupine=%v\n%v", i, len(ops), chain, why, pc, ops)
		}
	}
	if counts[[2]bool{false, false}] < 100 || counts[[2]bool{true, true}] < 100 {
		t.Fatalf("too few histories of one kind: %v", counts)
	}
	t.Logf("agreement on 4000 histories: %d linearizable, %d not", counts[[2]bool{true, true}], counts[[2]bool{false, false}])
}
