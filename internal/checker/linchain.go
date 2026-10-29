package checker

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"

	"hotledger/internal/model"
)

// chainLinearizable decides linearizability of one auction's history
// exactly, in O(n log n), using the structure of the object:
//
//  1. Only accepted bids change the state, and each must beat the previous
//     high, so in any linearization the accepted bids appear in increasing
//     amount order. That fixes the sequence of states s_0, s_1, ..., s_n
//     (replayed with AuctionRules.Decide; a bid that the rules would not
//     accept in its predecessor state makes the history illegal).
//  2. Every other operation (a rejected bid, a read) changes nothing and
//     reports the high it saw, which names exactly one state s_j; its
//     output must be what the rules give in s_j.
//  3. What remains is timing. With p_i the linearization point of accepted
//     bid i: call_i <= p_i <= ret_i, p_1 <= ... <= p_n, and an observer of
//     s_j (interval [call, ret]) needs p_j <= ret and p_{j+1} >= call. These
//     are lower bounds from the left and upper bounds per point, so the
//     greedy earliest assignment p_i = max(call_i, p_{i-1}, L_i) is feasible
//     iff a linearization exists.
//
// Porcupine solves the general problem (NP-complete) and runs out of memory
// on a hot auction with tens of thousands of concurrent bids; this checker is
// cross-validated against Porcupine on random small histories
// (linchain_test.go) and Porcupine still runs on every auction small enough.
func chainLinearizable(rules model.AuctionRules, init model.AuctionState, ops []porcupine.Operation) (bool, string) {
	type acc struct {
		in        bidIn
		out       bidOut
		call, ret int64
	}
	var accepted []acc
	type obs struct {
		high      int64
		check     func(s model.AuctionState) bool
		call, ret int64
		desc      string
	}
	var observers []obs
	for _, op := range ops {
		switch in := op.Input.(type) {
		case bidIn:
			o := op.Output.(bidOut)
			if o.accepted {
				accepted = append(accepted, acc{in, o, op.Call, op.Return})
				continue
			}
			observers = append(observers, obs{high: o.high, call: op.Call, ret: op.Return,
				desc: fmt.Sprintf("bid %d by %d rejected %s at high %d", in.amount, in.bidder, o.reason, o.high),
				check: func(s model.AuctionState) bool {
					r, _ := rules.Decide(s, model.Bid{Bidder: in.bidder, Amount: in.amount}, o.ts)
					return !r.Accepted && r.Reason == o.reason && r.High == o.high
				}})
		case readIn:
			o := op.Output.(readOut)
			observers = append(observers, obs{high: o.high, call: op.Call, ret: op.Return,
				desc: fmt.Sprintf("read saw %d by %d, deadline %d", o.high, o.leader, o.deadline),
				check: func(s model.AuctionState) bool {
					return s.High == o.high && s.Bidder == o.leader && s.DeadlineUs == o.deadline
				}})
		}
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].in.amount < accepted[j].in.amount })
	n := len(accepted)
	states := make([]model.AuctionState, n+1)
	states[0] = init
	index := map[int64]int{init.High: 0}
	for i, a := range accepted {
		r, ns := rules.Decide(states[i], model.Bid{Bidder: a.in.bidder, Amount: a.in.amount}, a.out.ts)
		if !r.Accepted || r.High != a.out.high {
			return false, fmt.Sprintf("accepted bid %d by %d (received %d us) cannot follow state high %d deadline %d: the rules give %+v",
				a.in.amount, a.in.bidder, a.out.ts, states[i].High, states[i].DeadlineUs, r)
		}
		if _, dup := index[ns.High]; dup {
			return false, fmt.Sprintf("two accepted bids of %d", ns.High)
		}
		states[i+1] = ns
		index[ns.High] = i + 1
	}
	// lower bound on p_i from observers of state i-1, upper bound on p_i from observers of state i
	lo := make([]int64, n+2)
	hi := make([]int64, n+2)
	for i := range hi {
		lo[i], hi[i] = -1<<62, 1<<62
	}
	for _, o := range observers {
		j, ok := index[o.high]
		if !ok {
			return false, fmt.Sprintf("%s: no accepted bid ever set high %d", o.desc, o.high)
		}
		if !o.check(states[j]) {
			return false, fmt.Sprintf("%s: inconsistent with state high %d by %d, deadline %d", o.desc, states[j].High, states[j].Bidder, states[j].DeadlineUs)
		}
		hi[j] = min(hi[j], o.ret)
		lo[j+1] = max(lo[j+1], o.call)
	}
	prev := int64(-1 << 62)
	for i, a := range accepted {
		k := i + 1
		p := max(a.call, prev, lo[k])
		if p > a.ret || p > hi[k] {
			return false, fmt.Sprintf("accepted bid %d (interval [%d, %d]) cannot be placed: it must follow time %d and precede %d",
				a.in.amount, a.call, a.ret, max(prev, lo[k]), min(a.ret, hi[k]))
		}
		prev = p
	}
	return true, ""
}
