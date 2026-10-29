package checker

// Auction checker. Invariants over the system's bid record and final state:
//
//	A1 highest wins        each auction's final high and leader are the highest accepted bid in the record
//	A2 deadline rule       replaying accepted bids in order, every one was received before the deadline in
//	                       force and beat the high by the increment, and the deadline extensions reproduce the
//	                       final deadline exactly (Extended mode: a bid in the last window moves the deadline
//	                       to ts + window, never past the hard end)
//	A3 no lost bids        a bid a client was told "accepted" is accepted in the record; a bid told
//	                       "rejected" is not; what the client was told matches what the record says
//	A4 at-most-once        no bid id appears twice in the record
//	A5 received in time    the receive timestamp a design stamped lies inside the client's request interval
//	A6 no phantoms         every recorded bid was issued by a client with the same auction, bidder, amount
//	A7 consistent replies  the same id never got both "accepted" and "rejected"
//	A8 linearizable        the history of bids and reads on each auction is linearizable against
//	                       model.AuctionRules: decided exactly for every auction by the chain checker
//	                       (linchain.go), and by Porcupine for every auction narrow enough for it

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"hotledger/internal/history"
	"hotledger/internal/model"
)

type AuctionReport struct {
	Ops              int            `json:"ops"`
	Bids             int            `json:"bids"`
	Reads            int            `json:"reads"`
	Results          map[string]int `json:"results"`
	DupSubmissions   int            `json:"dup_submissions"`
	Accepted         int            `json:"accepted_bids"`
	RecordedRejected int            `json:"recorded_rejections"`
	ClosedRejections int            `json:"closed_rejections"`
	// closed rejections of bids the client sent before the auction's final
	// deadline: the bid lost to queueing between the client and the point
	// where the design stamps receive time
	ClosedSentInTime int         `json:"closed_but_sent_before_deadline"`
	Extensions       int         `json:"deadline_extensions"`
	LinChecked       int         `json:"lin_auctions_checked"` // exact chain checker, every auction
	LinOK            int         `json:"lin_ok"`
	LinIllegal       []int64     `json:"lin_illegal_auctions,omitempty"`
	LinOpsChecked    int         `json:"lin_ops_checked"`
	LinSeconds       float64     `json:"lin_seconds"`
	PorcupineChecked int         `json:"porcupine_auctions_checked"`
	PorcupineOps     int         `json:"porcupine_ops_checked"`
	PorcupineOK      int         `json:"porcupine_ok"`
	PorcupineIllegal []int64     `json:"porcupine_illegal_auctions,omitempty"`
	PorcupineUnknown int         `json:"porcupine_timeouts"`
	PorcupineSkipped int         `json:"porcupine_skipped_too_concurrent"`
	PorcupineSeconds float64     `json:"porcupine_seconds"`
	Violations       []Violation `json:"violations"`
	OK               bool        `json:"ok"`
}

// LinOptions bounds the Porcupine search.
type LinOptions struct {
	Timeout           time.Duration // per auction
	PorcupineMaxWidth int           // skip auctions with more overlapping operations than this
	PorcupineMaxOps   int
	OutDir            string // write an HTML visualization of every illegal auction here
}

// width is the largest number of operations open at the same instant.
func width(h []porcupine.Operation) int {
	type e struct {
		t int64
		d int
	}
	es := make([]e, 0, 2*len(h))
	for _, op := range h {
		es = append(es, e{op.Call, 1}, e{op.Return, -1})
	}
	sort.Slice(es, func(i, j int) bool {
		if es[i].t != es[j].t {
			return es[i].t < es[j].t
		}
		return es[i].d > es[j].d // closed intervals: calls first
	})
	w, best := 0, 0
	for _, x := range es {
		w += x.d
		best = max(best, w)
	}
	return best
}

// CheckAuction runs A1-A8.
func CheckAuction(rules model.AuctionRules, init map[int64]model.AuctionState, ops []history.Op, snap model.AuctionSnapshot, lo LinOptions) AuctionReport {
	c := &collector{m: map[string]*Violation{}}
	r := AuctionReport{Ops: len(ops), Results: map[string]int{}}

	// Client ops grouped by id (original and duplicate submissions).
	type group struct {
		first     history.Op
		attempts  []history.Op
		minInvoke int64
		maxDone   int64
		known     []history.Op
	}
	groups := map[int64]*group{}
	var reads []history.Op
	for _, op := range ops {
		r.Results[op.Kind+":"+op.Result]++
		if op.Kind == "read" {
			r.Reads++
			if op.Result == "ok" {
				reads = append(reads, op)
			}
			continue
		}
		r.Bids++
		if op.Dup {
			r.DupSubmissions++
		}
		g := groups[op.ID]
		if g == nil {
			g = &group{first: op, minInvoke: op.Invoke, maxDone: op.Complete}
			groups[op.ID] = g
		}
		g.attempts = append(g.attempts, op)
		g.minInvoke = min(g.minInvoke, op.Invoke)
		g.maxDone = max(g.maxDone, op.Complete)
		if op.Result == "applied" || op.Result == "rejected" {
			g.known = append(g.known, op)
		}
	}

	// A4, A6
	rec := make(map[int64]model.RecordedBid, len(snap.Bids))
	byAuction := map[int64][]model.RecordedBid{}
	for _, b := range snap.Bids {
		if _, dup := rec[b.ID]; dup {
			c.add("A4 at-most-once", "bid %d recorded twice", b.ID)
			continue
		}
		rec[b.ID] = b
		if b.Accepted {
			r.Accepted++
			byAuction[b.Auction] = append(byAuction[b.Auction], b)
		} else {
			r.RecordedRejected++
			if b.Reason == model.ReasonClosed {
				r.ClosedRejections++
			}
		}
		g, ok := groups[b.ID]
		if !ok {
			c.add("A6 no phantoms", "bid %d recorded but never issued", b.ID)
			continue
		}
		if g.first.Auction != b.Auction || g.first.Bidder != b.Bidder || g.first.Amount != b.Amount {
			c.add("A6 no phantoms", "bid %d recorded as auction %d bidder %d amount %d, issued as %d/%d/%d",
				b.ID, b.Auction, b.Bidder, b.Amount, g.first.Auction, g.first.Bidder, g.first.Amount)
		}
		// A5: one host, one clock; allow 2 ms for clock reads on either side.
		const slack = int64(2 * time.Millisecond)
		if ts := b.TsUs * 1000; ts < g.minInvoke-slack || ts > g.maxDone+slack {
			c.add("A5 received in time", "bid %d stamped %d us, outside its request interval [%d, %d] us",
				b.ID, b.TsUs, g.minInvoke/1000, g.maxDone/1000)
		}
	}

	for id, g := range groups {
		b, ok := rec[id]
		if !ok || b.Accepted || b.Reason != model.ReasonClosed {
			continue
		}
		if fin, ok := snap.Auctions[b.Auction]; ok && g.minInvoke/1000 < fin.DeadlineUs {
			r.ClosedSentInTime++
		}
	}

	// A1, A2
	ids := make([]int64, 0, len(init))
	for id := range init {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, a := range ids {
		s := init[a]
		acc := byAuction[a]
		sort.Slice(acc, func(i, j int) bool { return acc[i].Amount < acc[j].Amount })
		for _, b := range acc {
			if b.TsUs > s.DeadlineUs {
				c.add("A2 deadline rule", "auction %d: bid %d accepted at %d us, after the deadline %d us", a, b.ID, b.TsUs, s.DeadlineUs)
			}
			if b.Amount < rules.MinBid(s) {
				c.add("A2 deadline rule", "auction %d: bid %d of %d accepted below the minimum %d", a, b.ID, b.Amount, rules.MinBid(s))
			}
			nd := rules.ExtendedDeadline(s, b.TsUs)
			if nd != s.DeadlineUs {
				r.Extensions++
			}
			s.High, s.Bidder, s.DeadlineUs = b.Amount, b.Bidder, nd
		}
		fin, ok := snap.Auctions[a]
		if !ok {
			c.add("A1 highest wins", "auction %d missing from the final state", a)
			continue
		}
		if fin.High != s.High || fin.Bidder != s.Bidder {
			c.add("A1 highest wins", "auction %d ended at %d by bidder %d; highest accepted bid in the record is %d by %d",
				a, fin.High, fin.Bidder, s.High, s.Bidder)
		}
		if fin.DeadlineUs != s.DeadlineUs {
			c.add("A2 deadline rule", "auction %d final deadline %d us, the extension rule over the accepted bids gives %d us (diff %+d ms)",
				a, fin.DeadlineUs, s.DeadlineUs, (fin.DeadlineUs-s.DeadlineUs)/1000)
		}
	}

	// A3, A7
	for id, g := range groups {
		b, inRec := rec[id]
		seen := ""
		for _, op := range g.known {
			if seen != "" && seen != op.Result {
				c.add("A7 consistent replies", "bid %d answered both %s and %s", id, seen, op.Result)
			}
			seen = op.Result
			switch {
			case op.Result == "applied" && !(inRec && b.Accepted):
				c.add("A3 no lost bids", "bid %d (auction %d, %d) acked accepted, not accepted in the final record", id, op.Auction, op.Amount)
			case op.Result == "rejected" && inRec && b.Accepted:
				c.add("A3 no lost bids", "bid %d acked rejected (%s) but accepted in the record", id, op.Reason)
			case inRec && (op.TsUs != b.TsUs || op.High != b.High || op.Reason != b.Reason):
				c.add("A3 no lost bids", "bid %d reply (ts %d, high %d, %q) differs from the record (ts %d, high %d, %q)",
					id, op.TsUs, op.High, op.Reason, b.TsUs, b.High, b.Reason)
			}
		}
	}

	// A8: Porcupine, one auction at a time.
	hist := map[int64][]porcupine.Operation{}
	for id, g := range groups {
		in := bidIn{auction: g.first.Auction, bidder: g.first.Bidder, amount: g.first.Amount}
		var out bidOut
		ret := int64(math.MaxInt64)
		if len(g.known) > 0 {
			k := g.known[0]
			out = bidOut{accepted: k.Result == "applied", reason: k.Reason, ts: k.TsUs, high: k.High}
			for _, op := range g.known {
				ret = min(ret, op.Complete)
			}
		} else if b, ok := rec[id]; ok {
			// The client never learned the outcome but the system decided it:
			// the bid took effect somewhere after its invocation.
			out = bidOut{accepted: b.Accepted, reason: b.Reason, ts: b.TsUs, high: b.High}
		} else {
			continue // never took effect
		}
		hist[in.auction] = append(hist[in.auction], porcupine.Operation{ClientId: int(id & 0xff), Input: in, Output: out, Call: g.minInvoke, Return: ret})
	}
	for _, op := range reads {
		hist[op.Auction] = append(hist[op.Auction], porcupine.Operation{ClientId: int(op.ID & 0xff),
			Input: readIn{auction: op.Auction}, Output: readOut{high: op.High, leader: op.Leader, deadline: op.Deadline},
			Call: op.Invoke, Return: op.Complete})
	}
	// A8 exact, on every auction.
	aucs := make([]int64, 0, len(hist))
	for a := range hist {
		aucs = append(aucs, a)
	}
	sort.Slice(aucs, func(i, j int) bool { return aucs[i] < aucs[j] })
	start := time.Now()
	for _, a := range aucs {
		h := hist[a]
		r.LinChecked++
		r.LinOpsChecked += len(h)
		if ok, why := chainLinearizable(rules, init[a], h); ok {
			r.LinOK++
		} else {
			r.LinIllegal = append(r.LinIllegal, a)
			c.add("A8 linearizable", "auction %d (%d ops): %s", a, len(h), why)
		}
	}
	r.LinSeconds = time.Since(start).Seconds()

	// A8 again with Porcupine where it is tractable: its search is
	// exponential in the number of overlapping operations.
	start = time.Now()
	m := auctionModel(rules, init)
	for _, a := range aucs {
		h := hist[a]
		if width(h) > lo.PorcupineMaxWidth || len(h) > lo.PorcupineMaxOps {
			r.PorcupineSkipped++
			continue
		}
		res, info := porcupine.CheckOperationsVerbose(m, h, lo.Timeout)
		r.PorcupineChecked++
		r.PorcupineOps += len(h)
		switch res {
		case porcupine.Ok:
			r.PorcupineOK++
		case porcupine.Illegal:
			r.PorcupineIllegal = append(r.PorcupineIllegal, a)
			c.add("A8 linearizable (Porcupine)", "auction %d: %d operations, no linearization exists", a, len(h))
			if lo.OutDir != "" {
				if f, err := os.Create(filepath.Join(lo.OutDir, fmt.Sprintf("porcupine-auction-%d.html", a))); err == nil {
					porcupine.Visualize(m, info, f)
					f.Close()
				}
			}
		default:
			r.PorcupineUnknown++
		}
	}
	r.PorcupineSeconds = time.Since(start).Seconds()

	for _, v := range c.m {
		r.Violations = append(r.Violations, *v)
	}
	sort.Slice(r.Violations, func(i, j int) bool { return r.Violations[i].Invariant < r.Violations[j].Invariant })
	r.OK = len(r.Violations) == 0
	return r
}

func (r AuctionReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ops=%d bids=%d reads=%d results=%v dup_submissions=%d accepted=%d recorded_rejections=%d (closed %d) extensions=%d\n",
		r.Ops, r.Bids, r.Reads, r.Results, r.DupSubmissions, r.Accepted, r.RecordedRejected, r.ClosedRejections, r.Extensions)
	fmt.Fprintf(&b, "linearizability (exact chain check): %d auctions, %d ops, ok=%d illegal=%v in %.2fs\n",
		r.LinChecked, r.LinOpsChecked, r.LinOK, r.LinIllegal, r.LinSeconds)
	fmt.Fprintf(&b, "linearizability (Porcupine): %d auctions, %d ops, ok=%d illegal=%v timeouts=%d skipped (too concurrent)=%d in %.1fs\n",
		r.PorcupineChecked, r.PorcupineOps, r.PorcupineOK, r.PorcupineIllegal, r.PorcupineUnknown, r.PorcupineSkipped, r.PorcupineSeconds)
	if r.OK {
		b.WriteString("ALL AUCTION INVARIANTS HOLD (A1-A8)\n")
		return b.String()
	}
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "VIOLATION %s: %d\n", v.Invariant, v.Count)
		for _, e := range v.Examples {
			fmt.Fprintf(&b, "    %s\n", e)
		}
	}
	return b.String()
}

// --- Porcupine model ---------------------------------------------------------

type bidIn struct{ auction, bidder, amount int64 }
type bidOut struct {
	accepted bool
	reason   string
	ts, high int64
}
type readIn struct{ auction int64 }
type readOut struct{ high, leader, deadline int64 }

// aucState is one auction (Porcupine checks one auction per call). The zero
// value means "not initialised yet": the first step looks the auction up.
type aucState struct {
	init                         bool
	high, bidder, deadline, hard int64
}

func auctionModel(rules model.AuctionRules, init map[int64]model.AuctionState) porcupine.Model {
	load := func(s aucState, a int64) aucState {
		if s.init {
			return s
		}
		i := init[a]
		return aucState{init: true, high: i.High, bidder: i.Bidder, deadline: i.DeadlineUs, hard: i.HardEndUs}
	}
	return porcupine.Model{
		Init: func() interface{} { return aucState{} },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			s := state.(aucState)
			switch in := input.(type) {
			case bidIn:
				s = load(s, in.auction)
				o := output.(bidOut)
				res, ns := rules.Decide(model.AuctionState{High: s.high, Bidder: s.bidder, DeadlineUs: s.deadline, HardEndUs: s.hard},
					model.Bid{Bidder: in.bidder, Amount: in.amount}, o.ts)
				if res.Accepted != o.accepted || res.Reason != o.reason || res.High != o.high {
					return false, state
				}
				return true, aucState{init: true, high: ns.High, bidder: ns.Bidder, deadline: ns.DeadlineUs, hard: s.hard}
			case readIn:
				s = load(s, in.auction)
				o := output.(readOut)
				return o.high == s.high && o.leader == s.bidder && o.deadline == s.deadline, s
			}
			return false, state
		},
		Hash: func(state interface{}) uint64 {
			s := state.(aucState)
			return uint64(s.high)*0x9e3779b97f4a7c15 ^ uint64(s.deadline)
		},
		DescribeOperation: func(input, output interface{}) string {
			switch in := input.(type) {
			case bidIn:
				o := output.(bidOut)
				if o.accepted {
					return fmt.Sprintf("bid(%d by %d) -> accepted @%d", in.amount, in.bidder, o.ts)
				}
				return fmt.Sprintf("bid(%d by %d) -> %s @%d (high %d)", in.amount, in.bidder, o.reason, o.ts, o.high)
			case readIn:
				o := output.(readOut)
				return fmt.Sprintf("read() -> %d by %d, deadline %d", o.high, o.leader, o.deadline)
			}
			return "?"
		},
		DescribeState: func(state interface{}) string {
			s := state.(aucState)
			return fmt.Sprintf("high %d by %d, deadline %d", s.high, s.bidder, s.deadline)
		},
	}
}
