// Package checker validates a run: it compares what clients observed (the
// history) with the system's final state and the deterministic initial state.
//
// Invariants:
//
//	I1 conservation        sum of all balances equals the initial total
//	I2 non-negative        no account (or escrow slot) is below zero
//	I3 at-most-once        no idempotency key appears twice in the system record
//	I4 reconciliation      every balance equals initial + applied credits - applied debits
//	                       (catches double-apply and half-applied transfers that I1 misses)
//	I5 no phantoms         every applied transfer was issued by a client with the same src/dst/amount
//	I6 acked => durable    a client told "applied" must find the transfer applied
//	I7 rejected => absent  a client told "rejected" must not find it applied
//	I8 enqueued => decided an async design must decide every request it accepted
//	I9 consistent replies  the same key never got both "applied" and "rejected"
//	I10 sold <= stock      inventory: per SKU, units in applied purchase records <= initial stock
//	I11 no false rejection an account that is only ever debited (a viewer, a SKU) ended with at
//	                       least the amount of a request that was rejected for insufficient funds,
//	                       so the funds or stock were there all along (a lost sale)
//
// I2 is "no oversell" for inventory. Auctions have their own checker
// (auction.go) with Porcupine linearizability.
package checker

import (
	"fmt"
	"sort"
	"strings"

	"hotledger/internal/history"
	"hotledger/internal/model"
)

type Violation struct {
	Invariant string   `json:"invariant"`
	Count     int      `json:"count"`
	Examples  []string `json:"examples"`
}

type Report struct {
	Ops            int            `json:"ops"`
	Results        map[string]int `json:"results"`
	DupSubmissions int            `json:"dup_submissions"`
	Applied        int            `json:"applied_transfers"`
	RecordedReject int            `json:"recorded_rejections"`
	Violations     []Violation    `json:"violations"`
	OK             bool           `json:"ok"`
	// inventory: the two flash-sale SKUs
	HotUnitsSold int64 `json:"hot_units_sold,omitempty"`
	HotUnitsLeft int64 `json:"hot_units_left,omitempty"`
}

type collector struct{ m map[string]*Violation }

func (c *collector) add(inv, format string, a ...any) {
	v := c.m[inv]
	if v == nil {
		v = &Violation{Invariant: inv}
		c.m[inv] = v
	}
	v.Count++
	if len(v.Examples) < 5 {
		v.Examples = append(v.Examples, fmt.Sprintf(format, a...))
	}
}

// Check runs every invariant.
func Check(l model.Layout, ops []history.Op, st model.State) Report {
	c := &collector{m: map[string]*Violation{}}
	r := Report{Ops: len(ops), Results: map[string]int{}}

	// I2
	for _, n := range st.Negative {
		c.add("I2 non-negative", "%s", n)
	}

	// I3, I5 inputs
	issued := make(map[int64]history.Op, len(ops))
	for _, op := range ops {
		r.Results[op.Result]++
		if op.Dup {
			r.DupSubmissions++
		}
		if _, ok := issued[op.ID]; !ok {
			issued[op.ID] = op
		}
	}
	applied := make(map[int64]model.AppliedTransfer, len(st.Transfers))
	recorded := make(map[int64]bool, len(st.Transfers))
	expected := make(map[int64]int64)
	for _, t := range st.Transfers {
		if recorded[t.ID] {
			c.add("I3 at-most-once", "id %d recorded twice", t.ID)
			continue
		}
		recorded[t.ID] = true
		if !t.Applied {
			r.RecordedReject++
			continue
		}
		r.Applied++
		applied[t.ID] = t
		expected[t.Src] -= t.Amount
		expected[t.Dst] += t.Amount
		op, ok := issued[t.ID]
		if !ok {
			c.add("I5 no phantoms", "id %d applied but never issued", t.ID)
		} else if op.Src != t.Src || op.Dst != t.Dst || op.Amount != t.Amount {
			c.add("I5 no phantoms", "id %d applied as %d->%d %d, issued as %d->%d %d", t.ID, t.Src, t.Dst, t.Amount, op.Src, op.Dst, op.Amount)
		}
	}

	// I1, I2, I4
	var total int64
	for id := int64(1); id <= l.NumAccounts(); id++ {
		bal := st.Balances[id]
		total += bal
		if bal < 0 {
			c.add("I2 non-negative", "account %d balance %d", id, bal)
		}
		want := l.InitialBalance(id) + expected[id]
		if bal != want {
			c.add("I4 reconciliation", "account %d balance %d, applied transfers imply %d (diff %+d)", id, bal, want, bal-want)
		}
	}
	for id := range st.Balances {
		if id < 1 || id > l.NumAccounts() {
			c.add("I4 reconciliation", "unknown account %d in state", id)
		}
	}
	if init := l.TotalInitial(); total != init {
		c.add("I1 conservation", "total %d, initial %d (diff %+d)", total, init, total-init)
	}

	// I10
	if l.Inventory() {
		for _, sku := range []int64{l.Creator(0), l.Creator(1)} {
			r.HotUnitsSold += l.InitialBalance(sku) - st.Balances[sku]
			r.HotUnitsLeft += st.Balances[sku]
		}
		sold := map[int64]int64{}
		for _, t := range applied {
			sold[t.Src] += t.Amount
		}
		for sku, n := range sold {
			if stock := l.InitialBalance(sku); n > stock {
				c.add("I10 sold <= stock", "sku %d sold %d units, stock %d (oversold by %d)", sku, n, stock, n-stock)
			}
		}
	}

	// I11: rejections the system recorded, and rejections clients were told.
	falseRej := map[int64]bool{}
	checkRej := func(id, src, amt int64) {
		if falseRej[id] || !l.OnlyDebited(src) {
			return
		}
		if fin := st.Balances[src]; amt <= fin {
			falseRej[id] = true
			c.add("I11 no false rejection", "id %d: %d of account %d rejected, account ended with %d", id, amt, src, fin)
		}
	}
	for _, t := range st.Transfers {
		if !t.Applied {
			checkRej(t.ID, t.Src, t.Amount)
		}
	}
	for _, op := range ops {
		if op.Result == "rejected" {
			checkRej(op.ID, op.Src, op.Amount)
		}
	}

	// I6-I9
	seen := map[int64]string{}
	for _, op := range ops {
		_, isApplied := applied[op.ID]
		switch model.ParseOutcome(op.Result) {
		case model.Applied:
			if !isApplied {
				c.add("I6 acked => durable", "id %d (%s %d->%d %d) acked applied, not in final state", op.ID, op.Kind, op.Src, op.Dst, op.Amount)
			}
		case model.Rejected:
			if isApplied {
				c.add("I7 rejected => absent", "id %d acked rejected but applied", op.ID)
			}
		case model.Enqueued:
			if !recorded[op.ID] {
				c.add("I8 enqueued => decided", "id %d accepted but never decided", op.ID)
			}
		}
		if op.Result == "applied" || op.Result == "rejected" {
			if prev, ok := seen[op.ID]; ok && prev != op.Result {
				c.add("I9 consistent replies", "id %d answered both %s and %s", op.ID, prev, op.Result)
			}
			seen[op.ID] = op.Result
		}
	}

	for _, v := range c.m {
		r.Violations = append(r.Violations, *v)
	}
	sort.Slice(r.Violations, func(i, j int) bool { return r.Violations[i].Invariant < r.Violations[j].Invariant })
	r.OK = len(r.Violations) == 0
	return r
}

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ops=%d results=%v dup_submissions=%d applied=%d recorded_rejections=%d\n", r.Ops, r.Results, r.DupSubmissions, r.Applied, r.RecordedReject)
	if r.OK {
		b.WriteString("ALL INVARIANTS HOLD (I1-I11)\n")
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
