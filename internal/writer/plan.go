package writer

// The in-memory half of a batch: decode, in-batch dedupe, sort the touched
// accounts, apply in log order, and build the column arrays for the two
// set-based statements. It runs on the writer's critical path between the
// account lock and the commit, so its buffers are reused across batches
// (one plan per in-flight partition batch, from a sync.Pool): in the steady
// state it allocates nothing. BenchmarkPlan measures it; the writer's CPU
// profile (`-cpuprofile`) shows where the rest of the time goes.

import (
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/designs"
)

type batchPlan struct {
	cmds   []cmd
	seen   map[int64]struct{}
	exists map[int64]struct{}
	// touched accounts: sorted ids, and each id's position in that order;
	// balances and change flags are dense slices by position, so the apply
	// loop does two map lookups per transfer and the rest is slice indexing
	accts   []int64
	pos     map[int64]int32
	bal     []int64
	changed []bool
	cursor  int

	// columns of the transfer records, in log order
	tid, tsrc, tdst, tamt, tenq, tdone []int64
	tok                                []bool
	// changed balances, in account id order
	uid, ub []int64
}

var plans = sync.Pool{New: func() any {
	return &batchPlan{
		seen: map[int64]struct{}{}, exists: map[int64]struct{}{}, pos: map[int64]int32{},
	}
}}

func getPlan() *batchPlan { return plans.Get().(*batchPlan) }

func putPlan(b *batchPlan) {
	if len(b.cmds) > 1<<16 { // do not keep a pathological batch's buffers alive
		return
	}
	plans.Put(b)
}

// decode keeps the records at or after the stored offset, drops ids seen
// earlier in the batch, and returns the last offset seen (-1 if none).
func (b *batchPlan) decode(recs []*kgo.Record, stored int64) int64 {
	b.cmds = b.cmds[:0]
	clear(b.seen)
	last := int64(-1)
	for _, r := range recs {
		if r.Offset < stored {
			continue
		}
		last = r.Offset
		t, enq, err := designs.DecodeTransfer(r.Value)
		if err != nil {
			continue
		}
		if _, dup := b.seen[t.ID]; dup {
			continue
		}
		b.seen[t.ID] = struct{}{}
		b.cmds = append(b.cmds, cmd{t, enq})
	}
	return last
}

// ids returns the command ids (reusing the tid column as scratch).
func (b *batchPlan) ids() []int64 {
	b.tid = b.tid[:0]
	for _, c := range b.cmds {
		b.tid = append(b.tid, c.t.ID)
	}
	return b.tid
}

// dropExisting removes the commands whose id is in b.exists.
func (b *batchPlan) dropExisting() {
	if len(b.exists) == 0 {
		return
	}
	kept := b.cmds[:0]
	for _, c := range b.cmds {
		if _, ok := b.exists[c.t.ID]; !ok {
			kept = append(kept, c)
		}
	}
	b.cmds = kept
}

// accounts returns every account the commands touch, in id order (the lock
// order shared by all writers), and resets the balances to zero.
func (b *batchPlan) accounts() []int64 {
	clear(b.pos)
	b.accts = b.accts[:0]
	for _, c := range b.cmds {
		for _, id := range [2]int64{c.t.Src, c.t.Dst} {
			if _, ok := b.pos[id]; !ok {
				b.pos[id] = 0
				b.accts = append(b.accts, id)
			}
		}
	}
	slices.Sort(b.accts)
	for i, id := range b.accts {
		b.pos[id] = int32(i)
	}
	b.bal, b.changed = resize(b.bal, len(b.accts)), resize(b.changed, len(b.accts))
	clear(b.bal)
	clear(b.changed)
	b.cursor = 0
	return b.accts
}

// setBalance records a locked row's balance. Rows must arrive in id order
// (ORDER BY id); an account with no row keeps balance 0.
func (b *batchPlan) setBalance(id, bal int64) {
	for b.cursor < len(b.accts) && b.accts[b.cursor] < id {
		b.cursor++
	}
	if b.cursor < len(b.accts) && b.accts[b.cursor] == id {
		b.bal[b.cursor] = bal
	}
}

// apply runs the commands in log order against the locked balances and
// builds the record columns and the changed balances.
func (b *batchPlan) apply(now int64) {
	n := len(b.cmds)
	b.tid, b.tsrc, b.tdst, b.tamt = resize(b.tid, n), resize(b.tsrc, n), resize(b.tdst, n), resize(b.tamt, n)
	b.tenq, b.tdone, b.tok = resize(b.tenq, n), resize(b.tdone, n), resize(b.tok, n)
	for i, c := range b.cmds {
		t := c.t
		si, di := b.pos[t.Src], b.pos[t.Dst]
		ok := b.bal[si] >= t.Amount
		if ok {
			b.bal[si] -= t.Amount
			b.bal[di] += t.Amount
			b.changed[si], b.changed[di] = true, true
		}
		b.tid[i], b.tsrc[i], b.tdst[i], b.tamt[i], b.tok[i], b.tenq[i], b.tdone[i] = t.ID, t.Src, t.Dst, t.Amount, ok, c.enqNs, now
	}
	b.uid, b.ub = b.uid[:0], b.ub[:0]
	for i, id := range b.accts {
		if b.changed[i] {
			b.uid = append(b.uid, id)
			b.ub = append(b.ub, b.bal[i])
		}
	}
}

func resize[T any](s []T, n int) []T {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]T, n, n+n/4)
}
