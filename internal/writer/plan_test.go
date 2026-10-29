package writer

import (
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/designs"
	"hotledger/internal/model"
	"hotledger/internal/workload"
)

// batch builds n records of the wallet workload at skew s, with dup of them
// repeated (a client retry in the log).
func batch(n int, s float64, dup int, seed uint64) ([]*kgo.Record, map[int64]int64) {
	l := model.Layout{Viewers: 100_000, Creators: 10_000, Sem: model.SemWallet}
	g := workload.NewGen(workload.Config{Layout: l, Skew: s, WithdrawFrac: 0.02}, workload.NewZipf(int(l.Creators), s), seed)
	rng := rand.New(rand.NewPCG(seed, 7))
	var recs []*kgo.Record
	bal := map[int64]int64{}
	for i := 0; i < n; i++ {
		t := g.Next()
		t.ID = int64(i + 1)
		if dup > 0 && i > 0 && rng.IntN(n) < dup {
			t.ID = int64(rng.IntN(i) + 1) // same id as an earlier command
		}
		_, v := designs.EncodeTransfer(t, int64(i))
		recs = append(recs, &kgo.Record{Value: v, Offset: int64(i)})
		bal[t.Src], bal[t.Dst] = 50+int64(t.Src%3)*500, 1000
	}
	return recs, bal
}

func TestPlanMatchesReference(t *testing.T) {
	recs, bal0 := batch(3000, 1.5, 60, 1)
	p := getPlan()
	for round := 0; round < 3; round++ { // reuse must not leak state between batches
		last := p.decode(recs, 100)
		if last != int64(len(recs)-1) {
			t.Fatalf("last = %d", last)
		}
		// reference: the straightforward version
		seen := map[int64]bool{}
		var want []model.Transfer
		for _, r := range recs[100:] {
			tr, _, _ := designs.DecodeTransfer(r.Value)
			if !seen[tr.ID] {
				seen[tr.ID] = true
				want = append(want, tr)
			}
		}
		if len(p.cmds) != len(want) {
			t.Fatalf("round %d: %d cmds, want %d", round, len(p.cmds), len(want))
		}
		ref := map[int64]int64{}
		for _, id := range p.accounts() {
			p.setBalance(id, bal0[id])
			ref[id] = bal0[id]
		}
		p.apply(42)
		for i, tr := range want {
			ok := ref[tr.Src] >= tr.Amount
			if ok {
				ref[tr.Src] -= tr.Amount
				ref[tr.Dst] += tr.Amount
			}
			if p.tid[i] != tr.ID || p.tok[i] != ok || p.tamt[i] != tr.Amount {
				t.Fatalf("round %d cmd %d: got id %d ok %v, want id %d ok %v", round, i, p.tid[i], p.tok[i], tr.ID, ok)
			}
		}
		for i, id := range p.uid {
			if p.ub[i] != ref[id] {
				t.Fatalf("account %d: %d, want %d", id, p.ub[i], ref[id])
			}
			if i > 0 && p.uid[i-1] >= id {
				t.Fatal("changed balances not in id order")
			}
		}
	}
}

// BenchmarkPlan: the writer's in-memory work for one 4,000-record batch
// (the -max-poll default) at Zipf s = 2.0.
func BenchmarkPlan(b *testing.B) {
	recs, bal0 := batch(4000, 2.0, 40, 3)
	p := getPlan()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.decode(recs, 0)
		for _, id := range p.accounts() {
			p.setBalance(id, bal0[id])
		}
		p.apply(1)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/4000, "ns/transfer")
}

// naivePlan is the first version of the same work: fresh maps and slices per
// batch, balances in a map. Kept as the benchmark baseline.
func naivePlan(recs []*kgo.Record, bal0 map[int64]int64) int {
	var cmds []cmd
	seen := map[int64]bool{}
	for _, r := range recs {
		t, enq, err := designs.DecodeTransfer(r.Value)
		if err != nil || seen[t.ID] {
			continue
		}
		seen[t.ID] = true
		cmds = append(cmds, cmd{t, enq})
	}
	touched := map[int64]bool{}
	for _, c := range cmds {
		touched[c.t.Src], touched[c.t.Dst] = true, true
	}
	accts := make([]int64, 0, len(touched))
	for id := range touched {
		accts = append(accts, id)
	}
	sort.Slice(accts, func(i, j int) bool { return accts[i] < accts[j] })
	bal := make(map[int64]int64, len(accts))
	for _, id := range accts {
		bal[id] = bal0[id]
	}
	changed := map[int64]bool{}
	n := len(cmds)
	tid, tsrc, tdst, tamt, tenq, tdone := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
	tok := make([]bool, n)
	for i, c := range cmds {
		t := c.t
		ok := bal[t.Src] >= t.Amount
		if ok {
			bal[t.Src] -= t.Amount
			bal[t.Dst] += t.Amount
			changed[t.Src], changed[t.Dst] = true, true
		}
		tid[i], tsrc[i], tdst[i], tamt[i], tok[i], tenq[i], tdone[i] = t.ID, t.Src, t.Dst, t.Amount, ok, c.enqNs, 1
	}
	var uid, ub []int64
	for _, id := range accts {
		if changed[id] {
			uid, ub = append(uid, id), append(ub, bal[id])
		}
	}
	return len(uid) + len(tid) + len(tsrc) + len(tdst) + len(tamt) + len(tenq) + len(tdone) + len(tok) + len(ub)
}

func BenchmarkPlanNaive(b *testing.B) {
	recs, bal0 := batch(4000, 2.0, 40, 3)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		naivePlan(recs, bal0)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/4000, "ns/transfer")
}
