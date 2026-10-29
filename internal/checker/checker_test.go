package checker

import (
	"testing"

	"hotledger/internal/history"
	"hotledger/internal/model"
)

var l = model.Layout{Viewers: 4, Creators: 2}

// apply builds a consistent final state from a set of applied transfers.
func apply(ts ...model.AppliedTransfer) model.State {
	st := model.State{Balances: map[int64]int64{}}
	for id := int64(1); id <= l.NumAccounts(); id++ {
		st.Balances[id] = l.InitialBalance(id)
	}
	for _, t := range ts {
		st.Transfers = append(st.Transfers, t)
		if t.Applied {
			st.Balances[t.Src] -= t.Amount
			st.Balances[t.Dst] += t.Amount
		}
	}
	return st
}

func op(id, src, dst, amt int64, res string) history.Op {
	return history.Op{ID: id, Kind: "gift", Src: src, Dst: dst, Amount: amt, Result: res}
}

func has(r Report, inv string) bool {
	for _, v := range r.Violations {
		if v.Invariant == inv {
			return true
		}
	}
	return false
}

func TestCleanRun(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 10, Applied: true},
		model.AppliedTransfer{ID: 2, Src: 2, Dst: 6, Amount: 5000, Applied: false})
	ops := []history.Op{op(1, 1, 5, 10, "applied"), op(2, 2, 6, 5000, "rejected"), op(1, 1, 5, 10, "applied")}
	if r := Check(l, ops, st); !r.OK {
		t.Fatalf("expected OK, got\n%s", r)
	}
}

func TestDoubleApplyCaughtByReconciliation(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 10, Applied: true})
	st.Balances[1] -= 10 // the debit ran twice, the record only once
	st.Balances[5] += 10
	r := Check(l, []history.Op{op(1, 1, 5, 10, "applied")}, st)
	if !has(r, "I4 reconciliation") {
		t.Fatalf("double apply not caught:\n%s", r)
	}
	if has(r, "I1 conservation") {
		t.Fatalf("conservation should still hold for a balanced double apply")
	}
}

func TestHalfAppliedBreaksConservation(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 10, Applied: true})
	st.Balances[5] -= 10 // credit lost
	r := Check(l, []history.Op{op(1, 1, 5, 10, "applied")}, st)
	if !has(r, "I1 conservation") || !has(r, "I4 reconciliation") {
		t.Fatalf("half apply not caught:\n%s", r)
	}
}

func TestLostAck(t *testing.T) {
	st := apply()
	r := Check(l, []history.Op{op(1, 1, 5, 10, "applied")}, st)
	if !has(r, "I6 acked => durable") {
		t.Fatalf("lost ack not caught:\n%s", r)
	}
}

func TestRejectedButApplied(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 10, Applied: true})
	r := Check(l, []history.Op{op(1, 1, 5, 10, "rejected")}, st)
	if !has(r, "I7 rejected => absent") {
		t.Fatalf("not caught:\n%s", r)
	}
}

func TestNegativeAndPhantomAndDupRecord(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 60, Applied: true},
		model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 60, Applied: true},
		model.AppliedTransfer{ID: 9, Src: 3, Dst: 6, Amount: 1, Applied: true})
	// viewer 1 starts with 50 or 1000 depending on hash; force negative:
	st.Balances[2] = -1
	r := Check(l, []history.Op{op(1, 1, 5, 60, "applied")}, st)
	for _, inv := range []string{"I2 non-negative", "I3 at-most-once", "I5 no phantoms"} {
		if !has(r, inv) {
			t.Fatalf("%s not caught:\n%s", inv, r)
		}
	}
}

func TestEnqueuedLostAndInconsistentReplies(t *testing.T) {
	st := apply(model.AppliedTransfer{ID: 1, Src: 1, Dst: 5, Amount: 1, Applied: true})
	ops := []history.Op{op(1, 1, 5, 1, "applied"), op(1, 1, 5, 1, "rejected"), op(7, 2, 5, 1, "enqueued")}
	r := Check(l, ops, st)
	if !has(r, "I8 enqueued => decided") || !has(r, "I9 consistent replies") {
		t.Fatalf("not caught:\n%s", r)
	}
}
