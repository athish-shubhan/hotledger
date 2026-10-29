package checker

import (
	"math/rand/v2"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"hotledger/internal/model"
)

// TestPorcupineScale (HOTLEDGER_SCALE=1) shows how Porcupine's time and
// memory grow with history size at high concurrency, and that the chain
// checker stays linear.
func TestPorcupineScale(t *testing.T) {
	if os.Getenv("HOTLEDGER_SCALE") == "" {
		t.Skip("set HOTLEDGER_SCALE=1")
	}
	rules := model.AuctionRules{Extended: true, WindowUs: 6, MinInc: 1, Reserve: 3}
	init := model.AuctionState{ID: 1, DeadlineUs: 1 << 40, HardEndUs: 1 << 41}
	m := auctionModel(rules, map[int64]model.AuctionState{1: init})
	for _, clients := range []int{4, 16, 32} {
		for _, per := range []int{10, 40, 160} {
			r := rand.New(rand.NewPCG(uint64(clients), uint64(per)))
			ops := simulate(r, rules, init, clients, per, 0)
			var ms runtime.MemStats
			runtime.GC()
			t0 := time.Now()
			ok, _ := chainLinearizable(rules, init, ops)
			tc := time.Since(t0)
			t0 = time.Now()
			res := porcupine.CheckOperationsTimeout(m, ops, 20*time.Second)
			tp := time.Since(t0)
			runtime.ReadMemStats(&ms)
			t.Logf("clients=%2d ops=%5d chain=%v in %v; porcupine=%s in %v, heap %d MiB", clients, len(ops), ok, tc, res, tp.Round(time.Millisecond), ms.HeapAlloc>>20)
		}
	}
}
