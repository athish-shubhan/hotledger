// Package history records what every client saw, one JSON object per line,
// so the checker can compare client observations with the final system state.
package history

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"

	"hotledger/internal/model"
)

// Op is one client operation (all attempts of one idempotency key).
type Op struct {
	ID       int64  `json:"id"`
	Kind     string `json:"kind"`
	Src      int64  `json:"src"`
	Dst      int64  `json:"dst"`
	Amount   int64  `json:"amt"`
	Invoke   int64  `json:"t0"` // unix nanos, intended start (open loop) or actual start
	Complete int64  `json:"t1"` // unix nanos
	Result   string `json:"res"`
	Attempts int    `json:"att"`
	Dup      bool   `json:"dup,omitempty"` // a deliberate duplicate submission of an earlier id
	Err      string `json:"err,omitempty"`

	// Auctions. Kind is "bid" or "read"; Amount is the bid amount. Result is
	// "applied" (accepted), "rejected", "ok" (read) or "unknown".
	Auction  int64  `json:"auc,omitempty"`
	Bidder   int64  `json:"bidder,omitempty"`
	Reason   string `json:"reason,omitempty"` // rejected bid: low | closed
	TsUs     int64  `json:"ts,omitempty"`     // bid: when the system says it received it
	High     int64  `json:"high,omitempty"`   // bid: high after the decision; read: current high
	Leader   int64  `json:"leader,omitempty"` // read: current highest bidder
	Deadline int64  `json:"dl,omitempty"`     // read: current deadline (unix us)
}

// Writer is safe for concurrent use.
type Writer struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
	e  *json.Encoder
}

func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	return &Writer{f: f, w: w, e: json.NewEncoder(w)}, nil
}

func (h *Writer) Record(op Op) {
	h.mu.Lock()
	_ = h.e.Encode(op)
	h.mu.Unlock()
}

func (h *Writer) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.w.Flush(); err != nil {
		return err
	}
	if err := h.f.Sync(); err != nil {
		return err
	}
	return h.f.Close()
}

// Read loads every op from the given files.
func Read(paths ...string) ([]Op, error) {
	var ops []Op
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var op Op
			if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
				// A client killed with -9 can leave a torn last line. Skip it:
				// that op's outcome is unknown to us, which is the same as the
				// client never recording it.
				continue
			}
			ops = append(ops, op)
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	return ops, nil
}

func FromTransfer(t model.Transfer) Op {
	return Op{ID: t.ID, Kind: t.Kind.String(), Src: t.Src, Dst: t.Dst, Amount: t.Amount}
}
