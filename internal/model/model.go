// Package model holds the types shared by every ledger design, the workload
// generator and the checker.
package model

import "fmt"

// Kind of operation. Gift moves coins from a viewer to a creator (hot credit).
// Withdraw moves coins from a creator to the payout clearing account (hot,
// bounded debit: a creator's balance must never go negative). Purchase moves
// units of a LIVE-shopping SKU from its stock account to a buyer (hot,
// bounded decrement: stock must never go below zero, i.e. no oversell).
type Kind uint8

const (
	Gift Kind = iota + 1
	Withdraw
	Purchase
)

func (k Kind) String() string {
	switch k {
	case Gift:
		return "gift"
	case Withdraw:
		return "withdraw"
	case Purchase:
		return "purchase"
	}
	return fmt.Sprintf("kind(%d)", k)
}

// Semantics selects which LIVE feature a run exercises.
const (
	SemWallet    = "wallet"    // gifts and withdrawals: commutative credits, bounded debits
	SemInventory = "inventory" // flash-sale purchases: bounded decrements on a few SKUs
	SemAuction   = "auction"   // countdown bidding: ordered, highest bid wins, deadline extension
)

// Transfer is one idempotent request. ID is the idempotency key: a design must
// make the transfer take effect at most once no matter how many times the same
// ID is submitted, concurrently or later.
type Transfer struct {
	ID     int64
	Kind   Kind
	Src    int64
	Dst    int64
	Amount int64
}

// Outcome as seen by the client.
type Outcome uint8

const (
	// Unknown: the client never learned the result (error, timeout, crash).
	Unknown Outcome = iota
	// Applied: the system said the transfer took effect.
	Applied
	// Rejected: the system said the transfer was refused (insufficient funds).
	Rejected
	// Enqueued: an asynchronous design accepted the request durably; the
	// result is decided later and must be visible after the system drains.
	Enqueued
)

func (o Outcome) String() string {
	return [...]string{"unknown", "applied", "rejected", "enqueued"}[o]
}

// ParseOutcome is the inverse of String.
func ParseOutcome(s string) Outcome {
	switch s {
	case "applied":
		return Applied
	case "rejected":
		return Rejected
	case "enqueued":
		return Enqueued
	}
	return Unknown
}

// Layout describes the account id space. Viewers are 1..Viewers, creators
// follow, and the payout clearing account is last. Ids are ordered so that a
// transfer always locks a lower id before a higher one (viewer < creator <
// payout), which rules out lock-order deadlocks in the SQL designs.
//
// For the inventory semantics the same id space is reused: "viewers" are
// buyers (they start with zero units and only ever receive), "creators" are
// SKUs (they start with their stock and only ever lose units). Every
// purchase moves units from a SKU (higher id) to a buyer (lower id), so all
// purchases lock in the same order and still cannot deadlock.
type Layout struct {
	Viewers  int64
	Creators int64
	Sem      string // "" or SemWallet, SemInventory (auctions do not use accounts)
	Stock    int64  // inventory: initial units of every SKU except the two flash-sale SKUs
	HotStock int64  // inventory: initial units of SKUs 0 and 1 (the flash-sale items)
}

func (l Layout) Inventory() bool { return l.Sem == SemInventory }

// OnlyDebited reports accounts whose balance can only go down (viewers in the
// wallet, SKUs in inventory). For those, a recorded rejection of an amount
// that is at most the final balance was a false rejection: the funds or the
// stock were there the whole time.
func (l Layout) OnlyDebited(id int64) bool {
	if l.Inventory() {
		return id > l.Viewers && id <= l.Viewers+l.Creators
	}
	return id >= 1 && id <= l.Viewers
}

func (l Layout) Viewer(i int64) int64  { return 1 + i }
func (l Layout) Creator(i int64) int64 { return 1 + l.Viewers + i }
func (l Layout) Payout() int64         { return 1 + l.Viewers + l.Creators }
func (l Layout) NumAccounts() int64    { return l.Viewers + l.Creators + 1 }
func (l Layout) IsCreatorOrPayout(id int64) bool {
	return id > l.Viewers
}

// InitialBalance is deterministic so the checker can recompute the starting
// state without reading it back. 70% of viewers start with 50 coins and 30%
// with 1000, so insufficient-funds rejections actually happen. Creators start
// with 1000 so early withdrawals can succeed. The payout account starts at 0.
func (l Layout) InitialBalance(id int64) int64 {
	if l.Inventory() {
		switch {
		case id == l.Creator(0) || id == l.Creator(1):
			return l.HotStock
		case id > l.Viewers && id <= l.Viewers+l.Creators:
			return l.Stock
		}
		return 0
	}
	switch {
	case id >= 1 && id <= l.Viewers:
		if splitmix(uint64(id))%10 < 7 {
			return 50
		}
		return 1000
	case id > l.Viewers && id <= l.Viewers+l.Creators:
		return 1000
	}
	return 0
}

// TotalInitial is the conserved quantity.
func (l Layout) TotalInitial() int64 {
	var t int64
	for id := int64(1); id <= l.NumAccounts(); id++ {
		t += l.InitialBalance(id)
	}
	return t
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// AppliedTransfer is a row of the system's own record of what took effect.
type AppliedTransfer struct {
	ID      int64
	Src     int64
	Dst     int64
	Amount  int64
	Applied bool // false = recorded as rejected
}

// State is a final snapshot of a design, read after the run drains.
type State struct {
	Balances  map[int64]int64 // summed over escrow slots
	Negative  []string        // any slot or account below zero
	Transfers []AppliedTransfer
	Extra     map[string]string // design-specific notes (e.g. log lag)
}
