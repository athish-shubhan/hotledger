package model

import "time"

// Countdown auction (TikTok LIVE Countdown Bidding: Fixed and Extended
// modes, bids are binding). One auction is one hot object: every bid must
// see the current highest bid, so bids on the same auction cannot commute
// the way gifts do.

// AuctionRules is the sequential specification every design implements.
type AuctionRules struct {
	Extended bool  // Extended mode: late bids push the deadline out
	WindowUs int64 // a bid accepted with less than WindowUs left moves the deadline to ts + WindowUs (capped at the hard end)
	MinInc   int64 // a bid must beat the current high by at least MinInc
	Reserve  int64 // the first accepted bid must be at least Reserve
}

func DefaultAuctionRules() AuctionRules {
	return AuctionRules{Extended: true, WindowUs: 2_000_000, MinInc: 1, Reserve: 10}
}

// Bid is one idempotent request; ID is the idempotency key.
type Bid struct {
	ID      int64
	Auction int64
	Bidder  int64
	Amount  int64
}

const (
	ReasonLow    = "low"    // below the current high + MinInc (or the reserve)
	ReasonClosed = "closed" // received after the deadline
)

// BidResult is what the system answers.
type BidResult struct {
	Accepted bool
	Reason   string // "" when accepted
	TsUs     int64  // when the system received the bid (unix microseconds); the deadline is judged on it
	High     int64  // the auction's highest bid after this decision
}

// AuctionState is one auction's current value.
type AuctionState struct {
	ID         int64
	High       int64 // 0 = no bid yet
	Bidder     int64 // 0 = no bid yet
	DeadlineUs int64
	HardEndUs  int64 // extensions never move the deadline past this
}

// Decide applies one bid received at tsUs. Designs that run Go code (row
// lock, single writer) call it directly; the SQL and Lua designs implement
// the same rules in their own language, and the checker uses it as the
// model, so any disagreement shows up as a violation.
func (r AuctionRules) Decide(s AuctionState, b Bid, tsUs int64) (BidResult, AuctionState) {
	if tsUs > s.DeadlineUs {
		return BidResult{Reason: ReasonClosed, TsUs: tsUs, High: s.High}, s
	}
	if b.Amount < r.MinBid(s) {
		return BidResult{Reason: ReasonLow, TsUs: tsUs, High: s.High}, s
	}
	s.High, s.Bidder = b.Amount, b.Bidder
	s.DeadlineUs = r.ExtendedDeadline(s, tsUs)
	return BidResult{Accepted: true, TsUs: tsUs, High: s.High}, s
}

// MinBid is the smallest amount that can be accepted now.
func (r AuctionRules) MinBid(s AuctionState) int64 {
	return max(s.High+r.MinInc, r.Reserve)
}

// ExtendedDeadline is the deadline after a bid accepted at tsUs.
func (r AuctionRules) ExtendedDeadline(s AuctionState, tsUs int64) int64 {
	if r.Extended && s.DeadlineUs-tsUs < r.WindowUs {
		return max(s.DeadlineUs, min(tsUs+r.WindowUs, s.HardEndUs))
	}
	return s.DeadlineUs
}

// AuctionPlan places the auctions of one run on the clock. Auctions 1 and 2
// are the hot ones (the LIVE auction everyone is watching); the rest close
// spread out over [OtherMin, OtherMax] after the epoch.
type AuctionPlan struct {
	N        int
	Hot      time.Duration // deadline of auctions 1 and 2, after the epoch
	OtherMin time.Duration
	OtherMax time.Duration
	HardMax  time.Duration // hard end = deadline + HardMax
}

// Initial is auction id's state at the epoch (unix microseconds).
func (p AuctionPlan) Initial(id int64, epochUs int64) AuctionState {
	var d time.Duration
	if id <= 2 {
		d = p.Hot
	} else {
		span := p.OtherMax - p.OtherMin
		d = p.OtherMin
		if span > 0 {
			d += time.Duration(splitmix(uint64(id)) % uint64(span))
		}
	}
	dl := epochUs + d.Microseconds()
	return AuctionState{ID: id, DeadlineUs: dl, HardEndUs: dl + p.HardMax.Microseconds()}
}

// RecordedBid is a row of the system's own record of decided bids.
type RecordedBid struct {
	ID       int64
	Auction  int64
	Bidder   int64
	Amount   int64
	Accepted bool
	Reason   string
	TsUs     int64
	High     int64
}

// AuctionSnapshot is the final state of every auction plus the bid record.
type AuctionSnapshot struct {
	Auctions map[int64]AuctionState
	Bids     []RecordedBid
}
