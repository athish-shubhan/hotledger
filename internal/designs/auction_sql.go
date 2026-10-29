package designs

import (
	"context"
	"database/sql"
	"fmt"

	"hotledger/internal/model"
	"hotledger/internal/store"
)

// Auctioneer places idempotent bids on countdown auctions. Returning an error
// means the client does not know the outcome; the caller retries with the
// same bid ID.
type Auctioneer interface {
	Bid(ctx context.Context, b model.Bid) (model.BidResult, error)
	Read(ctx context.Context, auction int64) (model.AuctionState, error)
	Close() error
}

// ---------------------------------------------------------------------------
// Design 1 for auctions: SELECT ... FOR UPDATE on the auction row, decide in
// Go with model.AuctionRules, write the row and the bid record, commit. The
// receive time is the transaction's start (transaction_timestamp()).

type AuctionRowLock struct {
	db     *sql.DB
	rules  model.AuctionRules
	noLock bool // mutant "nolock": plain SELECT, so concurrent bids overwrite each other
}

func NewAuctionRowLock(db *sql.DB, rules model.AuctionRules, mutant string) *AuctionRowLock {
	return &AuctionRowLock{db: db, rules: rules, noLock: mutant == "nolock"}
}
func (d *AuctionRowLock) Close() error { return d.db.Close() }

func (d *AuctionRowLock) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	return store.ReadAuction(ctx, d.db, a)
}

func (d *AuctionRowLock) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BidResult{}, err
	}
	defer tx.Rollback()
	if r, ok, err := store.LookupBid(ctx, tx, b.ID); err != nil || ok {
		return r, err
	}
	q := `SELECT high, bidder, deadline_us, hard_end_us, (extract(epoch FROM transaction_timestamp()) * 1000000)::bigint
	      FROM auctions WHERE id = $1 FOR UPDATE`
	if d.noLock {
		q = `SELECT high, bidder, deadline_us, hard_end_us, (extract(epoch FROM transaction_timestamp()) * 1000000)::bigint
	      FROM auctions WHERE id = $1`
	}
	s := model.AuctionState{ID: b.Auction}
	var ts int64
	if err := tx.QueryRowContext(ctx, q, b.Auction).Scan(&s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs, &ts); err != nil {
		return model.BidResult{}, err
	}
	res, ns := d.rules.Decide(s, b, ts)
	if res.Accepted {
		if _, err := tx.ExecContext(ctx, `UPDATE auctions SET high = $2, bidder = $3, deadline_us = $4 WHERE id = $1`,
			b.Auction, ns.High, ns.Bidder, ns.DeadlineUs); err != nil {
			return model.BidResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, b.ID, b.Auction, b.Bidder, b.Amount, res.Accepted, res.Reason, res.TsUs, res.High); err != nil {
		if store.IsUniqueViolation(err) {
			tx.Rollback()
			return lookupBidAfterConflict(ctx, d.db, b.ID)
		}
		return model.BidResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.BidResult{}, err
	}
	return res, nil
}

func lookupBidAfterConflict(ctx context.Context, db *sql.DB, id int64) (model.BidResult, error) {
	r, ok, err := store.LookupBid(ctx, db, id)
	if err != nil {
		return r, err
	}
	if !ok {
		return r, fmt.Errorf("bid %d: unique violation but no row (concurrent duplicate rolled back)", id)
	}
	return r, nil
}

// ---------------------------------------------------------------------------
// Design 2 for auctions: one statement. The CTE locks the auction row with
// FOR UPDATE, which under READ COMMITTED returns the latest committed version
// once the lock is granted (not the statement's snapshot), decides in SQL
// with the same rules as model.AuctionRules, updates the row only if the bid
// wins, and records the decision. The receive time is statement_timestamp().
// A duplicate id fails the INSERT, which aborts the statement and with it the
// UPDATE.

const auctionCondSQL = `
WITH cur AS (
  SELECT high, deadline_us, hard_end_us,
         (extract(epoch FROM statement_timestamp()) * 1000000)::bigint AS ts
  FROM auctions WHERE id = $2 FOR UPDATE),
dec AS (
  SELECT ts, high, deadline_us, hard_end_us,
         CASE WHEN ts > deadline_us THEN 'closed'
              WHEN $4 < greatest(high + $5, $6) THEN 'low'
              ELSE '' END AS reason
  FROM cur),
upd AS (
  UPDATE auctions a SET high = $4, bidder = $3,
         deadline_us = CASE WHEN $7 AND d.deadline_us - d.ts < $8
                            THEN greatest(d.deadline_us, least(d.ts + $8, d.hard_end_us))
                            ELSE d.deadline_us END
  FROM dec d WHERE a.id = $2 AND d.reason = ''
  RETURNING 1),
ins AS (
  INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after)
  SELECT $1, $2, $3, $4, d.reason = '', d.reason, d.ts, CASE WHEN d.reason = '' THEN $4 ELSE d.high END
  FROM dec d
  RETURNING accepted, reason, ts_us, high_after)
SELECT accepted, reason, ts_us, high_after FROM ins`

// mutant "snapshot": the same statement without FOR UPDATE. The decision is
// made against the statement's snapshot, so two concurrent bids can both
// "beat" the same old high, and the conditional UPDATE then overwrites the
// higher one with whichever commits last.
const auctionCondSnapshotSQL = `
WITH cur AS (
  SELECT high, deadline_us, hard_end_us,
         (extract(epoch FROM statement_timestamp()) * 1000000)::bigint AS ts
  FROM auctions WHERE id = $2),
dec AS (
  SELECT ts, high, deadline_us, hard_end_us,
         CASE WHEN ts > deadline_us THEN 'closed'
              WHEN $4 < greatest(high + $5, $6) THEN 'low'
              ELSE '' END AS reason
  FROM cur),
upd AS (
  UPDATE auctions a SET high = $4, bidder = $3,
         deadline_us = CASE WHEN $7 AND d.deadline_us - d.ts < $8
                            THEN greatest(d.deadline_us, least(d.ts + $8, d.hard_end_us))
                            ELSE d.deadline_us END
  FROM dec d WHERE a.id = $2 AND d.reason = ''
  RETURNING 1),
ins AS (
  INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after)
  SELECT $1, $2, $3, $4, d.reason = '', d.reason, d.ts, CASE WHEN d.reason = '' THEN $4 ELSE d.high END
  FROM dec d
  RETURNING accepted, reason, ts_us, high_after)
SELECT accepted, reason, ts_us, high_after FROM ins`

type AuctionCond struct {
	db    *sql.DB
	rules model.AuctionRules
	sql   string
}

func NewAuctionCond(db *sql.DB, rules model.AuctionRules, mutant string) *AuctionCond {
	q := auctionCondSQL
	if mutant == "snapshot" {
		q = auctionCondSnapshotSQL
	}
	return &AuctionCond{db: db, rules: rules, sql: q}
}
func (d *AuctionCond) Close() error { return d.db.Close() }

func (d *AuctionCond) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	return store.ReadAuction(ctx, d.db, a)
}

func (d *AuctionCond) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	var r model.BidResult
	err := d.db.QueryRowContext(ctx, d.sql, b.ID, b.Auction, b.Bidder, b.Amount,
		d.rules.MinInc, d.rules.Reserve, d.rules.Extended, d.rules.WindowUs).Scan(&r.Accepted, &r.Reason, &r.TsUs, &r.High)
	if err != nil {
		if store.IsUniqueViolation(err) {
			return lookupBidAfterConflict(ctx, d.db, b.ID)
		}
		return r, err
	}
	return r, nil
}
