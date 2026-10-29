package designs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"hotledger/internal/model"
	"hotledger/internal/store"
)

// ---------------------------------------------------------------------------
// Designs 1 and 2 on MySQL 8 / InnoDB.
//
// MySQLRowLock is design 1 ported the way most people would write it:
// BEGIN, check the idempotency key, SELECT ... FOR UPDATE both rows in id
// order, two UPDATEs, INSERT the record, COMMIT. LockingIdem makes the
// idempotency check a locking read (SELECT ... FOR UPDATE on transfers), a
// common way to "reserve" the key. Under REPEATABLE READ a locking read of a
// missing key takes a next-key (gap) lock; every new id lands in the same gap
// at the end of the primary key, so concurrent transactions all hold that
// gap and then deadlock on each other's insert-intention locks. Under READ
// COMMITTED InnoDB takes no gap locks for searches and the same code is fine.
// The benchmark measures both.

type MySQLRowLock struct {
	db          *sql.DB
	lockingIdem bool
	Deadlocks   int64
}

func NewMySQLRowLock(db *sql.DB, lockingIdem bool) *MySQLRowLock {
	return &MySQLRowLock{db: db, lockingIdem: lockingIdem}
}
func (d *MySQLRowLock) Close() error { return d.db.Close() }

func mysqlLookup(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64, locking bool) (model.Outcome, bool, error) {
	query := `SELECT applied FROM transfers WHERE id = ?`
	if locking {
		query += ` FOR UPDATE`
	}
	var applied bool
	err := q.QueryRowContext(ctx, query, id).Scan(&applied)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Unknown, false, nil
	}
	if err != nil {
		return model.Unknown, false, err
	}
	return outcome(applied), true, nil
}

func (d *MySQLRowLock) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Unknown, err
	}
	defer tx.Rollback()
	if o, ok, err := mysqlLookup(ctx, tx, t.ID, d.lockingIdem); err != nil || ok {
		return o, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, balance FROM accounts WHERE slot = 0 AND id IN (?, ?) ORDER BY id FOR UPDATE`, t.Src, t.Dst)
	if err != nil {
		return model.Unknown, err
	}
	bal := map[int64]int64{}
	for rows.Next() {
		var id, b int64
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return model.Unknown, err
		}
		bal[id] = b
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return model.Unknown, err
	}
	ok := bal[t.Src] >= t.Amount
	if ok {
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = ? WHERE id = ? AND slot = 0`, bal[t.Src]-t.Amount, t.Src); err != nil {
			return model.Unknown, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = ? WHERE id = ? AND slot = 0`, bal[t.Dst]+t.Amount, t.Dst); err != nil {
			return model.Unknown, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO transfers (id, src, dst, amount, applied) VALUES (?, ?, ?, ?, ?)`,
		t.ID, t.Src, t.Dst, t.Amount, ok); err != nil {
		if store.IsMySQLDup(err) {
			tx.Rollback()
			o, found, err := mysqlLookup(ctx, d.db, t.ID, false)
			if err == nil && !found {
				err = fmt.Errorf("id %d: duplicate key but no row", t.ID)
			}
			return o, err
		}
		return model.Unknown, err
	}
	if err := tx.Commit(); err != nil {
		return model.Unknown, err
	}
	return outcome(ok), nil
}

// MySQLCond is design 2: CALL xfer(...), one round trip.
type MySQLCond struct{ db *sql.DB }

func NewMySQLCond(db *sql.DB) *MySQLCond { return &MySQLCond{db: db} }
func (d *MySQLCond) Close() error        { return d.db.Close() }

func (d *MySQLCond) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	var applied bool
	if err := d.db.QueryRowContext(ctx, `CALL xfer(?, ?, ?, ?)`, t.ID, t.Src, t.Dst, t.Amount).Scan(&applied); err != nil {
		return model.Unknown, err
	}
	return outcome(applied), nil
}

// ---------------------------------------------------------------------------
// Auctions on MySQL.

type MySQLAuctionRowLock struct {
	db    *sql.DB
	rules model.AuctionRules
}

func NewMySQLAuctionRowLock(db *sql.DB, rules model.AuctionRules) *MySQLAuctionRowLock {
	return &MySQLAuctionRowLock{db: db, rules: rules}
}
func (d *MySQLAuctionRowLock) Close() error { return d.db.Close() }

func mysqlReadAuction(ctx context.Context, db *sql.DB, a int64) (model.AuctionState, error) {
	s := model.AuctionState{ID: a}
	err := db.QueryRowContext(ctx, `SELECT high, bidder, deadline_us, hard_end_us FROM auctions WHERE id = ?`, a).Scan(&s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs)
	return s, err
}

func mysqlLookupBid(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64) (model.BidResult, bool, error) {
	var r model.BidResult
	err := q.QueryRowContext(ctx, `SELECT accepted, reason, ts_us, high_after FROM bids WHERE id = ?`, id).Scan(&r.Accepted, &r.Reason, &r.TsUs, &r.High)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}

func (d *MySQLAuctionRowLock) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	return mysqlReadAuction(ctx, d.db, a)
}

func (d *MySQLAuctionRowLock) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BidResult{}, err
	}
	defer tx.Rollback()
	if r, ok, err := mysqlLookupBid(ctx, tx, b.ID); err != nil || ok {
		return r, err
	}
	s := model.AuctionState{ID: b.Auction}
	var ts int64
	if err := tx.QueryRowContext(ctx, `SELECT high, bidder, deadline_us, hard_end_us, CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS SIGNED)
		FROM auctions WHERE id = ? FOR UPDATE`, b.Auction).Scan(&s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs, &ts); err != nil {
		return model.BidResult{}, err
	}
	res, ns := d.rules.Decide(s, b, ts)
	if res.Accepted {
		if _, err := tx.ExecContext(ctx, `UPDATE auctions SET high = ?, bidder = ?, deadline_us = ? WHERE id = ?`, ns.High, ns.Bidder, ns.DeadlineUs, b.Auction); err != nil {
			return model.BidResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		b.ID, b.Auction, b.Bidder, b.Amount, res.Accepted, res.Reason, res.TsUs, res.High); err != nil {
		if store.IsMySQLDup(err) {
			tx.Rollback()
			r, found, err := mysqlLookupBid(ctx, d.db, b.ID)
			if err == nil && !found {
				err = fmt.Errorf("bid %d: duplicate key but no row", b.ID)
			}
			return r, err
		}
		return model.BidResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.BidResult{}, err
	}
	return res, nil
}

type MySQLAuctionCond struct {
	db    *sql.DB
	rules model.AuctionRules
}

func NewMySQLAuctionCond(db *sql.DB, rules model.AuctionRules) *MySQLAuctionCond {
	return &MySQLAuctionCond{db: db, rules: rules}
}
func (d *MySQLAuctionCond) Close() error { return d.db.Close() }
func (d *MySQLAuctionCond) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	return mysqlReadAuction(ctx, d.db, a)
}

func (d *MySQLAuctionCond) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	var r model.BidResult
	err := d.db.QueryRowContext(ctx, `CALL place_bid(?, ?, ?, ?, ?, ?, ?, ?)`, b.ID, b.Auction, b.Bidder, b.Amount,
		d.rules.MinInc, d.rules.Reserve, d.rules.Extended, d.rules.WindowUs).Scan(&r.Accepted, &r.Reason, &r.TsUs, &r.High)
	return r, err
}
