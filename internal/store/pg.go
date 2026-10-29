// Package store owns the Postgres schema shared by the SQL-backed designs:
// accounts (optionally split into escrow slots), the transfer record that
// doubles as the idempotency table, and per-partition offsets for the Kafka
// single-writer design.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"hotledger/internal/model"
)

const Schema = `
DROP TABLE IF EXISTS accounts, transfers, partition_offsets, auctions, bids;
CREATE TABLE accounts (
  id      bigint NOT NULL,
  slot    int    NOT NULL,
  balance bigint NOT NULL,
  PRIMARY KEY (id, slot)
) WITH (fillfactor = 70);
CREATE TABLE transfers (
  id      bigint PRIMARY KEY,   -- idempotency key
  src     bigint NOT NULL,
  dst     bigint NOT NULL,
  amount  bigint NOT NULL,
  applied boolean NOT NULL,     -- false = recorded rejection (insufficient funds)
  enq_ns  bigint,               -- async designs: client enqueue time
  done_ns bigint                -- async designs: apply commit time
);
CREATE TABLE partition_offsets (
  part        int PRIMARY KEY,
  next_offset bigint NOT NULL
);
CREATE TABLE auctions (
  id          bigint PRIMARY KEY,
  high        bigint NOT NULL,
  bidder      bigint NOT NULL,
  deadline_us bigint NOT NULL,
  hard_end_us bigint NOT NULL
) WITH (fillfactor = 50);
CREATE TABLE bids (
  id         bigint PRIMARY KEY,  -- idempotency key
  auction    bigint NOT NULL,
  bidder     bigint NOT NULL,
  amount     bigint NOT NULL,
  accepted   boolean NOT NULL,
  reason     text NOT NULL,       -- '' | low | closed
  ts_us      bigint NOT NULL,     -- when the system received the bid
  high_after bigint NOT NULL,
  enq_ns     bigint,
  done_ns    bigint
);`

func Open(dsn string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

// Init drops and recreates the schema and seeds every account. Accounts with
// id > Viewers (creators and the payout account) get `slots` rows so the
// escrow design can spread a hot balance; the initial balance is split evenly
// across slots. Every other design uses slots = 1.
func Init(ctx context.Context, db *sql.DB, l model.Layout, slots, partitions int) error {
	if _, err := db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := tx.Prepare(pq.CopyIn("accounts", "id", "slot", "balance"))
	if err != nil {
		return err
	}
	for id := int64(1); id <= l.NumAccounts(); id++ {
		n := 1
		if l.IsCreatorOrPayout(id) {
			n = slots
		}
		b := l.InitialBalance(id)
		for s := 0; s < n; s++ {
			share := b / int64(n)
			if s == 0 {
				share += b % int64(n)
			}
			if _, err := st.Exec(id, s, share); err != nil {
				return err
			}
		}
	}
	if _, err := st.Exec(); err != nil {
		return err
	}
	if err := st.Close(); err != nil {
		return err
	}
	for p := 0; p < partitions; p++ {
		if _, err := tx.Exec(`INSERT INTO partition_offsets VALUES ($1, 0)`, p); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `VACUUM ANALYZE accounts`)
	return err
}

// Snapshot reads the final state.
func Snapshot(ctx context.Context, db *sql.DB) (model.State, error) {
	st := model.State{Balances: map[int64]int64{}, Extra: map[string]string{}}
	rows, err := db.QueryContext(ctx, `SELECT id, slot, balance FROM accounts`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var id, bal int64
		var slot int
		if err := rows.Scan(&id, &slot, &bal); err != nil {
			return st, err
		}
		st.Balances[id] += bal
		if bal < 0 {
			st.Negative = append(st.Negative, fmt.Sprintf("account %d slot %d balance %d", id, slot, bal))
		}
	}
	if err := rows.Err(); err != nil {
		return st, err
	}
	rows.Close()
	rows, err = db.QueryContext(ctx, `SELECT id, src, dst, amount, applied FROM transfers`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var t model.AppliedTransfer
		if err := rows.Scan(&t.ID, &t.Src, &t.Dst, &t.Amount, &t.Applied); err != nil {
			return st, err
		}
		st.Transfers = append(st.Transfers, t)
	}
	return st, rows.Err()
}

// IsUniqueViolation reports a duplicate idempotency key.
func IsUniqueViolation(err error) bool {
	var pe *pq.Error
	return errors.As(err, &pe) && pe.Code == "23505"
}

// IsRetryable reports errors where the transaction certainly did not commit
// (deadlock, serialization failure), so an immediate retry is safe and cheap.
func IsRetryable(err error) bool {
	var pe *pq.Error
	if errors.As(err, &pe) {
		return pe.Code == "40P01" || pe.Code == "40001"
	}
	return IsMySQLDeadlock(err)
}

// LookupResult returns the recorded outcome of an existing idempotency key.
func LookupResult(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64) (model.Outcome, bool, error) {
	var applied bool
	err := q.QueryRowContext(ctx, `SELECT applied FROM transfers WHERE id = $1`, id).Scan(&applied)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Unknown, false, nil
	}
	if err != nil {
		return model.Unknown, false, err
	}
	if applied {
		return model.Applied, true, nil
	}
	return model.Rejected, true, nil
}

// SeedAuctions writes every auction's initial state (deadlines are relative
// to the run's epoch, so they are seeded just before the load starts).
func SeedAuctions(ctx context.Context, db *sql.DB, states []model.AuctionState) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM auctions`); err != nil {
		return err
	}
	for _, s := range states {
		if _, err := tx.ExecContext(ctx, `INSERT INTO auctions VALUES ($1, $2, $3, $4, $5)`, s.ID, s.High, s.Bidder, s.DeadlineUs, s.HardEndUs); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SnapshotAuctions reads the final auction state and the bid record.
func SnapshotAuctions(ctx context.Context, db *sql.DB) (model.AuctionSnapshot, error) {
	snap := model.AuctionSnapshot{Auctions: map[int64]model.AuctionState{}}
	rows, err := db.QueryContext(ctx, `SELECT id, high, bidder, deadline_us, hard_end_us FROM auctions`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var s model.AuctionState
		if err := rows.Scan(&s.ID, &s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs); err != nil {
			rows.Close()
			return snap, err
		}
		snap.Auctions[s.ID] = s
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return snap, err
	}
	rows, err = db.QueryContext(ctx, `SELECT id, auction, bidder, amount, accepted, reason, ts_us, high_after FROM bids`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var b model.RecordedBid
		if err := rows.Scan(&b.ID, &b.Auction, &b.Bidder, &b.Amount, &b.Accepted, &b.Reason, &b.TsUs, &b.High); err != nil {
			return snap, err
		}
		snap.Bids = append(snap.Bids, b)
	}
	return snap, rows.Err()
}

// LookupBid returns the recorded decision for an existing bid id.
func LookupBid(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64) (model.BidResult, bool, error) {
	var r model.BidResult
	err := q.QueryRowContext(ctx, `SELECT accepted, reason, ts_us, high_after FROM bids WHERE id = $1`, id).Scan(&r.Accepted, &r.Reason, &r.TsUs, &r.High)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	return r, true, nil
}

// ReadAuction is a plain read of one auction's current value.
func ReadAuction(ctx context.Context, db *sql.DB, id int64) (model.AuctionState, error) {
	s := model.AuctionState{ID: id}
	err := db.QueryRowContext(ctx, `SELECT high, bidder, deadline_us, hard_end_us FROM auctions WHERE id = $1`, id).Scan(&s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs)
	return s, err
}
