// Package designs holds the ledger implementations compared by the lab. Every
// design implements Ledger; the load generator, fault injector and checker
// treat them identically.
package designs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lib/pq"

	"hotledger/internal/model"
	"hotledger/internal/store"
)

// Ledger applies one idempotent transfer. Returning an error means the client
// does not know the outcome; the caller retries with the same ID.
type Ledger interface {
	Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error)
	Close() error
}

// ---------------------------------------------------------------------------
// Design 1: SELECT ... FOR UPDATE. Read both rows under row locks (ordered by
// id), check the balance in the application, write both rows and the
// idempotency record, commit. Seven round trips; the hot row stays locked from
// the SELECT to the commit fsync.

type RowLock struct {
	db     *sql.DB
	noLock bool // mutant "nolock": plain SELECT, so concurrent transfers lose updates
}

func NewRowLock(db *sql.DB, mutant string) *RowLock {
	return &RowLock{db: db, noLock: mutant == "nolock"}
}
func (d *RowLock) Close() error { return d.db.Close() }

func (d *RowLock) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Unknown, err
	}
	defer tx.Rollback()
	if o, ok, err := store.LookupResult(ctx, tx, t.ID); err != nil || ok {
		return o, err
	}
	q := `SELECT id, balance FROM accounts WHERE slot = 0 AND id = ANY($1) ORDER BY id FOR UPDATE`
	if d.noLock {
		q = `SELECT id, balance FROM accounts WHERE slot = 0 AND id = ANY($1) ORDER BY id`
	}
	rows, err := tx.QueryContext(ctx, q, pq.Array([]int64{t.Src, t.Dst}))
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
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2 AND slot = 0`, bal[t.Src]-t.Amount, t.Src); err != nil {
			return model.Unknown, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2 AND slot = 0`, bal[t.Dst]+t.Amount, t.Dst); err != nil {
			return model.Unknown, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO transfers (id, src, dst, amount, applied) VALUES ($1,$2,$3,$4,$5)`,
		t.ID, t.Src, t.Dst, t.Amount, ok); err != nil {
		if store.IsUniqueViolation(err) {
			tx.Rollback()
			return lookupAfterConflict(ctx, d.db, t.ID)
		}
		return model.Unknown, err
	}
	if err := tx.Commit(); err != nil {
		return model.Unknown, err
	}
	return outcome(ok), nil
}

func outcome(applied bool) model.Outcome {
	if applied {
		return model.Applied
	}
	return model.Rejected
}

func lookupAfterConflict(ctx context.Context, db *sql.DB, id int64) (model.Outcome, error) {
	o, ok, err := store.LookupResult(ctx, db, id)
	if err != nil {
		return model.Unknown, err
	}
	if !ok {
		return model.Unknown, fmt.Errorf("id %d: unique violation but no row (concurrent duplicate rolled back)", id)
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// Design 2: one statement. The debit is a conditional UPDATE (balance >= amt),
// the credit runs only if the debit did, and the transfer row is a plain
// INSERT whose unique index is the idempotency arbiter. A concurrent or late
// duplicate fails the INSERT with 23505, which aborts the whole statement, so
// its debit and credit roll back with it. (ON CONFLICT DO NOTHING here would
// be a bug: the duplicate's debit would commit without a record.)

type CondUpdate struct {
	db  *sql.DB
	sql string
}

func NewCondUpdate(db *sql.DB, mutant string) *CondUpdate {
	q := condUpdateSQL
	if mutant == "onconflict" {
		// mutant "onconflict": the tempting "fix" for duplicate-key errors.
		// A duplicate now commits its debit and credit without a record.
		q = strings.Replace(condUpdateSQL, "SELECT $1, $2, $3, $4, EXISTS (SELECT 1 FROM deb)\n  RETURNING applied)",
			"SELECT $1, $2, $3, $4, EXISTS (SELECT 1 FROM deb)\n  ON CONFLICT (id) DO NOTHING\n  RETURNING applied)", 1)
	}
	return &CondUpdate{db: db, sql: q}
}
func (d *CondUpdate) Close() error { return d.db.Close() }

const condUpdateSQL = `
WITH deb AS (
  UPDATE accounts SET balance = balance - $4
  WHERE id = $2 AND slot = $5 AND balance >= $4
  RETURNING 1),
cred AS (
  UPDATE accounts SET balance = balance + $4
  WHERE id = $3 AND slot = $6 AND EXISTS (SELECT 1 FROM deb)
  RETURNING 1),
ins AS (
  INSERT INTO transfers (id, src, dst, amount, applied)
  SELECT $1, $2, $3, $4, EXISTS (SELECT 1 FROM deb)
  RETURNING applied)
SELECT applied FROM ins`

func (d *CondUpdate) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	var applied bool
	err := d.db.QueryRowContext(ctx, d.sql, t.ID, t.Src, t.Dst, t.Amount, 0, 0).Scan(&applied)
	if err != nil {
		if store.IsUniqueViolation(err) {
			return lookupAfterConflict(ctx, d.db, t.ID)
		}
		if errors.Is(err, sql.ErrNoRows) { // only possible with the onconflict mutant
			return lookupAfterConflict(ctx, d.db, t.ID)
		}
		return model.Unknown, err
	}
	return outcome(applied), nil
}

// ---------------------------------------------------------------------------
// Design 3: escrow / sub-account split (O'Neil 1986 in spirit). Creators and
// the payout account are split into N slots whose sum is the balance.
//
//   - Credits (gifts to a creator) pick a random slot: commutative, so any slot
//     is correct, and N slots divide lock contention by ~N.
//   - Bounded debits (withdrawals from a creator) try K random slots with the
//     same conditional statement as design 2, but the INSERT only runs if the
//     debit succeeded, so a slot that is too small leaves no record.
//   - If no single slot can cover the debit, the slow path locks every slot of
//     that account in slot order, decides against the true total (so there are
//     no false rejections), drains slots greedily and spreads what is left
//     evenly. That consolidation is the rebalancer.

//
// Sold-out hint (optional): for an account that is only ever debited (a
// flash-sale SKU), the total a consolidation saw under its locks is an upper
// bound on the balance from then on. A later request for more than that bound
// is rejected without touching the account rows (its rejection is still
// recorded under its idempotency key). Without it, every request after a sell
// out takes the consolidation path and serializes on all slots.

type Escrow struct {
	db        *sql.DB
	layout    model.Layout
	slots     int
	tries     int
	viewerMax int64 // ids <= viewerMax are viewers: single slot
	hint      bool
	bound     sync.Map // account id -> int64 upper bound on its balance
	fast      atomic.Int64
	slow      atomic.Int64
	hinted    atomic.Int64
}

func NewEscrow(db *sql.DB, l model.Layout, slots int, soldOutHint bool) *Escrow {
	return &Escrow{db: db, layout: l, slots: slots, tries: 2, viewerMax: l.Viewers, hint: soldOutHint}
}
func (d *Escrow) Close() error { return d.db.Close() }

// Stats reports how often withdrawals took the fast and the consolidation path.
func (d *Escrow) Stats() map[string]int64 {
	return map[string]int64{"withdraw_fast_path": d.fast.Load(), "withdraw_consolidations": d.slow.Load(), "soldout_hint_rejections": d.hinted.Load()}
}

const escrowFastDebitSQL = `
WITH deb AS (
  UPDATE accounts SET balance = balance - $4
  WHERE id = $2 AND slot = $5 AND balance >= $4
  RETURNING 1),
cred AS (
  UPDATE accounts SET balance = balance + $4
  WHERE id = $3 AND slot = $6 AND EXISTS (SELECT 1 FROM deb)
  RETURNING 1),
ins AS (
  INSERT INTO transfers (id, src, dst, amount, applied)
  SELECT $1, $2, $3, $4, true WHERE EXISTS (SELECT 1 FROM deb)
  RETURNING applied)
SELECT count(*) FROM ins`

func (d *Escrow) slotFor(id int64) int {
	if id <= d.viewerMax {
		return 0
	}
	return rand.IntN(d.slots)
}

func (d *Escrow) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	if t.Src <= d.viewerMax {
		// Gift: the viewer side is single-slot and uncontended, the creator
		// side is a random slot. Same statement as design 2.
		var applied bool
		err := d.db.QueryRowContext(ctx, condUpdateSQL, t.ID, t.Src, t.Dst, t.Amount, 0, d.slotFor(t.Dst)).Scan(&applied)
		if err != nil {
			if store.IsUniqueViolation(err) {
				return lookupAfterConflict(ctx, d.db, t.ID)
			}
			return model.Unknown, err
		}
		return outcome(applied), nil
	}
	if d.hint && d.layout.OnlyDebited(t.Src) {
		if b, ok := d.bound.Load(t.Src); ok && t.Amount > b.(int64) {
			_, err := d.db.ExecContext(ctx, `INSERT INTO transfers (id, src, dst, amount, applied) VALUES ($1, $2, $3, $4, false)`,
				t.ID, t.Src, t.Dst, t.Amount)
			if err != nil {
				if store.IsUniqueViolation(err) {
					return lookupAfterConflict(ctx, d.db, t.ID)
				}
				return model.Unknown, err
			}
			d.hinted.Add(1)
			return model.Rejected, nil
		}
	}
	// Withdrawal: fast path on up to `tries` random slots.
	start := rand.IntN(d.slots)
	for i := 0; i < d.tries && i < d.slots; i++ {
		var n int
		err := d.db.QueryRowContext(ctx, escrowFastDebitSQL, t.ID, t.Src, t.Dst, t.Amount, (start+i)%d.slots, d.slotFor(t.Dst)).Scan(&n)
		if err != nil {
			if store.IsUniqueViolation(err) {
				return lookupAfterConflict(ctx, d.db, t.ID)
			}
			return model.Unknown, err
		}
		if n == 1 {
			d.fast.Add(1)
			return model.Applied, nil
		}
	}
	d.slow.Add(1)
	return d.consolidate(ctx, t)
}

func (d *Escrow) consolidate(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Unknown, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT slot, balance FROM accounts WHERE id = $1 ORDER BY slot FOR UPDATE`, t.Src)
	if err != nil {
		return model.Unknown, err
	}
	var slots []int
	var bals []int64
	var total int64
	for rows.Next() {
		var s int
		var b int64
		if err := rows.Scan(&s, &b); err != nil {
			rows.Close()
			return model.Unknown, err
		}
		slots = append(slots, s)
		bals = append(bals, b)
		total += b
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return model.Unknown, err
	}
	// Re-check the idempotency record under the account lock: an earlier
	// attempt of this id may have succeeded on the fast path.
	if o, ok, err := store.LookupResult(ctx, tx, t.ID); err != nil || ok {
		return o, err
	}
	ok := total >= t.Amount
	if ok {
		remaining := total - t.Amount
		n := int64(len(slots))
		for i, s := range slots {
			nb := remaining / n
			if i == 0 {
				nb += remaining % n
			}
			if nb != bals[i] {
				if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2 AND slot = $3`, nb, t.Src, s); err != nil {
					return model.Unknown, err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2 AND slot = $3`, t.Amount, t.Dst, d.slotFor(t.Dst)); err != nil {
			return model.Unknown, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO transfers (id, src, dst, amount, applied) VALUES ($1,$2,$3,$4,$5)`,
		t.ID, t.Src, t.Dst, t.Amount, ok); err != nil {
		if store.IsUniqueViolation(err) {
			tx.Rollback()
			return lookupAfterConflict(ctx, d.db, t.ID)
		}
		return model.Unknown, err
	}
	if err := tx.Commit(); err != nil {
		return model.Unknown, err
	}
	if d.hint && d.layout.OnlyDebited(t.Src) {
		left := total
		if ok {
			left -= t.Amount
		}
		for {
			old, loaded := d.bound.LoadOrStore(t.Src, left)
			if !loaded || old.(int64) <= left || d.bound.CompareAndSwap(t.Src, old, left) {
				break
			}
		}
	}
	return outcome(ok), nil
}
