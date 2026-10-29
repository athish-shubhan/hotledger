package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"hotledger/internal/model"
)

// MySQL/InnoDB variant of the schema for designs 1 and 2. Design 2 needs one
// round trip, and MySQL has no data-modifying CTEs or RETURNING, so it is a
// stored procedure (xfer, place_bid) that runs the conditional statements
// inside the server.

var mysqlSchema = []string{
	`DROP TABLE IF EXISTS accounts, transfers, auctions, bids`,
	`DROP PROCEDURE IF EXISTS xfer`,
	`DROP PROCEDURE IF EXISTS place_bid`,
	`CREATE TABLE accounts (
	  id      BIGINT NOT NULL,
	  slot    INT    NOT NULL,
	  balance BIGINT NOT NULL,
	  PRIMARY KEY (id, slot)) ENGINE=InnoDB`,
	`CREATE TABLE transfers (
	  id      BIGINT PRIMARY KEY,
	  src     BIGINT NOT NULL,
	  dst     BIGINT NOT NULL,
	  amount  BIGINT NOT NULL,
	  applied BOOLEAN NOT NULL) ENGINE=InnoDB`,
	`CREATE TABLE auctions (
	  id          BIGINT PRIMARY KEY,
	  high        BIGINT NOT NULL,
	  bidder      BIGINT NOT NULL,
	  deadline_us BIGINT NOT NULL,
	  hard_end_us BIGINT NOT NULL) ENGINE=InnoDB`,
	`CREATE TABLE bids (
	  id         BIGINT PRIMARY KEY,
	  auction    BIGINT NOT NULL,
	  bidder     BIGINT NOT NULL,
	  amount     BIGINT NOT NULL,
	  accepted   BOOLEAN NOT NULL,
	  reason     VARCHAR(8) NOT NULL,
	  ts_us      BIGINT NOT NULL,
	  high_after BIGINT NOT NULL) ENGINE=InnoDB`,
	// Design 2 (wallet, inventory). The INSERT goes first: a duplicate id
	// waits on the original's record lock and then fails with 1062, so it
	// never touches a balance. The debit is conditional; the credit and the
	// applied flag follow only if the debit matched a row.
	`CREATE PROCEDURE xfer(IN p_id BIGINT, IN p_src BIGINT, IN p_dst BIGINT, IN p_amt BIGINT)
	BEGIN
	  DECLARE dup INT DEFAULT 0;
	  DECLARE ok BOOLEAN DEFAULT FALSE;
	  DECLARE CONTINUE HANDLER FOR 1062 SET dup = 1;
	  -- Any other error (deadlock, lock wait timeout, a killed query) must not
	  -- leave the transaction open on a pooled connection: the next CALL's
	  -- START TRANSACTION would commit its half.
	  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END;
	  START TRANSACTION;
	  INSERT INTO transfers (id, src, dst, amount, applied) VALUES (p_id, p_src, p_dst, p_amt, FALSE);
	  IF dup = 1 THEN
	    ROLLBACK;
	    SELECT applied FROM transfers WHERE id = p_id;
	  ELSE
	    UPDATE accounts SET balance = balance - p_amt WHERE id = p_src AND slot = 0 AND balance >= p_amt;
	    IF ROW_COUNT() = 1 THEN
	      UPDATE accounts SET balance = balance + p_amt WHERE id = p_dst AND slot = 0;
	      UPDATE transfers SET applied = TRUE WHERE id = p_id;
	      SET ok = TRUE;
	    END IF;
	    COMMIT;
	    SELECT ok;
	  END IF;
	END`,
	// Design 2 (auction). NOW(6) inside a routine is the start of the CALL,
	// i.e. when the server received the bid. FOR UPDATE reads the latest
	// committed row whatever the transaction's snapshot.
	`CREATE PROCEDURE place_bid(IN p_id BIGINT, IN p_auc BIGINT, IN p_bidder BIGINT, IN p_amt BIGINT,
	                            IN p_inc BIGINT, IN p_reserve BIGINT, IN p_ext BOOLEAN, IN p_window BIGINT)
	BEGIN
	  DECLARE dup INT DEFAULT 0;
	  DECLARE v_ts, v_high, v_dl, v_hard BIGINT;
	  DECLARE v_reason VARCHAR(8) DEFAULT '';
	  DECLARE CONTINUE HANDLER FOR 1062 SET dup = 1;
	  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END;
	  SET v_ts = CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS SIGNED);
	  START TRANSACTION;
	  SELECT high, deadline_us, hard_end_us INTO v_high, v_dl, v_hard FROM auctions WHERE id = p_auc FOR UPDATE;
	  IF v_ts > v_dl THEN
	    SET v_reason = 'closed';
	  ELSEIF p_amt < GREATEST(v_high + p_inc, p_reserve) THEN
	    SET v_reason = 'low';
	  END IF;
	  INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after)
	    VALUES (p_id, p_auc, p_bidder, p_amt, v_reason = '', v_reason, v_ts, IF(v_reason = '', p_amt, v_high));
	  IF dup = 1 THEN
	    ROLLBACK;
	    SELECT accepted, reason, ts_us, high_after FROM bids WHERE id = p_id;
	  ELSE
	    IF v_reason = '' THEN
	      UPDATE auctions SET high = p_amt, bidder = p_bidder,
	        deadline_us = IF(p_ext AND v_dl - v_ts < p_window, GREATEST(v_dl, LEAST(v_ts + p_window, v_hard)), v_dl)
	      WHERE id = p_auc;
	    END IF;
	    COMMIT;
	    SELECT v_reason = '', v_reason, v_ts, IF(v_reason = '', p_amt, v_high);
	  END IF;
	END`,
}

// OpenMySQL opens a pool. interpolateParams keeps every statement to one
// round trip (no server-side prepare), like the single-statement Postgres
// designs; isolation is "REPEATABLE-READ" (InnoDB's default) or "READ-COMMITTED".
func OpenMySQL(dsn, isolation string, maxConns int) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.InterpolateParams = true
	cfg.MultiStatements = false
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if isolation != "" {
		cfg.Params["transaction_isolation"] = "'" + isolation + "'"
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

// InitMySQL recreates the schema and seeds every account (one slot each).
func InitMySQL(ctx context.Context, db *sql.DB, l model.Layout) error {
	for _, q := range mysqlSchema {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("mysql schema: %w: %.60s", err, q)
		}
	}
	const batch = 5000
	var b strings.Builder
	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		_, err := db.ExecContext(ctx, "INSERT INTO accounts (id, slot, balance) VALUES "+b.String())
		b.Reset()
		n = 0
		return err
	}
	for id := int64(1); id <= l.NumAccounts(); id++ {
		if n > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d,0,%d)", id, l.InitialBalance(id))
		n++
		if n == batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "ANALYZE TABLE accounts")
	return err
}

func SnapshotMySQL(ctx context.Context, db *sql.DB) (model.State, error) {
	st := model.State{Balances: map[int64]int64{}, Extra: map[string]string{}}
	rows, err := db.QueryContext(ctx, `SELECT id, slot, balance FROM accounts`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var id, bal int64
		var slot int
		if err := rows.Scan(&id, &slot, &bal); err != nil {
			rows.Close()
			return st, err
		}
		st.Balances[id] += bal
		if bal < 0 {
			st.Negative = append(st.Negative, fmt.Sprintf("account %d slot %d balance %d", id, slot, bal))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
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

func SeedAuctionsMySQL(ctx context.Context, db *sql.DB, states []model.AuctionState) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM auctions`); err != nil {
		return err
	}
	for _, s := range states {
		if _, err := db.ExecContext(ctx, `INSERT INTO auctions VALUES (?, ?, ?, ?, ?)`, s.ID, s.High, s.Bidder, s.DeadlineUs, s.HardEndUs); err != nil {
			return err
		}
	}
	return nil
}

func SnapshotAuctionsMySQL(ctx context.Context, db *sql.DB) (model.AuctionSnapshot, error) {
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

// MySQL error classes.
func mysqlCode(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

func IsMySQLDup(err error) bool { return mysqlCode(err) == 1062 }

// IsMySQLDeadlock: 1213 deadlock (InnoDB rolled the transaction back), 1205
// lock wait timeout. Both mean the transaction did not commit.
func IsMySQLDeadlock(err error) bool {
	c := mysqlCode(err)
	return c == 1213 || c == 1205
}
