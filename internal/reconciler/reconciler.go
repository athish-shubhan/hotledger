// Package reconciler tails the Redis Stream written by design 4 and applies
// it to Postgres. It is at-least-once on the stream (XREADGROUP + XACK after
// the Postgres commit) and idempotent on the database (transfers.id is
// unique, and balances only move for rows that were actually inserted).
package reconciler

import (
	"context"
	"database/sql"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"hotledger/internal/resp"
)

type Reconciler struct {
	db      *sql.DB
	p       *resp.Pool
	Applied int64
}

func New(db *sql.DB, p *resp.Pool) *Reconciler { return &Reconciler{db: db, p: p} }

type entry struct {
	streamID          string
	id, src, dst, amt int64
	ok                bool
	enqNs             int64 // Redis time of the XADD (the stream id's milliseconds)
	// bids (t = b)
	bid             bool
	auc, bidder, dl int64
	res             string
}

func parse(v any) []entry {
	// XREADGROUP reply: [[stream, [[id, [f, v, ...]], ...]]]
	var out []entry
	streams, _ := v.([]any)
	for _, s := range streams {
		sv, _ := s.([]any)
		if len(sv) != 2 {
			continue
		}
		msgs, _ := sv[1].([]any)
		for _, m := range msgs {
			mv, _ := m.([]any)
			if len(mv) != 2 {
				continue
			}
			e := entry{streamID: mv[0].(string)}
			if ms, _, ok := strings.Cut(e.streamID, "-"); ok {
				n, _ := strconv.ParseInt(ms, 10, 64)
				e.enqNs = n * 1_000_000
			}
			fields, _ := mv[1].([]any)
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				val, _ := fields[i+1].(string)
				n, _ := strconv.ParseInt(val, 10, 64)
				switch k {
				case "id":
					e.id = n
				case "src":
					e.src = n
				case "dst":
					e.dst = n
				case "amt":
					e.amt = n
				case "ok":
					e.ok = val == "1"
				case "t":
					e.bid = val == "b"
				case "auc":
					e.auc = n
				case "bidder":
					e.bidder = n
				case "res":
					e.res = val
				case "dl":
					e.dl = n
				}
			}
			out = append(out, e)
		}
	}
	return out
}

// Run applies the stream until ctx ends. It first drains its own pending
// entries (delivered before a crash, never acked), then new ones, and rescans
// its pending list every two seconds: after a Redis failover the promoted
// replica can hold delivered entries whose XACK never replicated, and ">"
// never returns those again.
func (r *Reconciler) Run(ctx context.Context) error {
	pending := true
	lastScan := time.Now()
	for ctx.Err() == nil {
		if time.Since(lastScan) > 2*time.Second {
			pending, lastScan = true, time.Now()
		}
		start := ">"
		if pending {
			start = "0"
		}
		v, err := r.p.Do(ctx, "XREADGROUP", "GROUP", "reconciler", "r1", "COUNT", 2000, "BLOCK", 100, "STREAMS", "log", start)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if re, ok := err.(resp.Error); ok && len(re) >= 7 && re[:7] == "NOGROUP" {
				// Redis lost the group (restart without the stream): recreate.
				r.p.Do(ctx, "XGROUP", "CREATE", "log", "reconciler", "0", "MKSTREAM")
			}
			log.Printf("reconciler: read: %v", err)
			time.Sleep(200 * time.Millisecond)
			pending = true
			continue
		}
		es := parse(v)
		if pending && len(es) == 0 {
			pending = false
			continue
		}
		if len(es) == 0 {
			continue
		}
		for {
			if err := r.apply(ctx, es); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("reconciler: apply: %v", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			break
		}
		args := []any{"XACK", "log", "reconciler"}
		for _, e := range es {
			args = append(args, e.streamID)
		}
		r.p.Do(ctx, args...)
	}
	return nil
}

func (r *Reconciler) apply(ctx context.Context, all []entry) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var es, bs []entry
	for _, e := range all {
		if e.bid {
			bs = append(bs, e)
		} else {
			es = append(es, e)
		}
	}
	if len(bs) > 0 {
		if err := applyBids(ctx, tx, bs); err != nil {
			return err
		}
	}
	now := time.Now().UnixNano()
	n := len(es)
	id, src, dst, amt, enq, done := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
	ok := make([]bool, n)
	for i, e := range es {
		id[i], src[i], dst[i], amt[i], ok[i], enq[i], done[i] = e.id, e.src, e.dst, e.amt, e.ok, e.enqNs, now
	}
	rows, err := tx.QueryContext(ctx, `INSERT INTO transfers (id, src, dst, amount, applied, enq_ns, done_ns)
		SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::boolean[], $6::bigint[], $7::bigint[])
		ON CONFLICT (id) DO NOTHING RETURNING src, dst, amount, applied`,
		pq.Array(id), pq.Array(src), pq.Array(dst), pq.Array(amt), pq.Array(ok), pq.Array(enq), pq.Array(done))
	if err != nil {
		return err
	}
	delta := map[int64]int64{}
	for rows.Next() {
		var s, d, a int64
		var applied bool
		if err := rows.Scan(&s, &d, &a, &applied); err != nil {
			rows.Close()
			return err
		}
		if applied {
			delta[s] -= a
			delta[d] += a
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	ids := make([]int64, 0, len(delta))
	for k := range delta {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ds := make([]int64, len(ids))
	for i, k := range ids {
		ds[i] = delta[k]
	}
	if len(ids) > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE accounts a SET balance = a.balance + v.d
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, d) WHERE a.id = v.id AND a.slot = 0`,
			pq.Array(ids), pq.Array(ds)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.Applied += int64(len(all))
	return nil
}

// applyBids copies decided bids. The auction row only ever moves to a higher
// accepted bid, so applying a batch twice or out of order is harmless.
func applyBids(ctx context.Context, tx *sql.Tx, bs []entry) error {
	now := time.Now().UnixNano()
	n := len(bs)
	id, auc, bidder, amt, ts, hi, enq, done := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
	acc := make([]bool, n)
	reason := make([]string, n)
	best := map[int64]entry{}
	for i, e := range bs {
		f := strings.Split(e.res, "|")
		if len(f) != 4 {
			continue
		}
		id[i], auc[i], bidder[i], amt[i], enq[i], done[i] = e.id, e.auc, e.bidder, e.amt, e.enqNs, now
		acc[i], reason[i] = f[0] == "1", f[1]
		ts[i], _ = strconv.ParseInt(f[2], 10, 64)
		hi[i], _ = strconv.ParseInt(f[3], 10, 64)
		if acc[i] {
			if b, ok := best[e.auc]; !ok || e.amt > b.amt {
				best[e.auc] = e
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after, enq_ns, done_ns)
		SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::boolean[], $6::text[], $7::bigint[], $8::bigint[], $9::bigint[], $10::bigint[])
		ON CONFLICT (id) DO NOTHING`,
		pq.Array(id), pq.Array(auc), pq.Array(bidder), pq.Array(amt), pq.Array(acc), pq.Array(reason), pq.Array(ts), pq.Array(hi), pq.Array(enq), pq.Array(done)); err != nil {
		return err
	}
	aids := make([]int64, 0, len(best))
	for a := range best {
		aids = append(aids, a)
	}
	sort.Slice(aids, func(i, j int) bool { return aids[i] < aids[j] })
	for _, a := range aids {
		e := best[a]
		if _, err := tx.ExecContext(ctx, `UPDATE auctions SET high = $2, bidder = $3, deadline_us = greatest(deadline_us, $4) WHERE id = $1 AND high < $2`,
			a, e.amt, e.bidder, e.dl); err != nil {
			return err
		}
	}
	return nil
}
