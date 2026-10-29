package writer

import (
	"context"
	"sort"
	"time"

	"github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/designs"
	"hotledger/internal/model"
)

// applyBidBatch is applyBatch for the auction semantics: the same offset
// fence and transactional offset, with bids decided in log order by
// model.AuctionRules against the locked auction rows. After the commit every
// bid in the batch gets a reply, including duplicates of bids decided earlier
// (their recorded decision is replayed), so a client that resends after a
// lost reply always hears back.
func (w *Writer) applyBidBatch(ctx context.Context, p int32, recs []*kgo.Record) error {
	w.mu.Lock()
	expect, owned := w.next[p]
	w.mu.Unlock()
	if !owned {
		w.logf("p%d stale batch dropped: partition revoked or lost while the batch was in hand", p)
		return errFenced
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var stored int64
	if err := tx.QueryRowContext(ctx, `SELECT next_offset FROM partition_offsets WHERE part = $1 FOR UPDATE`, p).Scan(&stored); err != nil {
		return err
	}
	if stored != expect {
		tx.Rollback()
		w.fenced.Add(1)
		w.logf("p%d fenced: expected offset %d, stored %d; re-seeking", p, expect, stored)
		w.mu.Lock()
		if _, ok := w.next[p]; ok {
			w.next[p] = stored
		}
		w.mu.Unlock()
		w.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{w.cfg.Topic: {p: {Epoch: -1, Offset: stored}}})
		return errFenced
	}

	type bcmd struct {
		b         model.Bid
		ts, enqNs int64
	}
	var cmds []bcmd
	var replyIDs []int64 // every bid id in the batch, for replies
	seen := map[int64]bool{}
	last := int64(-1)
	for _, r := range recs {
		if r.Offset < stored {
			continue
		}
		last = r.Offset
		b, ts, enq, err := designs.DecodeBid(r.Value)
		if err != nil {
			continue
		}
		replyIDs = append(replyIDs, b.ID)
		if seen[b.ID] {
			continue
		}
		seen[b.ID] = true
		cmds = append(cmds, bcmd{b, ts, enq})
	}
	if last < 0 {
		return nil
	}
	decided := map[int64]model.BidResult{}
	if len(cmds) > 0 {
		ids := make([]int64, len(cmds))
		for i, c := range cmds {
			ids[i] = c.b.ID
		}
		rows, err := tx.QueryContext(ctx, `SELECT id, accepted, reason, ts_us, high_after FROM bids WHERE id = ANY($1)`, pq.Array(ids))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var r model.BidResult
			if err := rows.Scan(&id, &r.Accepted, &r.Reason, &r.TsUs, &r.High); err != nil {
				rows.Close()
				return err
			}
			decided[id] = r
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		kept := cmds[:0]
		for _, c := range cmds {
			if _, ok := decided[c.b.ID]; !ok {
				kept = append(kept, c)
			}
		}
		cmds = kept
	}
	if len(cmds) > 0 {
		touched := map[int64]bool{}
		for _, c := range cmds {
			touched[c.b.Auction] = true
		}
		aids := make([]int64, 0, len(touched))
		for a := range touched {
			aids = append(aids, a)
		}
		sort.Slice(aids, func(i, j int) bool { return aids[i] < aids[j] })
		rows, err := tx.QueryContext(ctx, `SELECT id, high, bidder, deadline_us, hard_end_us FROM auctions WHERE id = ANY($1) ORDER BY id FOR UPDATE`, pq.Array(aids))
		if err != nil {
			return err
		}
		st := map[int64]model.AuctionState{}
		for rows.Next() {
			var s model.AuctionState
			if err := rows.Scan(&s.ID, &s.High, &s.Bidder, &s.DeadlineUs, &s.HardEndUs); err != nil {
				rows.Close()
				return err
			}
			st[s.ID] = s
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		n := len(cmds)
		bid, auc, bidder, amt, ts, hi, enq, done := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
		acc := make([]bool, n)
		reason := make([]string, n)
		changed := map[int64]bool{}
		now := time.Now().UnixNano()
		for i, c := range cmds {
			res, ns := w.cfg.Rules.Decide(st[c.b.Auction], c.b, c.ts)
			if res.Accepted {
				st[c.b.Auction] = ns
				changed[c.b.Auction] = true
			}
			decided[c.b.ID] = res
			bid[i], auc[i], bidder[i], amt[i], acc[i], reason[i], ts[i], hi[i], enq[i], done[i] =
				c.b.ID, c.b.Auction, c.b.Bidder, c.b.Amount, res.Accepted, res.Reason, res.TsUs, res.High, c.enqNs, now
		}
		var uid, uh, ub, ud []int64
		for _, a := range aids {
			if changed[a] {
				s := st[a]
				uid, uh, ub, ud = append(uid, a), append(uh, s.High), append(ub, s.Bidder), append(ud, s.DeadlineUs)
			}
		}
		if len(uid) > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE auctions a SET high = v.h, bidder = v.b, deadline_us = v.d
				FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) AS v(id, h, b, d) WHERE a.id = v.id`,
				pq.Array(uid), pq.Array(uh), pq.Array(ub), pq.Array(ud)); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO bids (id, auction, bidder, amount, accepted, reason, ts_us, high_after, enq_ns, done_ns)
			SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::boolean[], $6::text[], $7::bigint[], $8::bigint[], $9::bigint[], $10::bigint[])`,
			pq.Array(bid), pq.Array(auc), pq.Array(bidder), pq.Array(amt), pq.Array(acc), pq.Array(reason), pq.Array(ts), pq.Array(hi), pq.Array(enq), pq.Array(done)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE partition_offsets SET next_offset = $2 WHERE part = $1`, p, last+1); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	w.mu.Lock()
	if _, ok := w.next[p]; ok {
		w.next[p] = last + 1
	}
	w.mu.Unlock()
	w.applied.Add(int64(len(cmds)))
	w.batches.Add(1)
	// Replies are best effort: a lost reply makes the client resend the same id.
	for _, id := range replyIDs {
		if r, ok := decided[id]; ok {
			w.replies.Produce(ctx, &kgo.Record{Topic: designs.ReplyTopic, Value: designs.EncodeReply(id, r)}, nil)
		}
	}
	return nil
}
