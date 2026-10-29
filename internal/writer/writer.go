// Package writer is the consumer side of design 5: one writer per partition
// applies batches of transfer commands to Postgres.
//
// Exactly-once effects come from three pieces, each covering a different
// failure:
//
//  1. Offsets live in Postgres, committed in the same transaction as the
//     balance changes ("transactional consumer"). Kafka transactions only
//     make Kafka-to-Kafka pipelines exactly-once; they cannot cover a write to
//     an external database, so the offset must move with the data. A writer
//     killed mid-batch rolls back both, and the next owner resumes from the
//     stored offset.
//  2. The batch first locks the partition's offset row and compares it with
//     the offset this writer expects. A zombie writer (paused past its session
//     timeout while a new owner took over) finds the row moved, aborts and
//     re-seeks. This is fencing by compare-and-set on the offset.
//  3. transfers.id is unique. A command that is in the log twice (a client
//     retry of the same idempotency key) is applied once and recorded once.
//
// Within a batch the writer locks every touched account in id order (no
// deadlocks across partitions), applies transfers in log order in memory,
// writes each changed row once, and inserts all records in one statement.
// The hot creator row is written once per batch instead of once per gift:
// that is the group-commit idea behind TigerBeetle's batching.
package writer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/designs"
	"hotledger/internal/model"
)

type Config struct {
	Brokers []string
	Topic   string
	Group   string
	MaxPoll int
	Name    string
	Verbose bool
	Mutant  string // "nodedupe": seeded bug, dedupes the record via ON CONFLICT but applies every command
	Auction bool   // apply bids (topic bids) instead of transfers, and reply to each one
	Rules   model.AuctionRules
}

type Writer struct {
	cfg     Config
	db      *sql.DB
	cl      *kgo.Client
	replies *kgo.Client // auction mode: decisions back to the waiting clients
	mu      sync.Mutex
	next    map[int32]int64 // offset this writer expects to apply next, per owned partition
	applied atomic.Int64
	batches atomic.Int64
	fenced  atomic.Int64
}

func New(cfg Config, db *sql.DB) (*Writer, error) {
	w := &Writer{cfg: cfg, db: db, next: map[int32]int64{}}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(time.Second),
		kgo.RebalanceTimeout(20*time.Second),
		kgo.FetchMaxWait(20*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.AdjustFetchOffsetsFn(w.loadOffsets),
		kgo.OnPartitionsRevoked(w.drop("revoked")),
		kgo.OnPartitionsLost(w.drop("lost")),
	)
	if err != nil {
		return nil, err
	}
	w.cl = cl
	if cfg.Auction {
		w.replies, err = kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.DefaultProduceTopic(designs.ReplyTopic), kgo.ProducerLinger(0))
		if err != nil {
			cl.Close()
			return nil, err
		}
	}
	return w, nil
}

// loadOffsets replaces whatever Kafka would resume from with the offsets
// stored next to the data.
func (w *Writer) loadOffsets(ctx context.Context, m map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic, parts := range m {
		for p := range parts {
			var off int64
			if err := w.db.QueryRowContext(ctx, `SELECT next_offset FROM partition_offsets WHERE part = $1`, p).Scan(&off); err != nil {
				return nil, fmt.Errorf("load offset p%d: %w", p, err)
			}
			w.next[p] = off
			m[topic][p] = kgo.NewOffset().At(off)
			w.logf("assigned p%d from offset %d", p, off)
		}
	}
	return m, nil
}

func (w *Writer) drop(why string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, parts := range m {
			for _, p := range parts {
				delete(w.next, p)
				w.logf("%s p%d", why, p)
			}
		}
	}
}

func (w *Writer) logf(f string, a ...any) {
	if w.cfg.Verbose {
		log.Printf("[%s] "+f, append([]any{w.cfg.Name}, a...)...)
	}
}

func (w *Writer) Close() {
	w.cl.Close()
	if w.replies != nil {
		w.replies.Close()
	}
}

func (w *Writer) Stats() (applied, batches, fenced int64) {
	return w.applied.Load(), w.batches.Load(), w.fenced.Load()
}

// StallNextBatch is a fault-injection hook: after the next poll that returns
// records, the writer parks while holding the fetched batch until Release
// receives a value. The orchestrator SIGSTOPs the parked process past its
// session timeout and SIGCONTs it, so the writer resumes as a zombie with a
// stale assignment at exactly the worst moment.
var (
	StallNextBatch atomic.Bool
	Release        = make(chan struct{}, 1)
)

// Run polls and applies until ctx is cancelled.
func (w *Writer) Run(ctx context.Context) error {
	for {
		fs := w.cl.PollRecords(ctx, w.cfg.MaxPoll)
		if fs.IsClientClosed() || ctx.Err() != nil {
			return ctx.Err()
		}
		if fs.NumRecords() > 0 && StallNextBatch.CompareAndSwap(true, false) {
			w.logf("fault: parked with %d fetched records not yet applied", fs.NumRecords())
			select {
			case <-Release:
			case <-ctx.Done():
				return ctx.Err()
			}
			w.logf("fault: released; applying the stale batch")
		}
		fs.EachError(func(t string, p int32, err error) {
			if err != context.Canceled {
				w.logf("fetch error %s/%d: %v", t, p, err)
			}
		})
		var wg sync.WaitGroup
		fs.EachPartition(func(fp kgo.FetchTopicPartition) {
			if len(fp.Records) == 0 {
				return
			}
			wg.Add(1)
			go func(p int32, recs []*kgo.Record) {
				defer wg.Done()
				w.applyWithRetry(ctx, p, recs)
			}(fp.Partition, fp.Records)
		})
		wg.Wait()
		w.cl.AllowRebalance()
	}
}

func (w *Writer) applyWithRetry(ctx context.Context, p int32, recs []*kgo.Record) {
	backoff := 50 * time.Millisecond
	apply := w.applyBatch
	if w.cfg.Auction {
		apply = w.applyBidBatch
	}
	for ctx.Err() == nil {
		err := apply(ctx, p, recs)
		if err == nil {
			return
		}
		if err == errFenced {
			return
		}
		w.logf("p%d batch failed, retrying: %v", p, err)
		time.Sleep(backoff + time.Duration(rand.Int64N(int64(backoff))))
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

var errFenced = fmt.Errorf("fenced")

type cmd struct {
	t     model.Transfer
	enqNs int64
}

func (w *Writer) applyBatch(ctx context.Context, p int32, recs []*kgo.Record) error {
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

	// (2) fence: the stored offset must be the one we expect.
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

	// Decode records we have not applied yet, dedupe ids inside the batch.
	plan := getPlan()
	defer putPlan(plan)
	last := plan.decode(recs, stored)
	if last < 0 {
		return nil
	}

	// (3) skip ids already recorded by an earlier batch.
	if len(plan.cmds) > 0 && w.cfg.Mutant != "nodedupe" {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM transfers WHERE id = ANY($1)`, pq.Array(plan.ids()))
		if err != nil {
			return err
		}
		clear(plan.exists)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			plan.exists[id] = struct{}{}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		plan.dropExisting()
	}

	if len(plan.cmds) > 0 {
		// Lock every touched account in id order.
		accts := plan.accounts()
		rows, err := tx.QueryContext(ctx, `SELECT id, balance FROM accounts WHERE slot = 0 AND id = ANY($1) ORDER BY id FOR UPDATE`, pq.Array(accts))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, b int64
			if err := rows.Scan(&id, &b); err != nil {
				rows.Close()
				return err
			}
			plan.setBalance(id, b)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Apply in log order.
		plan.apply(time.Now().UnixNano())
		if len(plan.uid) > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE accounts a SET balance = v.b
				FROM unnest($1::bigint[], $2::bigint[]) AS v(id, b)
				WHERE a.id = v.id AND a.slot = 0`, pq.Array(plan.uid), pq.Array(plan.ub)); err != nil {
				return err
			}
		}
		ins := `INSERT INTO transfers (id, src, dst, amount, applied, enq_ns, done_ns)
			SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::boolean[], $6::bigint[], $7::bigint[])`
		if w.cfg.Mutant == "nodedupe" {
			ins += ` ON CONFLICT (id) DO NOTHING`
		}
		if _, err := tx.ExecContext(ctx, ins,
			pq.Array(plan.tid), pq.Array(plan.tsrc), pq.Array(plan.tdst), pq.Array(plan.tamt), pq.Array(plan.tok), pq.Array(plan.tenq), pq.Array(plan.tdone)); err != nil {
			return err
		}
	}
	applied := int64(len(plan.cmds))

	// (1) the offset moves in the same transaction as the data.
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
	w.applied.Add(applied)
	w.batches.Add(1)
	return nil
}
