# What building HotLedger turned up

Things learned while building and running the lab. Each one is backed by a
run in `results/runs/` or a test. Numbers come from one 2-vCPU VM; the
direction of each effect is the finding, not the absolute value.

## 1. `ON CONFLICT DO NOTHING` turns a safe transfer into a double charge

The one-statement design relies on the idempotency `INSERT` failing for a
duplicate key, which aborts the whole statement and rolls back its debit and
credit. The obvious "fix" for duplicate-key errors in the logs, adding
`ON CONFLICT DO NOTHING`, makes the duplicate's debit and credit commit
without a record. Conservation still holds (the money moved twice, in
balance), so a sum-of-balances check misses it; per-account reconciliation
(I4) caught it on 928 accounts in a 10 s run. Run: `mutant-wallet-condupdate-onconflict`.

## 2. Kafka exactly-once stops at Kafka's edge

Kafka transactions make consume-transform-produce exactly-once inside Kafka.
A writer whose output is a Postgres transaction is outside that boundary: if
the offset is committed to Kafka separately, a crash between the database
commit and the offset commit re-applies the batch. HotLedger stores the
offset in the same Postgres transaction as the balances. Two more guards sit
behind it: the transfer id is unique (a client retry can put the same command
in the log twice), and the offset row is a fence (compare-and-set against the
offset the writer expects). The `nodedupe` mutant (record deduped, effect
applied) is caught by I4 on 526 accounts.

## 3. The fence is what stops a zombie writer

A writer parked on a fetched batch and stopped past its session timeout comes
back believing it owns partitions that were reassigned. In all five zombie runs (three wallet, one
inventory, one auction) it tried to apply stale partition batches, and the
offset fence rejected every one: 3 to 5 per run, 18 in all
(`writer_fenced_batches` in `results/runs/faults-*-kafka*`). Two side
observations: while the zombie is stopped *inside* a batch it holds the offset
row lock, so the new owner waits instead of double-applying (a liveness stall,
not a safety bug); and the first version of this fault was flaky because the
writer was sometimes stopped before it parked, which is why the orchestrator
now waits for the "parked" log line.

## 4. Redis `appendfsync everysec` can lose acknowledged writes on a process kill

Killing only the Redis process should lose nothing that Redis has already
handed to the kernel; `everysec` is usually described as bounding loss on an
OS crash or power cut. In 2 of 3 runs nothing was lost. In the third, 229
acknowledged transfers (about 20 ms of writes) were gone after the restart,
from Redis and therefore from the Postgres copy. Our reading, not
instrumented: with `everysec`, when the previous background fsync is still
running Redis postpones the AOF `write()` (for up to 2 s) but still replies to
clients, so those writes exist only in the process's memory. The VM's disk is
busy (Postgres, the AOF and the stream all fsync), which makes slow fsyncs
likely. Runs: `faults-wallet-redislua-restart-{1,2,3}`.

## 5. Redis failover loses acknowledged writes, and the durable copy goes negative

- Failover after `kill -9` of the primary: 0 lost in 3 runs, and 1
  acknowledged transfer lost in 3 runs with 5 ms of added replication
  latency. Our reading: Redis writes the replication stream and the client
  replies in the same event-loop pass, so when only the process dies the
  window is tiny.
- Partition primary from replica for 3 s, clients still writing to the
  primary, then fail over: 23,336 acknowledged transfers lost. The
  reconciler had already copied many of them to Postgres, so Redis and
  Postgres diverged (9,950 balances differ), and Redis then let viewers spend
  coins it had forgotten: 8 accounts negative in Postgres. The "durable" copy
  is only as correct as the fast path that decides what goes into it.
- `min-replicas-to-write 1` with `min-replicas-max-lag 1` cut the loss to
  3,999 and no negative accounts. It bounds the window; it does not close it.

## 6. A failover is visible to bidders as a linearizability violation

The auction makes the same loss concrete. In 1 of 3 Redis failover runs, 7
bids that clients had been told were accepted were missing from the promoted
replica, and the chain checker flagged 4 auctions as non-linearizable: on
auction 2 a bid of 3,709 was accepted, lost in the failover, and then a second
bidder's 3,709 was accepted too, so two bidders were each told they held the
high bid. Porcupine independently found auction 53 illegal (the other three
were too wide for it). The wallet checker reports this as "1 lost transfer";
the auction checker shows what that means to users.

## 7. After a failover, acknowledged stream entries can be stuck pending forever

The reconciler reads the stream with a consumer group and `XACK`s after the
Postgres commit. On the promoted replica, some entries were delivered (the
delivery replicated) but their `XACK`s had not, so they sat in the pending
list, and `XREADGROUP ... >` never returns pending entries. The drain never
finished. The reconciler now rescans its own pending list every 2 s
(equivalent to `XAUTOCLAIM` for a single consumer).

## 8. Escrow is the best SQL design until the stock runs out, then the worst

Escrow splits a SKU's stock over 16 slots, so orders on a hot SKU stop
queueing on one row: in the skew runs it sustained 3.3k orders/s at s = 2.0
against 1.3k for the conditional update. But after a sell out every slot is
empty, every order fails its fast path and takes the consolidation path,
which locks all 16 slots. In the flash-sale burst escrow's p99 reached 68 s,
worse than the one-row conditional update (12.8 s): 41,215 consolidations,
almost all of them to refuse an order. The fix is a sold-out hint: the total
a consolidation saw under its locks is an upper bound for an account that is
only ever debited, so later orders larger than the bound are refused without
touching the rows (still recorded under their idempotency keys). With it the
burst p99 fell to 2.4 s and 40,532 orders were refused by the hint. I11 (no
false rejection) is what makes the shortcut safe to ship: a hint that refused
an order while stock remained would show up as a lost sale.

## 9. Under a sniping burst, the SQL designs refuse bids their bidders placed in time

The deadline is checked against the receive time the system stamps. The SQL
designs stamp it at the start of the transaction, after the request has
waited for a connection and behind the hot row. In the auction-end burst
the conditional update on Postgres refused 34,714 bids as
`closed`, and 25,382 of them (73%) had been placed by their bidders before the
auction's final deadline; row locking and the MySQL versions were the same
(73-76%). Those bids queued behind the hot row (burst p99 22 to 43 s) and
were stamped after the deadline. Redis stamps with `TIME` inside the script (its burst p99
is 8 ms) and the single writer stamps at the API tier before the log; each
refused about 9,300 bids as closed, of which 3 and 6 were placed in time. The
SQL auctions also ended with fewer accepted bids (1.5k to 2.0k against 2.7k to
3.0k) and fewer deadline extensions.

Every one of these runs passes the checker. Linearizability is judged against
the receive times the system stamped (A5 only bounds them by the client's
request interval), so a design that stamps late is correct and still unfair.
The fix is where the time is taken: at admission, before the queue, as the
single writer does.

## 10. InnoDB's next-key locks turn "reserve the idempotency key" into a deadlock storm

Locking the idempotency key with `SELECT ... FOR UPDATE` before inserting it
is a common pattern. Under REPEATABLE READ (the InnoDB default) a locking read
of a missing key takes a gap lock on the gap where it would be. New ids
increase, so every transaction locks the same gap at the end of the index,
and then each one's insert waits for the others' gap locks: 30,287 deadlocks
in 30 s and 7 transfers/s, against 497/s for the same code under READ
COMMITTED (no gap locks for searches) and 573/s with a plain read of the key.
Postgres has no gap locks, so the same code is fine there.

## 11. Porcupine cannot decide a hot auction; the object's structure can

Porcupine searches for a linearization of a concurrent history; the problem
is NP-complete in general, and on a hot auction with tens of thousands of
overlapping bids it exhausted 6 GB of memory and the run was killed. A
max-register with a deadline has more structure than a general object:
accepted bids must appear in increasing amount order, which fixes the
sequence of states, and every other operation names the state it saw. That
reduces the check to a timing feasibility problem that a greedy pass decides
in O(n log n) (`internal/checker/linchain.go`). It agrees with Porcupine on
4,000 random small histories (`linchain_test.go`) and checks 280k
operations in under half a second; Porcupine still runs on every auction it
can handle.

## 12. kfake's idempotent producer state does not survive a broker kill -9

After killing and restarting the in-process broker with its data directory,
the franz-go idempotent producer got `OUT_OF_ORDER_SEQUENCE_NUMBER`, bumped its
epoch, and was then rejected with `INVALID_PRODUCER_EPOCH` on every retry;
producers stopped making progress (32 unknown outcomes, no invariant
violations). Apache Kafka snapshots producer state and rebuilds it from the
log on recovery, so this is a fidelity gap of the test broker, not of the
design. Worth re-checking against the latest kfake and, if it reproduces,
reporting upstream to franz-go with this repro:

```bash
./bin/hotledger broker -data /tmp/kd &      # start a persistent broker
# produce in a loop with an idempotent producer (any franz-go client), then:
kill -9 %1; ./bin/hotledger broker -data /tmp/kd &
# the producer logs INVALID_PRODUCER_EPOCH and never recovers
```

## 13. The escrow fast path mostly misses for withdrawals

With 16 slots, a creator's balance per slot is small relative to a 50 to 499
coin withdrawal, so 94% of withdrawals (1,661 of 1,769 at s = 0.99) took the
consolidation path that locks all 16 slots. It did not hurt throughput
because withdrawals are 2% of traffic, but escrow pays off for bounded debits
only when debits are small relative to balance / N. Fewer slots for accounts
that are debited, or a rebalancer that keeps a reserve in one slot, would fix
it.

## 14. The single writer's in-memory work is not where its time goes

Batch planning (decode, in-batch dedupe, sort the touched accounts, apply in
log order, build the column arrays) was rewritten to reuse its buffers and to
keep balances in a dense slice indexed by the account's position in lock
order, so the apply loop does two map lookups per transfer. On a 4,000-record
batch at s = 2.0 it takes about 245 ns per transfer with no allocations,
against about 900 ns, 215 allocations and 2.3 MB per batch for the first
version (`go test -bench Plan ./internal/writer`).

The CPU profile of a writer in a 36 s wallet run at s = 2.0
(`results/profile/`) puts that in proportion: the writer used 3.5 CPU-seconds
(10% of a core) to apply 78,024 transfers in 4,700 batches, about 45 us of CPU
per transfer, and batch planning was about 1% of it. Most of the rest is the
Postgres wire protocol: six statements plus `BEGIN` and `COMMIT` per batch,
each a round trip. Batches
averaged 17 transfers because each partition sees only a few hundred
commands per second at this load. On this box the design is limited by the
producers' synchronous enqueue (the broker fsyncs every produce) and then by
round trips per batch, not by the writer's CPU; the allocation work matters
when batches are large, which is exactly when the writer is busy.
