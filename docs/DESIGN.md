# HotLedger design

## 1. Problem

TikTok LIVE has three features where many users write the same few rows at
the same moment:

- **Gift wallet.** A gift moves coins from a viewer to a creator. A few
  creators receive most gifts, and at the end of a LIVE Match the two battling
  creators receive a burst in the final seconds. Credits commute; charges
  must happen exactly once.
- **Flash-sale inventory.** TikTok Shop LIVE puts a product on sale in front
  of the whole audience. Every order decrements the same stock row, and stock
  must never go below zero (no oversell), but a sale must not be refused while
  stock remains either.
- **Countdown auction.** Countdown Bidding: the highest bid received before the
  deadline wins, bids are binding, and in Extended mode a late bid pushes the
  deadline out. Bids on one auction do not commute: each one must see the
  current high.

These hot rows defeat the usual tools (row locks serialise on them, sharding
cannot split one row). This repo compares six designs on all three semantics,
under the same skewed workload and the same faults, judged by the same
history-based checker.

## 2. Semantics and workload

### Wallet

Accounts: 100,000 viewers, 10,000 creators, one payout clearing account. Ids
are ordered viewer < creator < payout, and every transfer moves money from a
lower to a higher id, so any design that locks rows in id order cannot
deadlock.

| operation | share | debit | credit | why it matters |
|---|---|---|---|---|
| gift | 98% | random viewer (70% start with 50 coins, 30% with 1,000) | creator chosen by Zipf(s) | hot **credit**, and rejections when a viewer runs out |
| withdrawal | 2% | creator chosen by Zipf(s), 50 to 499 coins | payout account | hot **bounded debit**: a creator must not go negative |

### Inventory

10,000 SKUs, each a stock account; a purchase debits units from the SKU's
stock and credits a buyer. SKUs are chosen by Zipf(s). Skew runs give every
SKU a million units, so stock never runs out and the runs measure pure
bounded-decrement contention. Burst runs put two SKUs with 10,000 units each
on sale at the start of the burst; they sell out within it, so the runs also
measure what happens after a sell out (every later order must be refused,
quickly). Fault runs use small stock so SKUs sell out while faults hit.

### Auction

200 auctions, popularity by Zipf(s). A bid is `(auction, bidder, amount)` with
an idempotency key; a read returns the current high and deadline.
`model.AuctionRules` is the sequential specification every design implements:

- a bid received after the deadline in force is rejected `closed`;
- a bid below `max(high + min increment, reserve)` is rejected `low`;
- otherwise it becomes the high, and in **Extended** mode a bid received in
  the last `window` before the deadline moves the deadline to
  `receive time + window`, never past the auction's hard end.

The receive time is stamped by the system, not by the client (a client clock
cannot be trusted for a deadline). Where it is stamped matters, and it differs
by design (section 3): a bid that waits in a queue before it is stamped can
miss a deadline its bidder made.

Skew runs place every deadline after the run (pure contention on a
max-register). Burst runs close the two hot auctions at 22 s, inside the burst,
with a 2 s window and a 6 s hard cap: the burst is sniping.

### Skew, duplicates, retries

Skew: s = 0.99, 1.5, 2.0 over the creators, SKUs or auctions. The hottest
receives about 10%, 38% and 61% of requests respectively.

Every run resubmits a share of requests with the same idempotency key: half
concurrently with the original (racing it), half after the answer. Clients
retry an unknown outcome (error, timeout) with the same key until the run's
grace period ends.

## 3. Designs

All designs implement one Go interface per semantics (`Ledger.Transfer` for
wallet and inventory, `Auctioneer.Bid` / `Read` for auctions), and every
design that applies business rules in Go calls the same model functions the
checker uses.

| design | wallet | inventory | auction | where the auction receive time is stamped |
|---|---|---|---|---|
| 1 row lock (Postgres, MySQL) | yes | yes | yes | transaction start, after waiting for a connection |
| 2 conditional update (Postgres, MySQL) | yes | yes | yes | statement start, after waiting for a connection |
| 3 escrow | yes | yes (+ sold-out hint) | no: a maximum cannot be split into slots | |
| 4 Redis Lua + async log | yes | yes | yes | Redis `TIME` inside the script |
| 5 Kafka single writer | yes (async) | yes (async) | yes (waits for the writer's reply) | the API tier, before producing |
| 6 TigerBeetle | yes | yes | no: no compare-against-current-high primitive | |

### 1. Row lock (`rowlock`, `mysql-rowlock`)

`BEGIN; SELECT ... FOR UPDATE` on both rows in id order, check the balance in
Go, two `UPDATE`s, `INSERT` the transfer record, `COMMIT`. Seven round trips;
the hot row stays locked from the `SELECT` until the commit's WAL fsync. The
idempotency record's primary key is the final arbiter: a racing duplicate
fails its `INSERT` with a unique violation, rolls back, and returns the
original's result.

For auctions: `SELECT ... FOR UPDATE` on the auction row, `Decide` in Go,
update the row, insert the bid record, commit.

**InnoDB.** The MySQL port is the same code. With `-mysql-locking-idem` the
idempotency check becomes a locking read (`SELECT ... FOR UPDATE` on the
transfers table), a common way to "reserve" a key. Under REPEATABLE READ (the
InnoDB default) a locking read of a missing key takes a next-key (gap) lock.
Every new id lands in the same gap at the end of the primary key, so all
concurrent transactions hold that gap and then deadlock on each other's
insert-intention locks. Under READ COMMITTED InnoDB takes no gap locks for
searches and the same code is fine. The `innodb` bench phase measures both.

### 2. Conditional update (`condupdate`, `mysql-condupdate`)

One statement with data-modifying CTEs:

```sql
WITH deb  AS (UPDATE accounts SET balance = balance - $amt
              WHERE id = $src AND balance >= $amt RETURNING 1),
     cred AS (UPDATE accounts SET balance = balance + $amt
              WHERE id = $dst AND EXISTS (SELECT 1 FROM deb) RETURNING 1),
     ins  AS (INSERT INTO transfers (...) SELECT ..., EXISTS (SELECT 1 FROM deb)
              RETURNING applied)
SELECT applied FROM ins
```

One round trip and no read-modify-write in the application. The hot row is
locked from the `UPDATE` until commit. The `INSERT` deliberately has no
`ON CONFLICT DO NOTHING`: a duplicate must fail the whole statement so its
debit and credit roll back. Adding `ON CONFLICT DO NOTHING` (a tempting fix
for "duplicate key" errors in logs) silently double-charges; the `onconflict`
mutant does exactly that and the checker catches it (section 6).

Under READ COMMITTED, a second transfer blocked on the hot row re-evaluates
`balance >= amt` against the committed new version when the lock is released
(Postgres EvalPlanQual), so the bound is checked against the latest balance,
not a stale snapshot.

MySQL has no data-modifying CTEs, so the MySQL version is a stored procedure
(`xfer`, `place_bid`): conditional `UPDATE`, branch on `ROW_COUNT()`, insert
the record, all in one call, with an `EXIT HANDLER` that rolls back and
re-signals so a duplicate key aborts the whole transfer.

For auctions the statement updates the auction row only if the bid beats the
high and the deadline has not passed, computing the extended deadline in SQL,
and inserts the bid record with the decision. The `snapshot` mutant decides on
the statement's snapshot instead of the locked row, and the checker catches
it.

### 3. Escrow sub-accounts (`escrow`)

Each creator, SKU and the payout account is split into 16 slots whose sum is
the balance, in the spirit of O'Neil's escrow method (1986):

- **credits** pick a random slot: credits commute, any slot is correct, and
  contention drops by about 16x;
- **bounded debits** (withdrawals, purchases) try two random slots with a
  conditional statement whose `INSERT` only runs if the debit succeeded;
- if no single slot can cover the debit, a **consolidation** transaction locks
  all 16 slots in slot order, decides against the true total (so there are no
  false rejections), drains slots greedily and spreads the remainder evenly.
  That consolidation is the rebalancer.

**Sold-out hint** (`-escrow-hint`). For an account that is only ever debited
(a SKU), the total a consolidation saw under its locks is an upper bound on
the balance from then on. A later request for more than that bound is
rejected without touching the account rows (its rejection is still recorded
under its idempotency key). Without the hint, every order after a sell out
takes the consolidation path and serialises on all 16 slots: escrow turns
from the best SQL design into the worst one exactly when the sale ends.

Escrow does not apply to auctions: the state is a maximum, and "highest bid
so far" cannot be split into slots that are checked independently.

### 4. Redis Lua fast path + async log (`redislua`)

The common industry pattern. A Lua script checks the idempotency key, checks
the balance (or stock, or the auction's high and deadline), applies the
change, records the result and `XADD`s it to a Redis Stream, all atomically
(Redis runs a script without interleaving). The client is answered
immediately. A reconciler process reads the stream with a consumer group,
applies batches to Postgres idempotently (`ON CONFLICT DO NOTHING RETURNING`,
and balances move only for rows actually inserted), then `XACK`s.

The primary has an asynchronously replicated replica; a proxy plays the role
of Sentinel. The failure window is the reason it is here: Redis acknowledges
before the write exists anywhere else. The **reconciliation lag** (time from
the client's answer to the Postgres commit) is reported as "apply latency".

### 5. Single writer per partition over Kafka (`kafka`)

Clients produce the command to a topic keyed by the **debit** account (or the
auction), so every debit of an account is applied by one writer, in log
order. The producer is idempotent (producer id, epoch and per-partition
sequence numbers, `acks=all`), so the producer's own retries never duplicate a
record. For the wallet and inventory the client hears "enqueued" when the
broker acks; the outcome is decided by the writer.

Each writer owns partitions through a consumer group (cooperative-sticky) and
applies a batch per partition in one Postgres transaction:

1. lock the partition's offset row and compare it with the offset this writer
   expects (**fencing**: a zombie writer that was paused past its session
   timeout finds the row moved, aborts and re-seeks);
2. drop commands whose id is already recorded (**dedupe**: a client retry put
   the same command in the log twice);
3. lock every touched account in id order, apply the batch in log order in
   memory, write each changed row once (the hot creator row is written once
   per batch, not once per gift: TigerBeetle-style group commit), insert all
   transfer records in one statement;
4. advance the stored offset **in the same transaction** and commit.

The in-memory part of step 3 (`internal/writer/plan.go`) reuses its buffers
across batches and keeps balances in a dense slice indexed by each account's
position in lock order: about 245 ns per transfer and no allocations on a
4,000-record batch. The writer's CPU profile shows it is not the bottleneck
(FINDINGS 14).

Why offsets live in Postgres rather than in Kafka: Kafka transactions
(`isolation.level=read_committed`, transactional offset commits) make a
Kafka-to-Kafka pipeline exactly-once, but they cannot include a write to an
external database. If the offset were committed to Kafka separately, a crash
between the database commit and the offset commit would re-apply the batch.
Keeping the offset next to the data makes the batch and its position one
atomic unit (the "transactional consumer" answer to the dual-write problem;
the alternative, an outbox, solves the producer side). The id dedupe and the
fence are defence in depth: either alone would have prevented a double apply
in the zombie test.

**Auctions** need an answer, not "enqueued": a bidder must know at once
whether the bid stands. The API tier stamps the receive time, produces the bid
keyed by auction, and waits for the writer's decision on a `bid-replies`
topic. The writer replies after its Postgres commit; if it dies in between,
the client times out and resends the same bid id, and the writer answers with
the recorded decision. Because the receive time is stamped before the queue,
a bid that waits behind the burst is still judged by when it arrived.

The broker in this repo is franz-go's `kfake`, a Kafka-wire-protocol broker
written for testing (persistent with `-data`, fsync per produce). It is not
Apache Kafka; see validity threats.

### 6. TigerBeetle (`tigerbeetle`)

The reference point: a purpose-built ledger (0.17.9, one replica, Direct IO,
one core applying batches in order). Every account that must not go negative
(viewers, creators, SKUs) has `debits_must_not_exceed_credits`. The transfer
id is the idempotency key: TigerBeetle answers a second transfer with the same
id with `exists` if it was created, or `id_already_failed` if it failed with a
transient error such as `exceeds_credits`, so a retried rejection stays
rejected. Initial balances are funded from a mint account. The Go client
batches concurrent requests into one message.

## 4. Correctness harness

Each client records one JSON line per operation: id, operands, invoke and
complete time (the due time in open-loop runs), result (`applied`,
`rejected`, `unknown`, `enqueued`; for bids also the receive time the system
stamped and the high it saw), attempts, and whether it was a deliberate
duplicate. After the run drains, the checker (`internal/checker`) reads the
histories, the deterministic initial state and the system's final state and
record of decisions.

### Wallet and inventory

| | invariant | catches |
|---|---|---|
| I1 | conservation: sum of balances equals the initial total | half-applied transfers, lost updates |
| I2 | no account or escrow slot below zero (inventory: no oversell) | broken bound checks |
| I3 | no idempotency key recorded twice | missing uniqueness |
| I4 | every balance = initial + applied credits - applied debits | double applies that I1 misses (balanced but twice) |
| I5 | every applied transfer was issued, with the same src/dst/amount | phantom or corrupted transfers |
| I6 | acked "applied" means applied in the final state | lost acknowledged writes |
| I7 | acked "rejected" means not applied | inconsistent replies |
| I8 | "enqueued" means decided after drain | lost async requests |
| I9 | one key never answered both "applied" and "rejected" | non-idempotent replies |
| I10 | per SKU, units in applied purchase records <= initial stock | oversell, counted from the records rather than the balance |
| I11 | an account that is only debited (viewer, SKU) ended with at least the amount of any request rejected for insufficient funds | false rejections: a lost sale while stock remained |

### Auction

| | invariant | catches |
|---|---|---|
| A1 | final high and leader are the highest accepted bid in the record | lost updates |
| A2 | replaying accepted bids in receive order, each was received before the deadline in force and beat the high by the increment, and the extensions reproduce the final deadline exactly | wrong deadline or increment logic |
| A3 | "accepted" to the client means accepted in the record, and "rejected" means not | lost acknowledged bids |
| A4 | no bid id recorded twice | non-idempotent bids |
| A5 | the stamped receive time lies inside the client's request interval | clock or stamping bugs |
| A6 | every recorded bid was issued by a client with the same operands | phantoms |
| A7 | one id never got both "accepted" and "rejected" | inconsistent replies |
| A8 | the history of bids and reads on each auction is linearizable against `AuctionRules` | stale reads, decisions on stale state |

**Linearizability (A8).** Porcupine searches for a linearization of a
concurrent history; the general problem is NP-complete, and on a hot auction
with tens of thousands of overlapping bids Porcupine ran out of memory (6 GB)
before deciding. The checker therefore decides every auction exactly with a
chain checker that uses the object's structure (`linchain.go`): accepted bids
must appear in increasing amount order, which fixes the sequence of states;
every other operation names the state it saw by the high it reports; what
remains is a timing problem that a greedy earliest-point assignment decides in
O(n log n). It is cross-validated against Porcupine on random small histories
(`linchain_test.go`), and Porcupine still runs on every auction whose history
is at most 10 operations wide. Illegal auctions are written out as Porcupine
HTML visualisations.

What linearizability cannot prove: it checks one object at a time (it says
nothing about a bid and a wallet debit being atomic together), it is judged
against the receive times the system itself stamped (A5 bounds them by the
client's interval, but a design that stamps late is still linearizable), and
it says nothing about liveness (a design that rejects every late bid as
`closed` is correct). The burst table therefore also reports closed
rejections of bids placed before the deadline.

The checker has unit tests for every invariant and is validated end to end
with seeded bugs (section 6).

## 5. Faults

| fault | how | designs |
|---|---|---|
| duplicate submissions | 2% of requests resubmitted with the same key, half racing the original | all |
| database crash | `pg_ctlcluster stop -m immediate` (crash, WAL recovery on restart); MySQL: `kill -9` of `mysqld` | SQL designs, writers, reconciler |
| network partition | TCP proxy stops moving bytes both ways for 4 s, then delivers the held bytes late | SQL designs, writers, TigerBeetle clients |
| connection reset | proxy closes every client-database connection | SQL designs, TigerBeetle |
| TigerBeetle kill -9 | restart from its data file | tigerbeetle |
| writer kill -9 | mid-batch, restarted 3 s later | kafka |
| zombie writer | the writer parks on a fetched, unapplied batch; it is SIGSTOPped for 10 s (past its 6 s session timeout) and resumed, so it tries to apply a batch for partitions now owned by another writer | kafka |
| rebalance | add a writer, remove it 4 s later | kafka |
| broker kill -9 | restart from its data dir | kafka (separate run) |
| Redis kill -9 | restart from AOF (`appendfsync everysec`) | redislua |
| Redis failover | kill -9 primary, promote the async replica, redirect clients (optionally with 5 ms added replication latency) | redislua |
| Redis partition, then failover | cut primary <-> replica for 3 s while clients still write to the primary, then fail over; optionally with `min-replicas-to-write 1` | redislua |

Every fault scenario is one command line in `bench/run_all.sh` (`-faults
pg-restart@6s,pg-partition@14s+4s,pg-reset@22s` and so on), so each is a
reproducible script.

The partition is the important one for idempotency: a `COMMIT` that reaches
the server while the reply is held leaves the client with an unknown outcome;
the client retries with the same key and must get the original result, not a
second debit.

## 6. Seeded bugs (checker validation)

| mutant | design | bug | expected catch |
|---|---|---|---|
| `nolock` | rowlock (wallet) | `SELECT` without `FOR UPDATE`, absolute writes | lost updates: I1, I4 |
| `onconflict` | condupdate (wallet) | `INSERT ... ON CONFLICT DO NOTHING` | duplicate commits its debit: I4 |
| `nodedupe` | kafka (wallet) | dedupes the record (`ON CONFLICT`) but applies every command's effect | I4 |
| `nolock` | rowlock (auction) | read the auction without `FOR UPDATE` | lost updates: A1, A2, A8 |
| `snapshot` | condupdate (auction) | decide on the statement snapshot, not the locked row | A2, A8 |

## 7. Metrics

- **Throughput and latency.** Closed loop (64 clients, 30 s) measures
  saturation throughput and its latency. Open-loop burst runs send on a
  schedule (500 ops/s, then 5,000 ops/s for 10 s on two keys) and measure
  latency from each request's due time, which avoids coordinated omission.
- **Apply latency** for async designs: from the client's answer ("enqueued"
  or Redis' reply) to the Postgres commit. For design 4 this is the
  reconciliation lag.
- **Recovery after a crash**, from the client history: client outage (the
  longest stretch with no successful completion in the 30 s after the crash,
  including the 2 s the orchestrator waits before restarting), resume (from
  the restart to the first success of a request sent after it), and time
  until throughput is back to 90% of its pre-crash level.
- **CPU per million operations**: server-side CPU seconds (Postgres backends
  and background processes, counted through the postmaster's cumulative child
  time; MySQL, Redis, TigerBeetle, broker, writers, reconciler; not the load
  generator) from the start of the load to the end of the drain, divided by
  completed operations. Async designs pay for applying what they accepted.

## 8. Decision matrix

What the measurements say, for a hot key on this hardware (numbers in
`results/SUMMARY.md`):

| if you need | use | because | watch out for |
|---|---|---|---|
| a few hot rows, rules in SQL, one database | 2 conditional update | one round trip, about 2x row lock at every skew | still one row lock per hot key: a 10x burst queues for tens of seconds |
| hot credits and bounded debits, Postgres only | 3 escrow | 16 slots: flat-ish across skew, fastest SQL design | debits larger than balance / slots take the 16-slot path; inventory needs the sold-out hint or the sell out is its slowest moment |
| an ordered object (auction), SQL | 2 conditional update | decides against the locked row in one statement | receive time is stamped after the queue: under a sniping burst most late bids are refused as closed although their bidders were on time |
| the lowest client latency, loss tolerable | 4 Redis Lua + async log | sub-20 ms p99 at every skew and in the burst | acknowledged writes vanish on failover, and the durable copy then diverges; not for money without compensation |
| high throughput with durability, and async is fine | 5 Kafka single writer | skew does not matter (the hot row is written once per batch), throughput is set by the enqueue path; exactly-once by offset-in-the-same-transaction | a broker, writers, rebalances and fencing to operate; noisy on this box; auctions need a reply path, and a writer crash stops bidding for a session timeout |
| a ledger and nothing else | 6 TigerBeetle | an order of magnitude more throughput and lowest CPU per op | one more system; fixed data model (no auction); one replica here |
| MySQL | 1 or 2, READ COMMITTED for any locking read of a missing key | REPEATABLE READ gap locks collapse a "reserve the key" pattern to single-digit ops/s | |

## 9. Validity threats

- **One small machine.** Every process (Postgres, MySQL, Redis, TigerBeetle,
  broker, writers, clients) shares one 2-vCPU VM. Absolute numbers are small;
  the comparison between designs and the shape of the curves are the result.
  Each configuration ran once (fault scenarios: three times where noted).
- **kfake is not Apache Kafka.** It speaks the Kafka protocol and implements
  idempotent producers, consumer groups and persistence, but its performance
  and its recovery behaviour differ. One concrete difference found here: after
  a broker kill -9 and restart, the franz-go idempotent producer was rejected
  with `INVALID_PRODUCER_EPOCH` repeatedly and stopped making progress
  (safety held; liveness did not). There is no broker leader change because
  there is one broker. Rerun design 5 against Apache Kafka before quoting its
  numbers (docker-compose in `deploy/`).
- **No Java variant.** The spec's Kafka Streams `exactly_once_v2` variant
  needs Maven Central, which the build environment's network policy blocks.
- **TigerBeetle runs one replica**, not the six-replica cluster it is designed
  for; replication would add a network round trip per batch.
- **Postgres defaults** apart from `max_connections = 200`; `synchronous_commit
  = on` (an acknowledged commit is on disk). MySQL 8 with its defaults
  (`innodb_flush_log_at_trx_commit = 1`, REPEATABLE READ unless stated).
- **The async designs' client latency is enqueue latency.** Their apply
  latency is reported separately and is what a viewer would wait to see the
  gift on screen.
- **Not modelled:** cross-region replication, a replicated Kafka cluster, a
  Raft-replicated single writer (stretch goals).
