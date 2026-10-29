#!/usr/bin/env bash
# Reproduces every number in results/. Needs Postgres 16 on 127.0.0.1:5432
# (user postgres / password postgres, database hotledger), redis-server on
# PATH, and bench/setup.sh run once (TigerBeetle, MySQL). The pg-restart
# fault uses `sudo -n pg_ctlcluster 16 main stop -m immediate`; override with
# HOTLEDGER_PG_STOP / HOTLEDGER_PG_START elsewhere.
#
#   bench/run_all.sh [all|skew|burst|faults|innodb|mutants|profile|plot]
set -uo pipefail
cd "$(dirname "$0")/.."
go build -o bin/hotledger ./cmd/hotledger
BIN=./bin/hotledger
OUT=${OUT:-results/runs}
DUR=${DUR:-30s}
mkdir -p "$OUT"

run() { # name, args...
  local name=$1; shift
  if [[ -f $OUT/$name/result.json && -z ${FORCE:-} ]]; then echo "=== $name (have it)"; return; fi
  echo "=== $name"
  $BIN run -out "$OUT/$name" "$@" 2>&1 | grep -v -E "drain:|netproxy:" | tail -16
  # keep the reports, drop the bulky raw data
  rm -rf "$OUT/$name/history.jsonl" "$OUT/$name/kafka-data" "$OUT/$name/redis"
}

WALLET="rowlock condupdate escrow redislua kafka tigerbeetle mysql-rowlock mysql-condupdate"
INVENTORY="$WALLET"
AUCTION="rowlock condupdate redislua kafka mysql-rowlock mysql-condupdate"
phase=${1:-all}

if [[ $phase == all || $phase == skew ]]; then
  for s in 0.99 1.5 2.0; do
    for d in $WALLET; do
      run "skew-wallet-$d-$s" -design $d -skew $s -duration $DUR -concurrency 64 -dup-frac 0.01
    done
    # Inventory with stock that never runs out: pure bounded-decrement contention.
    for d in $INVENTORY; do
      run "skew-inventory-$d-$s" -design $d -semantics inventory -skew $s -duration $DUR -concurrency 64 -dup-frac 0.01
    done
    # 200 auctions, Zipf popularity, deadlines after the run: contention on a max-register.
    for d in $AUCTION; do
      run "skew-auction-$d-$s" -design $d -semantics auction -auctions 200 -skew $s -duration $DUR -concurrency 64 -dup-frac 0.01
    done
  done
fi

if [[ $phase == all || $phase == burst ]]; then
  # Open loop: 500 ops/s for 20 s, then 10x (5,000 ops/s) for 10 s on two keys.
  # Latency is measured from each request's due time (no coordinated omission).
  for d in $WALLET; do  # LIVE Match end: gifts to the two battling creators
    run "burst-wallet-$d" -design $d -skew 1.5 -duration 30s -rate 500 -burst-secs 10 -concurrency 128 -dup-frac 0.01
  done
  # Flash sale: two SKUs with 10,000 units each go on sale in the burst and sell out in it.
  for d in $INVENTORY; do
    run "burst-inventory-$d" -design $d -semantics inventory -hot-stock 10000 -skew 1.5 -duration 30s -rate 500 -burst-secs 10 -concurrency 128 -dup-frac 0.01
  done
  run "burst-inventory-escrow-hint" -design escrow -escrow-hint -semantics inventory -hot-stock 10000 -skew 1.5 -duration 30s -rate 500 -burst-secs 10 -concurrency 128 -dup-frac 0.01
  # Auction end: the two hot auctions close at 22 s (Extended mode: a bid in the
  # last 2 s extends to ts + 2 s, at most 6 s past the deadline); the burst is sniping.
  for d in $AUCTION; do
    run "burst-auction-$d" -design $d -semantics auction -auctions 200 -auction-hot 22s -auction-other-min 5s -auction-other-max 40s \
      -auction-hard-max 6s -window 2s -skew 1.5 -duration 30s -rate 500 -burst-secs 10 -concurrency 128 -dup-frac 0.01
  done
fi

if [[ $phase == all || $phase == faults ]]; then
  for i in 1 2 3; do
    for d in rowlock condupdate escrow; do
      run "faults-wallet-$d-$i" -design $d -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 \
        -faults pg-restart@6s,pg-partition@14s+4s,pg-reset@22s
    done
    for d in mysql-rowlock mysql-condupdate; do
      run "faults-wallet-$d-$i" -design $d -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 \
        -faults mysql-restart@6s,pg-partition@14s+4s,pg-reset@22s
    done
    run "faults-wallet-tigerbeetle-$i" -design tigerbeetle -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 \
      -faults kill-tigerbeetle@6s,pg-partition@14s+4s,pg-reset@22s
    run "faults-wallet-kafka-$i" -design kafka -skew 1.5 -duration 40s -concurrency 32 -dup-frac 0.02 \
      -faults kill-writer@5s,zombie-writer@10s+10s,pg-restart@23s,pg-partition@29s+4s,rebalance@34s+4s
    run "faults-wallet-redislua-restart-$i" -design redislua -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 -faults kill-redis@8s
    run "faults-wallet-redislua-failover-$i" -design redislua -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 -faults redis-failover@8s
    run "faults-wallet-redislua-failover-lag5ms-$i" -design redislua -replica-lag 5ms -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 -faults redis-failover@8s
  done
  run "faults-wallet-redislua-partition-failover" -design redislua -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 -faults redis-partition-failover@8s+3s
  run "faults-wallet-redislua-partition-failover-minreplicas" -design redislua -redis-min-replicas -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02 -faults redis-partition-failover@8s+3s
  # Broker crash: safety holds, but with kfake the idempotent producer wedges
  # after restart (see docs/FINDINGS.md); kept separate so it does not mask the rest.
  run "faults-wallet-kafka-broker" -design kafka -skew 1.5 -duration 30s -concurrency 32 -dup-frac 0.02 -faults kill-broker@10s

  # Flash sale under faults (stock runs out during the run).
  INV="-semantics inventory -hot-stock 20000 -stock 2000 -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02"
  for d in rowlock condupdate escrow; do
    run "faults-inventory-$d" -design $d $INV -faults pg-restart@6s,pg-partition@14s+4s,pg-reset@22s
  done
  run "faults-inventory-escrow-hint" -design escrow -escrow-hint $INV -faults pg-restart@6s,pg-partition@14s+4s,pg-reset@22s
  run "faults-inventory-mysql-condupdate" -design mysql-condupdate $INV -faults mysql-restart@6s,pg-partition@14s+4s,pg-reset@22s
  run "faults-inventory-tigerbeetle" -design tigerbeetle $INV -faults kill-tigerbeetle@6s,pg-partition@14s+4s,pg-reset@22s
  run "faults-inventory-kafka" -design kafka $INV -faults kill-writer@5s,zombie-writer@10s+10s,pg-restart@23s
  run "faults-inventory-redislua-failover-lag5ms" -design redislua -replica-lag 5ms $INV -faults redis-failover@8s

  # Auctions under faults (the two hot auctions close mid-run).
  AUC="-semantics auction -auctions 200 -auction-hot 20s -auction-other-min 5s -auction-other-max 40s -auction-hard-max 6s -window 2s -skew 1.5 -duration $DUR -concurrency 32 -dup-frac 0.02"
  for d in rowlock condupdate; do
    run "faults-auction-$d" -design $d $AUC -faults pg-restart@6s,pg-partition@14s+4s,pg-reset@22s
  done
  run "faults-auction-mysql-condupdate" -design mysql-condupdate $AUC -faults mysql-restart@6s,pg-partition@14s+4s,pg-reset@22s
  run "faults-auction-kafka" -design kafka $AUC -faults kill-writer@5s,zombie-writer@10s+10s,pg-restart@23s
  for i in 1 2 3; do
    run "faults-auction-redislua-failover-lag5ms-$i" -design redislua -replica-lag 5ms $AUC -faults redis-failover@8s
  done
fi

if [[ $phase == all || $phase == innodb ]]; then
  # InnoDB next-key locks: a locking read of a missing idempotency key under
  # REPEATABLE READ takes a gap lock at the end of the index; every concurrent
  # insert then needs an insert-intention lock in that gap.
  for iso in REPEATABLE-READ READ-COMMITTED; do
    run "innodb-lockingidem-$iso" -design mysql-rowlock -mysql-locking-idem -mysql-iso $iso -skew 2.0 -duration $DUR -concurrency 64 -dup-frac 0.01
  done
fi

if [[ $phase == all || $phase == mutants ]]; then
  # Seeded bugs: each must be caught.
  run "mutant-wallet-rowlock-nolock" -design rowlock -mutant nolock -skew 1.5 -duration 10s -concurrency 32 -dup-frac 0.02
  run "mutant-wallet-condupdate-onconflict" -design condupdate -mutant onconflict -skew 1.5 -duration 10s -concurrency 32 -dup-frac 0.05
  run "mutant-wallet-kafka-nodedupe" -design kafka -mutant nodedupe -skew 1.5 -duration 15s -concurrency 32 -dup-frac 0.05
  run "mutant-auction-rowlock-nolock" -design rowlock -mutant nolock -semantics auction -auctions 50 -skew 1.5 -duration 10s -concurrency 32 -dup-frac 0.02
  run "mutant-auction-condupdate-snapshot" -design condupdate -mutant snapshot -semantics auction -auctions 50 -skew 1.5 -duration 10s -concurrency 32 -dup-frac 0.02
fi

if [[ $phase == all || $phase == profile ]]; then
  # CPU profile of one design-5 writer (results/profile/, not part of the tables)
  mkdir -p results/profile
  $BIN run -out results/profile/kafka-wallet-2.0 -design kafka -skew 2.0 -duration $DUR -concurrency 64 -dup-frac 0.01 -writer-cpuprofile | tail -3
  rm -rf results/profile/kafka-wallet-2.0/history.jsonl results/profile/kafka-wallet-2.0/kafka-data
  go test -run XXX -bench Plan -benchtime 2s ./internal/writer | tee results/profile/plan-bench.txt
fi

python3 bench/plot.py "$OUT" results
