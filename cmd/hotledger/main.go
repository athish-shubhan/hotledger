// Command hotledger runs the contended-state lab: one binary with
// subcommands for every role, so the orchestrator can kill -9 any of them.
//
//	hotledger run     one experiment end to end (init, processes, faults, load, check)
//	hotledger init    recreate schema and seed accounts
//	hotledger broker  Kafka-protocol broker (franz-go kfake) for design 5
//	hotledger writer  design 5 single-writer consumer
//	hotledger load    load generator for one design
//	hotledger reconciler  design 4: tail the Redis stream into Postgres
//	hotledger check   run the invariant checker on histories + final state
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"hotledger/internal/checker"
	"hotledger/internal/designs"
	"hotledger/internal/history"
	"hotledger/internal/load"
	"hotledger/internal/model"
	"hotledger/internal/reconciler"
	"hotledger/internal/resp"
	"hotledger/internal/run"
	"hotledger/internal/store"
	"hotledger/internal/workload"
	"hotledger/internal/writer"
)

const (
	defaultDSN      = "postgres://postgres:postgres@127.0.0.1:5432/hotledger?sslmode=disable&connect_timeout=5"
	defaultMySQLDSN = "root@tcp(127.0.0.1:3307)/hotledger?timeout=5s"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	cmd, args := os.Args[1], os.Args[2:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch cmd {
	case "run":
		err = cmdRun(ctx, args)
	case "init":
		err = cmdInit(ctx, args)
	case "broker":
		err = cmdBroker(ctx, args)
	case "writer":
		err = cmdWriter(ctx, args)
	case "load":
		err = cmdLoad(ctx, args)
	case "check":
		err = cmdCheck(ctx, args)
	case "reconciler":
		err = cmdReconciler(ctx, args)
	default:
		usage()
	}
	if err != nil {
		log.Fatalf("%s: %v", cmd, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: hotledger {run|init|broker|writer|load|check} [flags]  (see -h of each)")
	os.Exit(2)
}

type layoutFlags struct {
	viewers, creators, stock, hotStock int64
	sem                                string
}

func (l *layoutFlags) register(fs *flag.FlagSet) {
	fs.Int64Var(&l.viewers, "viewers", 100_000, "number of viewer (wallet) or buyer (inventory) accounts")
	fs.Int64Var(&l.creators, "creators", 10_000, "number of creator (wallet) or SKU (inventory) accounts")
	fs.StringVar(&l.sem, "semantics", model.SemWallet, "wallet | inventory | auction")
	fs.Int64Var(&l.stock, "stock", 1_000_000, "inventory: initial units of each SKU")
	fs.Int64Var(&l.hotStock, "hot-stock", 1_000_000, "inventory: initial units of the two flash-sale SKUs")
}
func (l layoutFlags) layout() model.Layout {
	sem := l.sem
	if sem == model.SemAuction {
		sem = model.SemWallet // auctions do not use accounts
	}
	return model.Layout{Viewers: l.viewers, Creators: l.creators, Sem: sem, Stock: l.stock, HotStock: l.hotStock}
}

type ruleFlags struct {
	extended       bool
	window         time.Duration
	minInc, reserv int64
}

func (r *ruleFlags) register(fs *flag.FlagSet) {
	d := model.DefaultAuctionRules()
	fs.BoolVar(&r.extended, "extended", d.Extended, "auction: Extended mode (late bids extend the deadline)")
	fs.DurationVar(&r.window, "window", time.Duration(d.WindowUs)*time.Microsecond, "auction: extension window")
	fs.Int64Var(&r.minInc, "min-inc", d.MinInc, "auction: minimum increment")
	fs.Int64Var(&r.reserv, "reserve", d.Reserve, "auction: reserve price")
}
func (r ruleFlags) rules() model.AuctionRules {
	return model.AuctionRules{Extended: r.extended, WindowUs: r.window.Microseconds(), MinInc: r.minInc, Reserve: r.reserv}
}

func cmdInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	var lf layoutFlags
	lf.register(fs)
	dsn := fs.String("dsn", defaultDSN, "postgres DSN")
	slots := fs.Int("slots", 1, "escrow slots per creator/payout account")
	parts := fs.Int("partitions", 8, "partition offset rows (design 5)")
	fs.Parse(args)
	db, err := store.Open(*dsn, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	return store.Init(ctx, db, lf.layout(), *slots, *parts)
}

func cmdBroker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("broker", flag.ExitOnError)
	port := fs.Int("port", 9092, "listen port")
	parts := fs.Int("partitions", 8, "partitions of the transfers topic")
	topic := fs.String("topic", "transfers", "topic")
	data := fs.String("data", "", "data dir for persistence (empty = in memory)")
	fs.Parse(args)
	opts := []kfake.Opt{kfake.Ports(*port), kfake.SeedTopics(int32(*parts), *topic, designs.BidTopic, designs.ReplyTopic), kfake.GroupMinSessionTimeout(time.Second)}
	if *data != "" {
		// fsync every produce: with a single broker there is no replica to
		// fall back on, so an acked record must survive kill -9.
		opts = append(opts, kfake.DataDir(*data), kfake.SyncWrites())
	}
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		return err
	}
	log.Printf("broker listening on %v", c.ListenAddrs())
	<-ctx.Done()
	c.Close()
	return nil
}

func cmdWriter(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("writer", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN, "postgres DSN")
	brokers := fs.String("brokers", "127.0.0.1:9092", "comma-separated brokers")
	name := fs.String("name", "w0", "writer name for logs")
	maxPoll := fs.Int("max-poll", 4000, "max records per poll (batch size bound)")
	verbose := fs.Bool("v", false, "log assignments and fencing")
	mutant := fs.String("mutant", "", "seeded bug: nodedupe")
	auction := fs.Bool("auction", false, "apply bids (topic bids) and reply, instead of transfers")
	cpuprofile := fs.String("cpuprofile", "", "write a CPU profile of this writer here")
	var rf ruleFlags
	rf.register(fs)
	fs.Parse(args)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	db, err := store.Open(*dsn, 16)
	if err != nil {
		return err
	}
	defer db.Close()
	topic := "transfers"
	if *auction {
		topic = designs.BidTopic
	}
	w, err := writer.New(writer.Config{Brokers: strings.Split(*brokers, ","), Topic: topic, Group: "hotledger-writers",
		MaxPoll: *maxPoll, Name: *name, Verbose: *verbose, Mutant: *mutant, Auction: *auction, Rules: rf.rules()}, db)
	if err != nil {
		return err
	}
	defer w.Close()
	usr := make(chan os.Signal, 2)
	signal.Notify(usr, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for sig := range usr {
			if sig == syscall.SIGUSR1 {
				writer.StallNextBatch.Store(true)
			} else {
				select {
				case writer.Release <- struct{}{}:
				default:
				}
			}
		}
	}()
	err = w.Run(ctx)
	a, b, f := w.Stats()
	log.Printf("[%s] stopped: applied=%d batches=%d fenced=%d", *name, a, b, f)
	if err == context.Canceled {
		return nil
	}
	return err
}

type openOpts struct {
	design, dsn, mysqlDSN, mysqlIso, brokers, mutant string
	lockingIdem, escrowHint                          bool
	slots, conns                                     int
}

func openLedger(o openOpts, l model.Layout) (designs.Ledger, error) {
	design, dsn, brokers, slots, conns, mutant := o.design, o.dsn, o.brokers, o.slots, o.conns, o.mutant
	switch design {
	case "mysql-rowlock", "mysql-condupdate":
		db, err := store.OpenMySQL(o.mysqlDSN, o.mysqlIso, conns)
		if err != nil {
			return nil, err
		}
		if design == "mysql-rowlock" {
			return designs.NewMySQLRowLock(db, o.lockingIdem), nil
		}
		return designs.NewMySQLCond(db), nil
	case "tigerbeetle":
		return designs.NewTigerBeetle(strings.Split(brokers, ","))
	case "rowlock", "condupdate", "escrow":
		db, err := store.Open(dsn, conns)
		if err != nil {
			return nil, err
		}
		switch design {
		case "rowlock":
			return designs.NewRowLock(db, mutant), nil
		case "condupdate":
			return designs.NewCondUpdate(db, mutant), nil
		default:
			return designs.NewEscrow(db, l, slots, o.escrowHint), nil
		}
	case "kafka":
		return designs.NewKafkaSW(strings.Split(brokers, ","), "transfers")
	case "redislua":
		return designs.NewRedisLua(strings.Split(brokers, ","), l)
	}
	return nil, fmt.Errorf("unknown design %q", design)
}

func openAuctioneer(o openOpts, rules model.AuctionRules) (designs.Auctioneer, error) {
	switch o.design {
	case "rowlock", "condupdate":
		db, err := store.Open(o.dsn, o.conns)
		if err != nil {
			return nil, err
		}
		if o.design == "rowlock" {
			return designs.NewAuctionRowLock(db, rules, o.mutant), nil
		}
		return designs.NewAuctionCond(db, rules, o.mutant), nil
	case "mysql-rowlock", "mysql-condupdate":
		db, err := store.OpenMySQL(o.mysqlDSN, o.mysqlIso, o.conns)
		if err != nil {
			return nil, err
		}
		if o.design == "mysql-rowlock" {
			return designs.NewMySQLAuctionRowLock(db, rules), nil
		}
		return designs.NewMySQLAuctionCond(db, rules), nil
	case "redislua":
		return designs.NewAuctionRedis(strings.Split(o.brokers, ","), rules)
	case "kafka":
		db, err := store.Open(o.dsn, o.conns)
		if err != nil {
			return nil, err
		}
		return designs.NewAuctionKafka(strings.Split(o.brokers, ","), db)
	}
	return nil, fmt.Errorf("design %q has no auction implementation (escrow: bids do not commute; tigerbeetle: a ledger, not an auction)", o.design)
}

func cmdLoad(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	var lf layoutFlags
	lf.register(fs)
	design := fs.String("design", "condupdate", "rowlock | condupdate | escrow | redislua | kafka | tigerbeetle | mysql-rowlock | mysql-condupdate")
	dsn := fs.String("dsn", defaultDSN, "postgres DSN")
	brokers := fs.String("brokers", "127.0.0.1:9092", "kafka brokers or redis address")
	slots := fs.Int("slots", 16, "escrow slots")
	skew := fs.Float64("skew", 1.5, "Zipf s over creators")
	withdraw := fs.Float64("withdraw-frac", 0.02, "share of creator withdrawals")
	dur := fs.Duration("duration", 30*time.Second, "run length")
	conc := fs.Int("concurrency", 64, "workers (= max in-flight requests)")
	rate := fs.Float64("rate", 0, "open-loop ops/s (0 = closed loop)")
	burst := fs.Int("burst-secs", 0, "open loop: final seconds at 10x rate on two creators")
	dupFrac := fs.Float64("dup-frac", 0, "share of ops resubmitted with the same id")
	client := fs.Int64("client-id", 1, "client id (high bits of transfer ids)")
	hist := fs.String("history", "history.jsonl", "history output")
	summary := fs.String("summary", "summary.json", "summary output")
	seed := fs.Uint64("seed", 42, "workload seed")
	mutant := fs.String("mutant", "", "seeded bug for checker validation: nolock (rowlock), onconflict (condupdate), snapshot (condupdate, auction)")
	mysqlDSN := fs.String("mysql-dsn", defaultMySQLDSN, "mysql DSN (mysql-* designs)")
	mysqlIso := fs.String("mysql-iso", "", "mysql transaction isolation: REPEATABLE-READ (default) | READ-COMMITTED")
	lockingIdem := fs.Bool("mysql-locking-idem", false, "mysql-rowlock: SELECT ... FOR UPDATE on the idempotency key")
	escrowHint := fs.Bool("escrow-hint", false, "escrow: sold-out hint for accounts that are only debited")
	auctions := fs.Int("auctions", 200, "auction: number of auctions")
	bidders := fs.Int64("bidders", 100_000, "auction: number of bidders")
	readFrac := fs.Float64("read-frac", 0.1, "auction: share of operations that read an auction")
	epochMs := fs.Int64("epoch-ms", 0, "start the load at this unix time (ms)")
	var rf ruleFlags
	rf.register(fs)
	fs.Parse(args)

	l := lf.layout()
	o := openOpts{design: *design, dsn: *dsn, mysqlDSN: *mysqlDSN, mysqlIso: *mysqlIso, brokers: *brokers, mutant: *mutant,
		lockingIdem: *lockingIdem, escrowHint: *escrowHint, slots: *slots, conns: *conc}
	hw, err := history.Create(*hist)
	if err != nil {
		return err
	}
	wait := func() {
		if *epochMs > 0 {
			time.Sleep(time.Until(time.UnixMilli(*epochMs)))
		}
	}
	if lf.sem == model.SemAuction {
		d, err := openAuctioneer(o, rf.rules())
		if err != nil {
			return err
		}
		defer d.Close()
		wait()
		s := load.RunAuction(ctx, load.AuctionConfig{Auctions: *auctions, Bidders: *bidders, Skew: *skew, ReadFrac: *readFrac,
			ClientID: *client, Duration: *dur, Concurrency: *conc, Rate: *rate, BurstSecs: *burst, DupFrac: *dupFrac,
			AttemptTO: 5 * time.Second, Grace: 30 * time.Second, Seed: *seed}, d, hw)
		if err := hw.Close(); err != nil {
			return err
		}
		b, _ := json.MarshalIndent(s, "", "  ")
		return os.WriteFile(*summary, b, 0o644)
	}
	led, err := openLedger(o, l)
	if err != nil {
		return err
	}
	defer led.Close()
	wait()
	s := load.Run(ctx, load.Config{
		Workload: workload.Config{Layout: l, Skew: *skew, WithdrawFrac: *withdraw},
		ClientID: *client, Duration: *dur, Concurrency: *conc, Rate: *rate, BurstSecs: *burst,
		DupFrac: *dupFrac, AttemptTO: 5 * time.Second, Grace: 30 * time.Second, Seed: *seed,
	}, led, hw)
	if err := hw.Close(); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(s, "", "  ")
	return os.WriteFile(*summary, b, 0o644)
}

func cmdReconciler(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reconciler", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN, "postgres DSN")
	addr := fs.String("redis", "127.0.0.1:6390", "redis address")
	fs.Parse(args)
	db, err := store.Open(*dsn, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	p := resp.NewPool(*addr, 1)
	defer p.Close()
	r := reconciler.New(db, p)
	err = r.Run(ctx)
	log.Printf("reconciler stopped: applied=%d", r.Applied)
	return err
}

func cmdCheck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var lf layoutFlags
	lf.register(fs)
	design := fs.String("design", "condupdate", "design whose state to read")
	dsn := fs.String("dsn", defaultDSN, "postgres DSN")
	redisAddr := fs.String("redis", "127.0.0.1:6379", "redis address (redislua)")
	out := fs.String("out", "", "write report JSON here")
	fs.Parse(args)
	ops, err := history.Read(fs.Args()...)
	if err != nil {
		return err
	}
	st, err := run.ReadState(ctx, *design, *dsn, *redisAddr)
	if err != nil {
		return err
	}
	r := checker.Check(lf.layout(), ops, st)
	fmt.Print(r)
	if *out != "" {
		b, _ := json.MarshalIndent(r, "", "  ")
		return os.WriteFile(*out, b, 0o644)
	}
	return nil
}

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c run.Config
	var lf layoutFlags
	lf.register(fs)
	fs.StringVar(&c.Design, "design", "condupdate", "rowlock | condupdate | escrow | redislua | kafka | tigerbeetle | mysql-rowlock | mysql-condupdate")
	fs.StringVar(&c.DSN, "dsn", defaultDSN, "postgres DSN (direct)")
	fs.Float64Var(&c.Skew, "skew", 1.5, "Zipf s over creators")
	fs.Float64Var(&c.WithdrawFrac, "withdraw-frac", 0.02, "share of creator withdrawals")
	fs.DurationVar(&c.Duration, "duration", 30*time.Second, "load duration")
	fs.IntVar(&c.Concurrency, "concurrency", 64, "client workers")
	fs.Float64Var(&c.Rate, "rate", 0, "open-loop ops/s (0 = closed loop)")
	fs.IntVar(&c.BurstSecs, "burst-secs", 0, "final seconds at 10x on two creators (open loop)")
	fs.Float64Var(&c.DupFrac, "dup-frac", 0, "duplicate submission share")
	fs.IntVar(&c.Slots, "slots", 16, "escrow slots")
	fs.IntVar(&c.Partitions, "partitions", 8, "kafka partitions")
	fs.IntVar(&c.Writers, "writers", 2, "design 5 writer processes")
	fs.StringVar(&c.Faults, "faults", "", "comma list, e.g. pg-restart@10s,pg-partition@15s+4s,pg-reset@20s,kill-writer@8s,pause-writer@8s+10s,rebalance@12s,kill-broker@15s,kill-redis@10s,redis-failover@10s,redis-partition-failover@10s+3s")
	fs.StringVar(&c.Out, "out", "runs/latest", "output directory")
	fs.BoolVar(&c.RedisMinReplicas, "redis-min-replicas", false, "design redislua: primary refuses writes without a replica ack in the last second")
	fs.DurationVar(&c.ReplicaLag, "replica-lag", 0, "design redislua: one-way latency on the replication link (e.g. 5ms for a cross-zone replica)")
	fs.StringVar(&c.Brokers, "brokers", "", "design kafka: external brokers (default: start the built-in kfake broker)")
	fs.BoolVar(&c.WriterProfile, "writer-cpuprofile", false, "design kafka: CPU-profile the first writer (writer0.pprof in -out)")
	fs.StringVar(&c.Mutant, "mutant", "", "run a seeded bug to validate the checker: nolock | onconflict | nodedupe | snapshot")
	fs.StringVar(&c.MySQLDSN, "mysql-dsn", defaultMySQLDSN, "mysql DSN (mysql-* designs)")
	fs.StringVar(&c.MySQLIso, "mysql-iso", "", "mysql isolation: REPEATABLE-READ (InnoDB default) | READ-COMMITTED")
	fs.BoolVar(&c.MySQLLockingIdem, "mysql-locking-idem", false, "mysql-rowlock: lock the idempotency key with SELECT ... FOR UPDATE")
	fs.BoolVar(&c.EscrowHint, "escrow-hint", false, "escrow: sold-out hint for accounts that are only debited (SKUs)")
	fs.StringVar(&c.MySQLDataDir, "mysql-datadir", "/var/lib/hotledger-mysql", "mysqld data directory (bench/setup_mysql.sh)")
	fs.StringVar(&c.TBData, "tb-data", "/var/lib/hotledger-tb/0_0.tigerbeetle", "tigerbeetle data file (recreated per run)")
	fs.IntVar(&c.AuctionPlan.N, "auctions", 200, "auction: number of auctions")
	fs.DurationVar(&c.AuctionPlan.Hot, "auction-hot", 0, "auction: deadline of auctions 1 and 2 after the start (0 = after the run)")
	fs.DurationVar(&c.AuctionPlan.OtherMin, "auction-other-min", 0, "auction: earliest deadline of the other auctions (0 = after the run)")
	fs.DurationVar(&c.AuctionPlan.OtherMax, "auction-other-max", 0, "auction: latest deadline of the other auctions")
	fs.DurationVar(&c.AuctionPlan.HardMax, "auction-hard-max", 30*time.Second, "auction: hard end = deadline + this")
	fs.Int64Var(&c.Bidders, "bidders", 100_000, "auction: bidders")
	fs.Float64Var(&c.ReadFrac, "read-frac", 0.1, "auction: share of reads")
	fs.DurationVar(&c.LinTimeout, "lin-timeout", 10*time.Second, "auction: Porcupine budget per auction")
	fs.IntVar(&c.PorcupineMaxWidth, "porcupine-max-width", 10, "auction: run Porcupine only on auctions with at most this many overlapping operations")
	var rf ruleFlags
	rf.register(fs)
	fs.Parse(args)
	c.Layout = lf.layout()
	c.Auction = lf.sem == model.SemAuction
	c.Rules = rf.rules()
	if c.AuctionPlan.Hot == 0 {
		c.AuctionPlan.Hot = c.Duration + time.Minute
	}
	if c.AuctionPlan.OtherMin == 0 {
		c.AuctionPlan.OtherMin = c.Duration + time.Minute
		c.AuctionPlan.OtherMax = c.Duration + 2*time.Minute
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	c.Self = self
	rep, err := run.Run(ctx, c)
	if err != nil {
		return err
	}
	fmt.Print(rep)
	if !rep.OK {
		os.Exit(3)
	}
	return nil
}
