// Package run orchestrates one experiment: it seeds the state, starts the
// processes a design needs (broker, writers, Redis, reconciler) as separate
// OS processes so they can be killed with SIGKILL, puts a fault-injecting TCP
// proxy between clients and Postgres, runs the load generator, fires the
// scheduled faults, drains, and runs the checker.
package run

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/checker"
	"hotledger/internal/designs"
	"hotledger/internal/faults"
	"hotledger/internal/history"
	"hotledger/internal/load"
	"hotledger/internal/model"
	"hotledger/internal/resp"
	"hotledger/internal/store"
)

type Config struct {
	Design           string
	WriterProfile    bool
	DSN              string
	Layout           model.Layout
	Skew             float64
	WithdrawFrac     float64
	Duration         time.Duration
	Concurrency      int
	Rate             float64
	BurstSecs        int
	DupFrac          float64
	Slots            int
	Partitions       int
	Writers          int
	Faults           string
	Out              string
	Self             string
	Mutant           string
	Brokers          string        // design 5: use these Kafka brokers instead of starting kfake
	ReplicaLag       time.Duration // design 4: one-way latency on the primary -> replica link
	RedisMinReplicas bool          // design 4: min-replicas-to-write 1, min-replicas-max-lag 1 on the primary

	// Semantics (Layout.Sem carries wallet or inventory; auctions use the fields below).
	Auction           bool
	AuctionPlan       model.AuctionPlan
	Rules             model.AuctionRules
	Bidders           int64
	ReadFrac          float64
	LinTimeout        time.Duration // Porcupine budget per auction
	PorcupineMaxWidth int           // skip Porcupine on auctions with more overlapping operations

	MySQLDSN         string
	MySQLIso         string // REPEATABLE-READ (InnoDB default) or READ-COMMITTED
	MySQLLockingIdem bool   // design mysql-rowlock: SELECT ... FOR UPDATE on the idempotency key
	EscrowHint       bool   // design escrow: sold-out hint
	MySQLDataDir     string
	TBData           string // design tigerbeetle: data file
}

const (
	proxyPort         = 15432
	mysqlPort         = 3307
	tbPort            = 3033
	brokerPort        = 9092
	redisPort         = 6390
	redisProxyPort    = 6399
	redisReplLinkPort = 6398
)

type Result struct {
	Config       Config             `json:"config"`
	Load         load.Summary       `json:"load"`
	Check        checker.Report     `json:"check"`
	CheckPG      *checker.Report    `json:"check_postgres,omitempty"` // redislua: the durable copy
	Agreement    string             `json:"redis_postgres_agreement,omitempty"`
	E2ELatency   map[string]float64 `json:"e2e_latency_ms,omitempty"` // async designs: enqueue -> applied
	DrainSec     float64            `json:"drain_sec,omitempty"`
	WriterFenced int                `json:"writer_fenced_batches,omitempty"` // stale batches rejected by the offset fence
	FaultLog     []string           `json:"fault_log"`
	FaultEvents  []FaultEvent       `json:"fault_events,omitempty"`
	Recovery     []RecoveryStat     `json:"recovery,omitempty"`
	CPU          *CPUReport         `json:"cpu,omitempty"`

	AuctionCheck   *checker.AuctionReport `json:"auction_check,omitempty"`
	AuctionCheckPG *checker.AuctionReport `json:"auction_check_postgres,omitempty"` // redislua: the durable copy
	OK             bool                   `json:"ok"`
}

func (r Result) String() string {
	var b strings.Builder
	l := r.Load
	sem := r.Config.Layout.Sem
	if r.Config.Auction {
		sem = model.SemAuction
	}
	fmt.Fprintf(&b, "design=%s semantics=%s skew=%.2f rate=%.0f burst=%ds faults=%q mutant=%q\n", r.Config.Design, sem, r.Config.Skew, r.Config.Rate, r.Config.BurstSecs, r.Config.Faults, r.Config.Mutant)
	fmt.Fprintf(&b, "throughput=%.0f ops/s  latency p50=%.1fms p99=%.1fms p999=%.1fms  retries=%d\n",
		l.ThroughputOps, l.LatencyMs["p50"], l.LatencyMs["p99"], l.LatencyMs["p999"], l.Retries)
	if r.E2ELatency != nil {
		fmt.Fprintf(&b, "end-to-end (enqueue->applied) p50=%.1fms p99=%.1fms drain=%.1fs fenced stale batches=%d\n", r.E2ELatency["p50"], r.E2ELatency["p99"], r.DrainSec, r.WriterFenced)
	}
	if r.CPU != nil {
		fmt.Fprintf(&b, "cpu: server %.1fs (%.1f s per million ops), client %.1fs\n", r.CPU.ServerSec, r.CPU.ServerSecPerMops, r.CPU.ClientSec)
	}
	for _, f := range r.FaultLog {
		fmt.Fprintf(&b, "  fault: %s\n", f)
	}
	for _, rc := range r.Recovery {
		fmt.Fprintf(&b, "  recovery after %s: client outage %.0f ms, resume %.0f ms after restart, 90%% throughput after %.0f ms", rc.Fault, rc.OutageMs, rc.ResumeMs, rc.Recover90Ms)
		if rc.ApplyOutageMs > 0 {
			fmt.Fprintf(&b, ", apply outage %.0f ms", rc.ApplyOutageMs)
		}
		b.WriteString("\n")
	}
	if r.AuctionCheck != nil {
		b.WriteString(r.AuctionCheck.String())
		if r.AuctionCheckPG != nil {
			b.WriteString("postgres copy: " + r.AuctionCheckPG.String())
		}
		return b.String()
	}
	b.WriteString(r.Check.String())
	if r.CheckPG != nil {
		b.WriteString("postgres copy: " + r.CheckPG.String())
		b.WriteString("redis vs postgres: " + r.Agreement + "\n")
	}
	return b.String()
}

type proc struct {
	bin  string // empty: the hotledger binary itself (or redis-server for redis-*)
	name string
	cmd  *exec.Cmd
	args []string
	self string
	out  string
	done chan struct{} // closed when the current incarnation exits
}

func start(self, out, name string, args ...string) (*proc, error) {
	p := &proc{name: name, args: args, self: self, out: out}
	return p, p.start()
}

func (p *proc) start() error {
	f, err := os.OpenFile(filepath.Join(p.out, p.name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	bin := p.self
	args := p.args
	if p.bin != "" {
		bin = p.bin
	} else if strings.HasPrefix(p.name, "redis-") {
		bin, args = "redis-server", p.args
	}
	p.cmd = exec.Command(bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = f, f
	if err := p.cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	p.done = done
	cmd := p.cmd
	go func() { cmd.Wait(); f.Close(); close(done) }()
	return nil
}

func startBin(bin, out, name string, args ...string) (*proc, error) {
	p := &proc{bin: bin, name: name, args: args, out: out}
	return p, p.start()
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) signal(s syscall.Signal) { p.cmd.Process.Signal(s) }

func (p *proc) kill9() {
	p.cmd.Process.Kill()
	<-p.done
}

func (p *proc) stop() {
	p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		p.kill9()
	}
}

func waitPort(port int, d time.Duration) error {
	dl := time.Now().Add(d)
	for time.Now().Before(dl) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("port %d not ready", port)
}

func proxyDSN(dsn string) string {
	return strings.Replace(dsn, ":5432/", fmt.Sprintf(":%d/", proxyPort), 1)
}

func isMySQL(design string) bool { return strings.HasPrefix(design, "mysql-") }

// dbPort is the port of the design's system of record, where network faults go.
func dbPort(design string) int {
	switch {
	case isMySQL(design):
		return mysqlPort
	case design == "tigerbeetle":
		return tbPort
	}
	return 5432
}

func pgCtl(action string) error {
	env := map[string]string{"stop": "HOTLEDGER_PG_STOP", "start": "HOTLEDGER_PG_START"}[action]
	cmd := os.Getenv(env)
	if cmd == "" {
		cmd = map[string]string{
			"stop":  "sudo -n pg_ctlcluster 16 main stop -m immediate",
			"start": "sudo -n pg_ctlcluster 16 main start",
		}[action]
	}
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %v: %s", cmd, err, out)
	}
	return nil
}

func Run(ctx context.Context, c Config) (Result, error) {
	res := Result{Config: c}
	if err := os.MkdirAll(c.Out, 0o755); err != nil {
		return res, err
	}
	for _, f := range []string{"history.jsonl", "faults.log"} {
		os.Remove(filepath.Join(c.Out, f))
	}
	var procs []*proc
	defer func() {
		for i := len(procs) - 1; i >= 0; i-- {
			procs[i].stop()
		}
	}()
	// Postgres is the system of record for designs 1-3 and the durable copy
	// for 4 and 5; MySQL and TigerBeetle designs do not touch it.
	db, err := store.Open(c.DSN, 4)
	if err != nil {
		return res, err
	}
	defer db.Close()
	slots := 1
	if c.Design == "escrow" {
		slots = c.Slots
	}
	var mydb *sql.DB
	var mysqld, tbProc *proc
	tbAddr := fmt.Sprintf("127.0.0.1:%d", tbPort)
	switch {
	case isMySQL(c.Design):
		if mysqld, err = startMySQL(c); err != nil {
			return res, err
		}
		procs = append(procs, mysqld)
		if mydb, err = store.OpenMySQL(c.MySQLDSN, "", 4); err != nil {
			return res, err
		}
		defer mydb.Close()
		if err := store.InitMySQL(ctx, mydb, c.Layout); err != nil {
			return res, fmt.Errorf("init mysql: %w", err)
		}
	case c.Design == "tigerbeetle":
		if tbProc, err = startTigerBeetle(c, true); err != nil {
			return res, err
		}
		procs = append(procs, tbProc)
		if err := designs.SeedTigerBeetle([]string{tbAddr}, c.Layout); err != nil {
			return res, fmt.Errorf("seed tigerbeetle: %w", err)
		}
	default:
		if err := store.Init(ctx, db, c.Layout, slots, c.Partitions); err != nil {
			return res, fmt.Errorf("init: %w", err)
		}
	}
	// The fault proxy sits between every client/writer and the database only
	// when a network fault is scheduled, so clean benchmark runs do not pay
	// for an extra user-space hop on a small machine.
	clientDSN, clientMyDSN, clientTB := c.DSN, c.MySQLDSN, tbAddr
	var px *faults.Proxy
	if strings.Contains(c.Faults, "pg-partition") || strings.Contains(c.Faults, "pg-reset") {
		px, err = faults.Listen(fmt.Sprintf("127.0.0.1:%d", proxyPort), fmt.Sprintf("127.0.0.1:%d", dbPort(c.Design)))
		if err != nil {
			return res, err
		}
		defer px.Close()
		clientDSN = proxyDSN(c.DSN)
		clientMyDSN = strings.Replace(c.MySQLDSN, fmt.Sprintf(":%d)", mysqlPort), fmt.Sprintf(":%d)", proxyPort), 1)
		clientTB = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	}

	lay := []string{"-viewers", strconv.FormatInt(c.Layout.Viewers, 10), "-creators", strconv.FormatInt(c.Layout.Creators, 10),
		"-semantics", c.Layout.Sem, "-stock", strconv.FormatInt(c.Layout.Stock, 10), "-hot-stock", strconv.FormatInt(c.Layout.HotStock, 10)}
	ruleArgs := []string{"-extended=" + strconv.FormatBool(c.Rules.Extended), "-window", (time.Duration(c.Rules.WindowUs) * time.Microsecond).String(),
		"-min-inc", strconv.FormatInt(c.Rules.MinInc, 10), "-reserve", strconv.FormatInt(c.Rules.Reserve, 10)}
	if c.Auction {
		lay[5] = model.SemAuction
	}
	brokerArg := ""
	var broker *proc
	var writers []*proc
	var redis, replica *proc
	var rpx, replLink *faults.Proxy
	redisDir, _ := filepath.Abs(filepath.Join(c.Out, "redis"))
	switch c.Design {
	case "kafka":
		if c.Brokers != "" {
			// External Kafka (e.g. Apache Kafka from deploy/docker-compose.yml):
			// recreate the topic so offsets start at 0 like the seeded state.
			brokerArg = c.Brokers
			if err := recreateTopic(ctx, brokerArg, c.Partitions); err != nil {
				return res, fmt.Errorf("recreate topic: %w", err)
			}
		} else {
			data, _ := filepath.Abs(filepath.Join(c.Out, "kafka-data"))
			os.RemoveAll(data)
			broker, err = start(c.Self, c.Out, "broker", "broker", "-port", strconv.Itoa(brokerPort), "-partitions", strconv.Itoa(c.Partitions), "-data", data)
			if err != nil {
				return res, err
			}
			procs = append(procs, broker)
			if err := waitPort(brokerPort, 10*time.Second); err != nil {
				return res, err
			}
			brokerArg = fmt.Sprintf("127.0.0.1:%d", brokerPort)
		}
		for i := 0; i < c.Writers; i++ {
			wargs := append([]string{"writer", "-dsn", clientDSN, "-brokers", brokerArg, "-name", fmt.Sprintf("w%d", i), "-v", "-mutant", c.Mutant,
				"-auction=" + strconv.FormatBool(c.Auction)}, ruleArgs...)
			if c.WriterProfile && i == 0 {
				wargs = append(wargs, "-cpuprofile", filepath.Join(c.Out, "writer0.pprof"))
			}
			w, err := start(c.Self, c.Out, fmt.Sprintf("writer%d", i), wargs...)
			if err != nil {
				return res, err
			}
			procs = append(procs, w)
			writers = append(writers, w)
		}
		time.Sleep(3 * time.Second) // let the group form before load starts
	case "redislua":
		// Primary on redisPort, an asynchronously replicated replica on
		// redisPort+1, and a proxy on redisProxyPort that plays the role of
		// Sentinel: clients and the reconciler always talk to the proxy, and
		// a failover retargets it to the promoted replica.
		os.RemoveAll(redisDir)
		os.MkdirAll(filepath.Join(redisDir, "primary"), 0o755)
		os.MkdirAll(filepath.Join(redisDir, "replica"), 0o755)
		primaryArgs := []string{"--port", strconv.Itoa(redisPort), "--dir", filepath.Join(redisDir, "primary"),
			"--appendonly", "yes", "--appendfsync", "everysec", "--save", "", "--daemonize", "no"}
		redis, err = start(c.Self, c.Out, "redis-primary", primaryArgs...)
		if err != nil {
			return res, err
		}
		procs = append(procs, redis)
		// The replica pulls the replication stream through a proxy that adds
		// ReplicaLag of one-way latency, like a replica in another zone: the
		// async replication window that a failover can lose.
		replLink, err = faults.Listen(fmt.Sprintf("127.0.0.1:%d", redisReplLinkPort), fmt.Sprintf("127.0.0.1:%d", redisPort))
		if err != nil {
			return res, err
		}
		replLink.SetDelay(c.ReplicaLag)
		defer replLink.Close()
		replica, err = start(c.Self, c.Out, "redis-replica", "--port", strconv.Itoa(redisPort+1), "--dir", filepath.Join(redisDir, "replica"),
			"--appendonly", "yes", "--appendfsync", "everysec", "--save", "", "--daemonize", "no",
			"--replicaof", "127.0.0.1", strconv.Itoa(redisReplLinkPort))
		if err != nil {
			return res, err
		}
		procs = append(procs, replica)
		if err := waitPort(redisPort, 10*time.Second); err != nil {
			return res, err
		}
		if err := waitPort(redisPort+1, 10*time.Second); err != nil {
			return res, err
		}
		rpx, err = faults.Listen(fmt.Sprintf("127.0.0.1:%d", redisProxyPort), fmt.Sprintf("127.0.0.1:%d", redisPort))
		if err != nil {
			return res, err
		}
		defer rpx.Close()
		brokerArg = fmt.Sprintf("127.0.0.1:%d", redisProxyPort)
		rp := resp.NewPool(brokerArg, 1)
		if err := designs.SeedRedis(ctx, rp, c.Layout); err != nil {
			return res, fmt.Errorf("seed redis: %w", err)
		}
		if _, err := rp.Do(ctx, "WAIT", 1, 10000); err != nil {
			return res, fmt.Errorf("wait for replica: %w", err)
		}
		if c.RedisMinReplicas {
			// Refuse writes when no replica has acked within 1 s: bounds what
			// a partitioned primary can accept and then lose. Set after the
			// seed, once the replica is in sync.
			for _, kv := range [][2]string{{"min-replicas-to-write", "1"}, {"min-replicas-max-lag", "1"}} {
				if _, err := rp.Do(ctx, "CONFIG", "SET", kv[0], kv[1]); err != nil {
					return res, err
				}
			}
		}
		rp.Close()
		rec, err := start(c.Self, c.Out, "reconciler", "reconciler", "-dsn", clientDSN, "-redis", brokerArg)
		if err != nil {
			return res, err
		}
		procs = append(procs, rec)
	}

	if c.Design == "tigerbeetle" {
		brokerArg = clientTB
	}

	// Auctions are placed on the clock just before the load starts.
	epoch := time.Now().Add(1500 * time.Millisecond)
	var auctionInit map[int64]model.AuctionState
	if c.Auction {
		var states []model.AuctionState
		auctionInit = map[int64]model.AuctionState{}
		for id := int64(1); id <= int64(c.AuctionPlan.N); id++ {
			st := c.AuctionPlan.Initial(id, epoch.UnixMicro())
			states = append(states, st)
			auctionInit[id] = st
		}
		if isMySQL(c.Design) {
			if err := store.SeedAuctionsMySQL(ctx, mydb, states); err != nil {
				return res, err
			}
		} else if err := store.SeedAuctions(ctx, db, states); err != nil {
			return res, err
		}
		if c.Design == "redislua" {
			rp := resp.NewPool(brokerArg, 1)
			err := designs.SeedRedisAuctions(ctx, rp, states)
			if err == nil {
				_, err = rp.Do(ctx, "WAIT", 1, 10000)
			}
			rp.Close()
			if err != nil {
				return res, fmt.Errorf("seed redis auctions: %w", err)
			}
		}
	}

	loadArgs := append([]string{"load", "-design", c.Design, "-dsn", clientDSN, "-mysql-dsn", clientMyDSN, "-brokers", brokerArg,
		"-skew", fmt.Sprint(c.Skew), "-withdraw-frac", fmt.Sprint(c.WithdrawFrac), "-duration", c.Duration.String(),
		"-concurrency", strconv.Itoa(c.Concurrency), "-rate", fmt.Sprint(c.Rate), "-burst-secs", strconv.Itoa(c.BurstSecs),
		"-dup-frac", fmt.Sprint(c.DupFrac), "-slots", strconv.Itoa(c.Slots), "-mutant", c.Mutant,
		"-mysql-iso", c.MySQLIso, "-mysql-locking-idem=" + strconv.FormatBool(c.MySQLLockingIdem), "-escrow-hint=" + strconv.FormatBool(c.EscrowHint),
		"-auctions", strconv.Itoa(c.AuctionPlan.N), "-bidders", strconv.FormatInt(c.Bidders, 10), "-read-frac", fmt.Sprint(c.ReadFrac),
		"-epoch-ms", strconv.FormatInt(epoch.UnixMilli(), 10),
		"-history", filepath.Join(c.Out, "history.jsonl"), "-summary", filepath.Join(c.Out, "summary.json")}, append(lay, ruleArgs...)...)
	ld, err := start(c.Self, c.Out, "load", loadArgs...)
	if err != nil {
		return res, err
	}
	time.Sleep(time.Until(epoch))
	t0 := time.Now()
	serverPids := map[int]bool{}
	for _, p := range procs {
		if p.bin == "" && !strings.HasPrefix(p.name, "redis-") {
			serverPids[p.pid()] = true // broker, writers, reconciler
		}
	}
	cpu0 := cpuSnapshot(serverPids, map[int]bool{ld.pid(): true})

	// Fault schedule.
	flog := func(f string, a ...any) {
		s := fmt.Sprintf("t=%5.1fs ", time.Since(t0).Seconds()) + fmt.Sprintf(f, a...)
		res.FaultLog = append(res.FaultLog, s)
	}
	fev := func(kind, what string) {
		res.FaultEvents = append(res.FaultEvents, FaultEvent{Kind: kind, What: what, AtNs: time.Now().UnixNano()})
	}
	type step struct {
		at  time.Duration
		run func()
	}
	var steps []step
	for _, spec := range strings.Split(c.Faults, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		name, when, _ := strings.Cut(spec, "@")
		atS, durS, _ := strings.Cut(when, "+")
		at, err := time.ParseDuration(atS)
		if err != nil {
			return res, fmt.Errorf("fault %q: %w", spec, err)
		}
		dur := 5 * time.Second
		if durS != "" {
			if dur, err = time.ParseDuration(durS); err != nil {
				return res, fmt.Errorf("fault %q: %w", spec, err)
			}
		}
		switch name {
		case "pg-restart":
			steps = append(steps, step{at, func() {
				if err := pgCtl("stop"); err != nil {
					flog("pg-restart stop failed: %v", err)
					return
				}
				fev("crash", "postgres crash")
				flog("postgres crashed (stop -m immediate)")
				time.Sleep(2 * time.Second)
				if err := pgCtl("start"); err != nil {
					flog("pg start failed: %v", err)
					return
				}
				fev("restart", "postgres")
				flog("postgres restarted (crash recovery done)")
			}})
		case "pg-partition":
			steps = append(steps, step{at, func() {
				px.Partition()
				fev("partition", "partition clients<->database")
				flog("network partition clients<->%s for %s", dbName(c.Design), dur)
				time.Sleep(dur)
				px.Heal()
				fev("heal", "partition")
				flog("partition healed (held bytes delivered late)")
			}})
		case "pg-reset":
			steps = append(steps, step{at, func() { px.Reset(); flog("reset every client<->%s connection", dbName(c.Design)) }})
		case "kill-writer", "pause-writer", "rebalance", "zombie-writer":
			if c.Design != "kafka" {
				return res, fmt.Errorf("fault %s needs design kafka", name)
			}
			switch name {
			case "kill-writer":
				steps = append(steps, step{at, func() {
					writers[0].kill9()
					fev("crash", "writer kill -9")
					flog("kill -9 writer0 (mid-batch)")
					time.Sleep(3 * time.Second)
					if err := writers[0].start(); err != nil {
						flog("restart writer0: %v", err)
						return
					}
					fev("restart", "writer")
					flog("writer0 restarted")
				}})
			case "pause-writer":
				steps = append(steps, step{at, func() {
					writers[0].signal(syscall.SIGSTOP)
					flog("SIGSTOP writer0 for %s (past its 6s session timeout: zombie)", dur)
					time.Sleep(dur)
					writers[0].signal(syscall.SIGCONT)
					flog("SIGCONT writer0 (zombie resumes with stale assignment)")
				}})
			case "zombie-writer":
				steps = append(steps, step{at, func() {
					logPath := filepath.Join(c.Out, writers[0].name+".log")
					before := countIn(logPath, "fault: parked")
					writers[0].signal(syscall.SIGUSR1) // park after the next fetch
					// stop it only once it is parked on a fetched batch
					for i := 0; i < 100 && countIn(logPath, "fault: parked") == before; i++ {
						time.Sleep(50 * time.Millisecond)
					}
					writers[0].signal(syscall.SIGSTOP)
					flog("writer0 parked holding a fetched, unapplied batch, then SIGSTOP for %s (past its 6s session timeout)", dur)
					time.Sleep(dur)
					writers[0].signal(syscall.SIGCONT)
					time.Sleep(100 * time.Millisecond)
					writers[0].signal(syscall.SIGUSR2) // release: the zombie applies its stale batch
					flog("SIGCONT + release writer0: the zombie tries to apply its stale batch")
				}})
			case "rebalance":
				steps = append(steps, step{at, func() {
					wargs := append([]string{"writer", "-dsn", clientDSN, "-brokers", brokerArg, "-name", "wx", "-v", "-auction=" + strconv.FormatBool(c.Auction)}, ruleArgs...)
					w, err := start(c.Self, c.Out, "writer-extra", wargs...)
					if err != nil {
						flog("extra writer: %v", err)
						return
					}
					flog("added writer (rebalance)")
					time.Sleep(dur)
					w.stop()
					flog("removed extra writer (rebalance)")
				}})
			}
		case "kill-broker":
			if c.Design != "kafka" || broker == nil {
				return res, fmt.Errorf("kill-broker needs design kafka with the built-in broker")
			}
			steps = append(steps, step{at, func() {
				broker.kill9()
				fev("crash", "broker kill -9")
				flog("kill -9 broker")
				time.Sleep(2 * time.Second)
				if err := broker.start(); err != nil {
					flog("restart broker: %v", err)
					return
				}
				fev("restart", "broker")
				flog("broker restarted from its data dir")
			}})
		case "redis-partition-failover":
			if c.Design != "redislua" {
				return res, fmt.Errorf("%s needs design redislua", name)
			}
			steps = append(steps, step{at, func() {
				replLink.Partition()
				flog("partition primary <-> replica for %s; clients still reach the primary", dur)
				time.Sleep(dur)
				redis.kill9()
				rp := resp.NewPool(fmt.Sprintf("127.0.0.1:%d", redisPort+1), 1)
				_, err := rp.Do(ctx, "REPLICAOF", "NO", "ONE")
				rp.Close()
				if err != nil {
					flog("promote replica: %v", err)
					return
				}
				rpx.Retarget(fmt.Sprintf("127.0.0.1:%d", redisPort+1))
				rpx.Reset()
				flog("Sentinel-style failover to the partitioned-away replica; old primary fenced (killed)")
			}})
		case "kill-redis", "redis-failover":
			if c.Design != "redislua" {
				return res, fmt.Errorf("%s needs design redislua", name)
			}
			if name == "kill-redis" {
				steps = append(steps, step{at, func() {
					redis.kill9()
					fev("crash", "redis primary kill -9")
					flog("kill -9 redis primary (appendfsync everysec)")
					time.Sleep(time.Second)
					if err := redis.start(); err != nil {
						flog("restart redis: %v", err)
						return
					}
					rpx.Reset()
					fev("restart", "redis")
					flog("redis primary restarted from its AOF")
				}})
			} else {
				steps = append(steps, step{at, func() {
					redis.kill9()
					fev("crash", "redis primary kill -9, failover")
					flog("kill -9 redis primary")
					rp := resp.NewPool(fmt.Sprintf("127.0.0.1:%d", redisPort+1), 1)
					_, err := rp.Do(ctx, "REPLICAOF", "NO", "ONE")
					rp.Close()
					if err != nil {
						flog("promote replica: %v", err)
						return
					}
					rpx.Retarget(fmt.Sprintf("127.0.0.1:%d", redisPort+1))
					rpx.Reset()
					fev("restart", "replica promoted")
					flog("replica promoted, clients and reconciler redirected (Sentinel-style failover)")
				}})
			}
		case "kill-tigerbeetle":
			if tbProc == nil {
				return res, fmt.Errorf("%s needs design tigerbeetle", name)
			}
			steps = append(steps, step{at, func() {
				tbProc.kill9()
				fev("crash", "tigerbeetle kill -9")
				flog("kill -9 tigerbeetle")
				time.Sleep(2 * time.Second)
				p, err := startTigerBeetle(c, false)
				if err != nil {
					flog("restart tigerbeetle: %v", err)
					return
				}
				*tbProc = *p
				fev("restart", "tigerbeetle")
				flog("tigerbeetle restarted from its data file")
			}})
		case "mysql-restart":
			if mysqld == nil {
				return res, fmt.Errorf("%s needs a mysql design", name)
			}
			steps = append(steps, step{at, func() {
				mysqld.kill9()
				fev("crash", "mysqld kill -9")
				flog("kill -9 mysqld")
				time.Sleep(2 * time.Second)
				p, err := startMySQL(c)
				if err != nil {
					flog("restart mysqld: %v", err)
					return
				}
				*mysqld = *p
				fev("restart", "mysqld")
				flog("mysqld restarted (InnoDB crash recovery done)")
			}})
		default:
			return res, fmt.Errorf("unknown fault %q", name)
		}
	}
	for _, s := range steps {
		go func(s step) {
			time.Sleep(s.at)
			s.run()
		}(s)
	}

	<-ld.done
	if code := ld.cmd.ProcessState.ExitCode(); code != 0 {
		return res, fmt.Errorf("load process exited with %d (see %s/load.log)", code, c.Out)
	}
	b, err := os.ReadFile(filepath.Join(c.Out, "summary.json"))
	if err != nil {
		return res, err
	}
	json.Unmarshal(b, &res.Load)

	// Drain asynchronous designs.
	topic, table := "transfers", "transfers"
	if c.Auction {
		topic, table = designs.BidTopic, "bids"
	}
	drainStart := time.Now()
	switch c.Design {
	case "kafka":
		if err := drainKafka(ctx, db, brokerArg, topic); err != nil {
			return res, err
		}
		res.DrainSec = time.Since(drainStart).Seconds()
		res.E2ELatency, _ = e2e(ctx, db, table)
	case "redislua":
		if err := drainRedis(ctx, db, brokerArg); err != nil {
			return res, err
		}
		res.DrainSec = time.Since(drainStart).Seconds()
		// reconciliation lag: Redis XADD time -> Postgres commit
		res.E2ELatency, _ = e2e(ctx, db, table)
	}
	cpu1 := cpuSnapshot(serverPids, map[int]bool{ld.pid(): true})
	var known int64
	for r, n := range res.Load.Results {
		if !strings.HasSuffix(r, "unknown") {
			known += n
		}
	}
	res.CPU = &CPUReport{ServerSec: cpuDelta(cpu0, cpu1), Ops: known}
	if ps := ld.cmd.ProcessState; ps != nil {
		res.CPU.ClientSec = (ps.UserTime() + ps.SystemTime()).Seconds()
	}
	if known > 0 {
		res.CPU.ServerSecPerMops = res.CPU.ServerSec / float64(known) * 1e6
		res.CPU.ClientSecPerMops = res.CPU.ClientSec / float64(known) * 1e6
	}

	if c.Design == "kafka" {
		logs, _ := filepath.Glob(filepath.Join(c.Out, "writer*.log"))
		for _, l := range logs {
			b, _ := os.ReadFile(l)
			res.WriterFenced += strings.Count(string(b), " fenced: expected offset")
		}
	}
	ops, err := history.Read(filepath.Join(c.Out, "history.jsonl"))
	if err != nil {
		return res, err
	}
	if len(res.FaultEvents) > 0 {
		var applied []int64
		if c.Design == "kafka" {
			applied = appliedTimes(ctx, db, table)
		}
		res.Recovery = recovery(ops, applied, res.FaultEvents, t0.UnixNano())
	}
	if c.Auction {
		snap, err := readAuctionState(ctx, c, db, mydb, brokerArg)
		if err != nil {
			return res, err
		}
		lo := checker.LinOptions{Timeout: c.LinTimeout, PorcupineMaxWidth: c.PorcupineMaxWidth, PorcupineMaxOps: 4000, OutDir: c.Out}
		r := checker.CheckAuction(c.Rules, auctionInit, ops, snap, lo)
		res.AuctionCheck = &r
		res.OK = r.OK
		if c.Design == "redislua" {
			psnap, err := store.SnapshotAuctions(ctx, db)
			if err != nil {
				return res, err
			}
			lo.OutDir = ""
			rp := checker.CheckAuction(c.Rules, auctionInit, ops, psnap, lo)
			res.AuctionCheckPG = &rp
			res.OK = res.OK && rp.OK
		}
	} else {
		st, err := readState(ctx, c, mydb, brokerArg)
		if err != nil {
			return res, err
		}
		res.Check = checker.Check(c.Layout, ops, st)
		res.OK = res.Check.OK
		if c.Design == "redislua" {
			pst, err := store.Snapshot(ctx, db)
			if err != nil {
				return res, err
			}
			r := checker.Check(c.Layout, ops, pst)
			res.CheckPG = &r
			res.Agreement = agreement(st, pst)
			res.OK = res.OK && r.OK && res.Agreement == "identical"
		}
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	os.WriteFile(filepath.Join(c.Out, "result.json"), out, 0o644)
	return res, nil
}

func dbName(design string) string {
	switch {
	case isMySQL(design):
		return "mysql"
	case design == "tigerbeetle":
		return "tigerbeetle"
	}
	return "postgres"
}

// startMySQL runs mysqld on its own data directory (created once by
// bench/setup_mysql.sh) with InnoDB's durable defaults: redo log flushed at
// every commit (innodb_flush_log_at_trx_commit=1) and the binlog synced at
// every commit (sync_binlog=1).
func startMySQL(c Config) (*proc, error) {
	os.MkdirAll("/run/hotledger-mysql", 0o755) // /run is a tmpfs: gone after a reboot
	p, err := startBin("mysqld", c.Out, "mysqld", "--no-defaults", "--user=root", "--datadir="+c.MySQLDataDir,
		"--port="+strconv.Itoa(mysqlPort), "--socket=/run/hotledger-mysql/mysqld.sock", "--mysqlx=OFF", "--skip-name-resolve",
		"--innodb-buffer-pool-size=1G", "--max-connections=600", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1")
	if err != nil {
		return nil, err
	}
	if err := waitPort(mysqlPort, 60*time.Second); err != nil {
		p.kill9()
		return nil, err
	}
	return p, nil
}

const tbBin = "tigerbeetle"

// startTigerBeetle runs one replica. fresh formats a new data file first.
func startTigerBeetle(c Config, fresh bool) (*proc, error) {
	if fresh {
		os.Remove(c.TBData)
		out, err := exec.Command(tbBin, "format", "--cluster=0", "--replica=0", "--replica-count=1", c.TBData).CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("tigerbeetle format: %v: %s", err, out)
		}
	}
	p, err := startBin(tbBin, c.Out, "tigerbeetle", "start", fmt.Sprintf("--addresses=127.0.0.1:%d", tbPort), "--cache-grid=512MiB", c.TBData)
	if err != nil {
		return nil, err
	}
	if err := waitPort(tbPort, 60*time.Second); err != nil {
		p.kill9()
		return nil, err
	}
	return p, nil
}

func appliedTimes(ctx context.Context, db *sql.DB, table string) []int64 {
	rows, err := db.QueryContext(ctx, `SELECT done_ns FROM `+table+` WHERE done_ns IS NOT NULL ORDER BY done_ns`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var ts []int64
	for rows.Next() {
		var t int64
		if rows.Scan(&t) == nil {
			ts = append(ts, t)
		}
	}
	return ts
}

func countIn(path, needle string) int {
	b, _ := os.ReadFile(path)
	return strings.Count(string(b), needle)
}

func recreateTopic(ctx context.Context, brokers string, parts int) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	adm.DeleteTopics(ctx, "transfers")
	for i := 0; i < 50; i++ { // topic deletion is asynchronous in Kafka
		r, err := adm.CreateTopics(ctx, int32(parts), -1, nil, "transfers")
		if err == nil && r["transfers"].Err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not create topic transfers")
}

func drainKafka(ctx context.Context, db *sql.DB, broker, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	dl := time.Now().Add(3 * time.Minute)
	for time.Now().Before(dl) {
		ends, err := adm.ListEndOffsets(ctx, topic)
		if err == nil {
			behind := false
			ends.Each(func(o kadm.ListedOffset) {
				var next int64
				if err := db.QueryRowContext(ctx, `SELECT next_offset FROM partition_offsets WHERE part = $1`, o.Partition).Scan(&next); err != nil || next < o.Offset {
					behind = true
				}
			})
			if !behind {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("writers did not drain within 3 minutes")
}

func drainRedis(ctx context.Context, db *sql.DB, addr string) error {
	p := resp.NewPool(addr, 1)
	defer p.Close()
	dl := time.Now().Add(3 * time.Minute)
	var last int64 = -1
	stable := 0
	for time.Now().Before(dl) && ctx.Err() == nil {
		var n int64
		db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM transfers) + (SELECT count(*) FROM bids)`).Scan(&n)
		v, err := p.Do(ctx, "XINFO", "GROUPS", "log")
		lag := int64(-1)
		if err == nil {
			if gs, ok := v.([]any); ok && len(gs) > 0 {
				g, _ := gs[0].([]any)
				for i := 0; i+1 < len(g); i += 2 {
					if k, _ := g[i].(string); k == "pending" {
						lag, _ = g[i+1].(int64)
					}
				}
			}
		}
		if stable == 0 && time.Since(dl.Add(-3*time.Minute)) > 10*time.Second {
			log.Printf("drain: postgres transfers=%d pending=%d err=%v", n, lag, err)
		}
		if n == last && lag == 0 {
			stable++
			if stable >= 4 {
				return nil
			}
		} else {
			stable = 0
		}
		last = n
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("reconciler did not drain")
}

func e2e(ctx context.Context, db *sql.DB, table string) (map[string]float64, error) {
	var p50, p99, p999, mx float64
	err := db.QueryRowContext(ctx, `SELECT
		percentile_cont(0.5) WITHIN GROUP (ORDER BY done_ns - enq_ns) / 1e6,
		percentile_cont(0.99) WITHIN GROUP (ORDER BY done_ns - enq_ns) / 1e6,
		percentile_cont(0.999) WITHIN GROUP (ORDER BY done_ns - enq_ns) / 1e6,
		max(done_ns - enq_ns) / 1e6
		FROM `+table+` WHERE enq_ns IS NOT NULL`).Scan(&p50, &p99, &p999, &mx)
	return map[string]float64{"p50": p50, "p99": p99, "p999": p999, "max": mx}, err
}

func agreement(a, b model.State) string {
	diff := 0
	for id, v := range a.Balances {
		if b.Balances[id] != v {
			diff++
		}
	}
	at := map[int64]bool{}
	for _, t := range a.Transfers {
		at[t.ID] = t.Applied
	}
	onlyPG := 0
	for _, t := range b.Transfers {
		if v, ok := at[t.ID]; !ok || v != t.Applied {
			onlyPG++
		}
	}
	if diff == 0 && onlyPG == 0 && len(a.Transfers) == len(b.Transfers) {
		return "identical"
	}
	return fmt.Sprintf("DIVERGED: %d balances differ, %d transfers in postgres not matching redis, redis=%d postgres=%d transfers",
		diff, onlyPG, len(a.Transfers), len(b.Transfers))
}

// ReadState reads a design's final state from its system of record (Redis
// for design 4, Postgres for the rest).
func ReadState(ctx context.Context, design, dsn, redisAddr string) (model.State, error) {
	if design != "redislua" {
		db, err := store.Open(dsn, 2)
		if err != nil {
			return model.State{}, err
		}
		defer db.Close()
		return store.Snapshot(ctx, db)
	}
	p := resp.NewPool(redisAddr, 1)
	defer p.Close()
	st := model.State{Balances: map[int64]int64{}, Extra: map[string]string{}}
	v, err := p.Do(ctx, "HGETALL", "bal")
	if err != nil {
		return st, err
	}
	kv, _ := v.([]any)
	for i := 0; i+1 < len(kv); i += 2 {
		id, _ := strconv.ParseInt(kv[i].(string), 10, 64)
		b, _ := strconv.ParseInt(kv[i+1].(string), 10, 64)
		st.Balances[id] = b
		if b < 0 {
			st.Negative = append(st.Negative, fmt.Sprintf("account %d balance %d", id, b))
		}
	}
	cursor := "-"
	for {
		v, err := p.Do(ctx, "XRANGE", "log", cursor, "+", "COUNT", 10000)
		if err != nil {
			return st, err
		}
		msgs, _ := v.([]any)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			mv := m.([]any)
			cursor = "(" + mv[0].(string)
			f := mv[1].([]any)
			var t model.AppliedTransfer
			for i := 0; i+1 < len(f); i += 2 {
				val := f[i+1].(string)
				n, _ := strconv.ParseInt(val, 10, 64)
				switch f[i].(string) {
				case "id":
					t.ID = n
				case "src":
					t.Src = n
				case "dst":
					t.Dst = n
				case "amt":
					t.Amount = n
				case "ok":
					t.Applied = val == "1"
				}
			}
			st.Transfers = append(st.Transfers, t)
		}
	}
	return st, nil
}

func readState(ctx context.Context, c Config, mydb *sql.DB, redisAddr string) (model.State, error) {
	switch {
	case isMySQL(c.Design):
		return store.SnapshotMySQL(ctx, mydb)
	case c.Design == "tigerbeetle":
		return designs.SnapshotTigerBeetle([]string{fmt.Sprintf("127.0.0.1:%d", tbPort)}, c.Layout)
	}
	return ReadState(ctx, c.Design, c.DSN, redisAddr)
}

// readAuctionState reads auctions and the bid record from the design's
// system of record (Redis for design 4).
func readAuctionState(ctx context.Context, c Config, db, mydb *sql.DB, redisAddr string) (model.AuctionSnapshot, error) {
	switch {
	case isMySQL(c.Design):
		return store.SnapshotAuctionsMySQL(ctx, mydb)
	case c.Design != "redislua":
		return store.SnapshotAuctions(ctx, db)
	}
	snap := model.AuctionSnapshot{Auctions: map[int64]model.AuctionState{}}
	p := resp.NewPool(redisAddr, 1)
	defer p.Close()
	for id := int64(1); id <= int64(c.AuctionPlan.N); id++ {
		v, err := p.Do(ctx, "HMGET", "auc:"+strconv.FormatInt(id, 10), "high", "bidder", "deadline", "hard")
		if err != nil {
			return snap, err
		}
		f, _ := v.([]any)
		n := func(i int) int64 {
			if i >= len(f) {
				return 0
			}
			s, _ := f[i].(string)
			x, _ := strconv.ParseInt(s, 10, 64)
			return x
		}
		snap.Auctions[id] = model.AuctionState{ID: id, High: n(0), Bidder: n(1), DeadlineUs: n(2), HardEndUs: n(3)}
	}
	cursor := "-"
	for {
		v, err := p.Do(ctx, "XRANGE", "log", cursor, "+", "COUNT", 10000)
		if err != nil {
			return snap, err
		}
		msgs, _ := v.([]any)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			mv := m.([]any)
			cursor = "(" + mv[0].(string)
			f := mv[1].([]any)
			var b model.RecordedBid
			isBid := false
			for i := 0; i+1 < len(f); i += 2 {
				val := f[i+1].(string)
				n, _ := strconv.ParseInt(val, 10, 64)
				switch f[i].(string) {
				case "t":
					isBid = val == "b"
				case "id":
					b.ID = n
				case "auc":
					b.Auction = n
				case "bidder":
					b.Bidder = n
				case "amt":
					b.Amount = n
				case "res":
					r, err := designs.ParseBidResult(val)
					if err == nil {
						b.Accepted, b.Reason, b.TsUs, b.High = r.Accepted, r.Reason, r.TsUs, r.High
					}
				}
			}
			if isBid {
				snap.Bids = append(snap.Bids, b)
			}
		}
	}
	return snap, nil
}
