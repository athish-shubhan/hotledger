package run

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"hotledger/internal/history"
)

// ---------------------------------------------------------------------------
// CPU per million operations.
//
// Server-side CPU is everything that serves the requests: Postgres (every
// backend plus background processes; backends that exit are counted through
// the postmaster's cumulative child time), MySQL, Redis, TigerBeetle, and the
// lab's own server-side processes (broker, writers, reconciler). The load
// generator and the orchestrator are excluded. Measured from the start of the
// load to the end of the drain, so async designs pay for applying what they
// accepted.

var serverComms = map[string]bool{"postgres": true, "redis-server": true, "mysqld": true, "tigerbeetle": true}

type procCPU struct {
	start uint64 // process start time (clock ticks since boot), to detect pid reuse
	ticks uint64 // utime + stime + cutime + cstime
}

const clkTck = 100 // sysconf(_SC_CLK_TCK) on Linux

func readProcStat(pid string) (comm string, ppid int, pc procCPU, ok bool) {
	b, err := os.ReadFile(filepath.Join("/proc", pid, "stat"))
	if err != nil {
		return
	}
	s := string(b)
	l, r := strings.IndexByte(s, '('), strings.LastIndexByte(s, ')')
	if l < 0 || r < l {
		return
	}
	comm = s[l+1 : r]
	f := strings.Fields(s[r+2:])
	// f[0]=state f[1]=ppid ... f[11]=utime f[12]=stime f[13]=cutime f[14]=cstime ... f[19]=starttime
	if len(f) < 20 {
		return
	}
	ppid, _ = strconv.Atoi(f[1])
	var t uint64
	for _, i := range []int{11, 12, 13, 14} {
		v, _ := strconv.ParseUint(f[i], 10, 64)
		t += v
	}
	st, _ := strconv.ParseUint(f[19], 10, 64)
	return comm, ppid, procCPU{start: st, ticks: t}, true
}

// cpuSnapshot returns per-pid CPU of the processes under test. include
// selects extra pids (the lab's own server-side children); exclude drops
// pids (the load generator).
func cpuSnapshot(include, exclude map[int]bool) map[int]procCPU {
	out := map[int]procCPU{}
	ents, _ := os.ReadDir("/proc")
	self := os.Getpid()
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self || exclude[pid] {
			continue
		}
		comm, _, pc, ok := readProcStat(e.Name())
		if !ok {
			continue
		}
		if serverComms[comm] || include[pid] {
			out[pid] = pc
		}
	}
	return out
}

func cpuDelta(before, after map[int]procCPU) float64 {
	var ticks uint64
	for pid, a := range after {
		if b, ok := before[pid]; ok && b.start == a.start {
			if a.ticks > b.ticks {
				ticks += a.ticks - b.ticks
			}
		} else {
			ticks += a.ticks // started during the window
		}
	}
	return float64(ticks) / clkTck
}

type CPUReport struct {
	ServerSec        float64 `json:"server_cpu_sec"`
	ClientSec        float64 `json:"client_cpu_sec"`
	Ops              int64   `json:"ops"` // operations with a known outcome
	ServerSecPerMops float64 `json:"server_cpu_sec_per_million_ops"`
	ClientSecPerMops float64 `json:"client_cpu_sec_per_million_ops"`
}

// ---------------------------------------------------------------------------
// Recovery after a fault, from the client history.

type FaultEvent struct {
	Kind string `json:"kind"` // crash | restart | partition | heal | other
	What string `json:"what"`
	AtNs int64  `json:"at_ns"`
}

type RecoveryStat struct {
	Fault string  `json:"fault"`
	AtSec float64 `json:"at_sec"` // since load start
	// Longest stretch with no successful client completion in the 30 s after the fault.
	OutageMs float64 `json:"client_outage_ms"`
	// From the matching restart (or heal) to the first successful completion of a request sent after it.
	ResumeMs float64 `json:"resume_after_restart_ms,omitempty"`
	// From the fault until a 1 s window again completes >= 90% of the pre-fault rate.
	Recover90Ms float64 `json:"recover_to_90pct_ms"`
	// Async designs: the same outage measured on the apply side (writer commits).
	ApplyOutageMs float64 `json:"apply_outage_ms,omitempty"`
}

func successTimes(ops []history.Op) []int64 {
	var ts []int64
	for _, op := range ops {
		switch op.Result {
		case "applied", "rejected", "enqueued", "ok":
			ts = append(ts, op.Complete)
		}
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	return ts
}

// longestGap is the longest interval without an event in [from, from+win],
// cut off at the last event of the run.
func longestGap(ts []int64, from int64, win time.Duration) float64 {
	if len(ts) == 0 {
		return 0
	}
	to := min(from+int64(win), ts[len(ts)-1])
	if to <= from {
		return 0
	}
	i := sort.Search(len(ts), func(i int) bool { return ts[i] >= from })
	prev := from
	var gap int64
	for ; i < len(ts) && ts[i] <= to; i++ {
		gap = max(gap, ts[i]-prev)
		prev = ts[i]
	}
	gap = max(gap, to-prev)
	return float64(gap) / 1e6
}

func countBetween(ts []int64, from, to int64) int {
	i := sort.Search(len(ts), func(i int) bool { return ts[i] >= from })
	j := sort.Search(len(ts), func(i int) bool { return ts[i] >= to })
	return j - i
}

func recovery(ops []history.Op, applied []int64, events []FaultEvent, loadStart int64) []RecoveryStat {
	ts := successTimes(ops)
	var out []RecoveryStat
	for i, ev := range events {
		if ev.Kind != "crash" && ev.Kind != "partition" {
			continue
		}
		r := RecoveryStat{Fault: ev.What, AtSec: float64(ev.AtNs-loadStart) / 1e9}
		r.OutageMs = longestGap(ts, ev.AtNs, 30*time.Second)
		// the next restart/heal after this crash/partition
		for _, e2 := range events[i+1:] {
			if (ev.Kind == "crash" && e2.Kind == "restart") || (ev.Kind == "partition" && e2.Kind == "heal") {
				first := int64(-1)
				for _, op := range ops {
					if op.Invoke >= e2.AtNs && (op.Result == "applied" || op.Result == "rejected" || op.Result == "enqueued" || op.Result == "ok") {
						if first < 0 || op.Complete < first {
							first = op.Complete
						}
					}
				}
				if first > 0 {
					r.ResumeMs = float64(first-e2.AtNs) / 1e6
				}
				break
			}
		}
		base := float64(countBetween(ts, ev.AtNs-int64(5*time.Second), ev.AtNs)) / 5
		r.Recover90Ms = -1
		for t := ev.AtNs; t < ev.AtNs+int64(30*time.Second); t += int64(100 * time.Millisecond) {
			if float64(countBetween(ts, t, t+int64(time.Second))) >= 0.9*base {
				r.Recover90Ms = float64(t-ev.AtNs) / 1e6
				break
			}
		}
		if len(applied) > 0 {
			r.ApplyOutageMs = longestGap(applied, ev.AtNs, 30*time.Second)
		}
		out = append(out, r)
	}
	return out
}
