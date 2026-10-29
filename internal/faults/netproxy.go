// Package faults holds the fault injectors: a TCP proxy that can partition or
// reset connections, and process-level faults (kill -9, SIGSTOP, database
// crash) driven by the run orchestrator.
package faults

import (
	"log"
	"net"
	"sync"
	"time"
)

// Proxy forwards TCP connections to a target. Partition() makes it stop
// moving bytes in both directions while keeping connections open, like a
// network partition: requests already sent sit in the proxy and are delivered
// late when the partition heals, after the client has probably given up and
// retried. That is the delayed-duplicate case idempotency keys exist for.
// Reset() instead closes every open connection.
type Proxy struct {
	ln     net.Listener
	target string

	mu          sync.Mutex
	cond        *sync.Cond
	partitioned bool
	conns       map[net.Conn]struct{}
	lastLog     time.Time
	delay       time.Duration // one-way latency added to every chunk (e.g. a lagging replication link)
}

// SetDelay adds one-way latency to every byte the proxy forwards. Order is
// preserved and throughput is not limited: chunks are timestamped on read and
// written when due.
func (p *Proxy) SetDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

func Listen(addr, target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	go p.accept()
	return p, nil
}

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) Close() error {
	p.Heal()
	p.Reset()
	return p.ln.Close()
}

// Retarget sends new connections to another backend (a failover); combine
// with Reset to move existing clients too.
func (p *Proxy) Retarget(target string) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
}

func (p *Proxy) Partition() {
	p.mu.Lock()
	p.partitioned = true
	p.mu.Unlock()
}

func (p *Proxy) Heal() {
	p.mu.Lock()
	p.partitioned = false
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *Proxy) Reset() {
	p.mu.Lock()
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
}

func (p *Proxy) waitHealed() {
	p.mu.Lock()
	for p.partitioned {
		p.cond.Wait()
	}
	p.mu.Unlock()
}

func (p *Proxy) track(c net.Conn, add bool) {
	p.mu.Lock()
	if add {
		p.conns[c] = struct{}{}
	} else {
		delete(p.conns, c)
	}
	p.mu.Unlock()
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serve(c)
	}
}

func (p *Proxy) serve(client net.Conn) {
	p.waitHealed() // new connections hang during a partition, like SYNs being dropped
	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	server, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		p.mu.Lock()
		if time.Since(p.lastLog) > 2*time.Second {
			log.Printf("netproxy: dial %s: %v (further dial errors suppressed for 2s)", target, err)
			p.lastLog = time.Now()
		}
		p.mu.Unlock()
		client.Close()
		return
	}
	p.track(client, true)
	p.track(server, true)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pump(server, client) }()
	go func() { defer wg.Done(); p.pump(client, server) }()
	wg.Wait()
	p.track(client, false)
	p.track(server, false)
}

func (p *Proxy) pump(dst, src net.Conn) {
	type chunk struct {
		b   []byte
		due time.Time
		eof bool
	}
	ch := make(chan chunk, 1024)
	go func() {
		for c := range ch {
			if d := time.Until(c.due); d > 0 {
				time.Sleep(d)
			}
			p.waitHealed()
			if c.eof {
				break
			}
			if _, err := dst.Write(c.b); err != nil {
				break
			}
		}
		dst.Close()
		src.Close()
		for range ch {
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		p.mu.Lock()
		d := p.delay
		p.mu.Unlock()
		if n > 0 {
			ch <- chunk{b: append([]byte(nil), buf[:n]...), due: time.Now().Add(d)}
		}
		if err != nil {
			// EOF or reset: the close is delivered after the bytes before it,
			// and is also held during a partition.
			ch <- chunk{eof: true, due: time.Now().Add(d)}
			break
		}
	}
	close(ch)
}
