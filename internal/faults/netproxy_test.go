package faults

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func echoServer(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				r := bufio.NewReader(c)
				for {
					l, err := r.ReadString('\n')
					if err != nil {
						c.Close()
						return
					}
					c.Write([]byte(l))
				}
			}()
		}
	}()
	return ln
}

func roundTrip(c net.Conn, r *bufio.Reader, msg string, to time.Duration) (string, error) {
	c.SetDeadline(time.Now().Add(to))
	if _, err := c.Write([]byte(msg + "\n")); err != nil {
		return "", err
	}
	return r.ReadString('\n')
}

func TestPartitionHoldsBytesAndHealDeliversThem(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	p, err := Listen("127.0.0.1:0", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	if got, err := roundTrip(c, r, "a", time.Second); err != nil || got != "a\n" {
		t.Fatalf("before partition: %q %v", got, err)
	}
	p.Partition()
	if _, err := roundTrip(c, r, "b", 300*time.Millisecond); err == nil {
		t.Fatal("expected timeout during partition")
	}
	p.Heal()
	c.SetDeadline(time.Now().Add(time.Second))
	got, err := r.ReadString('\n')
	if err != nil || got != "b\n" {
		t.Fatalf("held message not delivered after heal: %q %v", got, err)
	}
}

func TestResetClosesClientsAndRetargetMovesNewOnes(t *testing.T) {
	a, b := echoServer(t), echoServer(t)
	defer a.Close()
	defer b.Close()
	p, err := Listen("127.0.0.1:0", a.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	if _, err := roundTrip(c, r, "x", time.Second); err != nil {
		t.Fatal(err)
	}
	p.Retarget(b.Addr().String())
	p.Reset()
	if _, err := roundTrip(c, r, "y", time.Second); err == nil {
		t.Fatal("expected the old connection to be closed by Reset")
	}
	c2, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := roundTrip(c2, bufio.NewReader(c2), "z", time.Second); err != nil || got != "z\n" {
		t.Fatalf("new connection after retarget: %q %v", got, err)
	}
}

func TestDelayAddsLatencyKeepsOrder(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	p, err := Listen("127.0.0.1:0", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetDelay(50 * time.Millisecond)
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	start := time.Now()
	for _, m := range []string{"1", "2", "3"} {
		if _, err := c.Write([]byte(m + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	c.SetDeadline(time.Now().Add(2 * time.Second))
	for _, want := range []string{"1\n", "2\n", "3\n"} {
		got, err := r.ReadString('\n')
		if err != nil || got != want {
			t.Fatalf("got %q %v, want %q", got, err, want)
		}
	}
	// 50 ms each way through the proxy
	if el := time.Since(start); el < 100*time.Millisecond {
		t.Fatalf("round trip %v, expected at least 100ms of added latency", el)
	}
}
