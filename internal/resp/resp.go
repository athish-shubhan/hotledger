// Package resp is a minimal Redis (RESP2) client with a fixed-size
// connection pool. It supports what the lab needs: commands with bulk-string
// arguments and replies of every RESP2 type.
package resp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type Error string

func (e Error) Error() string { return string(e) }

type conn struct {
	c  net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
	ok bool
}

type Pool struct {
	addr string
	ch   chan *conn
}

func NewPool(addr string, size int) *Pool {
	p := &Pool{addr: addr, ch: make(chan *conn, size)}
	for i := 0; i < size; i++ {
		p.ch <- &conn{}
	}
	return p
}

func (p *Pool) Close() {
	for {
		select {
		case c := <-p.ch:
			if c.c != nil {
				c.c.Close()
			}
		default:
			return
		}
	}
}

// Do sends one command and reads one reply. Replies: string (simple or bulk),
// int64, nil (null bulk), []any (array), or an Error value returned as err.
func (p *Pool) Do(ctx context.Context, args ...any) (any, error) {
	var c *conn
	select {
	case c = <-p.ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { p.ch <- c }()
	if c.c == nil || !c.ok {
		if c.c != nil {
			c.c.Close()
		}
		d := net.Dialer{Timeout: 2 * time.Second}
		nc, err := d.DialContext(ctx, "tcp", p.addr)
		if err != nil {
			c.c = nil
			return nil, err
		}
		c.c, c.r, c.w, c.ok = nc, bufio.NewReader(nc), bufio.NewWriter(nc), true
	}
	if dl, ok := ctx.Deadline(); ok {
		c.c.SetDeadline(dl)
	} else {
		c.c.SetDeadline(time.Time{})
	}
	if err := writeCmd(c.w, args); err != nil {
		c.ok = false
		return nil, err
	}
	v, err := ReadReply(c.r)
	if err != nil {
		var re Error
		if !errors.As(err, &re) {
			c.ok = false // protocol or network error: drop the connection
		}
		return nil, err
	}
	return v, nil
}

func writeCmd(w *bufio.Writer, args []any) error {
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		var s string
		switch v := a.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case int:
			s = strconv.Itoa(v)
		case int64:
			s = strconv.FormatInt(v, 10)
		default:
			s = fmt.Sprint(v)
		}
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	}
	return w.Flush()
}

// ReadReply parses one RESP2 reply.
func ReadReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("resp: short line")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, Error(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := ReadReply(r)
			if err != nil {
				var re Error
				if errors.As(err, &re) {
					out[i] = re
					continue
				}
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("resp: bad type byte %q", line[0])
}
