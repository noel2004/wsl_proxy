package transport

import (
	"net"
	"os"
	"sync"
	"time"
)

// Conn is a concurrent-safe ordered stream implementing net.Conn. Write copies
// bytes into a bounded retransmission window; success means queued, not remotely
// processed. Close aborts. CloseWrite queues ordered EOF and leaves reads usable.
type Conn struct {
	ep                          *endpoint
	peer                        net.Addr
	target, request             string
	state                       *session
	err                         error
	changed                     chan struct{}
	done                        chan struct{}
	readDeadline, writeDeadline time.Time
	writeMu                     sync.Mutex
}

var _ net.Conn = (*Conn)(nil)

func (c *Conn) signalLocked() { close(c.changed); c.changed = make(chan struct{}) }

// Target is the opaque name supplied during opening. The application must resolve
// it against its configured registry and authorize access separately.
func (c *Conn) Target() string       { return c.target }
func (c *Conn) LocalAddr() net.Addr  { return c.ep.pc.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr { return c.peer }

// Done closes on local Close, peer RESET, listener shutdown, or transport failure.
// FIN alone does not close it. Adapters can use this signal to cancel blocked
// target I/O even when neither forwarding worker is currently reading the stream.
func (c *Conn) Done() <-chan struct{} {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if c.done == nil {
		c.done = make(chan struct{})
		if c.err != nil {
			close(c.done)
		}
	}
	return c.done
}

// waitChange releases the state lock while waiting. Deadline changes also signal
// the channel so blocked calls recalculate timers after waking.
func waitChange(ch <-chan struct{}, deadline time.Time) {
	if deadline.IsZero() {
		<-ch
		return
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	}
}

func (c *Conn) Read(dst []byte) (int, error) {
	for {
		c.ep.mu.Lock()
		if c.err != nil {
			err := c.err
			c.ep.mu.Unlock()
			return 0, err
		}
		if len(dst) == 0 {
			c.ep.mu.Unlock()
			return 0, nil
		}
		if !c.readDeadline.IsZero() && !time.Now().Before(c.readDeadline) {
			c.ep.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		n, packets, err := c.state.read(dst, time.Now())
		c.ep.emitLocked(c, packets)
		if n > 0 || err != nil {
			c.ep.mu.Unlock()
			return n, err
		}
		if c.err != nil {
			err := c.err
			c.ep.mu.Unlock()
			return 0, err
		}
		changed, deadline := c.changed, c.readDeadline
		c.ep.mu.Unlock()
		waitChange(changed, deadline)
	}
}

func (c *Conn) Write(src []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n := 0
	for {
		c.ep.mu.Lock()
		if c.err != nil {
			err := c.err
			c.ep.mu.Unlock()
			return n, err
		}
		if c.state.writeClosed {
			c.ep.mu.Unlock()
			return n, ErrWriteClosed
		}
		if n == len(src) {
			c.ep.mu.Unlock()
			return n, nil
		}
		if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
			c.ep.mu.Unlock()
			return n, os.ErrDeadlineExceeded
		}
		if c.state.canSend() {
			size := min(len(src)-n, c.ep.cfg.MaxDatagramBytes-headerSize)
			p := c.state.queue(src[n:n+size], false, time.Now())
			c.ep.emitLocked(c, []packet{p})
			n += size
			c.ep.mu.Unlock()
			continue
		}
		changed, deadline := c.changed, c.writeDeadline
		c.ep.mu.Unlock()
		waitChange(changed, deadline)
	}
}

// CloseWrite waits for capacity, then queues FIN after preceding writes. The write
// deadline applies. Retransmission continues after this call returns.
func (c *Conn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for {
		c.ep.mu.Lock()
		if c.err != nil {
			err := c.err
			c.ep.mu.Unlock()
			return err
		}
		if c.state.writeClosed {
			c.ep.mu.Unlock()
			return nil
		}
		if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
			c.ep.mu.Unlock()
			return os.ErrDeadlineExceeded
		}
		if c.state.canSend() {
			p := c.state.queue(nil, true, time.Now())
			c.ep.emitLocked(c, []packet{p})
			err := c.err
			c.ep.mu.Unlock()
			return err
		}
		changed, deadline := c.changed, c.writeDeadline
		c.ep.mu.Unlock()
		waitChange(changed, deadline)
	}
}

// Close discards pending bytes and sends one best-effort RESET. A lost RESET is
// bounded by the remote receive-inactivity and delivery deadlines.
func (c *Conn) Close() error {
	c.ep.mu.Lock()
	if c.err == nil {
		_ = c.ep.sendLocked(packet{kind: kindReset, id: c.state.id}, c.peer)
		c.ep.failLocked(c, net.ErrClosed)
	}
	c.ep.mu.Unlock()
	if !c.ep.server {
		c.ep.close()
	}
	return nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.readDeadline, c.writeDeadline = t, t
	c.signalLocked()
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.readDeadline = t
	c.signalLocked()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.writeDeadline = t
	c.signalLocked()
	return nil
}
