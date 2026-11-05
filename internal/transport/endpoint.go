package transport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Listener accepts streams over one UDP socket. Close closes the socket and all
// accepted connections. Acceptance does not mean a Windows target was opened.
type Listener struct{ ep *endpoint }

var _ net.Listener = (*Listener)(nil)

type openRequest struct {
	conn     *Conn
	response packet
}

// endpoint serializes state under mu. Workers own reads and time scans. Socket
// writes under mu have a bounded deadline. Closing the endpoint closes the socket
// first to interrupt blocked socket operations before acquiring mu.
type endpoint struct {
	mu                   sync.Mutex
	pc                   net.PacketConn
	cfg                  Config
	server               bool
	closed               error
	done                 chan struct{}
	workers              sync.WaitGroup
	conns                map[uint16]*Conn
	pool                 *idPool
	requests             map[string]*openRequest
	accept               chan *Conn
	reserved             int64
	dial                 *Conn
	opening              packet
	token                uint64
	openStart, openRetry time.Time
	openInterval         time.Duration
}

// Listen binds UDP. This transport is unauthenticated; callers must choose an
// appropriate bind address and authorize clients before exposing host resources.
func Listen(address string, cfg Config) (*Listener, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	pc, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, err
	}
	return ListenPacket(pc, cfg)
}

// ListenPacket takes ownership of pc, even on validation failure. It permits
// socket configuration and datagram fault injection by the caller.
func ListenPacket(pc net.PacketConn, cfg Config) (*Listener, error) {
	if err := cfg.Validate(); err != nil {
		pc.Close()
		return nil, err
	}
	e := newEndpoint(pc, cfg, true)
	e.start()
	return &Listener{ep: e}, nil
}

// DialContext opens a stream for a target name. The context limits opening only;
// use connection deadlines or Close to cancel subsequent I/O.
func DialContext(ctx context.Context, address, target string, cfg Config) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	remote, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	network, local := "udp6", "[::]:0"
	if remote.IP.To4() != nil {
		network, local = "udp4", "0.0.0.0:0"
	}
	pc, err := net.ListenPacket(network, local)
	if err != nil {
		return nil, err
	}
	return DialPacket(ctx, pc, remote, target, cfg)
}

// DialPacket owns pc, even on error, and uses it for one client connection. The
// listener multiplexes connections on its shared socket.
func DialPacket(ctx context.Context, pc net.PacketConn, remote net.Addr, target string, cfg Config) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		pc.Close()
		return nil, err
	}
	if remote == nil || target == "" || len(target) > cfg.MaxDatagramBytes-headerSize-openSize {
		pc.Close()
		return nil, errors.New("transport: invalid peer or target name")
	}
	if err := ctx.Err(); err != nil {
		pc.Close()
		return nil, err
	}
	var tokenBytes [8]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		pc.Close()
		return nil, err
	}
	e := newEndpoint(pc, cfg, false)
	now := time.Now()
	e.token = binary.BigEndian.Uint64(tokenBytes[:])
	e.opening = packet{kind: kindOpen, limit: cfg.ReceiveWindowPackets, payload: openPayload(e.token, cfg, target)}
	e.openStart, e.openRetry, e.openInterval = now, now.Add(cfg.InitialRetryInterval), cfg.InitialRetryInterval
	c := &Conn{ep: e, peer: remote, target: target, changed: make(chan struct{})}
	e.dial = c
	e.mu.Lock()
	err := e.sendLocked(e.opening, remote)
	if err != nil {
		e.shutdownLocked(err)
	}
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	e.start()
	for {
		e.mu.Lock()
		if c.err != nil {
			err := c.err
			e.mu.Unlock()
			e.close()
			return nil, err
		}
		if c.state != nil {
			e.mu.Unlock()
			return c, nil
		}
		changed := c.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			e.close()
			return nil, ctx.Err()
		}
	}
}

func newEndpoint(pc net.PacketConn, cfg Config, server bool) *endpoint {
	e := &endpoint{pc: pc, cfg: cfg, server: server, done: make(chan struct{}), conns: make(map[uint16]*Conn)}
	if server {
		e.pool = newIDPool(cfg.MaxConnections)
		e.requests = make(map[string]*openRequest)
		e.accept = make(chan *Conn, cfg.AcceptBacklog)
	}
	return e
}

func (e *endpoint) start() {
	e.workers.Add(2)
	go e.readLoop()
	go e.tickLoop()
}

func (e *endpoint) sendLocked(p packet, peer net.Addr) error {
	if err := e.pc.SetWriteDeadline(time.Now().Add(e.cfg.SocketWriteTimeout)); err != nil {
		return err
	}
	b := p.encode()
	n, err := e.pc.WriteTo(b, peer)
	if err == nil && n != len(b) {
		return errors.New("transport: short datagram write")
	}
	return err
}

func (e *endpoint) emitLocked(c *Conn, packets []packet) {
	for _, p := range packets {
		if err := e.sendLocked(p, c.peer); err != nil {
			e.failLocked(c, err)
			return
		}
	}
}

func (e *endpoint) readLoop() {
	defer e.workers.Done()
	// Read a complete ordinary UDP datagram, then apply our smaller protocol
	// limit. A short receive buffer can produce a socket error on Windows.
	b := make([]byte, 65536)
	for {
		n, peer, err := e.pc.ReadFrom(b)
		if err != nil {
			e.mu.Lock()
			e.shutdownLocked(err)
			e.mu.Unlock()
			return
		}
		p, err := decode(b[:n], e.cfg.MaxDatagramBytes)
		if err != nil {
			continue
		}
		e.mu.Lock()
		if e.closed != nil {
			e.mu.Unlock()
			return
		}
		e.dispatchLocked(p, peer, time.Now())
		e.mu.Unlock()
	}
}

func peerKey(peer net.Addr) string { return peer.Network() + "|" + peer.String() }

func (e *endpoint) dispatchLocked(p packet, peer net.Addr, now time.Time) {
	if e.server && p.kind == kindOpen {
		e.openLocked(p, peer, now)
		return
	}
	if !e.server {
		c := e.dial
		if peerKey(peer) != peerKey(c.peer) {
			return
		}
		if c.state == nil {
			if (p.kind != kindOpened && p.kind != kindReject) || binary.BigEndian.Uint64(p.payload[:8]) != e.token {
				return
			}
			if p.kind == kindReject {
				e.shutdownLocked(ErrRejected)
				return
			}
			if !compatibleOpen(p, e.cfg) {
				e.shutdownLocked(ErrRejected)
				return
			}
			c.state = newSession(e.cfg, p.id, p.limit, now)
			e.conns[p.id] = c
			c.signalLocked()
			return
		}
	}
	c := e.conns[p.id]
	if c == nil || peerKey(peer) != peerKey(c.peer) {
		return
	}
	if p.kind == kindReset {
		e.failLocked(c, ErrPeerClosed)
		return
	}
	packets, valid := c.state.input(p, now)
	if valid {
		e.emitLocked(c, packets)
		c.signalLocked()
	}
}

func (e *endpoint) openLocked(p packet, peer net.Addr, now time.Time) {
	token := binary.BigEndian.Uint64(p.payload[:8])
	request := fmt.Sprintf("%s|%016x", peerKey(peer), token)
	reject := func() {
		_ = e.sendLocked(packet{kind: kindReject, payload: append([]byte(nil), p.payload[:8]...)}, peer)
	}
	if !compatibleOpen(p, e.cfg) {
		reject()
		return
	}
	if old := e.requests[request]; old != nil {
		if old.conn == nil {
			reject()
		} else if old.conn.target != string(p.payload[openSize:]) || old.conn.state.peerWindow != p.limit {
			reject()
		} else {
			e.emitLocked(old.conn, []packet{old.response})
		}
		return
	}
	budget := e.cfg.connectionBudget()
	if len(e.accept) >= cap(e.accept) || budget > e.cfg.MaxBufferedBytes-e.reserved {
		reject()
		return
	}
	slot, ok := e.pool.take(now)
	if !ok {
		reject()
		return
	}
	delete(e.requests, slot.request)
	c := &Conn{ep: e, peer: peer, target: string(p.payload[openSize:]), request: request, changed: make(chan struct{}), state: newSession(e.cfg, slot.id, p.limit, now)}
	response := packet{kind: kindOpened, id: slot.id, limit: e.cfg.ReceiveWindowPackets, payload: openPayload(token, e.cfg, "")}
	e.conns[slot.id] = c
	e.requests[request] = &openRequest{conn: c, response: response}
	e.reserved += budget
	e.emitLocked(c, []packet{response})
	if c.err == nil {
		e.accept <- c
	}
}

func (e *endpoint) tickLoop() {
	defer e.workers.Done()
	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.mu.Lock()
			e.tickLocked(time.Now())
			e.mu.Unlock()
		}
	}
}

func (e *endpoint) tickLocked(now time.Time) {
	if e.closed != nil {
		return
	}
	if !e.server && e.dial.state == nil {
		if now.Sub(e.openStart) >= e.cfg.ConnectTimeout {
			e.shutdownLocked(ErrConnectTimeout)
			return
		}
		if !now.Before(e.openRetry) {
			if err := e.sendLocked(e.opening, e.dial.peer); err != nil {
				e.shutdownLocked(err)
				return
			}
			e.openInterval = e.cfg.backoff(e.openInterval)
			e.openRetry = now.Add(e.openInterval)
		}
		return
	}
	for _, c := range e.conns {
		packets, err := c.state.tick(now)
		if err != nil {
			e.failLocked(c, err)
			continue
		}
		e.emitLocked(c, packets)
		if len(packets) > 0 {
			c.signalLocked()
		}
	}
}

// failLocked retires a slot once and retains its opening tombstone. A client
// failure also closes its dedicated socket so background workers cannot leak.
func (e *endpoint) failLocked(c *Conn, err error) {
	if c.err != nil {
		return
	}
	c.err = err
	if c.done != nil {
		close(c.done)
	}
	if c.state != nil {
		delete(e.conns, c.state.id)
		if e.server {
			e.reserved -= e.cfg.connectionBudget()
			e.pool.release(c.state.id, c.request, time.Now().Add(e.cfg.IDReuseDelay))
			if request := e.requests[c.request]; request != nil {
				request.conn = nil
			}
		}
		clear(c.state.pending)
		clear(c.state.received)
		clear(c.state.acks)
	}
	c.signalLocked()
	if !e.server {
		e.shutdownLocked(err)
	}
}

func (e *endpoint) shutdownLocked(err error) {
	if e.closed != nil {
		return
	}
	e.closed = err
	close(e.done)
	_ = e.pc.Close()
	for _, c := range e.conns {
		e.failLocked(c, err)
	}
	if e.dial != nil && e.dial.err == nil {
		e.dial.err = err
		if e.dial.done != nil {
			close(e.dial.done)
		}
		e.dial.signalLocked()
	}
}

func (e *endpoint) close() {
	_ = e.pc.Close()
	e.mu.Lock()
	e.shutdownLocked(net.ErrClosed)
	e.mu.Unlock()
	e.workers.Wait()
}

// Accept implements net.Listener. AcceptContext additionally supports cancellation
// and returns the concrete stream API (Target and CloseWrite).
func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.AcceptContext(context.Background())
	if err != nil {
		return nil, err
	}
	return c, nil
}

// AcceptContext skips connections that failed while waiting in the accept queue.
func (l *Listener) AcceptContext(ctx context.Context) (*Conn, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-l.ep.done:
			return nil, net.ErrClosed
		case c := <-l.ep.accept:
			l.ep.mu.Lock()
			err := c.err
			closed := l.ep.closed
			l.ep.mu.Unlock()
			if closed != nil {
				return nil, net.ErrClosed
			}
			if err == nil {
				return c, nil
			}
		}
	}
}

func (l *Listener) Close() error   { l.ep.close(); return nil }
func (l *Listener) Addr() net.Addr { return l.ep.pc.LocalAddr() }
