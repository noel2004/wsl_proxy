package e2ebench

import (
	"container/heap"
	"errors"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

type memoryAddr string

func (a memoryAddr) Network() string { return "inprocess-udp" }
func (a memoryAddr) String() string  { return string(a) }

// impairment has one seeded PRNG per direction. The network mutex serializes
// access. A dropped packet never reaches the queue and still appears locally
// sent, just as with UDP loss. Gaussian samples are censored at zero, not retried.
type impairment struct {
	rng                           *rand.Rand
	lossPercent, meanMS, stddevMS float64
}

func (f *impairment) sample() (drop bool, delay time.Duration) {
	if f.rng.Float64()*100 < f.lossPercent {
		return true, 0
	}
	ms := max(0, f.meanMS+f.stddevMS*f.rng.NormFloat64())
	return false, time.Duration(ms * float64(time.Millisecond))
}

type directionStats struct {
	wireStats
	Pending                                               int
	Sent, RandomDrops, QueueDrops, ClosedDrops, Delivered uint64
	WireBytes, Scheduled                                  uint64
	ScheduledDelayNS, ObservedDelayNS                     float64
	PeakQueued                                            int
}

// memoryNetwork owns two sockets and a bounded receive queue for each. The socket
// ReadFrom calls drive delivery, so there is no goroutine or timer per packet.
// Both the queue and PRNGs are protected by mu; snapshots need no atomics.
type memoryNetwork struct {
	mu        sync.Mutex
	sockets   [2]*memorySocket
	filters   [2]impairment
	stats     [2]directionStats // Indexed by sending side: client, then server.
	observers [2]wireObserver
	capacity  int
	nextOrder uint64
}

type memorySocket struct {
	network                     *memoryNetwork
	side                        int
	addr                        memoryAddr
	closed                      bool
	readDeadline, writeDeadline time.Time
	changed                     chan struct{}
	queue                       deliveries
}

var _ net.PacketConn = (*memorySocket)(nil)

func newMemoryNetwork(c benchConfig) *memoryNetwork {
	n := &memoryNetwork{capacity: c.QueueCapacityPackets}
	for side, name := range []memoryAddr{"client", "server"} {
		n.sockets[side] = &memorySocket{network: n, side: side, addr: name, changed: make(chan struct{})}
		seed := c.Seed
		if side == 1 {
			seed ^= 0x5deece66d
		}
		n.filters[side] = impairment{rand.New(rand.NewSource(seed)), c.PacketLossPercent, c.LatencyMeanMS, c.LatencyStddevMS}
	}
	return n
}

// delivery owns a copy of the datagram, preserving WriteTo's buffer-reuse
// contract. Sorting by due time allows independent latency samples to reorder
// packets; order breaks ties without losing FIFO behavior for equal delays.
type delivery struct {
	payload   []byte
	sent, due time.Time
	order     uint64
}

type deliveries []delivery

func (q deliveries) Len() int { return len(q) }
func (q deliveries) Less(i, j int) bool {
	if q[i].due.Equal(q[j].due) {
		return q[i].order < q[j].order
	}
	return q[i].due.Before(q[j].due)
}
func (q deliveries) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *deliveries) Push(v any)   { *q = append(*q, v.(delivery)) }
func (q *deliveries) Pop() any {
	last := len(*q) - 1
	d := (*q)[last]
	(*q)[last] = delivery{} // Do not retain payload storage in the heap's backing array.
	*q = (*q)[:last]
	return d
}

func (s *memorySocket) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *memorySocket) LocalAddr() net.Addr { return s.addr }

func (s *memorySocket) WriteTo(b []byte, to net.Addr) (int, error) {
	n := s.network
	n.mu.Lock()
	defer n.mu.Unlock()
	if s.closed {
		return 0, net.ErrClosed
	}
	now := time.Now()
	if !s.writeDeadline.IsZero() && !now.Before(s.writeDeadline) {
		return 0, os.ErrDeadlineExceeded
	}
	peer := n.sockets[1-s.side]
	if to == nil || to.Network() != peer.addr.Network() || to.String() != peer.addr.String() {
		return 0, errors.New("in-process network: unknown destination")
	}
	if len(b) > 65507 {
		return 0, errors.New("in-process network: datagram exceeds UDP payload limit")
	}
	stats := &n.stats[s.side]
	stats.Sent++
	stats.WireBytes += uint64(len(b))
	n.observers[s.side].observe(b, &stats.wireStats)
	drop, delay := n.filters[s.side].sample()
	if drop {
		stats.RandomDrops++
		return len(b), nil
	}
	// Simulate finite UDP receive buffering. Overflow is reported separately from
	// configured random loss so throughput results cannot conceal simulator load.
	if peer.closed {
		stats.ClosedDrops++
		return len(b), nil
	}
	if len(peer.queue) >= n.capacity {
		stats.QueueDrops++
		return len(b), nil
	}
	n.nextOrder++
	heap.Push(&peer.queue, delivery{append([]byte(nil), b...), now, now.Add(delay), n.nextOrder})
	stats.Scheduled++
	stats.ScheduledDelayNS += float64(delay)
	stats.PeakQueued = max(stats.PeakQueued, len(peer.queue))
	peer.signalLocked()
	return len(b), nil
}

func (s *memorySocket) ReadFrom(dst []byte) (int, net.Addr, error) {
	n := s.network
	for {
		n.mu.Lock()
		if s.closed {
			n.mu.Unlock()
			return 0, nil, net.ErrClosed
		}
		now := time.Now()
		if !s.readDeadline.IsZero() && !now.Before(s.readDeadline) {
			n.mu.Unlock()
			return 0, nil, os.ErrDeadlineExceeded
		}
		if len(s.queue) > 0 && !now.Before(s.queue[0].due) {
			d := heap.Pop(&s.queue).(delivery)
			stats := &n.stats[1-s.side]
			stats.Delivered++
			stats.ObservedDelayNS += float64(now.Sub(d.sent))
			copied := copy(dst, d.payload) // UDP discards the remainder of a short read.
			peer := n.sockets[1-s.side].addr
			n.mu.Unlock()
			return copied, peer, nil
		}
		wakeAt := s.readDeadline
		if len(s.queue) > 0 && (wakeAt.IsZero() || s.queue[0].due.Before(wakeAt)) {
			wakeAt = s.queue[0].due
		}
		changed := s.changed
		n.mu.Unlock()
		// Deadline changes, closure, or an earlier arrival invalidate this wait.
		// A heap (rather than a sleeping forwarding loop) avoids head-of-line delay.
		if wakeAt.IsZero() {
			<-changed
			continue
		}
		timer := time.NewTimer(time.Until(wakeAt))
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (s *memorySocket) Close() error {
	s.network.mu.Lock()
	defer s.network.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.network.stats[1-s.side].ClosedDrops += uint64(len(s.queue))
		s.queue = nil
		s.signalLocked()
	}
	return nil
}

func (s *memorySocket) SetDeadline(t time.Time) error {
	s.network.mu.Lock()
	defer s.network.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	s.readDeadline, s.writeDeadline = t, t
	s.signalLocked()
	return nil
}

func (s *memorySocket) SetReadDeadline(t time.Time) error {
	s.network.mu.Lock()
	defer s.network.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	s.readDeadline = t
	s.signalLocked()
	return nil
}

func (s *memorySocket) SetWriteDeadline(t time.Time) error {
	s.network.mu.Lock()
	defer s.network.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	s.writeDeadline = t
	return nil
}

func (n *memoryNetwork) close() {
	for _, s := range n.sockets {
		_ = s.Close()
	}
}

func (n *memoryNetwork) snapshot() [2]directionStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	stats := n.stats
	for side := range stats {
		stats[side].Pending = len(n.sockets[1-side].queue)
	}
	return stats
}
