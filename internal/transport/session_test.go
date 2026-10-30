package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"
)

var epoch = time.Unix(1000, 0)

func ack(id uint16, limit uint32, seqs ...uint32) packet {
	p := packet{kind: kindACK, id: id, limit: limit}
	for _, seq := range seqs {
		p.payload = binary.BigEndian.AppendUint32(p.payload, seq)
	}
	return p
}

func tick(t *testing.T, s *session, now time.Time) []packet {
	t.Helper()
	p, err := s.tick(now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLostACKAndDuplicateAfterConsumption(t *testing.T) {
	cfg := DefaultConfig()
	a, b := newSession(cfg, 1, 64, epoch), newSession(cfg, 1, 64, epoch)
	p := a.queue([]byte("hello"), false, epoch)
	b.input(p, epoch)
	dst := make([]byte, 10)
	n, _, err := b.read(dst, epoch) // Drop the ACK emitted by consumption.
	if n != 5 || string(dst[:n]) != "hello" || err != nil {
		t.Fatalf("read = %q, %v", dst[:n], err)
	}
	retries := tick(t, a, epoch.Add(30*time.Millisecond))
	if len(retries) != 1 {
		t.Fatalf("retries: %v", retries)
	}
	b.input(retries[0], epoch.Add(30*time.Millisecond))
	out := tick(t, b, epoch.Add(40*time.Millisecond))
	if len(out) != 1 {
		t.Fatalf("expected a fresh ACK: %v", out)
	}
	a.input(out[0], epoch.Add(40*time.Millisecond))
	if len(a.pending) != 0 {
		t.Fatal("duplicate was not acknowledged")
	}
	n, _, err = b.read(dst, epoch.Add(40*time.Millisecond))
	if n != 0 || err != nil {
		t.Fatal("duplicate delivered twice")
	}
}

func TestACKDeadlineAndEarlyBatchFlush(t *testing.T) {
	cfg := DefaultConfig()
	s := newSession(cfg, 1, 64, epoch)
	for i, ms := range []int{0, 3, 8} {
		s.input(packet{kind: kindData, id: 1, seq: uint32(i + 1), payload: []byte("x")}, epoch.Add(time.Duration(ms)*time.Millisecond))
	}
	if len(tick(t, s, epoch.Add(9*time.Millisecond))) != 0 {
		t.Fatal("ACK flushed early")
	}
	out := tick(t, s, epoch.Add(10*time.Millisecond))
	if len(out) != 1 || len(out[0].payload) != 12 {
		t.Fatal("arrivals postponed first ACK deadline")
	}
	cfg.AckBatchSize = 2
	s = newSession(cfg, 1, 64, epoch)
	s.input(packet{kind: kindData, seq: 1, payload: []byte("x")}, epoch)
	out, _ = s.input(packet{kind: kindData, seq: 2, payload: []byte("y")}, epoch)
	if len(out) != 1 || len(out[0].payload) != 8 {
		t.Fatal("full batch did not flush")
	}
}

func TestRetryEligibilityBackoffAndAbsoluteTimeout(t *testing.T) {
	cfg := DefaultConfig()
	s := newSession(cfg, 1, 64, epoch)
	s.queue([]byte("old"), false, epoch)
	s.queue([]byte("fresh"), false, epoch.Add(25*time.Millisecond))
	out := tick(t, s, epoch.Add(30*time.Millisecond))
	if len(out) != 1 || out[0].seq != 1 {
		t.Fatal("fresh packet retransmitted early")
	}
	if s.retryInterval != 60*time.Millisecond {
		t.Fatal("partial window did not back off")
	}
	s.input(ack(1, 64, 999), epoch.Add(31*time.Millisecond))
	if s.retryInterval != 60*time.Millisecond {
		t.Fatal("unknown ACK counted as progress")
	}
	s.input(ack(1, 64, 1), epoch.Add(32*time.Millisecond))
	if s.retryInterval != cfg.InitialRetryInterval {
		t.Fatal("new ACK did not reset backoff")
	}
	if len(tick(t, s, epoch.Add(54*time.Millisecond))) != 0 {
		t.Fatal("fresh packet retried before its own delay")
	}
	out = tick(t, s, epoch.Add(55*time.Millisecond))
	if len(out) != 1 || out[0].seq != 2 {
		t.Fatal("packet not retried when due")
	}
	s.lastRecv = epoch.Add(cfg.DeliveryTimeout)
	_, err := s.tick(epoch.Add(25*time.Millisecond + cfg.DeliveryTimeout))
	if !errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("retries extended deadline: %v", err)
	}
}

func TestReceiveWindowPartialReadsAndLostUpdate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiveWindowPackets = 2
	a, b := newSession(cfg, 1, 2, epoch), newSession(cfg, 1, 2, epoch)
	first, second := a.queue([]byte("abcd"), false, epoch), a.queue([]byte("efgh"), false, epoch)
	b.input(second, epoch)
	if n, _, _ := b.read(make([]byte, 4), epoch); n != 0 {
		t.Fatal("out-of-order delivery")
	}
	b.input(first, epoch)
	for _, p := range tick(t, b, epoch.Add(cfg.AckMaxDelay)) {
		a.input(p, epoch)
	}
	if len(a.pending) != 0 || a.canSend() {
		t.Fatal("ACK freed receive capacity before consumption")
	}
	_, out, _ := b.read(make([]byte, 1), epoch)
	if len(out) != 0 || b.receiveLimit() != 2 {
		t.Fatal("partial read freed full packet slot")
	}
	b.read(make([]byte, 3), epoch) // Drop window-only ACK.
	if b.receiveLimit() != 3 || a.canSend() {
		t.Fatal("invalid capacity update")
	}
	// Continuous reverse DATA must not suppress the repeated capacity advertisement.
	b.lastSent = epoch.Add(cfg.KeepaliveInterval - time.Millisecond)
	out = tick(t, b, epoch.Add(cfg.KeepaliveInterval))
	for _, p := range out {
		a.input(p, epoch.Add(cfg.KeepaliveInterval))
	}
	if !a.canSend() || a.peerLimit != 3 {
		t.Fatal("lost window update deadlocked sender")
	}
	a.input(ack(1, 2), epoch)
	if a.peerLimit != 3 {
		t.Fatal("stale advertisement shrank window")
	}
	if _, valid := a.input(ack(1, 100), epoch); valid {
		t.Fatal("accepted consumption beyond sent packets")
	}
	if _, valid := b.input(packet{kind: kindData, seq: 4, payload: []byte("x")}, epoch); valid {
		t.Fatal("accepted out-of-window packet")
	}
}

func TestWrappingSequencesAndFINOrdering(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiveWindowPackets = 4
	a, b := newSession(cfg, 1, 4, epoch), newSession(cfg, 1, 4, epoch)
	a.nextSend, b.recvNext = math.MaxUint32-1, math.MaxUint32-1
	a.peerLimit = b.receiveLimit()
	packets := []packet{a.queue([]byte("a"), false, epoch), a.queue([]byte("b"), false, epoch), a.queue([]byte("c"), false, epoch), a.queue(nil, true, epoch)}
	for i := len(packets) - 1; i >= 0; i-- {
		b.input(packets[i], epoch)
	}
	dst := make([]byte, 10)
	n, out, err := b.read(dst, epoch)
	if n != 3 || string(dst[:n]) != "abc" || err != nil {
		t.Fatalf("wraparound read: %q %v", dst[:n], err)
	}
	for _, p := range out {
		a.input(p, epoch)
	}
	if len(a.pending) != 0 || a.peerLimit != 4 {
		t.Fatalf("wrapped ACK/limit: pending=%d limit=%d", len(a.pending), a.peerLimit)
	}
	if _, _, err := b.read(dst, epoch); !errors.Is(err, io.EOF) {
		t.Fatal("ordered FIN did not produce EOF")
	}
	if _, valid := b.input(packets[3], epoch); !valid {
		t.Fatal("duplicate FIN not accepted")
	}
	if len(tick(t, b, epoch.Add(cfg.AckMaxDelay))) != 1 {
		t.Fatal("lost FIN ACK not recoverable")
	}
	if _, valid := b.input(packet{kind: kindData, seq: 2, payload: []byte("x")}, epoch); valid {
		t.Fatal("data accepted after FIN")
	}
	if _, valid := a.input(ack(1, a.peerLimit+1<<31), epoch); valid {
		t.Fatal("ambiguous half-range comparison accepted")
	}
}

func TestKeepaliveAndIdleFailure(t *testing.T) {
	cfg := DefaultConfig()
	a, b := newSession(cfg, 1, 64, epoch), newSession(cfg, 1, 64, epoch)
	for i := 1; i <= 10; i++ {
		now := epoch.Add(time.Duration(i) * cfg.KeepaliveInterval)
		for _, p := range tick(t, a, now) {
			b.input(p, now)
		}
		for _, p := range tick(t, b, now) {
			a.input(p, now)
		}
	}
	if _, err := a.tick(a.lastRecv.Add(cfg.ReceiveIdleTimeout)); !errors.Is(err, ErrIdleTimeout) {
		t.Fatal("missing peer did not expire")
	}
}

// This simulation exercises both directions under reproducible loss, duplication,
// jitter, slow partial consumption, and a sequence wrap. It uses no wall-clock waits.
func TestSimulatedUnreliableDuplex(t *testing.T) {
	for seed := int64(0); seed < 5; seed++ {
		t.Run(string(rune('A'+seed)), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ReceiveWindowPackets = 8
			cfg.SendWindowPackets = 8
			cfg.AckBatchSize = 4
			a, b := newSession(cfg, 1, 8, epoch), newSession(cfg, 1, 8, epoch)
			for _, s := range []*session{a, b} {
				s.nextSend = math.MaxUint32 - 20
				s.recvNext = s.nextSend
				s.peerLimit = s.receiveLimit()
			}
			want := [][]byte{bytes.Repeat([]byte("left->right"), 300), bytes.Repeat([]byte("right->left"), 300)}
			got := [2][]byte{}
			sent := [2]int{}
			type flight struct {
				destination int
				p           packet
				at          int
			}
			var flights []flight
			rng := rand.New(rand.NewSource(seed))
			enqueue := func(destination int, p packet, step int) {
				if rng.Intn(5) == 0 {
					return
				}
				flights = append(flights, flight{destination, p, step + rng.Intn(9)})
				if rng.Intn(6) == 0 {
					flights = append(flights, flight{destination, p, step + rng.Intn(12)})
				}
			}
			sessions := []*session{a, b}
			for step := 0; step < 60000; step++ {
				now := epoch.Add(time.Duration(step) * time.Millisecond)
				for side, s := range sessions {
					for sent[side] < len(want[side]) && s.canSend() {
						n := min(37, len(want[side])-sent[side])
						p := s.queue(want[side][sent[side]:sent[side]+n], false, now)
						sent[side] += n
						enqueue(1-side, p, step)
					}
					for _, p := range tick(t, s, now) {
						enqueue(1-side, p, step)
					}
					if step%7 == 0 {
						buf := make([]byte, 19)
						n, out, err := s.read(buf, now)
						if err != nil {
							t.Fatal(err)
						}
						got[side] = append(got[side], buf[:n]...)
						for _, p := range out {
							enqueue(1-side, p, step)
						}
					}
					if len(s.received) > 8 || len(s.pending) > 8 {
						t.Fatal("unbounded windows")
					}
				}
				current := flights
				flights = nil
				for _, f := range current {
					if f.at > step {
						flights = append(flights, f)
						continue
					}
					out, _ := sessions[f.destination].input(f.p, now)
					for _, p := range out {
						enqueue(1-f.destination, p, step)
					}
				}
				if bytes.Equal(got[0], want[1]) && bytes.Equal(got[1], want[0]) && len(a.pending)+len(b.pending) == 0 {
					return
				}
			}
			t.Fatalf("transfer stalled: received %d and %d bytes", len(got[0]), len(got[1]))
		})
	}
}
