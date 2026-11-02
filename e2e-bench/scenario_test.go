package e2ebench

import (
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestSegmentPlanBoundsAndExactTotal(t *testing.T) {
	for _, c := range []benchConfig{
		{TotalBytes: 4 << 20, SegmentMinBytes: 4 << 10, SegmentMaxBytes: 16 << 10},
		{TotalBytes: 100, SegmentMinBytes: 10, SegmentMaxBytes: 10},
		{TotalBytes: 99, SegmentMinBytes: 16, SegmentMaxBytes: 20},
		{TotalBytes: 13, SegmentMinBytes: 2, SegmentMaxBytes: 4},
	} {
		c.TimeoutSeconds, c.QueueCapacityPackets = 1, 1
		if err := c.validate(); err != nil {
			t.Fatal(err)
		}
		for seed := int64(0); seed < 20; seed++ {
			c.Seed = seed
			plan, same := newSegmentPlan(c), newSegmentPlan(c)
			var total int64
			for size := plan.next(); size != 0; size = plan.next() {
				if size < c.SegmentMinBytes || size > c.SegmentMaxBytes {
					t.Fatalf("out of range segment: %d", size)
				}
				if size != same.next() {
					t.Fatal("segment sizes changed for the same seed")
				}
				total += int64(size)
			}
			if total != c.TotalBytes || same.next() != 0 {
				t.Fatalf("total=%d want=%d", total, c.TotalBytes)
			}
		}
	}
	c := testConfig()
	c.TotalBytes, c.SegmentMinBytes, c.SegmentMaxBytes = 21, 10, 10
	if c.validate() == nil {
		t.Fatal("accepted a total with no valid segment partition")
	}
}

// checkedEcho only permits one segment at a time and returns it in small reads.
// A corrupted first echo must stop the sender before its second Write, proving
// that completing a read alone is not enough to advance the workload.
type checkedEcho struct {
	mu                 sync.Mutex
	ready              *sync.Cond
	pending            []byte
	writes             int
	pipelined, corrupt bool
}

func (e *checkedEcho) Write(b []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) != 0 {
		e.pipelined = true
		return 0, errors.New("next segment arrived before previous echo was drained")
	}
	e.writes++
	e.pending = append([]byte(nil), b...)
	if e.corrupt {
		e.pending[len(e.pending)-1] ^= 1
	}
	e.ready.Broadcast()
	return len(b), nil
}

func (e *checkedEcho) Read(b []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for len(e.pending) == 0 {
		e.ready.Wait()
	}
	n := copy(b[:min(len(b), 113)], e.pending)
	e.pending = e.pending[n:]
	return n, nil
}

func TestSegmentVerificationBarrier(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		echo := &checkedEcho{corrupt: corrupt}
		echo.ready = sync.NewCond(&echo.mu)
		cfg := testConfig()
		var workers sync.WaitGroup
		summary, err := exchangeSegments(echo, cfg, &workers)
		workers.Wait()
		if echo.pipelined {
			t.Fatal("segment writes were pipelined")
		}
		if corrupt {
			if err == nil || !strings.Contains(err.Error(), "echo mismatch") || echo.writes != 1 || summary.VerifiedBytes != 0 {
				t.Fatalf("corrupt segment allowed progress: writes=%d verified=%d err=%v", echo.writes, summary.VerifiedBytes, err)
			}
		} else if err != nil || summary.VerifiedBytes != cfg.TotalBytes || echo.writes != summary.Count {
			t.Fatalf("exchange: %+v err=%v", summary, err)
		}
	}
}

func metricPacket(kind byte, id uint16, seq uint32, payload []byte) []byte {
	b := make([]byte, 16+len(payload))
	b[0], b[1], b[2], b[3] = 'W', 'P', 1, kind
	binary.BigEndian.PutUint16(b[4:6], id)
	binary.BigEndian.PutUint16(b[6:8], uint16(len(payload)))
	binary.BigEndian.PutUint32(b[8:12], seq)
	copy(b[16:], payload)
	return b
}

func TestWireACKAndResendCounters(t *testing.T) {
	var observer wireObserver
	var stats wireStats
	for _, seq := range []uint32{0xfffffffe, 0xffffffff, 0, 0xffffffff, 1, 1} {
		observer.observe(metricPacket(4, 7, seq, []byte("data")), &stats)
	}
	observer.observe(metricPacket(5, 7, 0, make([]byte, 8)), &stats)
	observer.observe(metricPacket(5, 7, 0, nil), &stats) // Window-only ACK counts its header.
	observer.observe(metricPacket(7, 7, 2, nil), &stats) // FIN does not count as a DATA resend.
	observer.observe([]byte("invalid"), &stats)
	if stats.DataPackets != 6 || stats.DataResends != 2 || stats.ACKPackets != 2 || stats.ACKBytes != 40 || stats.ACKEntries != 2 {
		t.Fatalf("incorrect wire counters: %+v", stats)
	}
	// A separate connection's first packet is new, even with a reused sequence.
	observer.observe(metricPacket(4, 8, 1, []byte("data")), &stats)
	if stats.DataPackets != 7 || stats.DataResends != 2 {
		t.Fatal("mixed connection sequence spaces")
	}

	cfg := testConfig()
	cfg.PacketLossPercent = 100
	network := newMemoryNetwork(cfg)
	defer network.close()
	for i := 0; i < 2; i++ {
		if _, err := network.sockets[0].WriteTo(metricPacket(4, 1, 1, []byte("lost")), network.sockets[1].LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	s := network.snapshot()[0]
	if s.RandomDrops != 2 || s.DataPackets != 2 || s.DataResends != 1 {
		t.Fatalf("loss hid retransmissions: %+v", s)
	}
}
