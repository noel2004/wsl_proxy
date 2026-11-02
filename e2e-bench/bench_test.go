package e2ebench

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/noel2004/wsl_proxy/internal/transport"
)

var (
	configPath  = flag.String("bench-config", "", "explicit JSON config path; overrides -bench-scene and E2E_BENCH_SCENE")
	configScene = flag.String("bench-scene", "", "scene name or config.<scene>.json filename; overrides E2E_BENCH_SCENE")
)

// selectedConfig is shared by the test and benchmark runners so both honor the
// same selection rules. Log the resolved file before loading to diagnose typos.
func selectedConfig(tb testing.TB) benchConfig {
	tb.Helper()
	path, err := resolveConfigPath(*configPath, *configScene, os.Getenv("E2E_BENCH_SCENE"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Logf("config=%s", path)
	cfg, err := loadConfig(path)
	if err != nil {
		tb.Fatalf("load benchmark config %q: %v", path, err)
	}
	return cfg
}

type trialResult struct {
	Handshake, Transfer time.Duration
	Stats               [2]directionStats
	Segments            segmentSummary
}

// runTrial is the client-side usage example. It dials a target, sets a deadline,
// exchanges and verifies one segment at a time, half-closes, and reads through EOF.
// The real transport runs at both ends; only its net.PacketConn is simulated.
func runTrial(cfg benchConfig) (result trialResult, trialErr error) {
	if err := cfg.validate(); err != nil {
		return result, err
	}
	network := newMemoryNetwork(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout())
	defer cancel()
	deadline, _ := ctx.Deadline()
	var workers sync.WaitGroup
	var conn *transport.Conn
	var listener *transport.Listener
	defer func() {
		// Close the simulated sockets first to interrupt every transport operation,
		// even on early failure. Then close owners and join application workers.
		result.Stats = network.snapshot()
		network.close()
		if conn != nil {
			_ = conn.Close()
		}
		if listener != nil {
			_ = listener.Close()
		}
		workers.Wait()
	}()

	transportConfig := transport.DefaultConfig()
	var err error
	listener, err = transport.ListenPacket(network.sockets[1], transportConfig)
	if err != nil {
		return result, fmt.Errorf("listen: %w", err)
	}

	serverDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		serverDone <- serveEcho(ctx, listener, deadline, cfg.streamBytes())
	}()

	started := time.Now()
	conn, err = transport.DialPacket(ctx, network.sockets[0], listener.Addr(), "echo", transportConfig)
	result.Handshake = time.Since(started)
	if err != nil {
		return result, fmt.Errorf("dial echo target: %w", err)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return result, err
	}

	started = time.Now()
	result.Segments, err = exchangeSegments(conn, cfg, &workers)
	if err != nil {
		return result, err
	}
	if err := conn.CloseWrite(); err != nil {
		return result, err
	}
	var extra [1]byte
	if n, err := conn.Read(extra[:]); n != 0 || err != io.EOF {
		return result, fmt.Errorf("expected EOF after echo, got %d bytes and %v", n, err)
	}
	result.Transfer = time.Since(started)
	if err := <-serverDone; err != nil {
		return result, fmt.Errorf("echo server: %w", err)
	}
	return result, nil
}

type segmentSummary struct {
	Count         int
	Min, Max      int
	VerifiedBytes int64
}

// exchangeSegments permits exactly one application segment in flight. Writes and
// reads within that segment run concurrently to support bounded echo streams.
// Neither the next segment nor its Write is started until this one is verified.
// On failure the caller must close conn before joining workers to unblock I/O.
func exchangeSegments(conn io.ReadWriter, cfg benchConfig, workers *sync.WaitGroup) (summary segmentSummary, err error) {
	requests := make(chan []byte)
	writeDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for payload := range requests {
			n, err := conn.Write(payload)
			if err == nil && n != len(payload) {
				err = io.ErrShortWrite
			}
			writeDone <- err
			if err != nil {
				return
			}
		}
	}()
	defer close(requests)

	plan := newSegmentPlan(cfg)
	payload := make([]byte, min(int64(cfg.SegmentMaxBytes), cfg.TotalBytes))
	received := make([]byte, len(payload))
	for size := plan.next(); size != 0; size = plan.next() {
		// Vary data with stream offset so stale echoes from previous segments fail
		// verification, even when consecutive segment sizes happen to match.
		for i := range payload[:size] {
			offset := summary.VerifiedBytes + int64(i)
			payload[i] = byte(offset*31 + offset/251)
		}
		requests <- payload[:size]
		if _, err := io.ReadFull(conn, received[:size]); err != nil {
			return summary, fmt.Errorf("read segment %d: %w", summary.Count+1, err)
		}
		if !bytes.Equal(payload[:size], received[:size]) {
			return summary, fmt.Errorf("echo mismatch in segment %d", summary.Count+1)
		}
		if err := <-writeDone; err != nil {
			return summary, fmt.Errorf("write segment %d: %w", summary.Count+1, err)
		}
		if summary.Count == 0 || size < summary.Min {
			summary.Min = size
		}
		summary.Max = max(summary.Max, size)
		summary.Count++
		summary.VerifiedBytes += int64(size)
	}
	return summary, nil
}

func serveEcho(ctx context.Context, listener *transport.Listener, deadline time.Time, expected int64) error {
	conn, err := listener.AcceptContext(ctx)
	if err != nil {
		return err
	}
	if conn.Target() != "echo" {
		return fmt.Errorf("unknown target %q", conn.Target())
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	n, err := io.Copy(conn, conn)
	if err != nil {
		return err
	}
	if n != expected {
		return fmt.Errorf("received %d bytes, expected %d", n, expected)
	}
	// Do not defer conn.Close here: it is abortive and could discard queued echo
	// data or FIN. The owning trial closes the listener after the client reads EOF.
	return conn.CloseWrite()
}

// TestE2E runs with ordinary go test, including when invoked from this directory.
// Use -v to see metrics and -count=1 to prevent Go's test-result cache hiding a run.
func TestE2E(t *testing.T) {
	cfg := selectedConfig(t)
	t.Logf("seed=%d loss=%.2f%% one-way latency=N(%.3f, %.3f²)ms (clamped at zero)", cfg.Seed, cfg.PacketLossPercent, cfg.LatencyMeanMS, cfg.LatencyStddevMS)
	result, err := runTrial(cfg)
	var total wireStats
	for side, name := range []string{"client->server", "server->client"} {
		s := result.Stats[side]
		t.Logf("%s: sent=%d delivered=%d random_drops=%d (%.2f%%) queue_drops=%d closed_drops=%d pending=%d wire_bytes=%d peak_queued=%d scheduled_mean=%.3fms observed_mean=%.3fms",
			name, s.Sent, s.Delivered, s.RandomDrops, percent(s.RandomDrops, s.Sent), s.QueueDrops, s.ClosedDrops, s.Pending, s.WireBytes, s.PeakQueued,
			s.ScheduledDelayNS/float64(max(1, s.Scheduled))/1e6, s.ObservedDelayNS/float64(max(1, s.Delivered))/1e6)
		t.Logf("%s: ACK packets=%d bytes=%d mean_size=%.2fB entries=%d; DATA attempts=%d resends=%d resend_ratio=%.3f%%",
			name, s.ACKPackets, s.ACKBytes, float64(s.ACKBytes)/float64(max(1, s.ACKPackets)), s.ACKEntries, s.DataPackets, s.DataResends, percent(s.DataResends, s.DataPackets))
		total.add(s.wireStats)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verified %d bytes (%.2f MiB) in %d segments, sizes %d..%d bytes; handshake=%s transfer=%s payload_speed=%.3f MiB/s aggregate_echo_goodput=%.3f MiB/s",
		result.Segments.VerifiedBytes, float64(cfg.streamBytes())/(1<<20), result.Segments.Count, result.Segments.Min, result.Segments.Max,
		result.Handshake, result.Transfer, float64(cfg.streamBytes())/result.Transfer.Seconds()/(1<<20), float64(2*cfg.streamBytes())/result.Transfer.Seconds()/(1<<20))
	t.Logf("TOTAL ACK packets=%d bytes=%d mean_size=%.2fB entries=%d; DATA attempts=%d resends=%d resend_ratio=%.3f%% (resends/DATA attempts)",
		total.ACKPackets, total.ACKBytes, float64(total.ACKBytes)/float64(max(1, total.ACKPackets)), total.ACKEntries, total.DataPackets, total.DataResends, percent(total.DataResends, total.DataPackets))
}

// BenchmarkE2E additionally supports go test -bench. One operation is a complete
// trial, including setup, opening, verified transfer, EOF, and resource cleanup.
func BenchmarkE2E(b *testing.B) {
	cfg := selectedConfig(b)
	b.SetBytes(cfg.streamBytes())
	b.ReportAllocs()
	var handshake time.Duration
	var sent, lost uint64
	var total wireStats
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := runTrial(cfg)
		if err != nil {
			b.Fatal(err)
		}
		handshake += result.Handshake
		for _, stats := range result.Stats {
			sent += stats.Sent
			lost += stats.RandomDrops
			total.add(stats.wireStats)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(handshake)/float64(b.N)/1e6, "handshake-ms/op")
	b.ReportMetric(float64(sent)/float64(b.N), "datagrams/op")
	b.ReportMetric(percent(lost, sent), "loss-percent")
	b.ReportMetric(float64(total.ACKBytes)/float64(b.N), "ack-B/op")
	b.ReportMetric(percent(total.DataResends, total.DataPackets), "resend-percent")
}

func percent(n, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}
