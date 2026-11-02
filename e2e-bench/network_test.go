package e2ebench

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func testConfig() benchConfig {
	return benchConfig{Seed: 17, TotalBytes: 131072, SegmentMinBytes: 4096, SegmentMaxBytes: 16384, TimeoutSeconds: 3, QueueCapacityPackets: 512}
}

func TestImpairmentDistribution(t *testing.T) {
	f := impairment{rand.New(rand.NewSource(17)), 25, 10, 2}
	identical := impairment{rand.New(rand.NewSource(17)), 25, 10, 2}
	const samples = 100000
	dropped := 0
	var sum, squares float64
	for i := 0; i < samples; i++ {
		drop, delay := f.sample()
		dropAgain, delayAgain := identical.sample()
		if drop != dropAgain || delay != delayAgain {
			t.Fatal("same seed did not reproduce the filter's decisions")
		}
		if drop {
			dropped++
			continue
		}
		ms := float64(delay) / float64(time.Millisecond)
		sum += ms
		squares += ms * ms
	}
	count := float64(samples - dropped)
	mean := sum / count
	stddev := math.Sqrt(squares/count - mean*mean)
	if math.Abs(float64(dropped)/samples-0.25) > 0.005 || math.Abs(mean-10) > 0.05 || math.Abs(stddev-2) > 0.05 {
		t.Fatalf("unexpected distribution: drop=%.4f mean=%.4f stddev=%.4f", float64(dropped)/samples, mean, stddev)
	}
	f = impairment{rand.New(rand.NewSource(1)), 0, 0, 10}
	zero := 0
	for i := 0; i < 1000; i++ {
		_, delay := f.sample()
		if delay < 0 {
			t.Fatal("negative latency")
		}
		if delay == 0 {
			zero++
		}
	}
	if zero == 0 {
		t.Fatal("negative Gaussian samples were not clamped")
	}
}

func TestMemorySocketDeliveryAndDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.LatencyMeanMS = 10
		network := newMemoryNetwork(cfg)
		defer network.close()
		client, server := network.sockets[0], network.sockets[1]
		started := time.Now()
		slow := []byte("slow")
		if _, err := client.WriteTo(slow, server.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		slow[0] = 'X' // WriteTo must copy the caller's buffer before returning.
		network.mu.Lock()
		network.filters[0].meanMS = 1
		network.mu.Unlock()
		if _, err := client.WriteTo([]byte("fast"), server.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		n, from, err := server.ReadFrom(buf[:2])
		if err != nil || n != 2 || string(buf[:n]) != "fa" || from.String() != "client" || time.Since(started) != time.Millisecond {
			t.Fatalf("first arrival: %q from=%v elapsed=%s err=%v", buf[:n], from, time.Since(started), err)
		}
		n, _, err = server.ReadFrom(buf)
		if err != nil || string(buf[:n]) != "slow" || time.Since(started) != 10*time.Millisecond {
			t.Fatalf("second arrival: %q elapsed=%s err=%v", buf[:n], time.Since(started), err)
		}
		if err := server.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := server.ReadFrom(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read deadline: %v", err)
		}
		if err := client.SetWriteDeadline(time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := client.WriteTo(buf, server.LocalAddr()); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write deadline: %v", err)
		}
	})
}

func TestBlockedReadWakesOnDeadlineChangeAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := newMemoryNetwork(testConfig())
		defer network.close()
		server := network.sockets[1]
		finished := make(chan error, 1)
		read := func() { _, _, err := server.ReadFrom(make([]byte, 1)); finished <- err }
		go read()
		synctest.Wait()
		server.SetReadDeadline(time.Now())
		if err := <-finished; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
		server.SetReadDeadline(time.Time{})
		go read()
		synctest.Wait()
		server.Close()
		if err := <-finished; !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})
}

func TestLossAndQueueAccounting(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacityPackets = 1
	network := newMemoryNetwork(cfg)
	defer network.close()
	client, server := network.sockets[0], network.sockets[1]
	client.WriteTo([]byte("queued"), server.LocalAddr())
	client.WriteTo([]byte("overflow"), server.LocalAddr())
	network.mu.Lock()
	network.filters[0].lossPercent = 100
	network.mu.Unlock()
	if n, err := client.WriteTo([]byte("lost"), server.LocalAddr()); err != nil || n != 4 {
		t.Fatalf("loss should look like successful UDP send: %d %v", n, err)
	}
	stats := network.snapshot()[0]
	if stats.Sent != 3 || stats.RandomDrops != 1 || stats.QueueDrops != 1 || stats.Pending != 1 || stats.PeakQueued != 1 {
		t.Fatalf("incorrect queue/loss accounting: %+v", stats)
	}
	server.Close()
	stats = network.snapshot()[0]
	if stats.Pending != 0 || stats.ClosedDrops != 1 {
		t.Fatal("close did not release pending datagrams")
	}
}

func TestEndToEndProfilesAndCancellation(t *testing.T) {
	for _, loss := range []float64{0, 10, 100} {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.PacketLossPercent = loss
			cfg.LatencyMeanMS, cfg.LatencyStddevMS = 3, 1
			if loss == 100 {
				cfg.TimeoutSeconds = 0.1
			}
			result, err := runTrial(cfg)
			if loss == 100 {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("total loss must terminate via context: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, stats := range result.Stats {
				if stats.Sent != stats.RandomDrops+stats.QueueDrops+stats.ClosedDrops+stats.Delivered+uint64(stats.Pending) {
					t.Fatalf("lost accounting: %+v", stats)
				}
				if stats.QueueDrops != 0 {
					t.Fatal("unexpected queue overflow")
				}
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	valid := `{"seed":1,"packet_loss_percent":2,"latency_mean_ms":2,"latency_stddev_ms":1,"total_bytes":100,"segment_min_bytes":10,"segment_max_bytes":20,"timeout_seconds":2,"queue_capacity_packets":64}`
	if _, err := decodeConfig(strings.NewReader(valid)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		`null`, `{}`, valid + ` {}`, strings.Replace(valid, `"seed":1`, `"typo":1`, 1),
		strings.Replace(valid, `"packet_loss_percent":2`, `"packet_loss_percent":101`, 1),
		strings.Replace(valid, `"latency_mean_ms":2`, `"latency_mean_ms":-1`, 1),
		strings.Replace(valid, `"total_bytes":100`, `"total_bytes":0`, 1),
		strings.Replace(valid, `"segment_min_bytes":10`, `"segment_min_bytes":21`, 1),
		strings.Replace(valid, `"timeout_seconds":2`, `"timeout_seconds":0`, 1),
		strings.Replace(valid, `"queue_capacity_packets":64`, `"queue_capacity_packets":0`, 1),
	} {
		if _, err := decodeConfig(strings.NewReader(invalid)); err == nil {
			t.Fatalf("accepted invalid config: %s", invalid)
		}
	}
}
