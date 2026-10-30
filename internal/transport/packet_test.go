package transport

import (
	"bytes"
	"testing"
	"time"
)

func TestPacketValidationAndProfile(t *testing.T) {
	cfg := DefaultConfig()
	p := packet{kind: kindOpen, limit: 64, payload: openPayload(123, cfg, "ssh-agent")}
	b := p.encode()
	if decoded, err := decode(b, cfg.MaxDatagramBytes); err != nil || !compatibleOpen(decoded, cfg) {
		t.Fatal("valid OPEN rejected", err)
	}
	for _, bad := range [][]byte{b[:15], append(append([]byte(nil), b...), 0), b[:len(b)-1]} {
		if _, err := decode(bad, cfg.MaxDatagramBytes); err == nil {
			t.Fatal("malformed length accepted")
		}
	}
	b[2]++
	if _, err := decode(b, cfg.MaxDatagramBytes); err == nil {
		t.Fatal("unknown version accepted")
	}
	cfg.DeliveryTimeout++
	if compatibleOpen(p, cfg) {
		t.Fatal("incompatible retry horizon accepted")
	}
}

func TestConfigAndIDQuarantine(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*Config){
		func(c *Config) { c.MaxConnections = 65536 }, func(c *Config) { c.ReceiveWindowPackets = 1 << 31 },
		func(c *Config) { c.AckBatchSize = 10000 }, func(c *Config) { c.IDReuseDelay = c.DeliveryTimeout },
		func(c *Config) { c.KeepaliveInterval = c.ReceiveIdleTimeout }, func(c *Config) { c.MaxBufferedBytes = 1 },
		func(c *Config) { c.ScanInterval = 0 }, func(c *Config) { c.RetryBackoffFactor = 1 },
	} {
		bad := cfg
		alter(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid config accepted")
		}
	}
	pool := newIDPool(2)
	a, _ := pool.take(epoch)
	pool.release(a.id, "old", epoch.Add(time.Second))
	b, ok := pool.take(epoch)
	if !ok || b.id != 2 {
		t.Fatal("FIFO did not prefer unused ID")
	}
	if _, ok := pool.take(epoch); ok {
		t.Fatal("quarantine bypassed")
	}
	pool.release(b.id, "second", epoch.Add(2*time.Second))
	a, ok = pool.take(epoch.Add(time.Second))
	if !ok || a.id != 1 || a.request != "old" {
		t.Fatal("ID not recycled in FIFO order")
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(packet{kind: kindData, id: 1, seq: 1, payload: []byte("hello")}.encode())
	f.Add(packet{kind: kindOpen, limit: 64, payload: openPayload(1, DefaultConfig(), "agent")}.encode())
	f.Add(ack(1, 64, 1, 2).encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := decode(b, 1200)
		if err == nil && !bytes.Equal(p.encode(), b) {
			t.Fatal("accepted noncanonical packet")
		}
	})
}
