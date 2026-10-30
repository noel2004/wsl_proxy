package transport

import (
	"errors"
	"fmt"
	"time"
)

// Config bounds memory, batching, and connection lifetime. Start with
// DefaultConfig and override fields; a zero Config is not valid. Opening peers
// must agree on datagram size and lifetime settings (see docs/protocol.md).
type Config struct {
	MaxConnections       int
	SendWindowPackets    uint32
	ReceiveWindowPackets uint32
	MaxDatagramBytes     int
	AckMaxDelay          time.Duration
	AckBatchSize         int
	InitialRetryInterval time.Duration
	RetryBackoffFactor   int
	MaxRetryInterval     time.Duration
	ConnectTimeout       time.Duration
	DeliveryTimeout      time.Duration
	KeepaliveInterval    time.Duration
	ReceiveIdleTimeout   time.Duration
	IDReuseDelay         time.Duration
	ScanInterval         time.Duration
	SocketWriteTimeout   time.Duration
	AcceptBacklog        int
	MaxBufferedBytes     int64
}

// DefaultConfig returns the initial local-host profile. The byte budget reserves
// each admitted connection's worst-case window storage, allocated lazily.
func DefaultConfig() Config {
	return Config{
		MaxConnections: 65535, SendWindowPackets: 64, ReceiveWindowPackets: 64,
		MaxDatagramBytes: 1200, AckMaxDelay: 10 * time.Millisecond, AckBatchSize: 32,
		InitialRetryInterval: 30 * time.Millisecond, RetryBackoffFactor: 2,
		MaxRetryInterval: time.Second, ConnectTimeout: 5 * time.Second,
		DeliveryTimeout: 10 * time.Second, KeepaliveInterval: 2 * time.Second,
		ReceiveIdleTimeout: 8 * time.Second, IDReuseDelay: 20 * time.Second,
		ScanInterval: time.Millisecond, SocketWriteTimeout: time.Second,
		AcceptBacklog: 128, MaxBufferedBytes: 64 << 20,
	}
}

// Validate rejects ambiguous sequence ranges, impossible batches, and lifetime
// combinations that would release an ID before a peer has finished retrying.
func (c Config) Validate() error {
	if c.MaxConnections < 1 || c.MaxConnections > 65535 {
		return errors.New("transport: MaxConnections must be in [1, 65535]")
	}
	if c.SendWindowPackets == 0 || c.SendWindowPackets >= 1<<31 || c.ReceiveWindowPackets == 0 || c.ReceiveWindowPackets >= 1<<31 {
		return errors.New("transport: window sizes must be in [1, 2^31)")
	}
	if uint64(c.SendWindowPackets)+uint64(c.ReceiveWindowPackets) >= 1<<31 {
		return errors.New("transport: combined windows must stay below the sequence half-range")
	}
	if c.MaxDatagramBytes < headerSize+openSize+1 || c.MaxDatagramBytes > 65507 {
		return errors.New("transport: invalid datagram size")
	}
	if c.AckBatchSize < 1 || c.AckBatchSize > (c.MaxDatagramBytes-headerSize)/4 {
		return errors.New("transport: ACK batch does not fit in a datagram")
	}
	for name, d := range map[string]time.Duration{
		"AckMaxDelay": c.AckMaxDelay, "InitialRetryInterval": c.InitialRetryInterval,
		"MaxRetryInterval": c.MaxRetryInterval, "ConnectTimeout": c.ConnectTimeout,
		"DeliveryTimeout": c.DeliveryTimeout, "KeepaliveInterval": c.KeepaliveInterval,
		"ReceiveIdleTimeout": c.ReceiveIdleTimeout, "IDReuseDelay": c.IDReuseDelay,
		"ScanInterval": c.ScanInterval, "SocketWriteTimeout": c.SocketWriteTimeout,
	} {
		if d <= 0 {
			return fmt.Errorf("transport: %s must be positive", name)
		}
	}
	if c.RetryBackoffFactor < 2 || c.InitialRetryInterval <= c.AckMaxDelay || c.MaxRetryInterval < c.InitialRetryInterval {
		return errors.New("transport: invalid retry intervals or backoff factor")
	}
	if c.KeepaliveInterval >= c.ReceiveIdleTimeout || c.ScanInterval > c.AckMaxDelay {
		return errors.New("transport: keepalive or scheduler interval is too large")
	}
	if c.IDReuseDelay <= max(c.DeliveryTimeout, c.ConnectTimeout, c.ReceiveIdleTimeout) {
		return errors.New("transport: ID quarantine must exceed all peer retry/liveness horizons")
	}
	if c.AcceptBacklog < 1 || c.AcceptBacklog > c.MaxConnections || c.MaxBufferedBytes < c.connectionBudget() {
		return errors.New("transport: insufficient backlog or buffer budget")
	}
	return nil
}

func (c Config) connectionBudget() int64 {
	return (int64(c.SendWindowPackets) + int64(c.ReceiveWindowPackets)) * int64(c.MaxDatagramBytes-headerSize)
}

func (c Config) backoff(d time.Duration) time.Duration {
	// Divide before multiplying so even user-supplied durations cannot overflow.
	if d > c.MaxRetryInterval/time.Duration(c.RetryBackoffFactor) {
		return c.MaxRetryInterval
	}
	return min(d*time.Duration(c.RetryBackoffFactor), c.MaxRetryInterval)
}
