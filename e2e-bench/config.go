// Package e2ebench provides an in-process datagram network for exercising the
// complete transport through its public socket and stream interfaces. It does
// not bypass packet encoding, connection opening, ACKs, retries, or flow control.
package e2ebench

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// benchConfig describes one echo trial with a fixed payload volume, split into
// random segments verified one at a time over the same connection. Network
// settings apply independently to every attempted datagram in both directions.
type benchConfig struct {
	Seed                 int64   `json:"seed"`
	PacketLossPercent    float64 `json:"packet_loss_percent"`
	LatencyMeanMS        float64 `json:"latency_mean_ms"`
	LatencyStddevMS      float64 `json:"latency_stddev_ms"`
	TotalBytes           int64   `json:"total_bytes"`
	SegmentMinBytes      int     `json:"segment_min_bytes"`
	SegmentMaxBytes      int     `json:"segment_max_bytes"`
	TimeoutSeconds       float64 `json:"timeout_seconds"`
	QueueCapacityPackets int     `json:"queue_capacity_packets"`
}

func loadConfig(path string) (benchConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return benchConfig{}, err
	}
	defer f.Close()
	return decodeConfig(f)
}

// Reject misspelled options and multiple JSON values rather than silently
// running a different network profile from the one the author intended.
func decodeConfig(r io.Reader) (benchConfig, error) {
	var cfg benchConfig
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return cfg, errors.New("config must contain exactly one JSON object")
	}
	return cfg, cfg.validate()
}

func (c benchConfig) validate() error {
	for _, field := range []struct {
		name            string
		value, min, max float64
	}{
		{"packet_loss_percent", c.PacketLossPercent, 0, 100},
		{"latency_mean_ms", c.LatencyMeanMS, 0, 60000},
		{"latency_stddev_ms", c.LatencyStddevMS, 0, 60000},
		{"timeout_seconds", c.TimeoutSeconds, 0.001, 3600},
	} {
		if math.IsNaN(field.value) || math.IsInf(field.value, 0) || field.value < field.min || field.value > field.max {
			return fmt.Errorf("%s must be finite and in [%g, %g]", field.name, field.min, field.max)
		}
	}
	if c.TotalBytes < 1 || c.TotalBytes > 64<<30 {
		return errors.New("total_bytes must be in [1, 68719476736]")
	}
	if c.SegmentMinBytes < 1 || c.SegmentMaxBytes < c.SegmentMinBytes || c.SegmentMaxBytes > 64<<20 {
		return errors.New("segment sizes must satisfy 1 <= min <= max <= 67108864")
	}
	minCount := (c.TotalBytes + int64(c.SegmentMaxBytes) - 1) / int64(c.SegmentMaxBytes)
	maxCount := c.TotalBytes / int64(c.SegmentMinBytes)
	if minCount > maxCount {
		return errors.New("total_bytes cannot be partitioned into segments within the specified bounds")
	}
	if c.QueueCapacityPackets < 1 || c.QueueCapacityPackets > 65536 {
		return errors.New("queue_capacity_packets must be in [1, 65536]")
	}
	return nil
}

func (c benchConfig) timeout() time.Duration {
	return time.Duration(c.TimeoutSeconds * float64(time.Second))
}

func (c benchConfig) streamBytes() int64 {
	return c.TotalBytes
}
