package e2ebench

import "encoding/binary"

// Wire counters observe version 1 at WriteTo, before simulated loss or latency.
// Sizes include the 16-byte transport header, but not UDP/IP headers. Keep this
// small observer synchronized with docs/wire-v1.md; it does not alter packets.
const (
	metricHeaderBytes = 16
	metricDATA        = 4
	metricACK         = 5
)

type wireStats struct {
	DataPackets, DataResends         uint64
	ACKPackets, ACKBytes, ACKEntries uint64
}

func (s *wireStats) add(other wireStats) {
	s.DataPackets += other.DataPackets
	s.DataResends += other.DataResends
	s.ACKPackets += other.ACKPackets
	s.ACKBytes += other.ACKBytes
	s.ACKEntries += other.ACKEntries
}

type wireObserver struct{ highest map[uint16]uint32 }

func (o *wireObserver) observe(b []byte, stats *wireStats) {
	if len(b) < metricHeaderBytes || b[0] != 'W' || b[1] != 'P' || b[2] != 1 || int(binary.BigEndian.Uint16(b[6:8])) != len(b)-metricHeaderBytes {
		return
	}
	id := binary.BigEndian.Uint16(b[4:6])
	if id == 0 {
		return
	}
	switch b[3] {
	case metricDATA:
		if len(b) == metricHeaderBytes {
			return
		}
		stats.DataPackets++
		seq := binary.BigEndian.Uint32(b[8:12])
		previous, seen := o.highest[id]
		// First transmissions arrive here in sequence order, before the simulated
		// network can reorder them. An older/equal sequence is a retransmission.
		// Modular comparison handles wrapping without an ever-growing seen set.
		if seen && int32(seq-previous) <= 0 {
			stats.DataResends++
			return
		}
		if o.highest == nil {
			o.highest = make(map[uint16]uint32)
		}
		o.highest[id] = seq
	case metricACK:
		if (len(b)-metricHeaderBytes)%4 != 0 {
			return
		}
		stats.ACKPackets++
		stats.ACKBytes += uint64(len(b))
		stats.ACKEntries += uint64((len(b) - metricHeaderBytes) / 4)
	}
}
