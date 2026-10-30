package transport

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

type sentPacket struct {
	p                   packet
	firstSent, lastSent time.Time
	retryAt             time.Time
}

type fragment struct {
	data []byte
	fin  bool
}

// session is the deterministic stream state machine. Its owner serializes all
// calls and supplies monotonic time. It performs no I/O and starts no goroutines;
// returned packets are sent by the endpoint, so tests can drop or reorder them.
type session struct {
	cfg                                       Config
	id                                        uint16
	nextSend, recvNext, peerLimit, peerWindow uint32
	consumed                                  uint64
	pending                                   map[uint32]*sentPacket
	received                                  map[uint32]fragment
	readOffset                                int
	acks                                      map[uint32]struct{}
	ackDue                                    time.Time
	lastRecv, lastSent, lastAdvertised        time.Time
	retryInterval                             time.Duration
	writeClosed, remoteEOF                    bool
	finalSeq                                  *uint32
}

func newSession(c Config, id uint16, peerWindow uint32, now time.Time) *session {
	return &session{cfg: c, id: id, nextSend: 1, recvNext: 1, peerLimit: peerWindow, peerWindow: peerWindow,
		pending: make(map[uint32]*sentPacket), received: make(map[uint32]fragment), acks: make(map[uint32]struct{}),
		lastRecv: now, lastSent: now, lastAdvertised: now, retryInterval: c.InitialRetryInterval}
}

func (s *session) receiveLimit() uint32 { return s.recvNext + s.cfg.ReceiveWindowPackets - 1 }

func (s *session) canSend() bool {
	return uint32(len(s.pending)) < s.cfg.SendWindowPackets && !seqAfter(s.nextSend, s.peerLimit)
}

// queue takes a copy of the caller's bytes and reserves one sequence/window slot.
// A FIN occupies a slot too, so EOF cannot overtake earlier DATA.
func (s *session) queue(data []byte, fin bool, now time.Time) packet {
	k := kindData
	if fin {
		k = kindFIN
		s.writeClosed = true
	}
	p := packet{kind: k, id: s.id, seq: s.nextSend, payload: append([]byte(nil), data...)}
	s.nextSend++
	s.pending[p.seq] = &sentPacket{p: p, firstSent: now, lastSent: now, retryAt: now.Add(s.retryInterval)}
	s.lastSent = now
	return p
}

func (s *session) flushACK(now time.Time) packet {
	p := packet{kind: kindACK, id: s.id, limit: s.receiveLimit(), payload: make([]byte, 0, 4*len(s.acks))}
	for seq := range s.acks {
		p.payload = binary.BigEndian.AppendUint32(p.payload, seq)
	}
	clear(s.acks)
	s.ackDue = time.Time{}
	s.lastSent = now
	s.lastAdvertised = now
	return p
}

// input returns packets to send and whether the input was valid for this live
// state. Rejected data cannot refresh liveness or gain an acknowledgment.
func (s *session) input(p packet, now time.Time) ([]packet, bool) {
	switch p.kind {
	case kindData, kindFIN:
		behind := uint32(s.recvNext - p.seq)
		duplicateConsumed := behind > 0 && behind < 1<<31 && uint64(behind) <= s.consumed
		if duplicateConsumed && p.kind == kindFIN && (s.finalSeq == nil || *s.finalSeq != p.seq) {
			return nil, false
		}
		if s.finalSeq != nil && p.seq == *s.finalSeq && p.kind != kindFIN {
			return nil, false
		}
		if !duplicateConsumed {
			if uint32(p.seq-s.recvNext) >= s.cfg.ReceiveWindowPackets {
				return nil, false
			}
			if s.finalSeq != nil && (seqAfter(p.seq, *s.finalSeq) || (p.seq == *s.finalSeq && p.kind != kindFIN)) {
				return nil, false
			}
			if existing, ok := s.received[p.seq]; ok {
				if existing.fin != (p.kind == kindFIN) || !bytes.Equal(existing.data, p.payload) {
					return nil, false
				}
			} else {
				if p.kind == kindFIN {
					if s.finalSeq != nil && *s.finalSeq != p.seq {
						return nil, false
					}
					for seq := range s.received {
						if seqAfter(seq, p.seq) {
							return nil, false
						}
					}
					seq := p.seq
					s.finalSeq = &seq
				}
				s.received[p.seq] = fragment{data: append([]byte(nil), p.payload...), fin: p.kind == kindFIN}
			}
		}
		if len(s.acks) == 0 {
			s.ackDue = now.Add(s.cfg.AckMaxDelay)
		}
		s.acks[p.seq] = struct{}{}
		s.lastRecv = now
		if len(s.acks) >= s.cfg.AckBatchSize {
			return []packet{s.flushACK(now)}, true
		}
	case kindACK, kindKeepalive:
		// A peer cannot have consumed bytes that we never sent. This also bounds
		// modular comparisons when a stale or malformed advertisement arrives.
		if uint32(p.limit-s.peerLimit) == 1<<31 {
			return nil, false
		}
		if seqAfter(p.limit, s.peerLimit) {
			if seqAfter(p.limit, s.nextSend-1+s.peerWindow) {
				return nil, false
			}
			s.peerLimit = p.limit
		}
		progress := false
		for b := p.payload; len(b) >= 4; b = b[4:] {
			seq := binary.BigEndian.Uint32(b)
			if _, ok := s.pending[seq]; ok {
				delete(s.pending, seq)
				progress = true
			}
		}
		if progress {
			s.retryInterval = s.cfg.InitialRetryInterval
			for _, sent := range s.pending {
				// Resume promptly after progress without retrying a fresh packet early.
				sent.retryAt = sent.lastSent.Add(s.retryInterval)
			}
		}
		s.lastRecv = now
	default:
		return nil, false
	}
	return nil, true
}

// read releases a slot only after its entire payload has been consumed. Copying
// part of a fragment does not grant capacity for another full-size datagram.
func (s *session) read(dst []byte, now time.Time) (int, []packet, error) {
	if s.remoteEOF {
		return 0, nil, io.EOF
	}
	n, freed := 0, false
	for n < len(dst) {
		f, ok := s.received[s.recvNext]
		if !ok {
			break
		}
		if f.fin {
			if n > 0 {
				break
			}
			delete(s.received, s.recvNext)
			s.recvNext++
			s.consumed++
			s.remoteEOF = true
			return 0, []packet{s.flushACK(now)}, io.EOF
		}
		copied := copy(dst[n:], f.data[s.readOffset:])
		n += copied
		s.readOffset += copied
		if s.readOffset != len(f.data) {
			break
		}
		delete(s.received, s.recvNext)
		s.recvNext++
		s.consumed++
		s.readOffset = 0
		freed = true
	}
	if freed {
		return n, []packet{s.flushACK(now)}, nil
	}
	return n, nil, nil
}

// tick enforces absolute failure deadlines before producing retries. One scan
// containing due packets is a retry round: its next delay doubles once, not once
// per packet. Genuine ACK progress resets that delay; duplicate ACKs do not.
func (s *session) tick(now time.Time) ([]packet, error) {
	for _, sent := range s.pending {
		if now.Sub(sent.firstSent) >= s.cfg.DeliveryTimeout {
			return nil, ErrDeliveryTimeout
		}
	}
	if now.Sub(s.lastRecv) >= s.cfg.ReceiveIdleTimeout {
		return nil, ErrIdleTimeout
	}
	var out []packet
	if !s.ackDue.IsZero() && !now.Before(s.ackDue) {
		out = append(out, s.flushACK(now))
	}
	nextInterval := s.cfg.backoff(s.retryInterval)
	for _, sent := range s.pending {
		if now.Before(sent.retryAt) {
			continue
		}
		out = append(out, sent.p)
		sent.lastSent = now
		sent.retryAt = now.Add(nextInterval)
	}
	// An ACK-only flush is not a retry round.
	for _, p := range out {
		if p.kind == kindData || p.kind == kindFIN {
			s.retryInterval = nextInterval
			break
		}
	}
	if len(out) > 0 {
		s.lastSent = now
	}
	// Repeat capacity advertisements even during continuous DATA traffic.
	// Otherwise a lost window-only ACK could stall the opposite direction forever.
	if now.Sub(s.lastSent) >= s.cfg.KeepaliveInterval || now.Sub(s.lastAdvertised) >= s.cfg.KeepaliveInterval {
		out = append(out, packet{kind: kindKeepalive, id: s.id, limit: s.receiveLimit()})
		s.lastSent = now
		s.lastAdvertised = now
	}
	return out, nil
}
