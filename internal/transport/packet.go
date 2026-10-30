package transport

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	headerSize = 16
	openSize   = 48
	version    = 1
)

type kind uint8

const (
	kindOpen kind = iota + 1
	kindOpened
	kindReject
	kindData
	kindACK
	kindKeepalive
	kindFIN
	kindReset
)

// packet is the decoded v1 envelope. Payload aliases its input during decode;
// state that outlives dispatch must copy the payload. All integers are big endian.
type packet struct {
	kind    kind
	id      uint16
	seq     uint32
	limit   uint32
	payload []byte
}

var errPacket = errors.New("transport: malformed packet")

func (p packet) encode() []byte {
	b := make([]byte, headerSize+len(p.payload))
	b[0], b[1], b[2], b[3] = 'W', 'P', version, byte(p.kind)
	binary.BigEndian.PutUint16(b[4:6], p.id)
	binary.BigEndian.PutUint16(b[6:8], uint16(len(p.payload)))
	binary.BigEndian.PutUint32(b[8:12], p.seq)
	binary.BigEndian.PutUint32(b[12:16], p.limit)
	copy(b[headerSize:], p.payload)
	return b
}

func decode(b []byte, maxSize int) (packet, error) {
	if len(b) < headerSize || len(b) > maxSize || b[0] != 'W' || b[1] != 'P' || b[2] != version || int(binary.BigEndian.Uint16(b[6:8])) != len(b)-headerSize {
		return packet{}, errPacket
	}
	p := packet{kind: kind(b[3]), id: binary.BigEndian.Uint16(b[4:6]), seq: binary.BigEndian.Uint32(b[8:12]), limit: binary.BigEndian.Uint32(b[12:16]), payload: b[headerSize:]}
	switch p.kind {
	case kindOpen:
		if p.id != 0 || p.seq != 0 || len(p.payload) <= openSize {
			return packet{}, errPacket
		}
	case kindOpened:
		if p.id == 0 || p.seq != 0 || len(p.payload) != openSize {
			return packet{}, errPacket
		}
	case kindReject:
		if p.id != 0 || p.seq != 0 || p.limit != 0 || len(p.payload) != 8 {
			return packet{}, errPacket
		}
	case kindData, kindFIN:
		if p.id == 0 || p.limit != 0 || (p.kind == kindData && len(p.payload) == 0) || (p.kind == kindFIN && len(p.payload) != 0) {
			return packet{}, errPacket
		}
	case kindACK:
		if p.id == 0 || p.seq != 0 || len(p.payload)%4 != 0 {
			return packet{}, errPacket
		}
	case kindKeepalive, kindReset:
		if p.id == 0 || p.seq != 0 || len(p.payload) != 0 || (p.kind == kindReset && p.limit != 0) {
			return packet{}, errPacket
		}
	default:
		return packet{}, errPacket
	}
	return p, nil
}

// openPayload correlates only opening retries. The token is not present on
// established traffic and does not provide a session generation or authentication.
func openPayload(token uint64, c Config, target string) []byte {
	b := make([]byte, openSize+len(target))
	binary.BigEndian.PutUint64(b[:8], token)
	binary.BigEndian.PutUint32(b[8:12], c.ReceiveWindowPackets)
	binary.BigEndian.PutUint32(b[12:16], uint32(c.MaxDatagramBytes))
	for i, d := range []time.Duration{c.ConnectTimeout, c.DeliveryTimeout, c.KeepaliveInterval, c.ReceiveIdleTimeout} {
		binary.BigEndian.PutUint64(b[16+i*8:], uint64(d))
	}
	copy(b[openSize:], target)
	return b
}

func compatibleOpen(p packet, c Config) bool {
	if len(p.payload) < openSize {
		return false
	}
	w := binary.BigEndian.Uint32(p.payload[8:12])
	if w == 0 || w >= 1<<31 || p.limit != w || binary.BigEndian.Uint32(p.payload[12:16]) != uint32(c.MaxDatagramBytes) {
		return false
	}
	if uint64(w)+uint64(c.SendWindowPackets) >= 1<<31 {
		return false
	}
	for i, d := range []time.Duration{c.ConnectTimeout, c.DeliveryTimeout, c.KeepaliveInterval, c.ReceiveIdleTimeout} {
		if binary.BigEndian.Uint64(p.payload[16+i*8:]) != uint64(d) {
			return false
		}
	}
	return true
}

func seqAfter(a, b uint32) bool { return int32(a-b) > 0 }
