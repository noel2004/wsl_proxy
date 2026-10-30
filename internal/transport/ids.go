package transport

import "time"

type freeID struct {
	id      uint16
	ready   time.Time
	request string
}

// idPool is a bounded FIFO. Released slots retain their opening tombstone until
// quarantine expires and allocation reuses the slot. Uniform quarantine keeps
// the FIFO ordered by eligibility when releases use monotonic time.
type idPool struct {
	queue       []freeID
	head, count int
}

func newIDPool(size int) *idPool {
	p := &idPool{queue: make([]freeID, size), count: size}
	for i := range p.queue {
		p.queue[i].id = uint16(i + 1)
	}
	return p
}

func (p *idPool) take(now time.Time) (freeID, bool) {
	if p.count == 0 || now.Before(p.queue[p.head].ready) {
		return freeID{}, false
	}
	slot := p.queue[p.head]
	p.head = (p.head + 1) % len(p.queue)
	p.count--
	return slot, true
}

func (p *idPool) release(id uint16, request string, ready time.Time) {
	index := (p.head + p.count) % len(p.queue)
	p.queue[index] = freeID{id: id, request: request, ready: ready}
	p.count++
}
