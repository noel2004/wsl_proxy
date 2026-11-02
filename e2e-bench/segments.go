package e2ebench

import "math/rand"

// segmentPlan samples sizes within the requested range while preserving an exact
// total and never producing an undersized final segment. Choose a count near the
// range's midpoint, then constrain each draw so the remaining slots can still
// contain the remaining bytes. The final size is determined by the remainder.
// This PRNG is separate from network loss/jitter, keeping the workload stable.
type segmentPlan struct {
	rng              *rand.Rand
	remaining, slots int64
	min, max         int64
}

func newSegmentPlan(c benchConfig) *segmentPlan {
	low, high := int64(c.SegmentMinBytes), int64(c.SegmentMaxBytes)
	minCount := (c.TotalBytes + high - 1) / high
	maxCount := c.TotalBytes / low
	count := (2*c.TotalBytes + low + high - 1) / (low + high)
	return &segmentPlan{
		rng:       rand.New(rand.NewSource(c.Seed ^ 0x6a09e667)),
		remaining: c.TotalBytes, slots: min(max(count, minCount), maxCount), min: low, max: high,
	}
}

// next returns zero after all bytes have been assigned. Config must be validated
// before constructing the plan so every constrained interval is nonempty.
func (p *segmentPlan) next() int {
	if p.slots == 0 {
		return 0
	}
	low := max(p.min, p.remaining-(p.slots-1)*p.max)
	high := min(p.max, p.remaining-(p.slots-1)*p.min)
	size := low + p.rng.Int63n(high-low+1)
	p.remaining -= size
	p.slots--
	return int(size)
}
