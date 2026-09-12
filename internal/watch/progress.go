package watch

// RingProgress identifies candidates from user-ring indices only. A candidate
// is not a stall verdict: recovery requires subsequent live vhost confirmation.
type RingProgress struct {
	Since    float64
	used     uint16
	eligible bool
}

func (p *RingProgress) Observe(avail, used uint16, num uint32, now, threshold float64) bool {
	outstanding := avail - used
	eligible := outstanding > 0 && uint32(outstanding) <= num
	if !p.eligible || !eligible || p.used != used {
		p.Since = now
	}
	p.used, p.eligible = used, eligible
	return eligible && now-p.Since >= threshold
}

func (p *RingProgress) Reset(now float64) {
	p.Since, p.eligible = now, false
}
