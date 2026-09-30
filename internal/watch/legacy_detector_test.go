package watch

import "math"

// Historical policy reference retained only for comparison tests/benchmarks.
type Detector struct {
	Threshold, Cooldown float64
	Maximum             int
	Since, LastKick     float64
	Reported            bool
	identity            Snapshot
	used, consumed      uint16
	have                bool
	attempts            []float64
}

func NewDetector(threshold, cooldown float64, maximum int) *Detector {
	return &Detector{Threshold: threshold, Cooldown: cooldown, Maximum: maximum, LastKick: math.Inf(-1)}
}

func (d *Detector) Observe(s Snapshot, avail, used uint16, busy bool, now float64) bool {
	pending, outstanding := avail-s.LastAvail, avail-used
	invalid := uint32(pending) > s.Num || uint32(outstanding) > s.Num
	if !d.have || d.identity != s.Identity() || d.used != used || d.consumed != s.LastAvail || pending == 0 || busy || invalid {
		d.Since, d.Reported = now, false
	}
	d.identity, d.used, d.consumed, d.have = s.Identity(), used, s.LastAvail, true
	return pending > 0 && !invalid && !busy && now-d.Since >= d.Threshold
}

func (d *Detector) Allowed(now float64) bool {
	kept := d.attempts[:0]
	for _, t := range d.attempts {
		if now-t < 3600 {
			kept = append(kept, t)
		}
	}
	d.attempts = kept
	return now-d.LastKick >= d.Cooldown && len(d.attempts) < d.Maximum
}

func (d *Detector) Kicked(now float64) {
	d.LastKick = now
	d.attempts = append(d.attempts, now)
}
