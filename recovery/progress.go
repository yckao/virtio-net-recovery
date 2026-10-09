package recovery

import "time"

// progress owns the temporal interpretation of cached ring indices only.
type progress struct {
	since    time.Duration
	used     uint16
	eligible bool
}

func validRing(size uint32) bool {
	return size != 0 && size <= 32768 && size&(size-1) == 0
}

func (p *progress) observe(s CachedObservation, cadence time.Duration) (candidate, valid bool) {
	outstanding := s.Avail - s.Used
	valid = validRing(s.Size) && uint32(outstanding) <= s.Size
	eligible := valid && outstanding != 0
	if !p.eligible || !eligible || p.used != s.Used {
		p.since = s.At
	}
	p.used, p.eligible = s.Used, eligible
	return eligible && s.At-p.since >= cadence, valid
}

func (p *progress) reset() { *p = progress{} }
