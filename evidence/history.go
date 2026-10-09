package evidence

// sampleHistory owns storage; all snapshots are detached chronological copies.
type sampleHistory struct {
	values []Sample
	next   int
	count  int
}

func newHistory(limit int) sampleHistory {
	return sampleHistory{values: make([]Sample, limit)}
}

func (h *sampleHistory) add(sample Sample) {
	h.values[h.next] = sample
	h.next = (h.next + 1) % len(h.values)
	h.count = min(h.count+1, len(h.values))
}

func (h *sampleHistory) latest() (Sample, bool) {
	if h.count == 0 {
		return Sample{}, false
	}
	return h.values[(h.next+len(h.values)-1)%len(h.values)], true
}

func (h *sampleHistory) snapshot() []Sample {
	result := make([]Sample, h.count)
	for i := range result {
		result[i] = h.values[(h.next-h.count+i+len(h.values))%len(h.values)]
	}
	return result
}

func (h *sampleHistory) clear() {
	clear(h.values)
	h.next, h.count = 0, 0
}
