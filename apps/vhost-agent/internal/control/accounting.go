package control

import "sync/atomic"

// Accounting is authoritative, fixed-cardinality process accounting. Delivery
// cannot erase these counters. Scrapes read values without taking a worker lock.
type Accounting struct {
	Attempts, Accepted, Refused, WriteErrors, Verified, Polls, PollErrors, InputLost atomic.Uint64
	Sampled, Unavailable                                                             atomic.Int64
	MaxPollGapNS                                                                     atomic.Int64
}
type Counts struct {
	Attempts, Accepted, Refused, WriteErrors, Verified, Polls, PollErrors, InputLost uint64
	Sampled, Unavailable, MaxPollGapNS                                               int64
}

func (a *Accounting) Snapshot() Counts {
	return Counts{a.Attempts.Load(), a.Accepted.Load(), a.Refused.Load(), a.WriteErrors.Load(), a.Verified.Load(), a.Polls.Load(), a.PollErrors.Load(), a.InputLost.Load(), a.Sampled.Load(), a.Unavailable.Load(), a.MaxPollGapNS.Load()}
}
func (a *Accounting) recordPoll(gap int64, failed bool) {
	a.Polls.Add(1)
	if failed {
		a.PollErrors.Add(1)
	}
	for old := a.MaxPollGapNS.Load(); gap > old; old = a.MaxPollGapNS.Load() {
		if a.MaxPollGapNS.CompareAndSwap(old, gap) {
			break
		}
	}
}
