package control

import "sync/atomic"

// Accounting holds execution and coverage facts independently of optional output.
// Completed outcome counters partition attempts except operations still in flight.
type Accounting struct {
	Attempts, Accepted, Refused, ReadErrors, WriteErrors, Cancelled, Verified                   atomic.Uint64
	Polls, PollErrors, DiscoveryErrors, AdmissionErrors                                         atomic.Uint64
	Sampled, Unavailable, SelectedTargets, ActiveTargets, UnavailableTargets, DiscoveryProblems atomic.Int64
	InventoryTruncated                                                                          atomic.Int64
	MaxPollGapNS                                                                                atomic.Int64
}
type Counts struct {
	Attempts, Accepted, Refused, ReadErrors, WriteErrors, Cancelled, Verified                   uint64
	Polls, PollErrors, DiscoveryErrors, AdmissionErrors                                         uint64
	Sampled, Unavailable, SelectedTargets, ActiveTargets, UnavailableTargets, DiscoveryProblems int64
	InventoryTruncated                                                                          int64
	MaxPollGapNS                                                                                int64
}

func (a *Accounting) Snapshot() Counts {
	return Counts{InventoryTruncated: a.InventoryTruncated.Load(), Attempts: a.Attempts.Load(), Accepted: a.Accepted.Load(), Refused: a.Refused.Load(), ReadErrors: a.ReadErrors.Load(), WriteErrors: a.WriteErrors.Load(), Cancelled: a.Cancelled.Load(), Verified: a.Verified.Load(), Polls: a.Polls.Load(), PollErrors: a.PollErrors.Load(), DiscoveryErrors: a.DiscoveryErrors.Load(), AdmissionErrors: a.AdmissionErrors.Load(), Sampled: a.Sampled.Load(), Unavailable: a.Unavailable.Load(), SelectedTargets: a.SelectedTargets.Load(), ActiveTargets: a.ActiveTargets.Load(), UnavailableTargets: a.UnavailableTargets.Load(), DiscoveryProblems: a.DiscoveryProblems.Load(), MaxPollGapNS: a.MaxPollGapNS.Load()}
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
