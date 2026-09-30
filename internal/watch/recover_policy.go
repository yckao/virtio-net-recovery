package watch

import "math"

// RecoveryPolicy uses a single cadence for observation and retry pacing. It
// has no elapsed-incident deadline or lifetime recovery budget. An attempt is
// charged before live validation, including a refused or unsuccessful attempt.
type RecoveryPolicy struct {
	Cadence             float64
	Progress            RingProgress
	LastAttempt         float64
	LastAttemptStarted  float64
	MaxAttemptGap       float64
	Attempts, Writes    uint64
	Refusals, Confirmed uint64
	verification        *recoveryVerification
}

type recoveryVerification struct {
	at             float64
	used, consumed uint16
	unconfirmed    bool
}

func NewRecoveryPolicy(cadence float64) *RecoveryPolicy {
	return &RecoveryPolicy{Cadence: cadence, LastAttempt: math.Inf(-1), LastAttemptStarted: math.Inf(-1)}
}

func (p *RecoveryPolicy) Observe(avail, used uint16, num uint32, now float64) bool {
	return p.Progress.Observe(avail, used, num, now, p.Cadence)
}

func (p *RecoveryPolicy) Attempt(now float64) bool {
	if now-p.LastAttempt < p.Cadence {
		return false
	}
	p.LastAttempt = now
	if !math.IsInf(p.LastAttemptStarted, -1) {
		p.MaxAttemptGap = max(p.MaxAttemptGap, now-p.LastAttemptStarted)
	}
	p.LastAttemptStarted = now
	p.Attempts++
	return true
}

func (p *RecoveryPolicy) Written(now float64, used, consumed uint16) {
	p.Writes++
	// A repeated write must not postpone the original verification deadline.
	if p.verification == nil {
		p.verification = &recoveryVerification{at: now, used: used, consumed: consumed}
	}
}

// FinishAttempt paces from the end of validation/write, so a slow live check
// cannot make two completed writes occur less than one cadence apart.
func (p *RecoveryPolicy) FinishAttempt(now float64) {
	p.LastAttempt = max(p.LastAttempt, now)
}

// Verify requires both completion and consumption progress from a later live
// snapshot. A successful eventfd write alone never confirms recovery.
func (p *RecoveryPolicy) Verify(now float64, used, consumed uint16) (float64, bool) {
	v := p.verification
	if v == nil || used == v.used || consumed == v.consumed {
		return 0, false
	}
	p.verification = nil
	p.Confirmed++
	return now - v.at, true
}

func (p *RecoveryPolicy) NeedsVerification(used uint16) bool {
	return p.verification != nil && used != p.verification.used
}

func (p *RecoveryPolicy) Unconfirmed(now, timeout float64) bool {
	v := p.verification
	if v == nil || v.unconfirmed || now-v.at < timeout {
		return false
	}
	v.unconfirmed = true
	return true
}

func (p *RecoveryPolicy) PendingAge(now float64) float64 {
	if p.verification == nil {
		return 0
	}
	return max(0, now-p.verification.at)
}

// Refused distinguishes a charged recovery attempt from a verification-only
// read failure. It returns false when the caller should count a polling error.
func (p *RecoveryPolicy) Refused(attempt bool) bool {
	if !attempt {
		return false
	}
	p.Refusals++
	return true
}

func (p *RecoveryPolicy) Invalidate(now float64) {
	p.Progress.Reset(now)
}

// noteDiscoveryFailure prevents an unsupported or not-yet-configured slot from
// turning healthy discovery into a cadence-rate ioctl loop. A previously
// supported attachment gets one quick rediscovery retry. Subsequent failures,
// even with changed error text, use the inventory interval until success or
// disappearance clears the failure episode.
func noteDiscoveryFailure(failures map[int]string, fd int, failure string, previouslySupported bool) (report, refresh bool) {
	_, failedBefore := failures[fd]
	report = failures[fd] != failure
	failures[fd] = failure
	return report, previouslySupported && !failedBefore
}
