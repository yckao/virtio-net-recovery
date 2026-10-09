package recovery

import "time"

type verification struct {
	origin         uint64
	acceptedAt     time.Duration
	used, consumed uint16
	overdue        bool
}

func (v verification) confirmed(live *LiveObservation) bool {
	return live != nil && live.Valid && live.At > v.acceptedAt &&
		live.Used != v.used && live.Consumed != v.consumed
}

func (p *Policy) verify(live *LiveObservation, at time.Duration, u *Update) {
	if !p.verificationPending || !p.verification.confirmed(live) {
		return
	}
	v := p.verification
	p.verificationPending = false
	p.verification = verification{}
	p.totals.Verified++
	u.Facts = append(u.Facts, Fact{Kind: VerificationConfirmed, At: at,
		Generation: p.generation, OriginAttemptID: v.origin, AcceptedAt: v.acceptedAt,
		ObservedAt: live.At, Used: live.Used, Consumed: live.Consumed,
		Latency: live.At - v.acceptedAt})
}

func (p *Policy) checkTimeout(at time.Duration, u *Update) {
	v := &p.verification
	if !p.verificationPending || v.overdue || at-v.acceptedAt < p.config.VerificationTimeout {
		return
	}
	v.overdue = true
	u.Facts = append(u.Facts, Fact{Kind: VerificationOverdue, At: at,
		Generation: p.generation, OriginAttemptID: v.origin,
		AcceptedAt: v.acceptedAt, Latency: at - v.acceptedAt})
}

func (p *Policy) discardVerification(at time.Duration, u *Update) {
	if !p.verificationPending {
		return
	}
	u.Facts = append(u.Facts, Fact{Kind: VerificationDiscarded, At: at,
		Generation: p.generation, OriginAttemptID: p.verification.origin,
		AcceptedAt: p.verification.acceptedAt, Reason: ReasonIdentityChanged})
	p.verificationPending = false
	p.verification = verification{}
}
