package recovery

// Complete validates the entire receipt before changing state. A rejected
// result leaves the operation in flight so the caller can supply the actual
// correctly correlated result. Duplicate completions never change counters.
func (p *Policy) Complete(result OperationResult) (Update, error) {
	if err := p.validateResult(result); err != nil {
		return Update{}, err
	}
	op := *p.inFlight
	p.inFlight = nil
	p.lastAt = result.CompletedAt
	p.hasLiveCompletion, p.lastLiveCompletion = true, result.CompletedAt
	u := Update{Candidate: p.candidate}
	u.Facts = append(u.Facts, Fact{Kind: OperationCompleted, At: result.CompletedAt,
		Generation: p.generation, OperationID: op.ID, AttemptID: op.AttemptID,
		Outcome: result.Outcome, Reason: result.Reason, AcceptedAt: result.AcceptedAt})
	if result.Live != nil {
		u.Facts[0].ObservedAt = result.Live.At
		u.Facts[0].Used, u.Facts[0].Consumed = result.Live.Used, result.Live.Consumed
	}
	if op.Kind == Notify {
		p.hasAttemptCompletion, p.lastAttemptCompletion = true, result.CompletedAt
		p.accountAttempt(result.Outcome)
	}
	// A valid same-generation observation can verify a previous accepted write
	// even when the current notification was refused. ValidateResult excludes
	// contradictory invalid/identity-change classifications from this path.
	p.verify(result.Live, result.CompletedAt, &u)
	if result.Reason == ReasonIdentityChanged {
		p.discardVerification(result.CompletedAt, &u)
	}
	if result.Outcome == Accepted && !p.verificationPending {
		live := *result.Live
		p.verificationPending = true
		p.verification = verification{origin: op.AttemptID, acceptedAt: result.AcceptedAt,
			used: live.Used, consumed: live.Consumed}
		u.Facts = append(u.Facts, Fact{Kind: VerificationStarted, At: result.CompletedAt,
			Generation: p.generation, OriginAttemptID: op.AttemptID,
			AcceptedAt: result.AcceptedAt, ObservedAt: live.At, Used: live.Used, Consumed: live.Consumed})
	}
	if result.Outcome != Accepted && !(result.Outcome == Observed && result.Reason == ReasonPending) {
		p.progress.reset()
		p.setCandidate(false, result.CompletedAt, result.Reason, &u)
	}
	p.checkTimeout(result.CompletedAt, &u)
	return u, nil
}

func (p *Policy) validateResult(r OperationResult) error {
	if p.stopped {
		return ErrStopped
	}
	op := p.inFlight
	if op == nil || r.ID != op.ID || r.Generation != op.Generation {
		return ErrUnexpectedResult
	}
	if r.CompletedAt < p.lastAt || r.CompletedAt < 0 {
		return ErrInvalidTime
	}
	if !knownReason(r.Reason) {
		return ErrInvalidResult
	}
	if r.Live != nil {
		if r.Live.Generation != op.Generation || r.Live.At < op.StartedAt || r.Live.At > r.CompletedAt {
			return ErrInvalidResult
		}
		if r.Live.Valid && (r.Reason == ReasonInvalidRing || r.Reason == ReasonIdentityChanged || r.Reason == ReasonUnsupported) {
			return ErrInvalidResult
		}
	}
	if r.Outcome != Accepted && r.AcceptedAt != 0 {
		return ErrInvalidResult
	}
	switch r.Outcome {
	case Accepted:
		if op.Kind != Notify || r.Live == nil || !r.Live.Valid ||
			r.Live.Used != op.ExpectedUsed || r.AcceptedAt < r.Live.At || r.AcceptedAt > r.CompletedAt ||
			(r.Reason != ReasonNone && r.Reason != ReasonPending) {
			return ErrInvalidResult
		}
	case Observed:
		if op.Kind != Inspect || r.Live == nil ||
			!inspectionReason(r.Reason) ||
			(r.Reason == ReasonPending && (!r.Live.Valid || r.Live.Used != op.ExpectedUsed)) {
			return ErrInvalidResult
		}
	case Refused:
		if op.Kind != Notify || (!inspectionReason(r.Reason) && r.Reason != ReasonContended) ||
			r.Reason == ReasonNone || r.Reason == ReasonPending {
			return ErrInvalidResult
		}
	case Unavailable:
		if r.Reason != ReasonNone && r.Reason != ReasonUnavailable &&
			r.Reason != ReasonIdentityChanged && r.Reason != ReasonUnsupported {
			return ErrInvalidResult
		}
		if r.Live != nil && r.Live.Valid {
			return ErrInvalidResult
		}
	case Cancelled:
		if r.Reason != ReasonNone && r.Reason != ReasonCancelled {
			return ErrInvalidResult
		}
	case WriteFailed:
		if op.Kind != Notify || (r.Reason != ReasonNone && r.Reason != ReasonWriteFailed) {
			return ErrInvalidResult
		}
	default:
		return ErrInvalidResult
	}
	return nil
}

func inspectionReason(reason Reason) bool {
	switch reason {
	case ReasonNone, ReasonPending, ReasonUsedProgress, ReasonDrained, ReasonWorkQueued,
		ReasonConsumed, ReasonInvalidRing, ReasonIdentityChanged, ReasonUnsupported:
		return true
	default:
		return false
	}
}

func knownReason(reason Reason) bool {
	switch reason {
	case ReasonNone, ReasonPending, ReasonUsedProgress, ReasonDrained,
		ReasonWorkQueued, ReasonConsumed, ReasonInvalidRing, ReasonIdentityChanged,
		ReasonUnsupported, ReasonContended, ReasonUnavailable, ReasonCancelled, ReasonWriteFailed:
		return true
	default:
		return false
	}
}

func (p *Policy) accountAttempt(outcome Outcome) {
	switch outcome {
	case Accepted:
		p.totals.Writes++
	case Refused:
		p.totals.Refusals++
	case Unavailable:
		p.totals.Unavailable++
	case Cancelled:
		p.totals.Cancellations++
	case WriteFailed:
		p.totals.WriteFailures++
	}
}
