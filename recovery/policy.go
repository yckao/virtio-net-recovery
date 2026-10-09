package recovery

import (
	"errors"
	"time"
)

var (
	ErrInvalidConfig     = errors.New("invalid recovery configuration")
	ErrInvalidTime       = errors.New("negative or backward monotonic time")
	ErrGeneration        = errors.New("unexpected or zero attachment generation")
	ErrOperationInFlight = errors.New("complete the in-flight operation first")
	ErrUnexpectedResult  = errors.New("result does not match the in-flight operation")
	ErrInvalidResult     = errors.New("invalid operation result")
	ErrStopped           = errors.New("policy is stopped")
	ErrSequenceExhausted = errors.New("operation sequence exhausted")
)

// Policy owns one still-present slot. Calls must be serialized. Do not copy a
// live Policy; use Status for a value snapshot. Discard it when the slot is
// removed, and create a new policy for a newly discovered slot.
type Policy struct {
	config                Config
	generation            uint64
	lastAt                time.Duration
	progress              progress
	candidate, stopped    bool
	sequence              uint64
	inFlight              *Operation
	hasLiveCompletion     bool
	lastLiveCompletion    time.Duration
	hasAttemptCompletion  bool
	lastAttemptCompletion time.Duration
	verificationPending   bool
	verification          verification
	totals                Totals
}

func New(config Config, generation uint64) (*Policy, error) {
	if (config.Mode != Observe && config.Mode != Recover) || config.Cadence <= 0 || config.VerificationTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	if generation == 0 {
		return nil, ErrGeneration
	}
	return &Policy{config: config, generation: generation}, nil
}

func (p *Policy) ready(at time.Duration) error {
	if p.stopped {
		return ErrStopped
	}
	if p.inFlight != nil {
		return ErrOperationInFlight
	}
	if at < 0 || at < p.lastAt {
		return ErrInvalidTime
	}
	return nil
}

func (p *Policy) setCandidate(value bool, at time.Duration, reason Reason, u *Update) {
	if value != p.candidate {
		kind := CandidateCleared
		if value {
			kind = CandidateDetected
		}
		u.Facts = append(u.Facts, Fact{Kind: kind, At: at, Generation: p.generation, Reason: reason})
	}
	p.candidate, u.Candidate = value, value
}

// Observe ages a candidate and may issue one operation. Repeated identical
// observations at the same time never issue additional completed operations.
// An invalid ring sample is accepted as invalidation, not an API error.
func (p *Policy) Observe(sample CachedObservation) (Update, error) {
	if err := p.ready(sample.At); err != nil {
		return Update{}, err
	}
	if sample.Generation != p.generation {
		return Update{}, ErrGeneration
	}
	// Work on a value copy so even sequence-exhaustion errors are transactional.
	next := *p
	u := Update{Candidate: p.candidate}
	next.lastAt = sample.At
	candidate, valid := next.progress.observe(sample, next.config.Cadence)
	reason := ReasonPending
	if !valid {
		reason = ReasonInvalidRing
		u.Facts = append(u.Facts, Fact{Kind: ObservationRejected, At: sample.At, Generation: p.generation, Reason: reason})
	}
	next.setCandidate(candidate, sample.At, reason, &u)
	next.checkTimeout(sample.At, &u)
	verify := valid && next.verificationPending && sample.Used != next.verification.used
	due := !next.hasLiveCompletion || sample.At-next.lastLiveCompletion >= next.config.Cadence
	if (candidate || verify) && due {
		if next.sequence == ^uint64(0) {
			return Update{}, ErrSequenceExhausted
		}
		next.sequence++
		op := Operation{ID: next.sequence, Generation: next.generation, Kind: Inspect,
			StartedAt: sample.At, ExpectedUsed: sample.Used, ForCandidate: candidate, ForVerification: verify}
		if candidate && next.config.Mode == Recover {
			op.Kind = Notify
			next.totals.Attempts++
			op.AttemptID = next.totals.Attempts
		} else {
			next.totals.Inspections++
		}
		next.inFlight = &op
		copy := op
		u.Operation = &copy
		u.Facts = append(u.Facts, Fact{Kind: OperationStarted, At: sample.At, Generation: next.generation,
			OperationID: op.ID, AttemptID: op.AttemptID})
	}
	*p = next
	return u, nil
}

// Unavailable resets candidate timing without erasing verification or pacing.
func (p *Policy) Unavailable(at time.Duration) (Update, error) {
	if err := p.ready(at); err != nil {
		return Update{}, err
	}
	p.lastAt = at
	p.progress.reset()
	u := Update{}
	p.setCandidate(false, at, ReasonUnavailable, &u)
	p.checkTimeout(at, &u)
	return u, nil
}

// Advance checks verification age. Time alone never requests a notification.
func (p *Policy) Advance(at time.Duration) (Update, error) {
	if err := p.ready(at); err != nil {
		return Update{}, err
	}
	p.lastAt = at
	u := Update{Candidate: p.candidate}
	p.checkTimeout(at, &u)
	return u, nil
}

// ReplaceGeneration retains slot pacing/counters but discards the previous
// attachment's candidate and verification. The caller supplies unique nonzero
// generation IDs; an equal ID is an idempotent no-op except for time advance.
func (p *Policy) ReplaceGeneration(generation uint64, at time.Duration) (Update, error) {
	if err := p.ready(at); err != nil {
		return Update{}, err
	}
	if generation == 0 {
		return Update{}, ErrGeneration
	}
	p.lastAt = at
	u := Update{Candidate: p.candidate}
	if generation == p.generation {
		p.checkTimeout(at, &u)
		return u, nil
	}
	p.progress.reset()
	p.setCandidate(false, at, ReasonIdentityChanged, &u)
	p.discardVerification(at, &u)
	p.generation = generation
	u.Facts = append(u.Facts, Fact{Kind: GenerationChanged, At: at, Generation: generation, Reason: ReasonIdentityChanged})
	return u, nil
}

// Stop closes the policy after its last operation has been accounted. It
// retains unresolved verification in Status, but never confirms it on stop.
func (p *Policy) Stop(at time.Duration) (Update, error) {
	if err := p.ready(at); err != nil {
		return Update{}, err
	}
	p.lastAt = at
	p.progress.reset()
	u := Update{}
	p.setCandidate(false, at, ReasonNone, &u)
	p.checkTimeout(at, &u)
	p.stopped = true
	u.Facts = append(u.Facts, Fact{Kind: PolicyStopped, At: at, Generation: p.generation})
	return u, nil
}

func (p *Policy) Status() Status {
	s := Status{Generation: p.generation, Candidate: p.candidate, Stopped: p.stopped,
		InFlight: p.inFlight != nil, VerificationPending: p.verificationPending,
		HasCompletedAttempt: p.hasAttemptCompletion, LastAttemptCompletedAt: p.lastAttemptCompletion, Totals: p.totals}
	if p.inFlight != nil {
		s.Operation = *p.inFlight
	}
	if p.verificationPending {
		s.OriginAttemptID, s.VerificationStartedAt = p.verification.origin, p.verification.acceptedAt
		s.VerificationTimedOut = p.verification.overdue
	}
	return s
}
