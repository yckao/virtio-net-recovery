// Package evidence reduces explicitly supplied queue observations into bounded
// diagnostic episodes. It makes no recovery decisions and performs no effects.
package evidence

import (
	"errors"
	"time"
)

const (
	// MaxHistoryLimit bounds memory even for a misconfigured caller.
	MaxHistoryLimit = 256
	// MaxRecordsPerInput is the maximum number of records returned by Apply.
	MaxRecordsPerInput = 4
)

var (
	ErrInvalidOptions = errors.New("invalid evidence options")
	ErrInvalidInput   = errors.New("invalid evidence input")
	ErrBackwardTime   = errors.New("evidence time moved backward")
	ErrRetired        = errors.New("evidence stream is retired")
	ErrUninitialized  = errors.New("evidence recorder must be constructed with New")
)

// StreamID identifies one attachment generation in the caller's namespace.
// AttemptID is a caller-supplied correlation reference, not retry permission.
type StreamID uint64
type AttemptID uint64
type EpisodeID uint64

type Options struct {
	HistoryLimit   int
	Lifetime       time.Duration
	RecordInterval time.Duration
	QuietPeriod    time.Duration
	ReopenDelay    time.Duration
}

func DefaultOptions() Options {
	return Options{HistoryLimit: 16, Lifetime: 30 * time.Second,
		RecordInterval: time.Second, QuietPeriod: time.Second, ReopenDelay: time.Second}
}

// Source describes provenance only; each optional field carries its own freshness.
type Source uint8

const (
	SourceUnknown Source = iota
	SourceCached
	SourceLive
)

// Sample is an owned value. Valid is supplied by the producer: the recorder does
// not validate ring geometry or infer whether pending work warrants recovery.
// Unknown consumed/work values must be zero and have their Fresh flag cleared.
type Sample struct {
	At              time.Duration
	Avail           uint16
	Used            uint16
	Consumed        uint16
	ConsumedFresh   bool
	WorkQueued      bool
	WorkQueuedFresh bool
	Source          Source
	Valid           bool
}

type ActionOutcome uint8

const (
	ActionAccepted ActionOutcome = iota + 1
	ActionRefused
	ActionFailed
	ActionCancelled
	ActionUnavailable
)

// VerificationOutcome is an externally established verdict. Neither value
// causes the recorder to check indices or start/change a verification deadline.
type VerificationOutcome uint8

const (
	VerificationProgress VerificationOutcome = iota + 1
	VerificationUnconfirmed
)

// Input is a closed set of the value types declared below. Apply type-checks
// inputs without invoking methods on user-supplied implementations. Pass values,
// not pointers. Embedding Input does not add a supported variant.
type Input interface{ evidenceInput() }

// Observation supplies one atomic state observation. When HasSample is true,
// Sample.At is the acquisition time and must not exceed At, the observation's
// presentation time. Both use the same monotonic epoch. A missing sample is not
// a negative candidate verdict.
// CandidateKnown distinguishes an explicit negative verdict from unavailable
// state. A positive verdict cancels quiet before this observation checks expiry.
type Observation struct {
	At                        time.Duration
	Sample                    Sample
	HasSample                 bool
	CandidateKnown, Candidate bool
}
type ActionObserved struct {
	At        time.Duration
	AttemptID AttemptID
	Outcome   ActionOutcome
}
type VerificationObserved struct {
	At        time.Duration
	AttemptID AttemptID
	Outcome   VerificationOutcome
}
type GapObserved struct {
	At   time.Duration
	Lost uint64
}
type TimeAdvanced struct{ At time.Duration }
type Retired struct{ At time.Duration }

func (Observation) evidenceInput()          {}
func (ActionObserved) evidenceInput()       {}
func (VerificationObserved) evidenceInput() {}
func (GapObserved) evidenceInput()          {}
func (TimeAdvanced) evidenceInput()         {}
func (Retired) evidenceInput()              {}

type RecordKind uint8

const (
	EpisodeOpened RecordKind = iota + 1
	EpisodeSnapshot
	ActionRecorded
	VerificationRecorded
	EpisodeClosed
	GapRecorded
	StreamRetired
)

type CloseReason uint8

const (
	CloseQuiet CloseReason = iota + 1
	CloseTimeout
	CloseGap
	CloseRetired
)

// ActionTotals counts input action facts within this episode. The producer owns
// exactly-once delivery; the recorder deliberately keeps no attempt dedup ledger.
type ActionTotals struct {
	Accepted    uint64
	Refused     uint64
	Failed      uint64
	Cancelled   uint64
	Unavailable uint64
}

// Record is detached from recorder state and every other returned record. The
// caller owns Samples and may retain or modify them. EpisodeID is local to the
// stream; zero means a record is not attributed to an episode. Verification
// records always use zero and correlate by AttemptID, including late verdicts.
// Progress is relative to the episode baseline, not evidence of write causation.
type Record struct {
	Kind             RecordKind
	StreamID         StreamID
	EpisodeID        EpisodeID
	At               time.Duration
	StartedAt        time.Duration
	Samples          []Sample
	Actions          ActionTotals
	UsedProgress     bool
	ConsumedProgress bool
	AttemptID        AttemptID
	Action           ActionOutcome
	Verification     VerificationOutcome
	CloseReason      CloseReason
	Incomplete       bool
	LostInputs       uint64
}

// Recorder owns one stream's bounded diagnostic state. Its zero value is not
// usable. The caller serializes Apply calls and supplies monotonic time.
type Recorder struct {
	options     Options
	stream      StreamID
	history     sampleHistory
	active      *episode
	sequence    EpisodeID
	lastAt      time.Duration
	closedAt    time.Duration
	haveClosed  bool
	initialized bool
	retired     bool
}

func New(options Options, streamID StreamID) (*Recorder, error) {
	if streamID == 0 || options.HistoryLimit < 1 || options.HistoryLimit > MaxHistoryLimit ||
		options.Lifetime <= 0 || options.RecordInterval <= 0 || options.QuietPeriod <= 0 || options.ReopenDelay < 0 {
		return nil, ErrInvalidOptions
	}
	return &Recorder{options: options, stream: streamID,
		history: newHistory(options.HistoryLimit), initialized: true}, nil
}

// Apply accepts one fact and returns at most MaxRecordsPerInput records. Invalid
// input, backward time and post-retirement input leave all state unchanged.
// Repeated retirement is idempotent. Time alone never opens or infers quiet.
func (r *Recorder) Apply(input Input) ([]Record, error) {
	if r == nil || !r.initialized {
		return nil, ErrUninitialized
	}
	at, err := validateInput(input)
	if err != nil {
		return nil, err
	}
	if at < r.lastAt {
		return nil, ErrBackwardTime
	}
	if in, ok := input.(Observation); ok && in.HasSample {
		if last, exists := r.history.latest(); exists && in.Sample.At < last.At {
			return nil, ErrBackwardTime
		}
	}
	if r.retired {
		if _, ok := input.(Retired); ok {
			return nil, nil
		}
		return nil, ErrRetired
	}
	r.lastAt = at
	var records []Record
	// Explicit loss/retirement take precedence over inferred time expiry.
	switch in := input.(type) {
	case GapObserved:
		r.close(at, CloseGap, &records)
		r.history.clear()
		r.haveClosed = false // A new known candidate may begin immediately.
		records = append(records, Record{Kind: GapRecorded, StreamID: r.stream,
			At: at, Incomplete: true, LostInputs: in.Lost})
		return records, nil
	case Retired:
		r.close(at, CloseRetired, &records)
		r.history.clear()
		r.retired = true
		records = append(records, Record{Kind: StreamRetired, StreamID: r.stream, At: at})
		return records, nil
	}
	// A current positive candidate cancels quiet immediately, but cannot extend
	// an episode beyond its lifetime. Deadlines are checked when inputs arrive.
	observation, observing := input.(Observation)
	r.expire(at, observing && observation.CandidateKnown && observation.Candidate, &records)
	switch in := input.(type) {
	case Observation:
		if in.HasSample {
			r.history.add(in.Sample)
			if r.active != nil {
				r.active.observe(in.Sample)
			}
		}
		if in.CandidateKnown && in.Candidate {
			if r.active != nil {
				r.active.quiet = false
			} else {
				r.open(at, &records)
			}
		} else if in.CandidateKnown && r.active != nil && !r.active.quiet {
			r.active.quiet, r.active.quietSince = true, at
		}
		if in.HasSample && r.active != nil && at-r.active.lastRecord >= r.options.RecordInterval {
			records = append(records, r.episodeRecord(EpisodeSnapshot, at, false))
		}
	case ActionObserved:
		if r.active == nil {
			records = append(records, Record{Kind: ActionRecorded, StreamID: r.stream,
				At: at, AttemptID: in.AttemptID, Action: in.Outcome})
		} else {
			r.active.action(in)
			if !r.active.actionReported || at-r.active.lastRecord >= r.options.RecordInterval {
				r.active.actionReported = true
				records = append(records, r.episodeRecord(ActionRecorded, at, false))
			}
		}
	case VerificationObserved:
		// No attribution to whichever episode happens to be open now: the
		// originating action can belong to an older, already closed episode.
		records = append(records, Record{Kind: VerificationRecorded, StreamID: r.stream,
			At: at, AttemptID: in.AttemptID, Verification: in.Outcome})
	case TimeAdvanced:
	}
	return records, nil
}

func validateInput(input Input) (time.Duration, error) {
	var at time.Duration
	switch in := input.(type) {
	case Observation:
		at = in.At
		if !in.CandidateKnown && in.Candidate || !in.HasSample && in.Sample != (Sample{}) {
			return 0, ErrInvalidInput
		}
		if in.HasSample && (in.Sample.At < 0 || in.Sample.At > at || in.Sample.Source > SourceLive ||
			!in.Sample.ConsumedFresh && in.Sample.Consumed != 0 || !in.Sample.WorkQueuedFresh && in.Sample.WorkQueued) {
			return 0, ErrInvalidInput
		}
	case ActionObserved:
		at = in.At
		if in.AttemptID == 0 || in.Outcome < ActionAccepted || in.Outcome > ActionUnavailable {
			return 0, ErrInvalidInput
		}
	case VerificationObserved:
		at = in.At
		if in.AttemptID == 0 || in.Outcome < VerificationProgress || in.Outcome > VerificationUnconfirmed {
			return 0, ErrInvalidInput
		}
	case GapObserved:
		at = in.At
		if in.Lost == 0 {
			return 0, ErrInvalidInput
		}
	case TimeAdvanced:
		at = in.At
	case Retired:
		at = in.At
	default:
		return 0, ErrInvalidInput
	}
	if at < 0 {
		return 0, ErrInvalidInput
	}
	return at, nil
}
