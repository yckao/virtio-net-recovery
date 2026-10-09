// Package recovery implements a serial, deterministic policy for one queue slot.
// It requests observations and conditional notifications; it never performs I/O.
package recovery

import "time"

type Mode uint8

const (
	Observe Mode = iota + 1
	Recover
)

// Config is copied by New. The cadence controls observation aging and completed
// operation spacing. VerificationTimeout only produces a diagnostic fact.
type Config struct {
	Mode                Mode
	Cadence             time.Duration
	VerificationTimeout time.Duration
}

// CachedObservation contains only indices available without a live inspection.
// At is monotonic elapsed time from the caller's run epoch.
type CachedObservation struct {
	Generation  uint64
	At          time.Duration
	Avail, Used uint16
	Size        uint32
}

// LiveObservation is progress from a backend-validated live attachment. Valid
// must be false for incomplete, unsupported or invalid observations. The core
// does not implement the backend's physical notification eligibility checks.
type LiveObservation struct {
	Generation     uint64
	At             time.Duration
	Used, Consumed uint16
	Valid          bool
}

type OperationKind uint8

const (
	Inspect OperationKind = iota + 1
	Notify
)

// Operation is an advisory request, never a capability or permission to write.
// A notifier must revalidate physical eligibility, attachment and process
// identity. The caller completes each issued operation exactly once.
type Operation struct {
	ID, AttemptID, Generation     uint64
	Kind                          OperationKind
	StartedAt                     time.Duration
	ExpectedUsed                  uint16
	ForCandidate, ForVerification bool
}

type Outcome string

const (
	Observed    Outcome = "observed"
	Accepted    Outcome = "accepted"
	Refused     Outcome = "refused"
	Unavailable Outcome = "unavailable"
	Cancelled   Outcome = "cancelled"
	WriteFailed Outcome = "write_failed"
)

// Reason is the backend's semantic classification, not an error message.
type Reason string

const (
	ReasonNone            Reason = ""
	ReasonPending         Reason = "pending"
	ReasonUsedProgress    Reason = "used_progress"
	ReasonDrained         Reason = "drained"
	ReasonWorkQueued      Reason = "work_queued"
	ReasonConsumed        Reason = "consumed"
	ReasonInvalidRing     Reason = "invalid_ring"
	ReasonIdentityChanged Reason = "identity_changed"
	ReasonUnsupported     Reason = "unsupported"
	ReasonContended       Reason = "contended"
	ReasonUnavailable     Reason = "unavailable"
	ReasonCancelled       Reason = "cancelled"
	ReasonWriteFailed     Reason = "write_failed"
)

// OperationResult preserves the actual operation outcome independently of any
// later reporting failure. An accepted automatic write requires its validated
// pre-write Live baseline and the time at which the write was accepted.
// Live, when provided, is copied; the policy retains no caller-owned pointers.
// Unavailable may carry ReasonIdentityChanged or ReasonUnsupported when the
// backend cannot return a live sample. Identity change discards verification.
// An unavailable outcome cannot contain a valid live observation.
type OperationResult struct {
	ID, Generation uint64
	CompletedAt    time.Duration
	Outcome        Outcome
	Reason         Reason
	Live           *LiveObservation
	AcceptedAt     time.Duration
}

type FactKind string

const (
	CandidateDetected     FactKind = "candidate_detected"
	CandidateCleared      FactKind = "candidate_cleared"
	ObservationRejected   FactKind = "observation_rejected"
	OperationStarted      FactKind = "operation_started"
	OperationCompleted    FactKind = "operation_completed"
	VerificationStarted   FactKind = "verification_started"
	VerificationConfirmed FactKind = "verification_confirmed"
	VerificationOverdue   FactKind = "verification_overdue"
	VerificationDiscarded FactKind = "verification_discarded"
	GenerationChanged     FactKind = "generation_changed"
	PolicyStopped         FactKind = "stopped"
)

// Fact is an immutable value describing a transition. At is accounting time;
// ObservedAt and AcceptedAt retain the distinct physical observation/write
// times. Unused fields are zero. Verification facts identify OriginAttemptID.
type Fact struct {
	Kind                       FactKind
	At, ObservedAt, AcceptedAt time.Duration
	Generation                 uint64
	OperationID, AttemptID     uint64
	OriginAttemptID            uint64
	Outcome                    Outcome
	Reason                     Reason
	Used, Consumed             uint16
	Latency                    time.Duration
}

// MaxFactsPerUpdate bounds memory per transition. No history is retained.
const MaxFactsPerUpdate = 6

// Update owns its returned facts and operation. Mutating these values does not
// mutate the policy. An empty Operation means no external work is requested.
type Update struct {
	Candidate bool
	Operation *Operation
	Facts     []Fact
}

// Totals count automatic attempts only, except Inspections. Outcome counters
// partition completed attempts. Accepted writes do not establish causation.
type Totals struct {
	Attempts, Writes, Refusals, Unavailable uint64
	WriteFailures, Cancellations            uint64
	Inspections, Verified                   uint64
}

// Status is a value snapshot. A stopped policy may retain unresolved
// verification for its final status; it can no longer perform any operation.
type Status struct {
	Generation                   uint64
	Candidate, InFlight, Stopped bool
	Operation                    Operation
	VerificationPending          bool
	OriginAttemptID              uint64
	VerificationStartedAt        time.Duration
	VerificationTimedOut         bool
	HasCompletedAttempt          bool
	LastAttemptCompletedAt       time.Duration
	Totals                       Totals
}
