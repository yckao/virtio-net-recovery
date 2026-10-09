// Package vhost observes Linux vhost-net TX queues and provides generation-bound,
// verified notification capabilities. Observation alone grants no write operation.
package vhost

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnsupported     = errors.New("unsupported platform or kernel capability")
	ErrClosed          = errors.New("session closed")
	ErrIdentityChanged = errors.New("process or queue generation changed")
	ErrQueueLimit      = errors.New("queue capacity exceeded")
	ErrBusy            = errors.New("another mutation owns this process")
)

// Options describes one shared BPF owner. MaxQueues is a host-wide bound.
// BPFObject must be the matching observation or trace object from this release.
// StateDir coordinates cooperating writers; it is not a security boundary.
type Options struct {
	BPFObject string
	Trace     bool
	StateDir  string
	MaxQueues int
}

func (o Options) validate() error {
	if o.BPFObject == "" || o.StateDir == "" {
		return errors.New("BPF object and mutation state directory are required")
	}
	if o.MaxQueues < 1 || o.MaxQueues > 4096 {
		return errors.New("max queues must be 1..4096")
	}
	return nil
}

// ProcessIdentity is the expected /proc process generation in this PID namespace.
type ProcessIdentity struct {
	PID       int
	StartTime uint64
}

// Queue is an opaque, comparable handle. A zero value or a handle from a different
// process session is invalid. Slot is diagnostic, never authority to reconstruct it.
type queueOwner struct{ token [16]byte }
type Queue struct {
	owner      *queueOwner
	generation uint64
	slot       int
}

func (q Queue) Slot() int          { return q.slot }
func (q Queue) Generation() uint64 { return q.generation }
func (q Queue) String() string {
	if q.owner == nil {
		return "queue:invalid"
	}
	return fmt.Sprintf("queue:%x:%d:%d", q.owner.token, q.slot, q.generation)
}

type QueueProblem struct {
	Slot int
	Err  error
}

// Inventory is complete only when Complete is true. Problems retain slots that
// were found but could not be observed; their absence from Queues is not removal.
// Truncated means a queue, scan, or result capacity limit prevented full coverage.
// Queues and Problems together contain at most Options.MaxQueues entries. Known
// queues take precedence over new slots. Truncated always implies !Complete.
type Inventory struct {
	Queues    []Queue
	Problems  []QueueProblem
	Complete  bool
	Truncated bool
}

type Source string

const (
	SourceCached Source = "cached_indices"
	SourceLive   Source = "live_attachment"
)

type Observation struct {
	Queue                 Queue
	At                    time.Time
	Source                Source
	Live                  bool
	Num                   uint32
	Avail, Used, Consumed uint16
	Outstanding, Pending  uint16
	WorkQueued            bool
}

type PhysicalDecision string

const (
	Eligible        PhysicalDecision = "eligible"
	InvalidRing     PhysicalDecision = "invalid_ring"
	UsedAdvanced    PhysicalDecision = "used_advanced"
	Drained         PhysicalDecision = "drained"
	WorkQueued      PhysicalDecision = "work_queued"
	NoPending       PhysicalDecision = "no_pending"
	IdentityChanged PhysicalDecision = "identity_changed"
	Unavailable     PhysicalDecision = "unavailable"
	Unsupported     PhysicalDecision = "unsupported"
	Cancelled       PhysicalDecision = "cancelled"
	Contended       PhysicalDecision = "contended"
)

// Classify uses only fresh physical evidence. It neither chooses cadence nor
// infers a lost notification. Queue progress is not an atomic kernel snapshot.
func Classify(o Observation, expectedUsed uint16) PhysicalDecision {
	if !o.Live || o.Source != SourceLive {
		return Unavailable
	}
	if o.Num < 1 || o.Num > 32768 || o.Num&(o.Num-1) != 0 || uint32(o.Pending) > o.Num || uint32(o.Outstanding) > o.Num {
		return InvalidRing
	}
	if o.Used != expectedUsed {
		return UsedAdvanced
	}
	if o.Outstanding == 0 {
		return Drained
	}
	if o.WorkQueued {
		return WorkQueued
	}
	if o.Pending == 0 {
		return NoPending
	}
	return Eligible
}

type WriteOutcome string

const (
	WriteNotAttempted WriteOutcome = "not_attempted"
	WriteFailed       WriteOutcome = "attempted_failed"
	WriteAccepted     WriteOutcome = "accepted"
)

type WriteReceipt struct {
	Queue          Queue
	AcceptedAt     time.Time
	ObservationAt  time.Time
	HasBaseline    bool
	Used, Consumed uint16
}

// Result never changes an accepted write into an error because of later reporting
// or cancellation. CompletedAt is set on every outcome for completion-based pacing.
type Result struct {
	Decision    PhysicalDecision
	Write       WriteOutcome
	CompletedAt time.Time
	Observation *Observation
	Receipt     *WriteReceipt
	Err         error
}

type ReadSession interface {
	Identity() ProcessIdentity
	Inventory(context.Context) (Inventory, error)
	Sample(context.Context, []Queue) ([]Observation, error)
	Inspect(context.Context, Queue) (Observation, error)
	Close() error
}
type ConditionalNotifier interface {
	Notify(context.Context, Queue, uint16) Result
}
type ManualNotifier interface {
	Notify(context.Context, Queue) Result
}

type Counters struct {
	Signals, Writes, Wakeups, Handlers                     uint64
	LastSignalNS, LastWriteNS, LastWakeupNS, LastHandlerNS uint64
	Active                                                 uint64
}
type TraceObservation struct {
	Observation Observation
	Counters    Counters
}
type TraceSession interface {
	Sample(context.Context, []Queue) ([]TraceObservation, error)
	Close() error
}
