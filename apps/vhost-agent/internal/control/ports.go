// Package control owns application use cases. All side effects enter through
// consumer-owned ports; it imports no Linux, discovery, evidence or output adapter.
package control

import (
	"context"
	"time"
)

type Target struct {
	PID       int
	StartTime uint64
	Name      string
}

func (t Target) Same(other Target) bool { return t.PID == other.PID && t.StartTime == other.StartTime }

type Queue struct {
	Slot       int
	Generation uint64
}
type Inventory struct {
	Queues      []Queue
	Unavailable []int
	Complete    bool
}
type Sample struct {
	Queue                 Queue
	At                    time.Time
	Num                   uint32
	Avail, Used, Consumed uint16
	Live, WorkQueued      bool
}
type Decision uint8

const (
	DecisionUnknown Decision = iota
	Pending
	UsedProgress
	Drained
	Queued
	Consumed
	Invalid
	Changed
	Unavailable
	Cancelled
	Busy
)

type Outcome uint8

const (
	Observed Outcome = iota
	Accepted
	Refused
	ReadFailed
	WriteFailed
	Aborted
)

type Result struct {
	Outcome                 Outcome
	Decision                Decision
	CompletedAt, AcceptedAt time.Time
	Live                    *Sample
	Err                     error
}

// Reader exposes observation only. A Writer is supplied only to mutating use cases.
type Reader interface {
	Inventory(context.Context) (Inventory, error)
	Sample(context.Context, []Queue) ([]Sample, error)
	Inspect(context.Context, Queue, uint16) Result
	Close() error
}
type Writer interface {
	Notify(context.Context, Queue, uint16) Result
}
type ManualWriter interface {
	Kick(context.Context, Queue) Result
}
type TraceSample struct {
	Sample                             Sample
	Signals, Writes, Wakeups, Handlers uint64
}
type Tracer interface {
	Read(context.Context, []Queue) ([]TraceSample, error)
	Close() error
}

type EventKind uint8

const (
	EventDecision EventKind = iota
	EventAction
	EventVerified
	EventUnconfirmed
)

type Event struct {
	Kind      EventKind
	Outcome   Outcome
	Decision  Decision
	AttemptID uint64
	Latency   time.Duration
}

// Frame is a bounded application projection, not a transport or component DTO.
// No kernel handles or policy/evidence objects cross this port.
type Frame struct {
	Stream, Sequence                              uint64
	Target                                        Target
	Queue                                         Queue
	At                                            time.Duration
	Sample                                        Sample
	HasSample, CandidateKnown, Candidate, Retired bool
	Events                                        [6]Event
	EventCount                                    int
}
type Reporter interface{ TryOffer(Frame) bool }
type Clock interface {
	Now() time.Time
	Wait(context.Context, time.Duration) error
}
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
