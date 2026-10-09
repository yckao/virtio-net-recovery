// Package report owns presentation values shared by application reporting ports.
// They contain no policy, backend or evidence implementation types.
package report

import "time"

const (
	MaxSamples      = 256
	MaxNameBytes    = 128
	MaxMessageBytes = 256
)

type Kind string

const (
	Candidate       Kind = "candidate"
	EpisodeSnapshot Kind = "episode_snapshot"
	EpisodeAction   Kind = "episode_action"
	EpisodeClosed   Kind = "episode_closed"
	Gap             Kind = "evidence_gap"
	Retired         Kind = "stream_retired"
	Action          Kind = "action"
	Decision        Kind = "decision"
	Verification    Kind = "verification"
	Status          Kind = "status"
	Manual          Kind = "manual_result"
	Trace           Kind = "trace_sample"
)

type Sample struct {
	At                                                time.Duration
	Avail, Used, Consumed                             uint16
	ConsumedFresh, WorkQueued, WorkQueuedFresh, Valid bool
	Source                                            string
}

type ActionTotals struct {
	Accepted, Refused, Failed, Cancelled, Unavailable uint64
}

type Stages struct{ Signals, Writes, Wakeups, Handlers uint64 }

// Record is an explicit presentation projection. Producers bound all strings
// and sample counts; sinks validate these bounds before encoding. Samples are
// borrowed only for the synchronous duration of TryWrite.
type Record struct {
	Kind                                       Kind
	Stream, Sequence, Episode, Attempt         uint64
	PID                                        int
	Name                                       string
	StartTime                                  uint64
	Slot                                       int
	Generation                                 uint64
	At, StartedAt, Latency                     time.Duration
	Samples                                    []Sample
	Actions                                    ActionTotals
	Stages                                     Stages
	Outcome, Reason, Message                   string
	UsedProgress, ConsumedProgress, Incomplete bool
	LostInputs                                 uint64
}

// Sink admits bounded records without waiting for a destination. Implementations
// must not retain borrowed slices or call an external writer before returning.
type Sink interface{ TryWrite(Record) bool }
