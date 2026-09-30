package watch

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	EpisodeSnapshotLimit  = 16
	EpisodeLifetime       = 30.0
	EpisodeRecordInterval = 1.0
	EpisodeQuietPeriod    = 1.0
	EpisodeReopenDelay    = 1.0
)

type EpisodeSource string

const (
	EpisodeSourceUser    EpisodeSource = "cached_user_indices"
	EpisodeSourceLive    EpisodeSource = "live_snapshot"
	EpisodeSourceUnknown EpisodeSource = "unknown"
)

// EpisodeSample deliberately contains no kernel addresses or queue identity.
// Consumed and WorkQueued are meaningful only when their Fresh field is true.
// WorkQueued=false does not establish that a vhost worker is idle.
type EpisodeSample struct {
	At              float64       `json:"at"`
	Avail           uint16        `json:"avail"`
	Used            uint16        `json:"used"`
	Source          EpisodeSource `json:"source"`
	Consumed        uint16        `json:"consumed"`
	ConsumedFresh   bool          `json:"consumed_fresh"`
	WorkQueued      bool          `json:"work_queued"`
	WorkQueuedFresh bool          `json:"work_queued_fresh"`
	Invalid         bool          `json:"invalid,omitempty"`
}

type EpisodeReason string

const (
	EpisodeReasonCandidate      EpisodeReason = "pending_no_used_progress"
	EpisodeReasonLivePending    EpisodeReason = "live_pending"
	EpisodeReasonNoPending      EpisodeReason = "no_pending"
	EpisodeReasonUnconsumed     EpisodeReason = "unconsumed"
	EpisodeReasonUsedProgress   EpisodeReason = "used_progress"
	EpisodeReasonDrained        EpisodeReason = "drained"
	EpisodeReasonBusy           EpisodeReason = "work_queued"
	EpisodeReasonInvalidRing    EpisodeReason = "invalid_ring"
	EpisodeReasonIdentityChange EpisodeReason = "identity_change"
	EpisodeReasonUnavailable    EpisodeReason = "unavailable"
	EpisodeReasonCancelled      EpisodeReason = "cancelled"
	EpisodeReasonWriteFailed    EpisodeReason = "write_failed"
	EpisodeReasonNone           EpisodeReason = "none"
	EpisodeReasonUnknown        EpisodeReason = "unknown"
)

type EpisodeWrite string

const (
	EpisodeWriteSuccess EpisodeWrite = "success"
	EpisodeWriteError   EpisodeWrite = "error"
	EpisodeWriteRefused EpisodeWrite = "refused"
)

type EpisodeOutcome string

const (
	EpisodeOutcomeConsumed       EpisodeOutcome = "consumed_progress"
	EpisodeOutcomeUsed           EpisodeOutcome = "used_progress"
	EpisodeOutcomeBoth           EpisodeOutcome = "both_progress"
	EpisodeOutcomeQuiet          EpisodeOutcome = "quiet"
	EpisodeOutcomeTimeout        EpisodeOutcome = "timeout"
	EpisodeOutcomeIdentityChange EpisodeOutcome = "identity_change"
	EpisodeOutcomeUnavailable    EpisodeOutcome = "unavailable"
	EpisodeOutcomeInvalidRing    EpisodeOutcome = "invalid_ring"
	EpisodeOutcomeStopped        EpisodeOutcome = "stopped"
)

var (
	episodePrefix = func() string {
		var prefix [12]byte
		if _, err := rand.Read(prefix[:]); err != nil {
			panic(err)
		}
		return hex.EncodeToString(prefix[:])
	}()
	episodeSequence atomic.Uint64
	episodeClock    = time.Now()
)

type candidateEpisode struct {
	id                                 string
	reason                             EpisodeReason
	started, lastRecord, quietSince    float64
	quiet                              bool
	current                            EpisodeSample
	used, consumed                     uint16
	haveConsumed                       bool
	usedProgress, consumedProgress     bool
	writes, writeErrors, writeRefusals uint64
	firstWriteAt                       float64
	lastWrite                          EpisodeWrite
	lastWriteReason                    EpisodeReason
	lastDecision                       EpisodeReason
	writeReported                      bool
}

// EpisodeJournal is a bounded diagnostic for one queue, independent of the
// recovery policy. Calls for a queue must be serialized by its existing loop.
// The callback should emit a single JSON record; the journal starts no
// goroutines and performs no disk I/O. Callback fields/slices are owned by that
// record and will not be mutated by later polling.
type EpisodeJournal struct {
	emit        func(string, map[string]any)
	now         func() float64
	ring        [EpisodeSnapshotLimit]EpisodeSample
	next, count int
	active      *candidateEpisode
	nextOpen    float64
}

func NewEpisodeJournal(emit func(string, map[string]any), now func() float64) *EpisodeJournal {
	if now == nil {
		now = func() float64 { return time.Since(episodeClock).Seconds() }
	}
	if emit == nil {
		emit = func(string, map[string]any) {}
	}
	return &EpisodeJournal{emit: emit, now: now}
}

func (j *EpisodeJournal) Active() bool { return j.active != nil }

func (j *EpisodeJournal) EventID() string {
	if j.active == nil {
		return ""
	}
	return j.active.id
}

// Observe saves the existing poll sample even when there is no candidate.
// candidate comes from the shared detector, never from WorkQueued. A new
// candidate cancels quiet hysteresis immediately; journaling never gates a kick.
func (j *EpisodeJournal) Observe(sample EpisodeSample, candidate bool) {
	if sample.Source != EpisodeSourceUser && sample.Source != EpisodeSourceLive {
		sample.Source = EpisodeSourceUnknown
	}
	j.ring[j.next] = sample
	j.next = (j.next + 1) % len(j.ring)
	j.count = min(j.count+1, len(j.ring))
	e := j.active
	if e == nil {
		return
	}
	e.current = sample
	e.usedProgress = e.usedProgress || (!sample.Invalid && sample.Used != e.used)
	if sample.ConsumedFresh && !sample.Invalid {
		if e.haveConsumed {
			e.consumedProgress = e.consumedProgress || sample.Consumed != e.consumed
		} else {
			e.consumed, e.haveConsumed = sample.Consumed, true
		}
	}
	now := j.now()
	if now-e.started >= EpisodeLifetime {
		j.Close(EpisodeOutcomeTimeout)
		return
	}
	if candidate {
		e.quiet = false
	} else if !e.quiet {
		e.quietSince, e.quiet = now, true
	} else if now-e.quietSince >= EpisodeQuietPeriod {
		j.Close(e.progressOutcome())
		return
	}
	if now-e.lastRecord >= EpisodeRecordInterval {
		j.record("episode_snapshot", now, []EpisodeSample{sample})
	}
}

// Open correlates pre/current samples with all later records. One active
// episode per queue and a short reopen delay bound a persistent candidate's
// output, without suppressing recovery after the diagnostic timeout.
func (j *EpisodeJournal) Open(reason EpisodeReason) bool {
	return j.open(reason, false)
}

// OpenForWrite lets a new verification's imminent first write retain an active
// origin record during reopen hysteresis. Only the paced recovery writer may
// use this exception; observation and repeated pending verification use Open.
func (j *EpisodeJournal) OpenForWrite(reason EpisodeReason) bool {
	return j.open(reason, true)
}

func (j *EpisodeJournal) open(reason EpisodeReason, firstWrite bool) bool {
	now := j.now()
	if j.active != nil || j.count == 0 || (!firstWrite && now < j.nextOpen) {
		return false
	}
	current := j.ring[(j.next+len(j.ring)-1)%len(j.ring)]
	j.active = &candidateEpisode{
		id:     episodePrefix + "-" + strconv.FormatUint(episodeSequence.Add(1), 16),
		reason: boundedEpisodeReason(reason), started: now, lastRecord: now,
		current: current, used: current.Used,
		consumed: current.Consumed, haveConsumed: current.ConsumedFresh,
		lastWriteReason: EpisodeReasonNone,
		lastDecision:    EpisodeReasonCandidate,
	}
	snapshots := make([]EpisodeSample, j.count)
	for i := range snapshots {
		snapshots[i] = j.ring[(j.next-j.count+i+len(j.ring))%len(j.ring)]
	}
	j.record("candidate", now, snapshots)
	return true
}

// RecordDecision retains the latest fixed live/cached decision for the next
// bounded record. It never adds a record or delays the caller's recovery work.
func (j *EpisodeJournal) RecordDecision(reason EpisodeReason) {
	if j.active != nil {
		j.active.lastDecision = boundedEpisodeReason(reason)
	}
}

// RecordWrite keeps totals for every attempt. The first write is reported
// immediately; later writes and snapshots share the one-record/second limit.
// Closing always includes the latest sample and totals, including suppressed
// records. A successful write and later progress are observations, not proof
// that a notification was lost or that the write caused the progress.
func (j *EpisodeJournal) RecordWrite(result EpisodeWrite, reason EpisodeReason) {
	e := j.active
	if e == nil {
		return
	}
	now := j.now()
	switch result {
	case EpisodeWriteSuccess:
		if e.writes == 0 {
			e.firstWriteAt = now
		}
		e.writes++
	case EpisodeWriteError:
		e.writeErrors++
	case EpisodeWriteRefused:
		e.writeRefusals++
	default:
		return
	}
	e.lastWrite, e.lastWriteReason = result, boundedEpisodeReason(reason)
	if !e.writeReported || now-e.lastRecord >= EpisodeRecordInterval {
		e.writeReported = true
		j.record("episode_write", now, []EpisodeSample{e.current})
	}
}

// Tick checks the deadline without pretending an unavailable poll is quiet.
// No independent timer is required; call it on failed/unavailable polls too.
func (j *EpisodeJournal) Tick() {
	if j.active != nil && j.now()-j.active.started >= EpisodeLifetime {
		j.Close(EpisodeOutcomeTimeout)
	}
}

// Close emits the latest sample even inside the rate-limit interval. Identity
// changes discard the old identity's pre-history before another episode opens.
func (j *EpisodeJournal) Close(outcome EpisodeOutcome) {
	if outcome == EpisodeOutcomeIdentityChange || outcome == EpisodeOutcomeStopped {
		j.next, j.count = 0, 0
	}
	e := j.active
	if e == nil {
		return
	}
	switch outcome {
	case EpisodeOutcomeConsumed, EpisodeOutcomeUsed, EpisodeOutcomeBoth,
		EpisodeOutcomeQuiet, EpisodeOutcomeTimeout, EpisodeOutcomeIdentityChange,
		EpisodeOutcomeUnavailable, EpisodeOutcomeInvalidRing, EpisodeOutcomeStopped:
	default:
		outcome = EpisodeOutcomeUnavailable
	}
	now := j.now()
	fields := j.fields(now, []EpisodeSample{e.current})
	fields["outcome"] = outcome
	j.emit("episode_closed", fields)
	j.active, j.nextOpen = nil, now+EpisodeReopenDelay
}

func (j *EpisodeJournal) record(event string, now float64, samples []EpisodeSample) {
	j.active.lastRecord = now
	j.emit(event, j.fields(now, samples))
}

func (j *EpisodeJournal) fields(now float64, samples []EpisodeSample) map[string]any {
	e := j.active
	fields := map[string]any{
		"event_id": e.id, "reason": e.reason, "age": max(0, now-e.started),
		"snapshots": samples,
		// Progress is relative to the candidate baseline and may predate a
		// write. after_write describes only the current sample's chronology.
		"used_progress": e.usedProgress, "consumption_progress": e.consumedProgress,
		"write_accepted":  e.writes != 0,
		"after_write":     e.writes != 0 && e.current.At > e.firstWriteAt,
		"write_successes": e.writes, "write_errors": e.writeErrors,
		"write_refusals": e.writeRefusals, "last_write": e.lastWrite,
		"last_write_reason": e.lastWriteReason,
		"decision":          e.lastDecision,
	}
	if e.writes != 0 {
		fields["first_write_at"] = e.firstWriteAt
	}
	return fields
}

func (e *candidateEpisode) progressOutcome() EpisodeOutcome {
	switch {
	case e.usedProgress && e.consumedProgress:
		return EpisodeOutcomeBoth
	case e.usedProgress:
		return EpisodeOutcomeUsed
	case e.consumedProgress:
		return EpisodeOutcomeConsumed
	default:
		return EpisodeOutcomeQuiet
	}
}

func boundedEpisodeReason(reason EpisodeReason) EpisodeReason {
	switch reason {
	case EpisodeReasonCandidate, EpisodeReasonLivePending, EpisodeReasonNoPending,
		EpisodeReasonUnconsumed, EpisodeReasonUsedProgress, EpisodeReasonDrained,
		EpisodeReasonBusy, EpisodeReasonInvalidRing, EpisodeReasonIdentityChange,
		EpisodeReasonUnavailable, EpisodeReasonCancelled, EpisodeReasonWriteFailed,
		EpisodeReasonNone:
		return reason
	default:
		return EpisodeReasonUnknown
	}
}
