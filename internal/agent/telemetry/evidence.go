package telemetry

import (
	"time"

	"github.com/yckao/virtio-net-recovery/evidence"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

type stream struct {
	recorder   *evidence.Recorder
	sequence   uint64
	at         time.Duration
	sampleAt   time.Duration
	haveSample bool
}

func (t *Telemetry) newStream(s *scope) *stream {
	options := evidence.DefaultOptions()
	options.HistoryLimit = 16
	r, err := evidence.New(options, evidence.StreamID(s.id))
	if err != nil {
		t.recorderErrors.Add(1)
		value := base(s, 0, 0)
		value.Event, value.Incomplete, value.Message = "status", true, "evidence recorder could not be created"
		t.emit(value)
	}
	return &stream{recorder: r}
}

func (t *Telemetry) observe(s *scope, state *stream, in input) {
	f := in.frame
	// Product facts remain reportable if the optional recorder is disabled.
	for _, e := range f.Events[:f.EventCount] {
		r := base(s, in.sequence, f.At)
		r.Attempt, r.Latency, r.ObservedAt, r.AcceptedAt = e.AttemptID, e.Latency, e.ObservedAt, e.AcceptedAt
		r.Reason, r.Message = decisionName(e.Decision), e.Error
		switch e.Kind {
		case control.EventAction:
			r.Event, r.Outcome = "action", outcomeName(e.Outcome)
		case control.EventDecision:
			r.Event = "decision"
		case control.EventVerified:
			r.Event, r.Outcome = "verification", "progress"
		case control.EventUnconfirmed:
			r.Event, r.Outcome = "verification", "unconfirmed"
		default:
			continue
		}
		t.emit(r)
	}
	if f.At < state.at {
		t.disable(s, state)
		state.sequence = in.sequence
		return
	}
	missing := in.sequence - state.sequence - 1
	state.sequence = in.sequence
	if missing > 0 {
		t.apply(s, state, evidence.GapObserved{At: f.At, Lost: missing})
		state.haveSample = false
	}
	state.sequence, state.at = in.sequence, f.At
	observation := evidence.Observation{At: f.At, CandidateKnown: f.CandidateKnown, Candidate: f.Candidate}
	if f.HasSample && (!state.haveSample || f.SampleAt > state.sampleAt) {
		observation.HasSample = true
		observation.Sample = evidence.Sample{At: f.SampleAt, Avail: f.Sample.Avail, Used: f.Sample.Used,
			Valid: f.SampleValid, Source: evidence.SourceCached}
		if f.Sample.Live {
			observation.Sample.Source = evidence.SourceLive
			observation.Sample.Consumed, observation.Sample.ConsumedFresh = f.Sample.Consumed, true
			observation.Sample.WorkQueued, observation.Sample.WorkQueuedFresh = f.Sample.WorkQueued, true
		}
		state.haveSample, state.sampleAt = true, f.SampleAt
	}
	t.apply(s, state, observation)
	// Verification facts are projected directly above. They do not change
	// episode history or totals, so the reducer need not repeat that projection.
	for _, event := range f.Events[:f.EventCount] {
		if event.Kind == control.EventAction && event.AttemptID != 0 {
			if outcome, ok := actionOutcome(event.Outcome); ok {
				t.apply(s, state, evidence.ActionObserved{At: f.At, AttemptID: evidence.AttemptID(event.AttemptID), Outcome: outcome})
			}
		}
	}
}

func (t *Telemetry) apply(s *scope, state *stream, in evidence.Input) {
	if state.recorder == nil {
		return
	}
	records, err := state.recorder.Apply(in)
	if err != nil {
		t.disable(s, state)
		return
	}
	for _, e := range records {
		t.emit(fromEvidence(s, state.sequence, e))
	}
}

func (t *Telemetry) disable(s *scope, state *stream) {
	if state.recorder == nil {
		return
	}
	state.recorder = nil
	t.recorderErrors.Add(1)
	r := base(s, state.sequence, state.at)
	r.Event, r.Incomplete, r.Message = "status", true, "evidence disabled after invalid observation"
	t.emit(r)
}

func (t *Telemetry) retire(states map[*scope]*stream, s *scope, state *stream) {
	at := state.at
	if s.closed.Load() {
		at = max(at, s.closeAt)
		// Trailing rejected frames have no later input to expose their gap.
		if s.sequence > state.sequence {
			t.apply(s, state, evidence.GapObserved{At: at, Lost: s.sequence - state.sequence})
		}
	}
	t.apply(s, state, evidence.Retired{At: at})
	delete(states, s)
	t.streamCount.Add(-1)
}

func base(s *scope, sequence uint64, at time.Duration) record {
	return record{Stream: s.id, Sequence: sequence, PID: s.target.PID, StartTime: s.target.StartTime,
		Name: s.target.Name, Slot: s.queue.Slot, Generation: s.queue.Generation, At: at}
}

func fromEvidence(s *scope, sequence uint64, e evidence.Record) record {
	r := base(s, sequence, e.At)
	r.Episode, r.Attempt, r.StartedAt = uint64(e.EpisodeID), uint64(e.AttemptID), e.StartedAt
	r.UsedProgress, r.ConsumedProgress, r.Incomplete, r.LostInputs = e.UsedProgress, e.ConsumedProgress, e.Incomplete, e.LostInputs
	r.Actions = totals{e.Actions.Accepted, e.Actions.Refused, e.Actions.Failed, e.Actions.Cancelled, e.Actions.Unavailable}
	switch e.Kind {
	case evidence.EpisodeOpened:
		r.Event = "candidate"
	case evidence.EpisodeSnapshot:
		r.Event = "episode_snapshot"
	case evidence.ActionRecorded:
		r.Event = "episode_action"
	case evidence.EpisodeClosed:
		r.Event = "episode_closed"
		switch e.CloseReason {
		case evidence.CloseQuiet:
			r.Reason = "quiet"
		case evidence.CloseTimeout:
			r.Reason = "timeout"
		case evidence.CloseGap:
			r.Reason = "gap"
		case evidence.CloseRetired:
			r.Reason = "retired"
		}
	case evidence.GapRecorded:
		r.Event = "evidence_gap"
	case evidence.StreamRetired:
		r.Event = "stream_retired"
	}
	for _, value := range e.Samples {
		source := "cached"
		if value.Source == evidence.SourceLive {
			source = "live"
		}
		r.Samples = append(r.Samples, sample{At: value.At, Avail: value.Avail, Used: value.Used, Consumed: value.Consumed,
			ConsumedFresh: value.ConsumedFresh, WorkQueued: value.WorkQueued, WorkQueuedFresh: value.WorkQueuedFresh,
			Valid: value.Valid, Source: source})
	}
	return r
}

func actionOutcome(o control.Outcome) (evidence.ActionOutcome, bool) {
	switch o {
	case control.Accepted:
		return evidence.ActionAccepted, true
	case control.Refused:
		return evidence.ActionRefused, true
	case control.ReadFailed:
		return evidence.ActionUnavailable, true
	case control.WriteFailed:
		return evidence.ActionFailed, true
	case control.Aborted:
		return evidence.ActionCancelled, true
	default:
		return 0, false
	}
}

func outcomeName(o control.Outcome) string {
	switch o {
	case control.Accepted:
		return "accepted"
	case control.Refused:
		return "refused"
	case control.ReadFailed:
		return "unavailable"
	case control.WriteFailed:
		return "write_failed"
	case control.Aborted:
		return "cancelled"
	default:
		return "observed"
	}
}

func decisionName(d control.Decision) string {
	switch d {
	case control.Pending:
		return "pending"
	case control.UsedProgress:
		return "used_progress"
	case control.Drained:
		return "drained"
	case control.Queued:
		return "work_queued"
	case control.Consumed:
		return "consumed"
	case control.Invalid:
		return "invalid_ring"
	case control.Changed:
		return "identity_changed"
	case control.Cancelled:
		return "cancelled"
	case control.Busy:
		return "contended"
	default:
		return "unavailable"
	}
}
