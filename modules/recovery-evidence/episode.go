package evidence

import "time"

type episode struct {
	id                  EpisodeID
	started, lastRecord time.Duration
	quietSince          time.Duration
	quiet               bool
	used, consumed      uint16
	haveConsumed        bool
	usedProgress        bool
	consumedProgress    bool
	actions             ActionTotals
	lastAttempt         AttemptID
	lastAction          ActionOutcome
	actionReported      bool
}

func (e *episode) observe(sample Sample) {
	if !sample.Valid {
		return
	}
	e.usedProgress = e.usedProgress || sample.Used != e.used
	if sample.ConsumedFresh {
		if e.haveConsumed {
			e.consumedProgress = e.consumedProgress || sample.Consumed != e.consumed
		} else {
			e.consumed, e.haveConsumed = sample.Consumed, true
		}
	}
}

func (e *episode) action(in ActionObserved) {
	switch in.Outcome {
	case ActionAccepted:
		e.actions.Accepted++
	case ActionRefused:
		e.actions.Refused++
	case ActionFailed:
		e.actions.Failed++
	case ActionCancelled:
		e.actions.Cancelled++
	case ActionUnavailable:
		e.actions.Unavailable++
	}
	e.lastAttempt, e.lastAction = in.AttemptID, in.Outcome
}

func (r *Recorder) open(at time.Duration, records *[]Record) {
	latest, ok := r.history.latest()
	if !ok || !latest.Valid || r.haveClosed && at-r.closedAt < r.options.ReopenDelay {
		return
	}
	r.sequence++
	r.active = &episode{id: r.sequence, started: at, lastRecord: at,
		used: latest.Used, consumed: latest.Consumed, haveConsumed: latest.ConsumedFresh}
	*records = append(*records, r.episodeRecord(EpisodeOpened, at, true))
}

func (r *Recorder) expire(at time.Duration, cancelQuiet bool, records *[]Record) {
	e := r.active
	if e == nil {
		return
	}
	if at-e.started >= r.options.Lifetime {
		r.close(at, CloseTimeout, records)
	} else if !cancelQuiet && e.quiet && at-e.quietSince >= r.options.QuietPeriod {
		r.close(at, CloseQuiet, records)
	}
}

func (r *Recorder) close(at time.Duration, reason CloseReason, records *[]Record) {
	if r.active == nil {
		return
	}
	record := r.episodeRecord(EpisodeClosed, at, false)
	record.CloseReason, record.Incomplete = reason, reason == CloseGap
	*records = append(*records, record)
	r.active = nil
	r.closedAt, r.haveClosed = at, true
}

func (r *Recorder) episodeRecord(kind RecordKind, at time.Duration, history bool) Record {
	e := r.active
	record := Record{Kind: kind, StreamID: r.stream, EpisodeID: e.id,
		At: at, StartedAt: e.started, Actions: e.actions,
		UsedProgress: e.usedProgress, ConsumedProgress: e.consumedProgress,
		AttemptID: e.lastAttempt, Action: e.lastAction}
	if history {
		record.Samples = r.history.snapshot()
	} else if sample, ok := r.history.latest(); ok {
		record.Samples = []Sample{sample}
	}
	e.lastRecord = at
	return record
}
