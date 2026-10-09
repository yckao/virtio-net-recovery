package diagnostics

import (
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
	"github.com/yckao/virtio-net-recovery/modules/recovery-evidence"
)

func base(f control.Frame) report.Record {
	return report.Record{Stream: f.Stream, Sequence: f.Sequence, PID: f.Target.PID, Name: f.Target.Name, StartTime: f.Target.StartTime,
		Slot: f.Queue.Slot, Generation: f.Queue.Generation, At: f.At}
}

func fromEvidence(frame control.Frame, e evidence.Record) report.Record {
	r := base(frame)
	r.Episode, r.Attempt, r.At, r.StartedAt = uint64(e.EpisodeID), uint64(e.AttemptID), e.At, e.StartedAt
	r.UsedProgress, r.ConsumedProgress, r.Incomplete, r.LostInputs = e.UsedProgress, e.ConsumedProgress, e.Incomplete, e.LostInputs
	r.Actions = report.ActionTotals{Accepted: e.Actions.Accepted, Refused: e.Actions.Refused, Failed: e.Actions.Failed, Cancelled: e.Actions.Cancelled, Unavailable: e.Actions.Unavailable}
	switch e.Kind {
	case evidence.EpisodeOpened:
		r.Kind = report.Candidate
	case evidence.EpisodeSnapshot:
		r.Kind = report.EpisodeSnapshot
	case evidence.ActionRecorded:
		r.Kind = report.EpisodeAction
	case evidence.VerificationRecorded:
		r.Kind = report.Verification
		if e.Verification == evidence.VerificationProgress {
			r.Outcome = "progress"
		} else {
			r.Outcome = "unconfirmed"
		}
	case evidence.EpisodeClosed:
		r.Kind = report.EpisodeClosed
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
		r.Kind = report.Gap
	case evidence.StreamRetired:
		r.Kind = report.Retired
	}
	if len(e.Samples) > 0 {
		r.Samples = make([]report.Sample, len(e.Samples))
	}
	for i, s := range e.Samples {
		source := "unknown"
		switch s.Source {
		case evidence.SourceCached:
			source = "cached"
		case evidence.SourceLive:
			source = "live"
		}
		r.Samples[i] = report.Sample{At: s.At, Avail: s.Avail, Used: s.Used, Consumed: s.Consumed, ConsumedFresh: s.ConsumedFresh,
			WorkQueued: s.WorkQueued, WorkQueuedFresh: s.WorkQueuedFresh, Valid: s.Valid, Source: source}
	}
	return r
}

func actionOutcome(outcome control.Outcome) (evidence.ActionOutcome, bool) {
	switch outcome {
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
		return "failed"
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
		return "invalid"
	case control.Changed:
		return "identity_changed"
	case control.Unavailable:
		return "unavailable"
	case control.Cancelled:
		return "cancelled"
	case control.Busy:
		return "busy"
	default:
		return "unknown"
	}
}
