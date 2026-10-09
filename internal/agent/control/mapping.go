package control

import (
	"time"

	recovery "github.com/yckao/virtio-net-recovery/recovery"
)

func toCoreResult(op recovery.Operation, r Result, epoch time.Time) recovery.OperationResult {
	result := recovery.OperationResult{ID: op.ID, Generation: op.Generation, CompletedAt: r.CompletedAt.Sub(epoch), Reason: coreReason(r.Decision)}
	switch r.Outcome {
	case Observed:
		result.Outcome = recovery.Observed
	case Accepted:
		result.Outcome = recovery.Accepted
		result.AcceptedAt = r.AcceptedAt.Sub(epoch)
	case Refused:
		result.Outcome = recovery.Refused
	case ReadFailed:
		result.Outcome = recovery.Unavailable
	case WriteFailed:
		result.Outcome = recovery.WriteFailed
		result.Reason = recovery.ReasonWriteFailed
	case Aborted:
		result.Outcome = recovery.Cancelled
		result.Reason = recovery.ReasonCancelled
	}
	if r.Live != nil {
		s := r.Live
		result.Live = &recovery.LiveObservation{Generation: s.Queue.Generation, At: s.At.Sub(epoch), Used: s.Used, Consumed: s.Consumed, Valid: s.Live && r.Decision != Invalid && r.Decision != Changed && r.Decision != Unavailable}
	}
	return result
}
func coreReason(d Decision) recovery.Reason {
	switch d {
	case Pending:
		return recovery.ReasonPending
	case UsedProgress:
		return recovery.ReasonUsedProgress
	case Drained:
		return recovery.ReasonDrained
	case Queued:
		return recovery.ReasonWorkQueued
	case Consumed:
		return recovery.ReasonConsumed
	case Invalid:
		return recovery.ReasonInvalidRing
	case Changed:
		return recovery.ReasonIdentityChanged
	case Cancelled:
		return recovery.ReasonCancelled
	case Busy:
		return recovery.ReasonContended
	default:
		return recovery.ReasonUnavailable
	}
}
func decision(r recovery.Reason) Decision {
	switch r {
	case recovery.ReasonPending:
		return Pending
	case recovery.ReasonUsedProgress:
		return UsedProgress
	case recovery.ReasonDrained:
		return Drained
	case recovery.ReasonWorkQueued:
		return Queued
	case recovery.ReasonConsumed:
		return Consumed
	case recovery.ReasonInvalidRing:
		return Invalid
	case recovery.ReasonIdentityChanged:
		return Changed
	case recovery.ReasonCancelled:
		return Cancelled
	case recovery.ReasonContended:
		return Busy
	default:
		return Unavailable
	}
}
func toEvent(f recovery.Fact) (Event, bool) {
	e := Event{Decision: decision(f.Reason), AttemptID: f.AttemptID, Latency: f.Latency, ObservedAt: f.ObservedAt, AcceptedAt: f.AcceptedAt}
	switch f.Kind {
	case recovery.OperationCompleted:
		if f.AttemptID == 0 {
			e.Kind = EventDecision
			return e, true
		}
		e.Kind = EventAction
		switch f.Outcome {
		case recovery.Accepted:
			e.Outcome = Accepted
		case recovery.Refused:
			e.Outcome = Refused
		case recovery.Unavailable:
			e.Outcome = ReadFailed
		case recovery.WriteFailed:
			e.Outcome = WriteFailed
		case recovery.Cancelled:
			e.Outcome = Aborted
		default:
			e.Outcome = Observed
		}
		return e, true
	case recovery.VerificationConfirmed:
		e.Kind = EventVerified
		e.AttemptID = f.OriginAttemptID
		return e, true
	case recovery.VerificationOverdue:
		e.Kind = EventUnconfirmed
		e.AttemptID = f.OriginAttemptID
		return e, true
	default:
		return e, false
	}
}
