package cli

import (
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"time"
)

// These are command presentation values. No component types are serialized.
type targetRecord struct {
	PID       int    `json:"pid"`
	StartTime uint64 `json:"start_time"`
	Name      string `json:"name,omitempty"`
}

func targetDTO(t control.Target) targetRecord { return targetRecord{t.PID, t.StartTime, t.Name} }

type queueRecord struct {
	FD         int        `json:"fd"`
	Generation uint64     `json:"generation"`
	Outcome    string     `json:"outcome,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}
type queueListRecord struct {
	Unavailable []int           `json:"unavailable,omitempty"`
	Truncated   bool            `json:"truncated,omitempty"`
	Schema      int             `json:"schema_version"`
	Command     string          `json:"command"`
	Target      targetRecord    `json:"target"`
	Complete    bool            `json:"complete"`
	Queues      []queueIdentity `json:"queues,omitempty"`
	Error       string          `json:"error,omitempty"`
}

func outcome(o control.Outcome) string {
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
func reason(d control.Decision) string {
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

type queueIdentity struct {
	FD         int    `json:"fd"`
	Generation uint64 `json:"generation"`
}
type kickRecord struct {
	Schema   int           `json:"schema_version"`
	Command  string        `json:"command"`
	Target   targetRecord  `json:"target"`
	Complete bool          `json:"complete"`
	Queues   []queueRecord `json:"queues,omitempty"`
	Error    string        `json:"error,omitempty"`
}

func kickDTO(r control.KickReport) kickRecord {
	out := kickRecord{Schema: 1, Command: "kick", Target: targetDTO(r.Target), Complete: r.Complete(), Error: message(r.Err)}
	for _, receipt := range r.Batch.Receipts {
		result := receipt.Result
		q := queueRecord{FD: receipt.Queue.Slot, Generation: receipt.Queue.Generation, Outcome: outcome(result.Outcome), Reason: reason(result.Decision), Error: bounded(message(result.Err), 96)}
		if result.Outcome == control.Accepted {
			at := result.AcceptedAt
			q.AcceptedAt = &at
		}
		out.Queues = append(out.Queues, q)
	}
	return out
}
