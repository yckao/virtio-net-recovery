package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/backend"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/selection"
	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
)

const deliveryFailed = 3

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
type commandRecord struct {
	Schema   int           `json:"schema_version"`
	Command  string        `json:"command"`
	Target   targetRecord  `json:"target"`
	Complete bool          `json:"complete"`
	Queues   []queueRecord `json:"queues,omitempty"`
	Error    string        `json:"error,omitempty"`
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
func message(err error) string {
	if err == nil {
		return ""
	}
	return bounded(err.Error(), 256)
}

// One bounded encoded value, one potentially blocked writer. A timed out delivery
// ends the command; it never repeats completed effects or spawns another writer.
func deliver(ctx context.Context, w io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > 4<<20 {
		return errors.New("command record exceeds 4 MiB delivery limit")
	}
	encoded = append(encoded, '\n')
	done := make(chan error, 1)
	go func() {
		n, err := w.Write(encoded)
		if err == nil && n != len(encoded) {
			err = io.ErrShortWrite
		}
		done <- err
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("output delivery timed out; completed writes must not be retried")
	}
}
func oneShot(ctx context.Context, o options, selector *selection.Selector, host *vhost.Host, out, stderr io.Writer) int {
	inv, err := selector.Resolve(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	status := 0
	if !inv.Complete || len(inv.Problems) > 0 || inv.ProblemsOmitted > 0 || len(inv.Targets) == 0 {
		status = 2
	}
	for _, target := range inv.Targets {
		record := executeTarget(ctx, o, host, target)
		if !record.Complete {
			status = 2
		}
		if err := deliver(ctx, out, record); err != nil {
			fmt.Fprintf(stderr, "PID %d execution_complete=%v delivery=failed: %v; prior writes must not be retried\n", target.PID, record.Complete, err)
			return deliveryFailed
		}
	}
	summary := struct {
		Schema          int      `json:"schema_version"`
		Event           string   `json:"event"`
		Command         string   `json:"command"`
		Execution       string   `json:"execution"`
		Targets         int      `json:"targets"`
		Problems        []string `json:"problems,omitempty"`
		ProblemsOmitted int      `json:"problems_omitted,omitempty"`
	}{Schema: 1, Event: "command_summary", Command: o.command, Execution: "complete", Targets: len(inv.Targets), ProblemsOmitted: inv.ProblemsOmitted}
	if status != 0 {
		summary.Execution = "incomplete"
	}
	for _, problem := range inv.Problems {
		summary.Problems = append(summary.Problems, message(problem))
	}
	if err := deliver(ctx, out, summary); err != nil {
		fmt.Fprintf(stderr, "execution=%s delivery=failed: %v; prior writes must not be retried\n", summary.Execution, err)
		return deliveryFailed
	}
	return status
}

// A single target result is bounded by admitted queues, independently of the
// number of selected targets. It is delivered before another target can mutate.
func executeTarget(ctx context.Context, o options, host *vhost.Host, target control.Target) commandRecord {
	record := commandRecord{Schema: 1, Command: o.command, Target: targetDTO(target), Complete: true}
	if o.command != "list" {
		session, err := backend.Open(ctx, host, target)
		if err != nil {
			record.Complete = false
			record.Error = message(err)
		} else {
			reader := session.Reader()
			if o.command == "kick" {
				writer, err := session.Manual()
				if err != nil {
					record.Complete = false
					record.Error = message(err)
				} else {
					batch, err := control.ExecuteManual(ctx, reader, writer)
					record.Complete = batch.Complete && err == nil
					record.Error = message(err)
					for _, receipt := range batch.Receipts {
						r := receipt.Result
						q := queueRecord{FD: receipt.Queue.Slot, Generation: receipt.Queue.Generation, Outcome: outcome(r.Outcome), Reason: reason(r.Decision), Error: bounded(message(r.Err), 96)}
						if r.Outcome == control.Accepted {
							at := r.AcceptedAt
							q.AcceptedAt = &at
						}
						record.Queues = append(record.Queues, q)
					}
				}
			} else {
				inventory, err := reader.Inventory(ctx)
				record.Complete = inventory.Complete && len(inventory.Unavailable) == 0 && err == nil
				record.Error = message(err)
				for _, q := range inventory.Queues {
					record.Queues = append(record.Queues, queueRecord{FD: q.Slot, Generation: q.Generation})
				}
				for _, fd := range inventory.Unavailable {
					record.Queues = append(record.Queues, queueRecord{FD: fd, Outcome: "unavailable"})
				}
			}
			if err := reader.Close(); err != nil {
				record.Complete = false
				if record.Error == "" {
					record.Error = message(err)
				} else {
					record.Error += "; " + err.Error()
				}
			}
		}
	}
	record.Error = bounded(record.Error, 256)
	return record
}
func bounded(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
