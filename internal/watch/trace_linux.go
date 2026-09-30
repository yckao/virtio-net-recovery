//go:build linux && amd64

package watch

import (
	"context"
	"time"
)

// runTrace measures existing stage counters only. It cannot call the recovery
// path, infer notification causality, or change kernel security/module policy.
func runTrace(ctx context.Context, c Config, target *Target, bpf *BPF, l *logger) (result error) {
	ticker := time.NewTicker(time.Duration(c.Interval * float64(time.Second)))
	defer ticker.Stop()
	snapshots := map[int]Snapshot{}
	var pending, failed bool
	var gap float64
	var sampled, total int
	lastCycle := monotonic()
	finishMetrics := func() {
		if pending && c.metricWorker != nil {
			c.metricWorker.RecordPoll(0, sampled, total-sampled, time.Duration(gap*float64(time.Second)), failed)
		}
		pending = false
	}
	defer finishMetrics()
	defer func() {
		for _, s := range snapshots {
			bpf.Forget(s)
		}
		l.emit("trace_stopped", map[string]any{"writes": uint64(0)})
		if result == nil {
			result = l.err
		}
	}()
	for {
		if ctx.Err() != nil {
			return l.err
		}
		now := monotonic()
		gap, lastCycle = now-lastCycle, now
		pending, failed, sampled, total = true, false, 0, 0
		alive, err := target.CheckAlive()
		if err != nil {
			failed = true
			return err
		}
		if !alive {
			l.emit("target_exited", map[string]any{})
			return l.err
		}
		if c.CheckIdentity != nil {
			if err := c.CheckIdentity(); err != nil {
				failed = true
				return err
			}
		}
		vhosts, _, err := target.Inventory()
		if err != nil {
			failed = true
			return err
		}
		rows := make([]QueueRow, 0, len(vhosts))
		total = len(vhosts)
		seen := map[int]bool{}
		for _, fd := range vhosts {
			if ctx.Err() != nil {
				return l.err
			}
			seen[fd] = true
			s, row, err := liveRow(target, bpf, fd)
			if err != nil {
				failed = true
				l.emit("queue_unavailable", map[string]any{"vhost_fd": fd, "error": err.Error()})
				continue
			}
			if old, ok := snapshots[fd]; ok && old.Identity() != s.Identity() {
				bpf.Forget(old)
				s, row, err = liveRow(target, bpf, fd)
				if err != nil {
					failed = true
					return err
				}
			}
			snapshots[fd] = s
			row.Stages, err = bpf.Counters(s.VQ)
			if err != nil {
				failed = true
				l.emit("queue_unavailable", map[string]any{"vhost_fd": fd, "error": err.Error()})
				continue
			}
			rows = append(rows, row)
			sampled++
		}
		for fd, s := range snapshots {
			if !seen[fd] {
				bpf.Forget(s)
				delete(snapshots, fd)
			}
		}
		l.emit("trace_sample", map[string]any{"queues": rows, "stages_measured": true, "writes": uint64(0), "source": "live_snapshot"})
		finishMetrics()
		if l.err != nil {
			return l.err
		}
		select {
		case <-ctx.Done():
			return l.err
		case <-ticker.C:
		}
	}
}
