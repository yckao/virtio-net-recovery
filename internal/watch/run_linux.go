//go:build linux && amd64

package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

type Config struct {
	PID                                                                   int
	Mode, StateDir, BPFObject                                             string
	Interval, InventoryInterval, VerifyTimeout, SummaryInterval, Duration float64
	VhostFD                                                               int
	Domain                                                                string
	CheckIdentity                                                         func() error
	Metrics                                                               *Metrics
	metricWorker                                                          *WorkerMetrics
}

func (c Config) Validate() error {
	if c.PID <= 0 {
		return errors.New("PID must be positive")
	}
	switch c.Mode {
	case "observe", "recover", "kick", "trace":
	case "guarded":
		return errors.New("guarded mode was removed: use --mode recover for conditional paced recovery, or --mode observe for read-only candidates")
	case "periodic":
		return errors.New("periodic mode was removed: use --mode recover for conditional paced recovery; --mode kick performs one explicit manual kick")
	default:
		return errors.New("mode must be observe, recover, kick, or trace")
	}
	for _, v := range []float64{c.Interval, c.InventoryInterval, c.VerifyTimeout, c.SummaryInterval} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > float64(math.MaxInt64)/float64(time.Second) {
			return errors.New("intervals must be finite positive seconds")
		}
	}
	if time.Duration(c.Interval*float64(time.Second)) < time.Nanosecond {
		return errors.New("snapshot interval is too small")
	}
	if math.IsNaN(c.Duration) || math.IsInf(c.Duration, 0) || c.Duration < 0 || c.Duration > float64(math.MaxInt64)/float64(time.Second) {
		return errors.New("duration must be finite nonnegative seconds within the timer range")
	}
	if c.Mode == "trace" && (c.Duration < 1 || c.Duration > 300) {
		return errors.New("trace requires --duration in 1..300 seconds")
	}
	if c.VhostFD < -1 || c.VhostFD >= 0 && c.Mode != "kick" {
		return errors.New("vhost-fd is only valid with --mode kick")
	}
	if c.StateDir == "" || c.BPFObject == "" {
		return errors.New("state directory and BPF object must be specified")
	}
	return nil
}

func monotonic() float64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		panic(err)
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9
}

type logger struct {
	encoder *json.Encoder
	err     error
	pid     int
	domain  string
}

func (l *logger) emit(event string, fields map[string]any) {
	if l.err != nil {
		return
	}
	fields["event"], fields["time"], fields["monotonic"] = event, float64(time.Now().UnixNano())/1e9, monotonic()
	if l.pid != 0 {
		fields["pid"] = l.pid
	}
	if l.domain != "" {
		fields["domain"] = l.domain
	}
	l.err = l.encoder.Encode(fields)
}

func fields(row QueueRow) map[string]any {
	return map[string]any{"vhost_fd": row.VhostFD, "eventfd_id": row.EventID, "num": row.Num,
		"avail": row.Avail, "used": row.Used, "consumed": row.Consumed, "pending": row.Pending,
		"outstanding": row.Outstanding, "stalled": row.Stalled, "work_queued": row.WorkQueued, "busy": row.WorkQueued, "stages": row.Stages}
}

// Run owns descriptors and BPF resources until cancellation or QEMU exit.
func Run(ctx context.Context, c Config, output io.Writer) error { return run(ctx, c, output, nil) }

// RunWithBPF uses the manager's serialized snapshot channel and output bounds.
func RunWithBPF(ctx context.Context, c Config, output io.Writer, shared *BPF) error {
	if shared == nil {
		return errors.New("shared BPF is required")
	}
	return run(ctx, c, output, shared)
}

func run(ctx context.Context, c Config, output io.Writer, shared *BPF) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Metrics != nil && c.Mode != "kick" {
		var err error
		c.metricWorker, err = c.Metrics.RegisterWorker()
		if err != nil {
			return err
		}
		defer c.metricWorker.Retire()
	}
	if c.Mode == "trace" && shared == nil {
		child, cancel := context.WithTimeout(ctx, time.Duration(c.Duration*float64(time.Second)))
		defer cancel()
		ctx = child
		output = newTraceWriter(output, cancel)
	}
	if c.Mode == "observe" || c.Mode == "recover" {
		if err := os.MkdirAll(c.StateDir, 0700); err != nil {
			return err
		}
		lock, err := os.OpenFile(filepath.Join(c.StateDir, fmt.Sprintf("%d.lock", c.PID)), os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return fmt.Errorf("another agent owns this PID: %w", err)
		}
	}
	target, err := OpenTarget(c.PID)
	if err != nil {
		return err
	}
	defer target.Close()
	if c.CheckIdentity != nil {
		if err := c.CheckIdentity(); err != nil {
			return err
		}
	}
	bpf := shared
	if bpf == nil {
		bpf, err = OpenBPF(c.BPFObject, c.Mode == "trace")
		if err != nil {
			return fmt.Errorf("%s: %w", c.Mode, err)
		}
		defer bpf.Close()
	}
	l := &logger{encoder: json.NewEncoder(output), pid: c.PID, domain: c.Domain}
	if c.Mode == "kick" {
		return kick(ctx, c, target, bpf, l)
	}
	var uts unix.Utsname
	_ = unix.Uname(&uts)
	l.emit("started", map[string]any{"implementation": "go", "mode": c.Mode, "kernel": unix.ByteSliceToString(uts.Release[:]),
		"interval": c.Interval, "inventory_interval": c.InventoryInterval, "trace_stages": c.Mode == "trace", "batch_rings": c.Mode != "trace"})
	if l.err != nil {
		return l.err
	}
	if c.Mode == "trace" {
		return runTrace(ctx, c, target, bpf, l)
	}
	return runRecover(ctx, c, target, bpf, l)
}

// kick is the manual, one-shot entry point to the same verified write primitive.
func kick(ctx context.Context, c Config, target liveQueueTarget, bpf snapshotter, l *logger) error {
	vhosts, events, err := target.Inventory()
	if err != nil {
		return err
	}
	written := false
	for _, remoteFD := range vhosts {
		if c.VhostFD >= 0 && c.VhostFD != remoteFD {
			continue
		}
		accepted := false
		err = func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if c.CheckIdentity != nil {
				if err := c.CheckIdentity(); err != nil {
					return err
				}
			}
			fd, err := target.Duplicate(remoteFD)
			if err != nil {
				return err
			}
			defer unix.Close(fd)
			s, err := bpf.Snapshot(fd)
			if err != nil {
				return err
			}
			if err := RekickContext(ctx, target, bpf, fd, s, events); err != nil {
				return err
			}
			accepted = true
			recordKickResult(c.Metrics, nil)
			l.emit("kick_written", map[string]any{"vhost_fd": remoteFD, "eventfd_id": s.EventID, "implementation": "go", "progress": "unmeasured"})
			return l.err
		}()
		if err != nil {
			if !accepted {
				recordKickResult(c.Metrics, err)
			}
			return err
		}
		written = true
	}
	if !written {
		return errors.New("no matching vhost TX queue in current inventory")
	}
	return nil
}

func recordKickResult(m *Metrics, err error) {
	if m == nil {
		return
	}
	var writeErr *KickWriteError
	switch {
	case err == nil:
		m.Write(WriteSuccess)
	case errors.As(err, &writeErr):
		m.Write(WriteError)
	default:
		m.Write(WriteRefused)
	}
}
