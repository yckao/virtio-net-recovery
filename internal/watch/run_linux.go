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
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

type Config struct {
	PID                                                                                   int
	Mode, StateDir, BPFObject                                                             string
	Interval, Threshold, Cooldown, KickInterval, VerifyTimeout, SummaryInterval, Duration float64
	MaxRecoveries                                                                         int
	Rescue                                                                                bool
	VhostFD                                                                               int
	InventoryInterval                                                                     float64
	TraceStages                                                                           bool
	BatchRings                                                                            bool
	Domain                                                                                string
	CheckIdentity                                                                         func() error
}

func (c Config) Validate() error {
	if c.PID <= 0 {
		return errors.New("PID must be positive")
	}
	if c.Mode != "observe" && c.Mode != "guarded" && c.Mode != "periodic" {
		return errors.New("mode must be observe, guarded, or periodic")
	}
	if c.BatchRings && (c.Mode != "guarded" || c.TraceStages || c.Rescue) {
		return errors.New("batch-rings requires guarded mode without stage tracing or rescue")
	}
	for _, v := range []float64{c.Interval, c.InventoryInterval, c.Threshold, c.Cooldown, c.KickInterval, c.VerifyTimeout, c.SummaryInterval} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > float64(math.MaxInt64)/float64(time.Second) {
			return errors.New("intervals and thresholds must be finite positive seconds")
		}
	}
	if time.Duration(c.Interval*float64(time.Second)) < time.Nanosecond {
		return errors.New("snapshot interval is too small")
	}
	if math.IsNaN(c.Duration) || math.IsInf(c.Duration, 0) || c.Duration < 0 || c.MaxRecoveries <= 0 {
		return errors.New("duration must be finite and nonnegative; recovery budget must be positive")
	}
	if c.VhostFD < -1 || c.VhostFD >= 0 && !c.Rescue {
		return errors.New("vhost-fd is only valid with rescue")
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
		"outstanding": row.Outstanding, "stalled": row.Stalled, "busy": row.Busy, "stages": row.Stages}
}

type verification struct {
	at             float64
	used, consumed uint16
	stalled        bool
}

// Run owns all descriptors and BPF resources until cancellation or QEMU exit.
func Run(ctx context.Context, c Config, output io.Writer) error {
	return run(ctx, c, output, nil)
}

// RunWithBPF shares one serialized snapshot channel between independent VMs.
// The caller closes shared only after every target worker has exited.
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
	var lock *os.File
	if !c.Rescue {
		if err := os.MkdirAll(c.StateDir, 0700); err != nil {
			return err
		}
		var err error
		lock, err = os.OpenFile(filepath.Join(c.StateDir, fmt.Sprintf("%d.lock", c.PID)), os.O_CREATE|os.O_WRONLY, 0600)
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
		bpf, err = OpenBPF(c.BPFObject, c.TraceStages)
		if err != nil {
			return err
		}
		defer bpf.Close()
	}
	l := &logger{encoder: json.NewEncoder(output), pid: c.PID, domain: c.Domain}
	if c.Rescue {
		return rescue(target, bpf, c.VhostFD, l)
	}
	var uts unix.Utsname
	_ = unix.Uname(&uts)
	l.emit("started", map[string]any{"implementation": "go", "mode": c.Mode, "kernel": unix.ByteSliceToString(uts.Release[:]),
		"interval": c.Interval, "kick_interval": c.KickInterval, "threshold": c.Threshold,
		"inventory_interval": c.InventoryInterval, "trace_stages": c.TraceStages, "batch_rings": c.BatchRings})
	if c.BatchRings {
		return runBatched(ctx, c, target, bpf, l)
	}
	started, lastSummary := monotonic(), math.Inf(-1)
	lastInventory := math.Inf(-1)
	var vhosts []int
	var events map[uint32][]int
	states := map[int]*Detector{}
	snapshots := map[int]Snapshot{}
	pending := map[int]verification{}
	rows := []QueueRow{}
	totalWrites := uint64(0)
	defer func() { l.emit("stopped", map[string]any{"writes": totalWrites, "queues": rows}) }()
	ticker := time.NewTicker(time.Duration(c.Interval * float64(time.Second)))
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return l.err
		}
		alive, err := target.CheckAlive()
		if err != nil {
			return fmt.Errorf("check QEMU pidfd: %w", err)
		}
		if !alive {
			l.emit("target_exited", map[string]any{})
			return l.err
		}
		now := monotonic()
		if c.Duration > 0 && now-started >= c.Duration {
			return l.err
		}
		// Cache only discovery results. Each queue still gets a fresh pidfd_getfd
		// and live BPF snapshot on every tick; no kernel pointers are trusted from
		// the discovery cache. Recovery always refreshes and revalidates below.
		if now-lastInventory >= c.InventoryInterval {
			vhosts, events, err = target.Inventory()
			if err != nil {
				return err
			}
			lastInventory = now
		}
		rows = []QueueRow{}
		seen := map[int]bool{}
		for _, remoteFD := range vhosts {
			seen[remoteFD] = true
			row, err := func() (QueueRow, error) {
				fd, err := target.Duplicate(remoteFD)
				if err != nil {
					return QueueRow{}, err
				}
				defer unix.Close(fd)
				s, err := bpf.Snapshot(fd)
				if err != nil {
					return QueueRow{}, err
				}
				if old, ok := snapshots[remoteFD]; ok && old.Identity() != s.Identity() {
					bpf.Forget(old)
					delete(pending, remoteFD)
					s, err = bpf.Snapshot(fd)
					if err != nil {
						return QueueRow{}, err
					}
				}
				snapshots[remoteFD] = s
				d := states[remoteFD]
				if d == nil {
					d = NewDetector(c.Threshold, c.Cooldown, c.MaxRecoveries)
					states[remoteFD] = d
				}
				avail, used, err := target.Ring(s)
				if err != nil {
					return QueueRow{}, err
				}
				counters, err := bpf.Counters(s.VQ)
				if err != nil {
					return QueueRow{}, err
				}
				// VHOST_WORK_QUEUED is bit index 1. Counter timestamps use CLOCK_MONOTONIC.
				busy := s.WorkFlags&(1<<1) != 0 || counters.Active != 0 || now-float64(counters.LastHandlerNS)/1e9 < c.Interval
				stalled := d.Observe(s, avail, used, busy, now)
				row := QueueRow{VhostFD: remoteFD, EventID: s.EventID, Num: s.Num, Avail: avail, Used: used, Consumed: s.LastAvail,
					Pending: avail - s.LastAvail, Outstanding: avail - used, Stalled: stalled, Busy: busy, Stages: counters}
				if v, ok := pending[remoteFD]; ok {
					if used != v.used && s.LastAvail != v.consumed {
						event := "progress_after_kick"
						if v.stalled {
							event = "recovered"
						}
						f := fields(row)
						f["latency"] = now - v.at
						l.emit(event, f)
						delete(pending, remoteFD)
					} else if now-v.at >= c.VerifyTimeout {
						l.emit("recovery_unconfirmed", fields(row))
						delete(pending, remoteFD)
					}
				}
				if stalled && !d.Reported {
					f := fields(row)
					f["age"] = now - d.Since
					l.emit("stall", f)
					d.Reported = true
				}
				if l.err != nil {
					return QueueRow{}, l.err
				}
				periodic := c.Mode == "periodic" && now-d.LastKick >= c.KickInterval
				guarded := c.Mode == "guarded" && stalled && d.Allowed(now)
				if (periodic || guarded) && ctx.Err() == nil {
					if guarded {
						// A rare guarded recovery must not depend on stale discovery.
						current, refreshed, err := target.Inventory()
						if err != nil {
							return QueueRow{}, err
						}
						if !slices.Contains(current, remoteFD) {
							return QueueRow{}, errors.New("vhost FD disappeared before recovery")
						}
						events = refreshed
					}
					// Re-duplicate even though fd is pinned above: QEMU may have
					// replaced that descriptor during discovery. Rekick compares the
					// fresh attachment against s before writing the pinned eventfd.
					currentFD, err := target.Duplicate(remoteFD)
					if err != nil {
						return QueueRow{}, err
					}
					err = Rekick(target, bpf, currentFD, s, events)
					unix.Close(currentFD)
					if err != nil {
						return QueueRow{}, err
					}
					d.LastKick = now
					if guarded {
						d.Kicked(now)
					}
					totalWrites++
					f := fields(row)
					f["reason"] = "stall"
					if periodic {
						f["reason"] = "periodic"
					}
					l.emit("rekick", f)
					if _, ok := pending[remoteFD]; row.Pending > 0 && !ok {
						pending[remoteFD] = verification{now, used, s.LastAvail, guarded || stalled}
					}
				}
				return row, nil
			}()
			if err != nil {
				lastInventory = math.Inf(-1)
				l.emit("queue_unavailable", map[string]any{"vhost_fd": remoteFD, "error": err.Error()})
				if d := states[remoteFD]; d != nil {
					d.Since = now
					d.Reported = false
				}
				delete(pending, remoteFD)
			} else {
				rows = append(rows, row)
			}
			if l.err != nil {
				return l.err
			}
		}
		for fd, s := range snapshots {
			if !seen[fd] {
				bpf.Forget(s)
				delete(snapshots, fd)
				delete(pending, fd)
			}
		}
		if now-lastSummary >= c.SummaryInterval {
			l.emit("sample", map[string]any{"writes": totalWrites, "queues": rows})
			lastSummary = now
		}
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

func rescue(target *Target, bpf *BPF, only int, l *logger) error {
	vhosts, events, err := target.Inventory()
	if err != nil {
		return err
	}
	written := false
	for _, remoteFD := range vhosts {
		if only >= 0 && only != remoteFD {
			continue
		}
		err = func() error {
			fd, err := target.Duplicate(remoteFD)
			if err != nil {
				return err
			}
			defer unix.Close(fd)
			s, err := bpf.Snapshot(fd)
			if err != nil {
				return err
			}
			if err = Rekick(target, bpf, fd, s, events); err != nil {
				return err
			}
			l.emit("rescue_kick", map[string]any{"vhost_fd": remoteFD, "eventfd_id": s.EventID, "implementation": "go"})
			return l.err
		}()
		if err != nil {
			return err
		}
		written = true
	}
	if !written {
		return errors.New("no matching vhost TX queue in current inventory")
	}
	return nil
}
