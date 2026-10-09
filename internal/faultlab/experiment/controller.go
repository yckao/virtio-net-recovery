// Package experiment owns the bounded injection/cleanup workflow through its
// own ports. It knows no kernel loader, queue backend, CLI or output transport.
package experiment

import (
	"context"
	"errors"
	"time"
)

type Plan struct {
	Delay, Window, CleanupTimeout time.Duration
	MaxDrops                      uint32
}

func (p Plan) Validate() error {
	if p.Delay < 0 || p.Delay > time.Minute || p.Delay%time.Millisecond != 0 ||
		p.Window < time.Millisecond || p.Window > time.Minute || p.Window%time.Millisecond != 0 {
		return errors.New("delay must be 0..60s and window 1ms..60s, in whole milliseconds")
	}
	if p.MaxDrops < 1 || p.MaxDrops > 1_000_000 || p.CleanupTimeout <= 0 || p.CleanupTimeout > time.Minute {
		return errors.New("drops must be 1..1000000 and cleanup timeout greater than zero and at most 60s")
	}
	return nil
}

type Statistics struct {
	Matched, Dropped uint64
	Active           bool
}

type LoadResult struct {
	Loaded bool
	Err    error
}

type RestoreOutcome string

const (
	RestoreNotAttempted RestoreOutcome = "not_attempted"
	RestoreRefused      RestoreOutcome = "refused"
	RestoreFailed       RestoreOutcome = "write_failed"
	RestoreAccepted     RestoreOutcome = "accepted"
)

type RestoreResult struct {
	Outcome     RestoreOutcome
	CompletedAt time.Time
	Err         error
}

type Injector interface {
	Present(context.Context) (bool, error)
	Arm(context.Context, Plan) LoadResult
	Statistics(context.Context) (Statistics, error)
	Disarm(context.Context) error
}

type Restorer interface {
	Restore(context.Context) RestoreResult
}

// Waiter is the cancellable time effect; a deterministic consumer can replay it.
type Waiter interface {
	Wait(context.Context, time.Duration) error
}

type Report struct {
	Loaded, Disarmed, StatisticsKnown bool
	Statistics                        Statistics
	Restore                           RestoreResult
	RunError, CleanupError            error
}

func (r Report) Err() error { return errors.Join(r.RunError, r.CleanupError, r.Restore.Err) }

type Controller struct {
	injector Injector
	restorer Restorer
	waiter   Waiter
}

func New(injector Injector, restorer Restorer, waiter Waiter) (*Controller, error) {
	if injector == nil || restorer == nil || waiter == nil {
		return nil, errors.New("injector, restorer and waiter are required")
	}
	return &Controller{injector: injector, restorer: restorer, waiter: waiter}, nil
}

// Run owns a module only after Arm reports Loaded. All owned loads are unloaded
// before restoration, using a fresh bounded context independent of cancellation.
// A failed unload prevents restoration. The returned report never depends on
// output delivery, and an accepted restoration never claims traffic recovered.
func (c *Controller) Run(ctx context.Context, plan Plan) (report Report) {
	report.Restore.Outcome = RestoreNotAttempted
	if err := plan.Validate(); err != nil {
		report.RunError = err
		return
	}
	if err := ctx.Err(); err != nil {
		report.RunError = err
		return
	}
	present, err := c.injector.Present(ctx)
	if err != nil {
		report.RunError = err
		return
	}
	if present {
		report.RunError = errors.New("fault module already loaded; this run does not own it")
		return
	}
	load := c.injector.Arm(ctx, plan)
	report.Loaded, report.RunError = load.Loaded, load.Err
	if !load.Loaded {
		if report.RunError == nil {
			report.RunError = errors.New("module load was not accepted")
		}
		return
	}
	defer c.cleanup(plan.CleanupTimeout, &report)
	if load.Err == nil {
		report.RunError = c.waiter.Wait(ctx, plan.Delay+plan.Window)
	}
	return
}

// cleanup preserves ownership even when loading or the wait was cancelled.
func (c *Controller) cleanup(timeout time.Duration, report *Report) {
	cleanup, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Statistics are useful but never a prerequisite for disarming.
	stats, statErr := c.injector.Statistics(cleanup)
	if statErr == nil {
		report.Statistics, report.StatisticsKnown = stats, true
	} else {
		report.RunError = errors.Join(report.RunError, statErr)
	}
	if err := c.injector.Disarm(cleanup); err != nil {
		report.CleanupError = err
		return
	}
	report.Disarmed = true
	report.Restore = c.restorer.Restore(cleanup)
	if report.Restore.Outcome != RestoreAccepted && report.Restore.Err == nil {
		report.Restore.Err = errors.New("restoration notification was not accepted")
	}
	if report.StatisticsKnown && report.Statistics.Dropped == 0 {
		report.RunError = errors.Join(report.RunError, errors.New("no matching wakeup was dropped; injection was not demonstrated"))
	}
}

type Timer struct{}

func (Timer) Wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
