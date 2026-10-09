// Package supervision owns target and worker lifetimes. Discovery and backend
// construction are injected; this package knows neither implementation.
package supervision

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
)

type Inventory struct {
	Targets  []control.Target
	Complete bool
	Problems int
}
type Selector interface {
	Resolve(context.Context) (Inventory, error)
}
type Factory interface {
	Open(context.Context, control.Target) (control.Reader, control.Writer, func() error, error)
}
type Notices interface{ TryNotice(string) bool }
type Options struct {
	Refresh    time.Duration
	MaxTargets int
	Worker     control.Options
}
type Dependencies struct {
	Selector   Selector
	Factory    Factory
	Reporter   control.Reporter
	Notices    Notices
	Accounting *control.Accounting
	Registry   *control.Registry
	Clock      control.Clock
}
type running struct {
	target control.Target
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}
type completion struct {
	pid int
	err error
}

// Run retains existing workers when discovery is incomplete. A replacement must
// join the previous worker and release its resources before acquiring a new lease.
func Run(ctx context.Context, o Options, d Dependencies) (result error) {
	if o.Refresh <= 0 || o.MaxTargets < 1 || o.MaxTargets > 4096 || d.Selector == nil || d.Factory == nil || d.Accounting == nil || d.Registry == nil || d.Clock == nil {
		return errors.New("invalid supervisor options/dependencies")
	}
	if err := o.Worker.Validate(); err != nil {
		return err
	}
	epoch := d.Clock.Now()
	workers := map[int]*running{}
	results := make(chan completion, o.MaxTargets)
	var sequence uint32
	stop := func(r *running) error { r.cancel(); <-r.done; return r.err }
	defer func() {
		for _, r := range workers {
			r.cancel()
		}
		for _, r := range workers {
			<-r.done
			if r.err != nil && !errors.Is(result, r.err) {
				result = errors.Join(result, r.err)
			}
		}
	}()
	notice := func(err error) {
		if err != nil && d.Notices != nil {
			d.Notices.TryNotice(err.Error())
		}
	}
	refresh := func() error {
		inv, err := d.Selector.Resolve(ctx)
		if err != nil {
			notice(err)
			return nil
		}
		if len(inv.Targets) > o.MaxTargets {
			return errors.New("target inventory exceeds capacity")
		}
		if inv.Problems > 0 {
			notice(fmt.Errorf("discovery reported %d unavailable targets", inv.Problems))
		}
		present := map[int]bool{}
		for _, target := range inv.Targets {
			present[target.PID] = true
		}
		if inv.Complete {
			for pid, r := range workers {
				if !present[pid] {
					if err := stop(r); err != nil {
						return fmt.Errorf("retire PID %d: %w", pid, err)
					}
					delete(workers, pid)
				}
			}
		}
		for _, target := range inv.Targets {
			if old, ok := workers[target.PID]; ok {
				if old.target.Same(target) {
					continue
				}
				if err := stop(old); err != nil {
					return fmt.Errorf("retire PID %d: %w", target.PID, err)
				}
				delete(workers, target.PID)
			}
			if len(workers) >= o.MaxTargets {
				notice(errors.New("worker capacity reached; incomplete inventory retains existing workers"))
				continue
			}
			reader, writer, close, err := d.Factory.Open(ctx, target)
			if err != nil {
				notice(fmt.Errorf("open PID %d: %w", target.PID, err))
				continue
			}
			if sequence == ^uint32(0) {
				close()
				return errors.New("worker identity exhausted")
			}
			sequence++
			worker, err := control.NewWorker(o.Worker, control.Dependencies{Reader: reader, Writer: writer, Reporter: d.Reporter, Clock: d.Clock, Accounting: d.Accounting, Registry: d.Registry}, target, epoch, sequence)
			if err != nil {
				close()
				return err
			}
			child, cancel := context.WithCancel(ctx)
			r := &running{target: target, cancel: cancel, done: make(chan struct{})}
			workers[target.PID] = r
			go func() {
				err := worker.Run(child)
				cleanup := close()
				if err == nil || child.Err() != nil && errors.Is(err, child.Err()) {
					err = cleanup
				} else {
					err = errors.Join(err, cleanup)
				}
				r.err = err
				if err != nil {
					select {
					case results <- completion{target.PID, err}:
					default:
					}
				}
				closeChannel(r.done)
			}()
		}
		return nil
	}
	if err := refresh(); err != nil {
		return err
	}
	ticker := time.NewTicker(o.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c := <-results:
			return fmt.Errorf("worker PID %d: %w", c.pid, c.err)
		case <-ticker.C:
			if err := refresh(); err != nil {
				return err
			}
		}
	}
}
func closeChannel(ch chan struct{}) { close(ch) }
