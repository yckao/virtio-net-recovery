// Package supervision owns process admission and the lifetime of acquired sessions.
package supervision

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

type Inventory struct {
	Targets  []control.Target
	Complete bool
	Problems int
}
type Selector interface {
	Resolve(context.Context) (Inventory, error)
}

// Session is one lifetime: its Close releases the reader and any recovery lease.
// Writer is nil for a read-only session. Ownership transfers only on a nil error.
type Session interface {
	control.Reader
	Writer() control.Writer
}
type Factory interface {
	Open(context.Context, control.Target) (Session, error)
}
type Notices interface{ TryNotice(string) bool }
type Options struct {
	Refresh    time.Duration
	MaxTargets int
	// RequireInitialTarget is appropriate for explicit process requests. Dynamic
	// selectors may instead wait for a future matching process.
	RequireInitialTarget bool
	Worker               control.Options
}
type Dependencies struct {
	Selector   Selector
	Factory    Factory
	Observer   control.Observer
	Notices    Notices
	Accounting *control.Accounting
	Clock      control.Clock
}

type supervisor struct {
	options          Options
	deps             Dependencies
	epoch            time.Time
	workers          map[int]*running
	completed        chan *running
	lastRefreshError error
}
type running struct {
	target control.Target
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// Run retains pinned workers under incomplete discovery. A replacement cannot
// acquire resources until its old session has joined and closed successfully.
func Run(ctx context.Context, o Options, d Dependencies) (result error) {
	if o.Refresh <= 0 || o.MaxTargets < 1 || o.MaxTargets > 4096 || d.Selector == nil || d.Factory == nil || d.Accounting == nil || d.Clock == nil {
		return errors.New("invalid supervisor options or dependencies")
	}
	if err := o.Worker.Validate(); err != nil {
		return err
	}
	if ctx.Err() == context.Canceled {
		return nil
	}
	s := supervisor{options: o, deps: d, epoch: d.Clock.Now(), workers: make(map[int]*running), completed: make(chan *running, o.MaxTargets)}
	defer func() { result = errors.Join(result, s.stopAll()) }()
	if err := s.refresh(ctx); err != nil {
		return err
	}
	if ctx.Err() == context.Canceled {
		return nil
	}
	if o.RequireInitialTarget && len(s.workers) == 0 {
		return errors.Join(errors.New("no requested target could be admitted"), s.lastRefreshError)
	}
	ticker := time.NewTicker(o.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()
		case worker := <-s.completed:
			return fmt.Errorf("worker PID %d: %w", worker.target.PID, worker.err)
		case <-ticker.C:
			if err := s.refresh(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *supervisor) notice(err error) {
	if err != nil && s.deps.Notices != nil {
		s.deps.Notices.TryNotice(err.Error())
	}
}

func (s *supervisor) refresh(ctx context.Context) error {
	s.lastRefreshError = nil
	inv, err := s.deps.Selector.Resolve(ctx)
	if err != nil {
		s.lastRefreshError = err
		s.deps.Accounting.DiscoveryErrors.Add(1)
		s.deps.Accounting.DiscoveryProblems.Store(int64(max(1, inv.Problems)))
		s.notice(err)
		return nil
	}
	if len(inv.Targets) > s.options.MaxTargets {
		return errors.New("target inventory exceeds capacity")
	}
	present := make(map[int]bool, len(inv.Targets))
	for _, target := range inv.Targets {
		if target.PID <= 0 || target.StartTime == 0 || present[target.PID] {
			return errors.New("invalid or duplicate target inventory")
		}
		present[target.PID] = true
	}
	s.deps.Accounting.SelectedTargets.Store(int64(len(inv.Targets)))
	s.deps.Accounting.DiscoveryProblems.Store(int64(inv.Problems))
	if !inv.Complete || inv.Problems > 0 {
		s.deps.Accounting.DiscoveryErrors.Add(1)
		s.notice(fmt.Errorf("discovery incomplete or unavailable: %d problems", inv.Problems))
	}
	if inv.Complete {
		for pid := range s.workers {
			if !present[pid] {
				if err := s.stop(pid); err != nil {
					return err
				}
			}
		}
	}
	unavailable := 0
	for _, target := range inv.Targets {
		if old := s.workers[target.PID]; old != nil {
			if old.target.Same(target) {
				continue
			}
			if err := s.stop(target.PID); err != nil {
				return err
			}
		}
		if len(s.workers) >= s.options.MaxTargets {
			unavailable++
			s.deps.Accounting.AdmissionErrors.Add(1)
			s.notice(errors.New("worker capacity reached while retaining uncertain targets"))
			continue
		}
		if err := s.start(ctx, target); err != nil {
			if s.lastRefreshError == nil {
				s.lastRefreshError = err
			}
			unavailable++
			s.deps.Accounting.AdmissionErrors.Add(1)
			s.notice(err)
		}
	}
	s.deps.Accounting.UnavailableTargets.Store(int64(unavailable))
	return nil
}

func (s *supervisor) start(ctx context.Context, target control.Target) error {
	session, err := s.deps.Factory.Open(ctx, target)
	if err != nil {
		return fmt.Errorf("open PID %d: %w", target.PID, err)
	}
	if session == nil {
		return fmt.Errorf("open PID %d returned no session", target.PID)
	}
	worker, err := control.NewWorker(s.options.Worker, control.Dependencies{Reader: session, Writer: session.Writer(), Observer: s.deps.Observer, Clock: s.deps.Clock, Accounting: s.deps.Accounting}, target, s.epoch)
	if err != nil {
		return errors.Join(err, session.Close())
	}
	child, cancel := context.WithCancel(ctx)
	r := &running{target: target, cancel: cancel, done: make(chan struct{})}
	s.workers[target.PID] = r
	s.deps.Accounting.ActiveTargets.Add(1)
	go func() {
		runErr := worker.Run(child)
		if child.Err() != nil && runErr == child.Err() {
			runErr = nil
		}
		r.err = errors.Join(runErr, session.Close())
		close(r.done)
		if r.err != nil {
			s.completed <- r
		}
	}()
	return nil
}

func (s *supervisor) stop(pid int) error {
	r := s.workers[pid]
	r.cancel()
	<-r.done
	delete(s.workers, pid)
	s.deps.Accounting.ActiveTargets.Add(-1)
	if r.err != nil {
		return fmt.Errorf("retire PID %d: %w", pid, r.err)
	}
	return nil
}
func (s *supervisor) stopAll() error {
	for _, r := range s.workers {
		r.cancel()
	}
	var result error
	for pid := range s.workers {
		result = errors.Join(result, s.stop(pid))
	}
	return result
}
