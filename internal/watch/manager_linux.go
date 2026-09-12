//go:build linux && amd64

package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"vhost-watch/internal/selection"
)

type ManagerConfig struct {
	Agent      Config
	Selector   *selection.Selector
	Refresh    time.Duration
	List       bool
	ListQueues bool
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

type worker struct {
	target selection.Target
	cancel context.CancelFunc
	done   chan error
}

// RunSelected keeps one BPF snapshot probe and independent per-VM watchdogs.
func RunSelected(ctx context.Context, cfg ManagerConfig, output io.Writer) error {
	if cfg.Selector == nil || cfg.Refresh <= 0 {
		return errors.New("selector and positive target refresh interval required")
	}
	base := cfg.Agent
	base.PID = 1 // Validate the policy before discovering any processes.
	if err := base.Validate(); err != nil {
		return err
	}
	if base.Rescue && cfg.List {
		return errors.New("--once and --list cannot be combined")
	}
	if cfg.List && cfg.ListQueues {
		return errors.New("choose --list or --list-queues")
	}
	if base.Rescue && cfg.ListQueues {
		return errors.New("--once and --list-queues cannot be combined")
	}
	writer := &lockedWriter{w: output}
	log := &logger{encoder: json.NewEncoder(writer)}
	targets, problems, err := cfg.Selector.Resolve()
	if err != nil {
		return err
	}
	for _, err := range problems {
		log.emit("target_unavailable", map[string]any{"error": err.Error()})
	}
	if cfg.List {
		for _, t := range targets {
			log.emit("target", map[string]any{"pid": t.PID, "domain": t.Domain, "uuid": t.UUID, "start_time": t.StartTime})
		}
		if len(targets) == 0 || len(problems) > 0 {
			return errors.New("selection contains unavailable targets or has no matches")
		}
		return log.err
	}
	if (base.Rescue || cfg.ListQueues) && len(targets) == 0 {
		return errors.New("no selected running QEMU targets")
	}
	if base.VhostFD >= 0 && len(targets) != 1 {
		return errors.New("--vhost-fd requires exactly one selected VM")
	}
	bpf, err := OpenBPF(base.BPFObject, base.TraceStages)
	if err != nil {
		return err
	}
	defer bpf.Close()
	policy := func(t selection.Target) Config {
		c := cfg.Agent
		c.PID = t.PID
		c.Domain = t.Domain
		c.CheckIdentity = func() error { return cfg.Selector.Check(t) }
		return c
	}
	if base.Rescue || cfg.ListQueues {
		failures := len(problems)
		for _, t := range targets {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var e error
			if cfg.ListQueues {
				target, openErr := OpenTarget(t.PID)
				e = openErr
				if e == nil {
					e = cfg.Selector.Check(t)
					if e == nil {
						var rows []QueueRow
						rows, e = ListQueues(target, bpf)
						if e == nil {
							log.emit("queues", map[string]any{"pid": t.PID, "domain": t.Domain, "queues": rows})
						}
					}
					target.Close()
				}
			} else {
				e = RunWithBPF(ctx, policy(t), writer, bpf)
			}
			if e != nil {
				failures++
				log.emit("target_failed", map[string]any{"pid": t.PID, "domain": t.Domain, "error": e.Error()})
			}
		}
		if failures > 0 {
			return fmt.Errorf("%d selected target(s) failed", failures)
		}
		return log.err
	}
	if cfg.Agent.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Agent.Duration*float64(time.Second)))
		defer cancel()
	}
	workers := map[string]*worker{}
	defer func() {
		for _, w := range workers {
			w.cancel()
		}
		for _, w := range workers {
			<-w.done
		}
	}()
	// An exited explicit PID stays retired; domain selection can attach its new
	// process generation. Failed workers retry only at the discovery cadence.
	retired := map[string]bool{}
	ticker := time.NewTicker(cfg.Refresh)
	defer ticker.Stop()
	for {
		wanted := map[string]selection.Target{}
		for _, t := range targets {
			wanted[t.Key()] = t
		}
		for key, w := range workers {
			if _, ok := wanted[key]; !ok {
				w.cancel()
				<-w.done
				delete(workers, key)
				log.emit("target_removed", map[string]any{"pid": w.target.PID, "domain": w.target.Domain})
				continue
			}
			select {
			case e := <-w.done:
				delete(workers, key)
				if e == nil {
					retired[key] = true
				}
				fields := map[string]any{"pid": w.target.PID, "domain": w.target.Domain}
				if e != nil {
					fields["error"] = e.Error()
				}
				log.emit("target_stopped", fields)
			default:
			}
		}
		for index, t := range targets {
			if workers[t.Key()] != nil || retired[t.Key()] {
				continue
			}
			c := policy(t)
			c.Duration = 0
			child, cancel := context.WithCancel(ctx)
			w := &worker{target: t, cancel: cancel, done: make(chan error, 1)}
			workers[t.Key()] = w
			log.emit("target_selected", map[string]any{"pid": t.PID, "domain": t.Domain, "uuid": t.UUID, "start_time": t.StartTime})
			delay := time.Duration(float64(index) * c.Interval / float64(max(1, len(targets))) * float64(time.Second))
			go func() {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-child.Done():
					w.done <- nil
					return
				case <-timer.C:
				}
				w.done <- RunWithBPF(child, c, writer, bpf)
			}()
		}
		if log.err != nil {
			return log.err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		targets, problems, err = cfg.Selector.Resolve()
		if err != nil {
			// Keep existing pidfds while discovery is temporarily unavailable.
			log.emit("discovery_failed", map[string]any{"error": err.Error()})
			targets = nil
			for _, w := range workers {
				targets = append(targets, w.target)
			}
		}
		for _, e := range problems {
			log.emit("target_unavailable", map[string]any{"error": e.Error()})
		}
	}
}
