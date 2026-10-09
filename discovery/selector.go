package discovery

import (
	"context"
	"errors"
	"slices"
)

// Resolve returns the observations collected before a directory or cancellation
// failure as well as the error. Only Complete inventories authorize retirement.
func (s *Selector) Resolve(ctx context.Context) (Result, error) {
	r := resolution{result: Result{Complete: true}, targets: make(map[int]Target), limit: s.options.MaxTargets}
	if err := s.explicit(ctx, &r); err != nil {
		r.result.Complete = false
		return r.finish(), err
	}
	if s.pattern != nil {
		if err := s.domains(ctx, &r); err != nil {
			r.result.Complete = false
			return r.finish(), err
		}
	}
	return r.finish(), nil
}

// resolution owns admission and the bounded result for one discovery pass.
// It is discarded after Resolve; only explicit PID anchors survive a pass.
type resolution struct {
	result  Result
	targets map[int]Target
	limit   int
}

func (r *resolution) add(t Target) {
	if _, exists := r.targets[t.PID]; !exists && len(r.targets) >= r.limit {
		r.result.Complete = false
		r.result.problem(t.PID, "", errors.New("target limit reached"))
		return
	}
	r.targets[t.PID] = t
}

func (r *resolution) finish() Result {
	for _, t := range r.targets {
		r.result.Targets = append(r.result.Targets, t)
	}
	slices.SortFunc(r.result.Targets, func(a, b Target) int { return a.PID - b.PID })
	return r.result
}

func (r *resolution) excluded(pid int, domain string, err error, definitive bool) {
	if !definitive {
		r.result.Complete = false
	}
	r.result.problem(pid, domain, err)
}

func (s *Selector) explicit(ctx context.Context, r *resolution) error {
	for _, pid := range s.options.PIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, err := s.process(pid)
		definitive := definitiveExclusion(err)
		if err == nil {
			if old, anchored := s.anchored[pid]; anchored && old != t.StartTime {
				err = errors.New("explicit PID generation replaced")
				definitive = true
			}
		}
		if err != nil {
			r.excluded(pid, "", err, definitive)
			continue
		}
		s.anchored[pid] = t.StartTime
		r.add(t)
	}
	return nil
}
