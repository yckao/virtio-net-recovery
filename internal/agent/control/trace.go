package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

var ErrTraceDelivery = errors.New("trace delivery incomplete")
var ErrTraceIncomplete = errors.New("trace did not capture every selected target")

type TraceOptions struct {
	Duration, Cadence     time.Duration
	MaxTargets, MaxQueues int
}

// TraceTarget transfers ownership of both acquired read-only capabilities to
// RunTrace. Its caller must not close or reuse them after the transfer.
type TraceTarget struct {
	Target Target
	Reader Reader
	Tracer Tracer
}

type TraceFrame struct {
	Target  Target
	Samples []TraceSample
}

// TraceSink receives an owned, bounded sample frame. Delivery must respect ctx;
// an error ends this capture and cannot affect an unrelated recovery worker.
type TraceSink interface {
	Deliver(context.Context, TraceFrame) error
}

type frozenTrace struct {
	TraceTarget
	queues []Queue
	index  map[Queue]int
}

// RunTrace inventories each selected generation once, freezes its queue set,
// and owns pacing, capture completion and all acquired capability cleanup.
// Duration expiry and ordinary cancellation end capture normally after at least
// one complete sweep of all targets. An incomplete initial sweep, failed required observation, incomplete
// frame or delivery ends capture with error.
// Cleanup errors are always returned, including after normal cancellation.
// Context bounds cannot interrupt a kernel syscall already executing.
func RunTrace(ctx context.Context, options TraceOptions, targets []TraceTarget, clock Clock, sink TraceSink) (result error) {
	defer func() {
		for _, t := range targets {
			if t.Tracer != nil {
				result = errors.Join(result, t.Tracer.Close())
			}
			if t.Reader != nil {
				result = errors.Join(result, t.Reader.Close())
			}
		}
	}()
	if options.Duration <= 0 || options.Duration > time.Hour || options.Cadence <= 0 ||
		options.MaxTargets < 1 || options.MaxTargets > 4096 || options.MaxQueues < 1 || options.MaxQueues > 4096 ||
		len(targets) == 0 || len(targets) > options.MaxTargets || clock == nil || sink == nil {
		return errors.New("invalid trace options or dependencies")
	}
	seen := make(map[int]bool, len(targets))
	for _, t := range targets {
		if t.Target.PID <= 0 || t.Target.StartTime == 0 || t.Reader == nil || t.Tracer == nil || seen[t.Target.PID] {
			return errors.New("trace requires distinct acquired process generations")
		}
		seen[t.Target.PID] = true
	}
	bounded, cancel := context.WithTimeout(ctx, options.Duration)
	defer cancel()
	sweeps := 0
	frozen, err := freezeTrace(bounded, targets, options.MaxQueues)
	if err != nil {
		return err
	}
	for {
		if bounded.Err() != nil {
			return traceEnd(ctx, bounded, sweeps)
		}
		start := clock.Now()
		for _, t := range frozen {
			if bounded.Err() != nil && err == bounded.Err() {
				return traceEnd(ctx, bounded, sweeps)
			}
			samples, err := t.Tracer.Read(bounded, t.queues)
			if err != nil {
				if bounded.Err() != nil && err == bounded.Err() {
					return traceEnd(ctx, bounded, sweeps)
				}
				return fmt.Errorf("trace PID %d: %w", t.Target.PID, err)
			}
			frame, err := t.frame(samples)
			if err != nil {
				return fmt.Errorf("trace PID %d: %w", t.Target.PID, err)
			}
			if err := sink.Deliver(bounded, frame); err != nil {
				return errors.Join(ErrTraceDelivery, err)
			}
		}
		sweeps++
		delay := max(0, options.Cadence-clock.Now().Sub(start))
		if err := clock.Wait(bounded, delay); err != nil {
			if bounded.Err() != nil && err == bounded.Err() {
				return traceEnd(ctx, bounded, sweeps)
			}
			return err
		}
	}
}

func freezeTrace(ctx context.Context, targets []TraceTarget, maxQueues int) ([]frozenTrace, error) {
	frozen := make([]frozenTrace, 0, len(targets))
	total := 0
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inventory, err := target.Reader.Inventory(ctx)
		if err != nil {
			return nil, fmt.Errorf("trace inventory PID %d: %w", target.Target.PID, err)
		}
		if !inventory.Complete || inventory.Truncated || len(inventory.Unavailable) != 0 || len(inventory.Queues) == 0 {
			return nil, fmt.Errorf("trace PID %d requires complete nonempty queue inventory", target.Target.PID)
		}
		if len(inventory.Queues) > maxQueues-total {
			return nil, errors.New("trace queue capacity exceeded")
		}
		total += len(inventory.Queues)
		t := frozenTrace{TraceTarget: target, queues: slices.Clone(inventory.Queues), index: make(map[Queue]int, len(inventory.Queues))}
		slots := make(map[int]bool, len(inventory.Queues))
		for i, q := range t.queues {
			if q.Slot < 0 || q.Generation == 0 || slots[q.Slot] {
				return nil, errors.New("trace inventory contains invalid or duplicate queue slots")
			}
			slots[q.Slot] = true
			t.index[q] = i
		}
		frozen = append(frozen, t)
	}
	return frozen, nil
}

func (t frozenTrace) frame(samples []TraceSample) (TraceFrame, error) {
	if len(samples) != len(t.queues) {
		return TraceFrame{}, errors.New("trace sample set is incomplete")
	}
	frame := TraceFrame{Target: t.Target, Samples: make([]TraceSample, len(samples))}
	seen := make([]bool, len(samples))
	for _, s := range samples {
		index, ok := t.index[s.Sample.Queue]
		if !ok || seen[index] {
			return TraceFrame{}, errors.New("trace sample has a duplicate or unexpected generation")
		}
		seen[index] = true
		frame.Samples[index] = s
	}
	return frame, nil
}

func traceEnd(parent, bounded context.Context, sweeps int) error {
	if sweeps == 0 {
		return errors.Join(ErrTraceIncomplete, bounded.Err())
	}
	if parent.Err() != nil && !errors.Is(parent.Err(), context.Canceled) {
		return parent.Err()
	}
	if errors.Is(bounded.Err(), context.Canceled) || errors.Is(bounded.Err(), context.DeadlineExceeded) {
		return nil
	}
	return bounded.Err()
}
