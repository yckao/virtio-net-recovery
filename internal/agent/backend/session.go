// Package backend translates the Linux backend's public contract into the
// application's ports. Native handles stay here and are never reconstructed.
package backend

import (
	"context"
	"errors"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	vhost "github.com/yckao/virtio-net-recovery/vhost"
)

type Session struct {
	host    *vhost.Host
	read    vhost.ReadSession
	handles map[control.Queue]vhost.Queue
}

func Open(ctx context.Context, host *vhost.Host, target control.Target) (*Session, error) {
	read, err := host.OpenProcess(ctx, vhost.ProcessIdentity{PID: target.PID, StartTime: target.StartTime})
	if err != nil {
		return nil, err
	}
	return &Session{host: host, read: read, handles: map[control.Queue]vhost.Queue{}}, nil
}
func (s *Session) Reader() control.Reader { return reader{s} }
func (s *Session) Writer() (control.Writer, error) {
	n, err := s.host.Conditional(s.read)
	if err != nil {
		return nil, err
	}
	return writer{s, n}, nil
}
func (s *Session) Manual() (control.ManualWriter, error) {
	n, err := s.host.Manual(s.read)
	if err != nil {
		return nil, err
	}
	return manual{s, n}, nil
}
func (s *Session) Trace() (control.Tracer, error) {
	t, err := s.host.Trace(s.read)
	if err != nil {
		return nil, err
	}
	return tracer{s, t}, nil
}

type reader struct{ s *Session }

func (r reader) Close() error { return r.s.read.Close() }
func (r reader) Inventory(ctx context.Context) (control.Inventory, error) {
	in, err := r.s.read.Inventory(ctx)
	if err != nil {
		return control.Inventory{}, err
	}
	out := control.Inventory{Complete: in.Complete, Truncated: in.Truncated}
	present := map[int]bool{}
	for _, q := range in.Queues {
		key := queue(q)
		out.Queues = append(out.Queues, key)
		r.s.handles[key] = q
		present[key.Slot] = true
		for old := range r.s.handles {
			if old.Slot == key.Slot && old != key {
				delete(r.s.handles, old)
			}
		}
	}
	for _, p := range in.Problems {
		out.Unavailable = append(out.Unavailable, p.Slot)
		present[p.Slot] = true
	}
	if in.Complete {
		for key := range r.s.handles {
			if !present[key.Slot] {
				delete(r.s.handles, key)
			}
		}
	}
	return out, nil
}
func (r reader) Sample(ctx context.Context, keys []control.Queue) ([]control.Sample, error) {
	native := make([]vhost.Queue, 0, len(keys))
	for _, key := range keys {
		if q, ok := r.s.handles[key]; ok {
			native = append(native, q)
		}
	}
	in, err := r.s.read.Sample(ctx, native)
	if err != nil {
		return nil, err
	}
	out := make([]control.Sample, 0, len(in))
	for _, o := range in {
		out = append(out, sample(o))
	}
	return out, nil
}
func (r reader) Inspect(ctx context.Context, key control.Queue, used uint16) control.Result {
	q, ok := r.s.handles[key]
	if !ok {
		return missing()
	}
	o, err := r.s.read.Inspect(ctx, q)
	if err != nil {
		return control.Result{Outcome: failureOutcome(err), Decision: failureDecision(err), CompletedAt: time.Now(), Err: err}
	}
	live := sample(o)
	return control.Result{Outcome: control.Observed, Decision: decision(vhost.Classify(o, used)), CompletedAt: time.Now(), Live: &live}
}

type writer struct {
	s *Session
	n vhost.ConditionalNotifier
}

func (w writer) Notify(ctx context.Context, key control.Queue, used uint16) control.Result {
	q, ok := w.s.handles[key]
	if !ok {
		return missing()
	}
	return result(w.n.Notify(ctx, q, used))
}

type manual struct {
	s *Session
	n vhost.ManualNotifier
}

func (w manual) Kick(ctx context.Context, key control.Queue) control.Result {
	q, ok := w.s.handles[key]
	if !ok {
		return missing()
	}
	return result(w.n.Notify(ctx, q))
}

type tracer struct {
	s *Session
	t vhost.TraceSession
}

func (t tracer) Close() error { return t.t.Close() }
func (t tracer) Read(ctx context.Context, keys []control.Queue) ([]control.TraceSample, error) {
	native := make([]vhost.Queue, 0, len(keys))
	for _, key := range keys {
		q, ok := t.s.handles[key]
		if !ok {
			return nil, vhost.ErrIdentityChanged
		}
		native = append(native, q)
	}
	in, err := t.t.Sample(ctx, native)
	if err != nil {
		return nil, err
	}
	out := make([]control.TraceSample, 0, len(in))
	for _, o := range in {
		out = append(out, control.TraceSample{Sample: sample(o.Observation), Signals: o.Counters.Signals, Writes: o.Counters.Writes, Wakeups: o.Counters.Wakeups, Handlers: o.Counters.Handlers})
	}
	return out, nil
}
func queue(q vhost.Queue) control.Queue {
	return control.Queue{Slot: q.Slot(), Generation: q.Generation()}
}
func sample(o vhost.Observation) control.Sample {
	return control.Sample{Queue: queue(o.Queue), At: o.At, Num: o.Num, Avail: o.Avail, Used: o.Used, Consumed: o.Consumed, Live: o.Live, WorkQueued: o.WorkQueued}
}
func missing() control.Result {
	return control.Result{Outcome: control.ReadFailed, Decision: control.Changed, CompletedAt: time.Now(), Err: vhost.ErrIdentityChanged}
}
func result(r vhost.Result) control.Result {
	out := control.Result{Decision: decision(r.Decision), CompletedAt: r.CompletedAt, Err: r.Err}
	switch r.Write {
	case vhost.WriteAccepted:
		out.Outcome = control.Accepted
		if r.Receipt != nil {
			out.AcceptedAt = r.Receipt.AcceptedAt
		}
	case vhost.WriteFailed:
		out.Outcome = control.WriteFailed
	default:
		switch r.Decision {
		case vhost.Cancelled:
			out.Outcome = control.Aborted
		case vhost.Unavailable, vhost.Unsupported, vhost.IdentityChanged:
			out.Outcome = control.ReadFailed
		default:
			out.Outcome = control.Refused
		}
	}
	if r.Observation != nil && r.Decision != vhost.IdentityChanged {
		o := sample(*r.Observation)
		out.Live = &o
	}
	return out
}
func failureOutcome(err error) control.Outcome {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return control.Aborted
	}
	return control.ReadFailed
}
func failureDecision(err error) control.Decision {
	if errors.Is(err, vhost.ErrIdentityChanged) {
		return control.Changed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return control.Cancelled
	}
	return control.Unavailable
}
func decision(d vhost.PhysicalDecision) control.Decision {
	switch d {
	case vhost.Eligible:
		return control.Pending
	case vhost.InvalidRing:
		return control.Invalid
	case vhost.UsedAdvanced:
		return control.UsedProgress
	case vhost.Drained:
		return control.Drained
	case vhost.WorkQueued:
		return control.Queued
	case vhost.NoPending:
		return control.Consumed
	case vhost.IdentityChanged:
		return control.Changed
	case vhost.Cancelled:
		return control.Cancelled
	case vhost.Contended:
		return control.Busy
	default:
		return control.Unavailable
	}
}
