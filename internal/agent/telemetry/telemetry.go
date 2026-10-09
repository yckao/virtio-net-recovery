// Package telemetry owns the application's optional evidence and JSON pipeline.
// Control supplies observations; it never runs a recorder, encoder or writer.
package telemetry

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

const (
	InputCapacity  = 256
	OutputCapacity = 64
	MaxRecordBytes = 64 << 10
	maxText        = 256
)

type Stats struct {
	InputLost, OutputLost, RecorderErrors, Processed uint64
	Streams                                          int
	Failed, Incomplete                               bool
}

// Telemetry has one input actor and one writer. Close is called after all
// producers have joined. An arbitrary blocked io.Writer cannot be cancelled;
// Close returns its context error instead of waiting indefinitely for Write.
type Telemetry struct {
	input                                            chan input
	output                                           chan record
	wake, stop, done                                 chan struct{}
	closeOnce                                        sync.Once
	closing, abort, failed, incomplete               atomic.Bool
	streamLimit                                      int64
	streamCount                                      atomic.Int64
	nextID                                           atomic.Uint64
	inputLost, outputLost, recorderErrors, processed atomic.Uint64
}

type input struct {
	scope    *scope
	sequence uint64
	frame    control.Frame
	notice   string
}

// Observe and Close on one scope are serialized by its owning control worker.
// The actor alone owns the corresponding recorder. pending bridges their
// lifetimes without asking control to manage stream IDs or retirement messages.
type scope struct {
	owner    *Telemetry
	id       uint64
	target   control.Target
	queue    control.Queue
	sequence uint64
	accepted bool
	closed   atomic.Bool
	pending  atomic.Int64
	closeAt  time.Duration
}

func Start(out io.Writer, maxStreams int) (*Telemetry, error) {
	if out == nil || maxStreams < 1 || maxStreams > 4096 {
		return nil, errors.New("telemetry requires a writer and stream limit in 1..4096")
	}
	t := &Telemetry{input: make(chan input, InputCapacity), output: make(chan record, OutputCapacity),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		streamLimit: int64(maxStreams + InputCapacity)}
	go t.write(out)
	go t.run()
	return t, nil
}

func (t *Telemetry) Open(target control.Target, queue control.Queue) control.QueueObserver {
	if t.closing.Load() {
		return discarded{t}
	}
	if t.streamCount.Add(1) > t.streamLimit {
		t.streamCount.Add(-1)
		return discarded{t}
	}
	target.Name = copyText(target.Name, 128)
	return &scope{owner: t, id: t.nextID.Add(1), target: target, queue: queue}
}

type discarded struct{ owner *Telemetry }

func (d discarded) Observe(control.Frame) { d.owner.inputLost.Add(1) }
func (discarded) Close(time.Duration)     {}

func (s *scope) Observe(frame control.Frame) {
	t := s.owner
	if s.closed.Load() || t.closing.Load() {
		t.inputLost.Add(1)
		return
	}
	s.sequence++
	if frame.At < 0 || frame.EventCount < 0 || frame.EventCount > len(frame.Events) {
		t.inputLost.Add(1)
		return
	}
	for i := range frame.EventCount {
		frame.Events[i].Error = copyText(frame.Events[i].Error, maxText)
	}
	clear(frame.Events[frame.EventCount:])
	s.pending.Add(1)
	select {
	case t.input <- input{scope: s, sequence: s.sequence, frame: frame}:
		s.accepted = true
	default:
		s.pending.Add(-1)
		t.inputLost.Add(1)
	}
}

func (s *scope) Close(at time.Duration) {
	if s.closed.Load() {
		return
	}
	s.closeAt = at
	s.closed.Store(true)
	if !s.accepted {
		s.owner.streamCount.Add(-1)
		return
	}
	select {
	case s.owner.wake <- struct{}{}:
	default:
	}
}

func (t *Telemetry) TryNotice(text string) bool {
	if !t.closing.Load() {
		select {
		case t.input <- input{notice: copyText(text, maxText)}:
			return true
		default:
		}
	}
	t.inputLost.Add(1)
	return false
}

func (t *Telemetry) Close(ctx context.Context) error {
	t.closeOnce.Do(func() { t.closing.Store(true); close(t.stop) })
	select {
	case <-t.done:
		if t.failed.Load() {
			return errors.New("telemetry destination failed")
		}
		if t.incomplete.Load() {
			return errors.New("telemetry drain was interrupted")
		}
		return nil
	case <-ctx.Done():
		t.abort.Store(true)
		t.incomplete.Store(true)
		return ctx.Err()
	}
}

func (t *Telemetry) Snapshot() Stats {
	return Stats{InputLost: t.inputLost.Load(), OutputLost: t.outputLost.Load(), RecorderErrors: t.recorderErrors.Load(),
		Processed: t.processed.Load(), Streams: int(t.streamCount.Load()), Failed: t.failed.Load(),
		Incomplete: t.incomplete.Load() || t.inputLost.Load() != 0 || t.outputLost.Load() != 0 || t.recorderErrors.Load() != 0}
}

func copyText(text string, limit int) string { return strings.Clone(text[:min(len(text), limit)]) }

func (t *Telemetry) run() {
	states := make(map[*scope]*stream)
	defer close(t.output)
	for {
		select {
		case in := <-t.input:
			t.consume(states, in)
		case <-t.wake:
			// Only lifecycle changes scan scopes. Normal frames touch one entry.
			for s, state := range states {
				if s.closed.Load() && s.pending.Load() == 0 {
					t.retire(states, s, state)
				}
			}
		case <-t.stop:
			for len(t.input) > 0 {
				t.consume(states, <-t.input)
			}
			for s, state := range states {
				t.retire(states, s, state)
			}
			t.streamCount.Store(0)
			return
		}
	}
}

func (t *Telemetry) consume(states map[*scope]*stream, in input) {
	defer t.processed.Add(1)
	if in.scope == nil {
		t.emit(record{Event: "status", Message: in.notice})
		return
	}
	s := in.scope
	state := states[s]
	if state == nil {
		state = t.newStream(s)
		states[s] = state
	}
	t.observe(s, state, in)
	s.pending.Add(-1)
	if s.closed.Load() && s.pending.Load() == 0 {
		t.retire(states, s, state)
	}
}

func (t *Telemetry) emit(value record) {
	select {
	case t.output <- value:
	default:
		t.outputLost.Add(1)
	}
}
