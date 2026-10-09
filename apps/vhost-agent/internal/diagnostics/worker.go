// Package diagnostics translates bounded application projections into optional
// evidence and presentation values. No work here runs on a control worker.
package diagnostics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
	"github.com/yckao/virtio-net-recovery/modules/recovery-evidence"
)

var (
	ErrInvalidOptions = errors.New("invalid diagnostic options or dependencies")
	ErrStarted        = errors.New("diagnostics already started")
	ErrClosed         = errors.New("diagnostics closed")
)

// Registry is a bounded, nonblocking current-identity lookup. It deliberately
// provides no access to control state or backend resources.
type Registry interface{ Active(uint64) bool }

type Options struct {
	QueueCapacity, MaxStreams int
	ReconcileInterval         time.Duration
}

func DefaultOptions() Options {
	return Options{QueueCapacity: 256, MaxStreams: 1024, ReconcileInterval: time.Second}
}

type Stats struct {
	Offered, Processed, Rejected, IgnoredInactive, Stale, Gaps, LostFrames, RecorderErrors, OutputLost uint64
	Recorders                                                                                          int64
}

type Worker struct {
	options                                                                              Options
	registry                                                                             Registry
	sink                                                                                 report.Sink
	queue                                                                                chan control.Frame
	stop, done                                                                           chan struct{}
	mu                                                                                   sync.Mutex // Only lifecycle/admission; no reducer or sink calls hold it.
	started, closed                                                                      bool
	streams                                                                              map[uint64]*stream
	offered, processed, rejected, ignored, stale, gaps, lost, recorderErrors, outputLost atomic.Uint64
	recorderCount                                                                        atomic.Int64
}

type stream struct {
	recorder     *evidence.Recorder
	sequence     uint64
	lastAt       time.Duration
	lastSampleAt time.Time
	haveSample   bool
	retired      bool
	frame        control.Frame
}

func New(options Options, registry Registry, sink report.Sink) (*Worker, error) {
	if options.QueueCapacity < 1 || options.QueueCapacity > 65536 || options.MaxStreams < 1 || options.MaxStreams > 65536 || options.ReconcileInterval <= 0 || registry == nil || sink == nil {
		return nil, ErrInvalidOptions
	}
	return &Worker{options: options, registry: registry, sink: sink, queue: make(chan control.Frame, options.QueueCapacity),
		stop: make(chan struct{}), done: make(chan struct{}), streams: make(map[uint64]*stream)}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.started {
		return ErrStarted
	}
	w.started = true
	go w.run(ctx)
	return nil
}

// TryOffer admits a copied bounded projection only. On rejection the control
// caller increments its authoritative input-loss counter. Neither the registry,
// evidence reducer nor output sink is called on this path.
func (w *Worker) TryOffer(frame control.Frame) bool {
	if frame.Stream == 0 || frame.Sequence == 0 || frame.At < 0 || frame.EventCount < 0 || frame.EventCount > len(frame.Events) {
		w.rejected.Add(1)
		return false
	}
	// Do not retain the backing allocation of an unbounded discovery name.
	frame.Target.Name = strings.Clone(frame.Target.Name[:min(len(frame.Target.Name), report.MaxNameBytes)])
	if !w.mu.TryLock() {
		w.rejected.Add(1)
		return false
	}
	defer w.mu.Unlock()
	if w.closed {
		w.rejected.Add(1)
		return false
	}
	select {
	case w.queue <- frame:
		w.offered.Add(1)
		return true
	default:
		w.rejected.Add(1)
		return false
	}
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.options.ReconcileInterval)
	defer ticker.Stop()
	defer w.retireAll()
	for {
		select {
		case <-ctx.Done():
			w.stopAdmission()
			w.drain()
			return
		case <-w.stop:
			w.drain()
			return
		case <-ticker.C:
			w.reconcile()
		case frame := <-w.queue:
			w.handle(frame)
		}
	}
}

func (w *Worker) stopAdmission() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.stop)
	}
	w.mu.Unlock()
}

func (w *Worker) drain() {
	for {
		select {
		case frame := <-w.queue:
			w.handle(frame)
		default:
			return
		}
	}
}

func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.stop)
	}
	if !w.started {
		w.started = true
		for {
			select {
			case <-w.queue:
				w.lost.Add(1)
			default:
				close(w.done)
				w.mu.Unlock()
				return nil
			}
		}
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) handle(frame control.Frame) {
	w.processed.Add(1)
	w.reconcile() // Retired queued frames may never resurrect a generation.
	if !w.registry.Active(frame.Stream) {
		w.ignored.Add(1)
		w.lost.Add(1)
		return
	}
	s := w.streams[frame.Stream]
	if s == nil {
		if len(w.streams) >= w.options.MaxStreams {
			w.lost.Add(1)
			return
		}
		r, err := evidence.New(evidence.DefaultOptions(), evidence.StreamID(frame.Stream))
		if err != nil {
			w.recorderErrors.Add(1)
			w.lost.Add(1)
			return
		}
		s = &stream{recorder: r}
		w.streams[frame.Stream] = s
		w.recorderCount.Add(1)
	}
	if s.retired {
		w.ignored.Add(1)
		w.lost.Add(1)
		return
	}
	if frame.Sequence <= s.sequence {
		w.stale.Add(1)
		return
	}
	if frame.At < s.lastAt {
		w.disable(s, frame)
		return
	}
	s.frame = frame
	if frame.Sequence-s.sequence > 1 {
		missing := frame.Sequence - s.sequence - 1
		w.gaps.Add(1)
		w.lost.Add(missing)
		w.apply(s, evidence.GapObserved{At: frame.At, Lost: missing})
		// The current frame is an explicit state resynchronization. Its sample
		// establishes a new baseline; no pre-gap progress is inferred.
		s.haveSample = false
	}
	s.sequence, s.lastAt = frame.Sequence, frame.At
	if frame.Retired {
		w.apply(s, evidence.Retired{At: s.lastAt})
		// Keep a bounded tombstone until the authoritative registry agrees;
		// an older queued frame must not resurrect this retired recorder.
		s.recorder, s.retired = nil, true
		return
	}
	if frame.HasSample && (!s.haveSample || frame.Sample.At.After(s.lastSampleAt)) {
		observation := evidence.Sample{At: frame.At, Avail: frame.Sample.Avail, Used: frame.Sample.Used,
			Valid:  frame.Sample.Num > 0 && uint32(frame.Sample.Avail-frame.Sample.Used) <= frame.Sample.Num,
			Source: evidence.SourceCached}
		if frame.Sample.Live {
			observation.Source = evidence.SourceLive
			observation.Consumed, observation.ConsumedFresh = frame.Sample.Consumed, true
			observation.WorkQueued, observation.WorkQueuedFresh = frame.Sample.WorkQueued, true
		}
		w.apply(s, evidence.SampleObserved{Sample: observation})
		s.lastSampleAt, s.haveSample = frame.Sample.At, true
	}
	if frame.CandidateKnown {
		w.apply(s, evidence.CandidateStateObserved{At: frame.At, Active: frame.Candidate})
	}
	for _, event := range frame.Events[:frame.EventCount] {
		record := base(frame)
		record.Attempt, record.Latency = event.AttemptID, event.Latency
		record.Reason = decisionName(event.Decision)
		switch event.Kind {
		case control.EventAction:
			record.Kind, record.Outcome = report.Action, outcomeName(event.Outcome)
			w.emit(record)
			if action, ok := actionOutcome(event.Outcome); ok && event.AttemptID != 0 {
				w.apply(s, evidence.ActionObserved{At: frame.At, AttemptID: evidence.AttemptID(event.AttemptID), Outcome: action})
			}
		case control.EventDecision:
			record.Kind = report.Decision
			w.emit(record)
		case control.EventVerified, control.EventUnconfirmed:
			if event.AttemptID != 0 {
				verdict := evidence.VerificationProgress
				if event.Kind == control.EventUnconfirmed {
					verdict = evidence.VerificationUnconfirmed
				}
				w.apply(s, evidence.VerificationObserved{At: frame.At, AttemptID: evidence.AttemptID(event.AttemptID), Outcome: verdict})
			}
		}
	}
	w.apply(s, evidence.TimeAdvanced{At: frame.At})
}

func (w *Worker) apply(s *stream, input evidence.Input) {
	if s.recorder == nil {
		return
	}
	records, err := s.recorder.Apply(input)
	if err != nil {
		w.disable(s, s.frame)
		return
	}
	for _, record := range records {
		w.emit(fromEvidence(s.frame, record))
	}
}

func (w *Worker) disable(s *stream, frame control.Frame) {
	if s.recorder == nil {
		return
	}
	s.recorder = nil // Keep one bounded tombstone until registry retirement.
	w.recorderErrors.Add(1)
	record := base(frame)
	record.Kind, record.Incomplete, record.Message = report.Status, true, "evidence recorder disabled after invalid input"
	w.emit(record)
}

func (w *Worker) reconcile() {
	for id, s := range w.streams {
		if !w.registry.Active(id) {
			w.retire(id, s)
		}
	}
}

func (w *Worker) retire(id uint64, s *stream) {
	w.apply(s, evidence.Retired{At: s.lastAt})
	delete(w.streams, id)
	w.recorderCount.Add(-1)
}

func (w *Worker) retireAll() {
	for id, s := range w.streams {
		w.retire(id, s)
	}
}

func (w *Worker) emit(record report.Record) {
	if !w.sink.TryWrite(record) {
		w.outputLost.Add(1)
	}
}

func (w *Worker) Snapshot() Stats {
	return Stats{Offered: w.offered.Load(), Processed: w.processed.Load(), Rejected: w.rejected.Load(), IgnoredInactive: w.ignored.Load(), Stale: w.stale.Load(),
		Gaps: w.gaps.Load(), LostFrames: w.lost.Load(), RecorderErrors: w.recorderErrors.Load(), OutputLost: w.outputLost.Load(), Recorders: w.recorderCount.Load()}
}
