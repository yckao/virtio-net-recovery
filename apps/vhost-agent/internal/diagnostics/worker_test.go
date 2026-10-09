package diagnostics_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/diagnostics"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/jsonlog"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
)

type capture struct {
	mu      sync.Mutex
	records []report.Record
}

func (c *capture) TryWrite(r report.Record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Samples = append([]report.Sample(nil), r.Samples...)
	c.records = append(c.records, r)
	return true
}
func (c *capture) all() []report.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]report.Record(nil), c.records...)
}

func wait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func makeWorker(t *testing.T, o diagnostics.Options, r diagnostics.Registry, s report.Sink) *diagnostics.Worker {
	t.Helper()
	w, err := diagnostics.New(o, r, s)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func start(t *testing.T, w *diagnostics.Worker) {
	t.Helper()
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func stop(t *testing.T, w *diagnostics.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
func offer(t *testing.T, w *diagnostics.Worker, f control.Frame) {
	t.Helper()
	if !w.TryOffer(f) {
		t.Fatal("unexpected admission refusal")
	}
}

func frame(stream, seq uint64, at time.Duration, used uint16) control.Frame {
	q := control.Queue{Slot: 3, Generation: stream}
	return control.Frame{Stream: stream, Sequence: seq, Target: control.Target{PID: 12, StartTime: 34}, Queue: q, At: at,
		HasSample: true, CandidateKnown: true, Candidate: true, Sample: control.Sample{Queue: q, At: time.Unix(100, 0).Add(at), Num: 256, Avail: used + 10, Used: used}}
}

type forbiddenRegistry struct{}

func (forbiddenRegistry) Active(uint64) bool { panic("registry called on producer") }

type forbiddenSink struct{}

func (forbiddenSink) TryWrite(report.Record) bool { panic("sink called on producer") }

func TestTryOfferOnlyCopiesBoundedFrames(t *testing.T) {
	o := diagnostics.DefaultOptions()
	o.QueueCapacity = 1
	w := makeWorker(t, o, forbiddenRegistry{}, forbiddenSink{})
	if !w.TryOffer(frame(1, 1, 0, 10)) || w.TryOffer(frame(1, 2, time.Second, 10)) {
		t.Fatal("queue capacity ignored")
	}
	if w.Snapshot().Processed != 0 {
		t.Fatal("producer ran telemetry")
	}
	stop(t, w)
	if w.TryOffer(frame(1, 3, 2*time.Second, 10)) {
		t.Fatal("closed worker admitted frame")
	}
}

func TestGapResynchronizesWithoutCrossGapProgress(t *testing.T) {
	var registry control.Registry
	registry.Add(1)
	sink := &capture{}
	w := makeWorker(t, diagnostics.DefaultOptions(), &registry, sink)
	start(t, w)
	defer stop(t, w)
	offer(t, w, frame(1, 1, 0, 10))
	wait(t, func() bool { return len(sink.all()) >= 1 })
	offer(t, w, frame(1, 4, time.Second, 30))
	wait(t, func() bool { return w.Snapshot().Gaps == 1 && len(sink.all()) >= 4 })
	var opened []report.Record
	var gap, closed bool
	for _, r := range sink.all() {
		if r.Kind == report.Candidate {
			opened = append(opened, r)
		}
		if r.Kind == report.Gap && r.LostInputs == 2 {
			gap = true
		}
		if r.Kind == report.EpisodeClosed && r.Reason == "gap" && r.Incomplete {
			closed = true
		}
	}
	if !gap || !closed || len(opened) != 2 || len(opened[1].Samples) != 1 || opened[1].Samples[0].Used != 30 || opened[1].UsedProgress {
		t.Fatalf("invalid gap handling: %+v", sink.all())
	}
	if w.Snapshot().RecorderErrors != 0 {
		t.Fatal(w.Snapshot())
	}
}

func TestFreshSampleTimestampIsNormalizedAndStaleSamplesIgnored(t *testing.T) {
	var registry control.Registry
	registry.Add(1)
	sink := &capture{}
	w := makeWorker(t, diagnostics.DefaultOptions(), &registry, sink)
	start(t, w)
	f := frame(1, 1, time.Second, 10)
	f.Sample.At = time.Unix(1, 0)
	offer(t, w, f)
	wait(t, func() bool { return len(sink.all()) >= 1 })
	f.Sequence, f.At, f.Sample.Used = 2, 2*time.Second, 99 // Same sample timestamp; changed fields are not fresh evidence.
	offer(t, w, f)
	wait(t, func() bool { return w.Snapshot().Processed >= 2 })
	stop(t, w)
	for _, r := range sink.all() {
		if r.Kind == report.EpisodeClosed && (r.UsedProgress || r.Samples[0].Used != 10 || r.Samples[0].At != time.Second) {
			t.Fatalf("stale sample admitted: %+v", r)
		}
	}
	if w.Snapshot().RecorderErrors != 0 {
		t.Fatal(w.Snapshot())
	}
}

func TestRegistryReconcilesLostRetirementAndRejectsStaleQueuedGeneration(t *testing.T) {
	var registry control.Registry
	registry.Add(1)
	registry.Add(2)
	o := diagnostics.DefaultOptions()
	o.MaxStreams = 1
	o.ReconcileInterval = 5 * time.Millisecond
	sink := &capture{}
	w := makeWorker(t, o, &registry, sink)
	start(t, w)
	defer stop(t, w)
	offer(t, w, frame(1, 1, 0, 10))
	wait(t, func() bool { return w.Snapshot().Recorders == 1 })
	offer(t, w, frame(2, 1, 0, 20))
	wait(t, func() bool { return w.Snapshot().LostFrames >= 1 })
	if w.Snapshot().Recorders != 1 {
		t.Fatal("recorder limit exceeded")
	}
	registry.Remove(1) // No retirement frame is sent.
	wait(t, func() bool { return w.Snapshot().Recorders == 0 })
	offer(t, w, frame(1, 2, time.Second, 30))
	wait(t, func() bool { return w.Snapshot().IgnoredInactive >= 1 })
	if w.Snapshot().Recorders != 0 {
		t.Fatal("retired generation resurrected")
	}
	offer(t, w, frame(2, 2, time.Second, 20))
	wait(t, func() bool { return w.Snapshot().Recorders == 1 })
}

func TestExplicitRetirementCannotReopenWhileRegistryCatchesUp(t *testing.T) {
	var registry control.Registry
	registry.Add(1)
	opts := diagnostics.DefaultOptions()
	opts.ReconcileInterval = 5 * time.Millisecond
	sink := &capture{}
	w := makeWorker(t, opts, &registry, sink)
	start(t, w)
	defer stop(t, w)
	offer(t, w, frame(1, 1, 0, 10))
	retired := frame(1, 2, time.Second, 10)
	retired.Retired = true
	offer(t, w, retired)
	wait(t, func() bool {
		for _, record := range sink.all() {
			if record.Kind == report.Retired {
				return true
			}
		}
		return false
	})
	offer(t, w, frame(1, 3, 2*time.Second, 20))
	wait(t, func() bool { return w.Snapshot().IgnoredInactive == 1 })
	var opened int
	for _, record := range sink.all() {
		if record.Kind == report.Candidate {
			opened++
		}
	}
	if opened != 1 || w.Snapshot().Recorders != 1 {
		t.Fatal("retired stream did not retain one tombstone", opened, w.Snapshot())
	}
	registry.Remove(1)
	wait(t, func() bool { return w.Snapshot().Recorders == 0 })
}

type blockedWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (b *blockedWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return len(p), nil
}

func TestBlockedDestinationDoesNotBlockEvidenceOrShutdown(t *testing.T) {
	output := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	o := jsonlog.DefaultOptions()
	o.MaxRecords = 2
	writer, err := jsonlog.New(o, output)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var registry control.Registry
	registry.Add(1)
	w := makeWorker(t, diagnostics.DefaultOptions(), &registry, writer)
	start(t, w)
	offer(t, w, frame(1, 1, 0, 10))
	select {
	case <-output.entered:
	case <-time.After(time.Second):
		t.Fatal("destination never entered")
	}
	for i := 2; i <= 20; i++ {
		f := frame(1, uint64(i), time.Duration(i)*time.Second, uint16(i))
		offer(t, w, f)
	}
	wait(t, func() bool { return w.Snapshot().Processed == 20 })
	if writer.Snapshot().Dropped == 0 {
		t.Fatal("blocked output did not report drops")
	}
	stop(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err = writer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	close(output.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = writer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

type deterministicClock struct{ at time.Time }

func (c *deterministicClock) Now() time.Time { return c.at }
func (c *deterministicClock) Wait(ctx context.Context, delay time.Duration) error {
	c.at = c.at.Add(delay)
	return ctx.Err()
}

type recoveringBackend struct {
	clock  *deterministicClock
	epoch  time.Time
	starts []time.Duration
}

func (b *recoveringBackend) Inventory(context.Context) (control.Inventory, error) {
	return control.Inventory{Queues: []control.Queue{{Slot: 1, Generation: 1}}, Complete: true}, nil
}
func (b *recoveringBackend) sample(live bool) control.Sample {
	return control.Sample{Queue: control.Queue{Slot: 1, Generation: 1}, At: b.clock.Now(),
		Num: 256, Avail: 10, Used: 3, Consumed: 4, Live: live}
}
func (b *recoveringBackend) Sample(context.Context, []control.Queue) ([]control.Sample, error) {
	return []control.Sample{b.sample(false)}, nil
}
func (b *recoveringBackend) Inspect(context.Context, control.Queue, uint16) control.Result {
	s := b.sample(true)
	return control.Result{Outcome: control.Observed, Decision: control.Pending, Live: &s, CompletedAt: b.clock.Now()}
}
func (b *recoveringBackend) Notify(context.Context, control.Queue, uint16) control.Result {
	b.starts = append(b.starts, b.clock.Now().Sub(b.epoch))
	b.clock.at = b.clock.at.Add(50 * time.Millisecond)
	s := b.sample(true)
	return control.Result{Outcome: control.Accepted, Decision: control.Pending, Live: &s,
		AcceptedAt: b.clock.Now(), CompletedAt: b.clock.Now()}
}
func (*recoveringBackend) Close() error { return nil }

func TestBlockedOutputLeavesActualControlPacingAndAcceptedCountsUnchanged(t *testing.T) {
	intervals := []time.Duration{0, 100 * time.Millisecond, 99 * time.Millisecond, time.Millisecond}
	for range 20 {
		intervals = append(intervals, 100*time.Millisecond)
	}
	run := func(reporter control.Reporter, registry *control.Registry, afterSecond func()) (control.Counts, []time.Duration) {
		clock := &deterministicClock{at: time.Unix(100, 0)}
		backend := &recoveringBackend{clock: clock, epoch: clock.Now()}
		accounting := &control.Accounting{}
		worker, err := control.NewWorker(control.Options{Recover: true, Cadence: 100 * time.Millisecond,
			InventoryInterval: 5 * time.Second, VerificationTimeout: time.Second, MaxQueues: 1},
			control.Dependencies{Reader: backend, Writer: backend, Reporter: reporter, Clock: clock, Accounting: accounting, Registry: registry},
			control.Target{PID: 123, StartTime: 456}, clock.Now(), 1)
		if err != nil {
			t.Fatal(err)
		}
		for i, interval := range intervals {
			clock.at = clock.at.Add(interval)
			if err := worker.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if i == 1 && afterSecond != nil {
				afterSecond()
			}
		}
		return accounting.Snapshot(), backend.starts
	}
	baseline, expectedStarts := run(nil, &control.Registry{}, nil)
	output := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	options := jsonlog.DefaultOptions()
	options.MaxRecords = 2
	writer, err := jsonlog.New(options, output)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var registry control.Registry
	diagnostic := makeWorker(t, diagnostics.DefaultOptions(), &registry, writer)
	start(t, diagnostic)
	actual, starts := run(diagnostic, &registry, func() {
		select {
		case <-output.entered:
		case <-time.After(time.Second):
			t.Fatal("output was not blocked before remaining control cycles")
		}
	})
	// InputLost is deliberately allowed to differ; optional reporting admission
	// must not affect any authoritative recovery, observation or timing result.
	actual.InputLost = 0
	if actual != baseline || !reflect.DeepEqual(starts, expectedStarts) || actual.Accepted < 2 {
		t.Fatalf("reporting changed control behavior: actual=%+v baseline=%+v starts=%v want=%v", actual, baseline, starts, expectedStarts)
	}
	for i := 1; i < len(starts); i++ {
		if starts[i]-starts[i-1] < 150*time.Millisecond {
			t.Fatal("completion pacing violated")
		}
	}
	wait(t, func() bool { stats := diagnostic.Snapshot(); return stats.Processed == stats.Offered })
	if writer.Snapshot().Dropped == 0 {
		t.Fatal("fixture failed to saturate output")
	}
	stop(t, diagnostic)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := writer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	close(output.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
