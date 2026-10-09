package control_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
)

type traceFixture struct {
	inventory                                    control.Inventory
	inventoryCalls, reads                        int
	requested                                    [][]control.Queue
	closed                                       []string
	read                                         func(context.Context, []control.Queue) ([]control.TraceSample, error)
	inventoryErr, readerCloseErr, tracerCloseErr error
}

type traceReader struct{ f *traceFixture }

func (r traceReader) Inventory(context.Context) (control.Inventory, error) {
	r.f.inventoryCalls++
	return r.f.inventory, r.f.inventoryErr
}
func (traceReader) Sample(context.Context, []control.Queue) ([]control.Sample, error) {
	panic("trace must not use cached observations")
}
func (traceReader) Inspect(context.Context, control.Queue, uint16) control.Result {
	panic("trace must use its acquired trace capability")
}
func (r traceReader) Close() error {
	r.f.closed = append(r.f.closed, "reader")
	return r.f.readerCloseErr
}

type traceView struct{ f *traceFixture }

func (v traceView) Read(ctx context.Context, queues []control.Queue) ([]control.TraceSample, error) {
	v.f.reads++
	v.f.requested = append(v.f.requested, slices.Clone(queues))
	if v.f.read != nil {
		return v.f.read(ctx, queues)
	}
	return traceSamples(queues), nil
}
func (v traceView) Close() error {
	v.f.closed = append(v.f.closed, "tracer")
	return v.f.tracerCloseErr
}

func traceSamples(queues []control.Queue) []control.TraceSample {
	var result []control.TraceSample
	for _, q := range queues {
		result = append(result, control.TraceSample{Sample: control.Sample{Queue: q, At: time.Unix(100, 0), Live: true}, Signals: 10})
	}
	return result
}
func newTraceTarget(pid int) (*traceFixture, control.TraceTarget) {
	f := &traceFixture{inventory: control.Inventory{Complete: true, Queues: []control.Queue{{Slot: 7, Generation: 1}, {Slot: 9, Generation: 2}}}}
	return f, control.TraceTarget{Target: control.Target{PID: pid, StartTime: 1000}, Reader: traceReader{f}, Tracer: traceView{f}}
}

type traceSink struct {
	frames  []control.TraceFrame
	deliver func(context.Context, control.TraceFrame) error
}

func (s *traceSink) Deliver(ctx context.Context, frame control.TraceFrame) error {
	s.frames = append(s.frames, frame)
	if s.deliver != nil {
		return s.deliver(ctx, frame)
	}
	return nil
}
func traceOptions() control.TraceOptions {
	return control.TraceOptions{Duration: 250 * time.Millisecond, Cadence: 100 * time.Millisecond, MaxTargets: 4, MaxQueues: 16}
}
func traceClock() *clock { return &clock{at: time.Unix(100, 0)} }
func assertTraceClosed(t *testing.T, f *traceFixture) {
	t.Helper()
	if !reflect.DeepEqual(f.closed, []string{"tracer", "reader"}) {
		t.Fatalf("capabilities not closed exactly once in dependency order: %v", f.closed)
	}
}

func TestTraceFreezesInventoryAndOwnsOrderedFrames(t *testing.T) {
	f, target := newTraceTarget(101)
	expected := slices.Clone(f.inventory.Queues)
	f.read = func(_ context.Context, queues []control.Queue) ([]control.TraceSample, error) {
		f.inventory.Queues[0].Generation = 999 // Mutate the reader's original storage.
		samples := traceSamples(queues)
		slices.Reverse(samples)
		return samples, nil
	}
	sink := &traceSink{}
	if err := control.RunTrace(context.Background(), traceOptions(), []control.TraceTarget{target}, traceClock(), sink); err != nil {
		t.Fatal(err)
	}
	if f.inventoryCalls != 1 || f.reads != 3 || len(sink.frames) != 3 {
		t.Fatalf("wrong frozen cadence: inventory=%d reads=%d frames=%d", f.inventoryCalls, f.reads, len(sink.frames))
	}
	for _, requested := range f.requested {
		if !reflect.DeepEqual(requested, expected) {
			t.Fatal("trace followed changed queue inventory", requested)
		}
	}
	for _, frame := range sink.frames {
		if frame.Target != target.Target || frame.Samples[0].Sample.Queue != expected[0] || frame.Samples[1].Sample.Queue != expected[1] {
			t.Fatal("frame lost selected identity or ordering", frame)
		}
	}
	sink.frames[2].Samples[0].Signals = 999
	if sink.frames[0].Samples[0].Signals != 10 {
		t.Fatal("returned frames share mutable sample storage")
	}
	assertTraceClosed(t, f)
}

func TestTraceRejectsPartialAndAmbiguousSamples(t *testing.T) {
	for _, kind := range []string{"missing", "duplicate", "wrong generation", "unexpected slot"} {
		t.Run(kind, func(t *testing.T) {
			f, target := newTraceTarget(101)
			f.read = func(_ context.Context, queues []control.Queue) ([]control.TraceSample, error) {
				samples := traceSamples(queues)
				switch kind {
				case "missing":
					samples = samples[:1]
				case "duplicate":
					samples[1] = samples[0]
				case "wrong generation":
					samples[0].Sample.Queue.Generation++
				case "unexpected slot":
					samples[0].Sample.Queue.Slot++
				}
				return samples, nil
			}
			sink := &traceSink{}
			err := control.RunTrace(context.Background(), traceOptions(), []control.TraceTarget{target}, traceClock(), sink)
			if err == nil || errors.Is(err, control.ErrTraceDelivery) || f.reads != 1 || len(sink.frames) != 0 {
				t.Fatal("incomplete evidence was delivered or retried", err, f.reads, len(sink.frames))
			}
			assertTraceClosed(t, f)
		})
	}
}

func TestTraceRejectsIncompleteInventoryBeforeReading(t *testing.T) {
	for _, kind := range []string{"partial", "unavailable", "empty", "duplicate slot"} {
		t.Run(kind, func(t *testing.T) {
			f, target := newTraceTarget(101)
			switch kind {
			case "partial":
				f.inventory.Complete = false
			case "unavailable":
				f.inventory.Unavailable = []int{20}
			case "empty":
				f.inventory.Queues = nil
			case "duplicate slot":
				f.inventory.Queues[1].Slot = f.inventory.Queues[0].Slot
			}
			sink := &traceSink{}
			if err := control.RunTrace(context.Background(), traceOptions(), []control.TraceTarget{target}, traceClock(), sink); err == nil || f.reads != 0 || len(sink.frames) != 0 {
				t.Fatal("partial inventory started capture", err)
			}
			assertTraceClosed(t, f)
		})
	}
}

func TestTraceDeliveryFailureStopsAndPreservesCleanupErrors(t *testing.T) {
	f, target := newTraceTarget(101)
	delivery, readerCleanup, tracerCleanup := errors.New("writer broke"), errors.New("reader close failed"), errors.New("tracer close failed")
	f.readerCloseErr, f.tracerCloseErr = readerCleanup, tracerCleanup
	sink := &traceSink{deliver: func(context.Context, control.TraceFrame) error { return delivery }}
	err := control.RunTrace(context.Background(), traceOptions(), []control.TraceTarget{target}, traceClock(), sink)
	for _, expected := range []error{control.ErrTraceDelivery, delivery, readerCleanup, tracerCleanup} {
		if !errors.Is(err, expected) {
			t.Fatal("failure lost", expected, err)
		}
	}
	if f.reads != 1 || len(sink.frames) != 1 {
		t.Fatal("delivery failure retried capture", f.reads)
	}
	assertTraceClosed(t, f)
}

func TestTraceCancellationDoesNotHideCleanupFailure(t *testing.T) {
	f, target := newTraceTarget(101)
	cleanup := errors.New("close failed")
	f.readerCloseErr = cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &traceSink{deliver: func(context.Context, control.TraceFrame) error { cancel(); return nil }}
	err := control.RunTrace(ctx, traceOptions(), []control.TraceTarget{target}, traceClock(), sink)
	if !errors.Is(err, cleanup) || f.reads != 1 {
		t.Fatal("cancellation hid failed cleanup", err, f.reads)
	}
	assertTraceClosed(t, f)
}

func TestTraceDeadlineCancelsReadOnlyEffectAndCloses(t *testing.T) {
	f, target := newTraceTarget(101)
	f.read = func(ctx context.Context, _ []control.Queue) ([]control.TraceSample, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("trace effect received no deadline")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	o := traceOptions()
	o.Duration = 10 * time.Millisecond
	sink := &traceSink{}
	if err := control.RunTrace(context.Background(), o, []control.TraceTarget{target}, control.RealClock{}, sink); err != nil {
		t.Fatal(err)
	}
	if f.reads != 1 || len(sink.frames) != 0 {
		t.Fatal("deadline caused a retry or invented evidence")
	}
	assertTraceClosed(t, f)
}

func TestTraceCapacityAndFailedAdmissionCloseEveryAcquiredTarget(t *testing.T) {
	for _, failedInventory := range []bool{false, true} {
		first, a := newTraceTarget(101)
		second, b := newTraceTarget(202)
		o := traceOptions()
		o.MaxQueues = 3
		if failedInventory {
			second.inventoryErr = errors.New("inventory failed")
		}
		err := control.RunTrace(context.Background(), o, []control.TraceTarget{a, b}, traceClock(), &traceSink{})
		if err == nil || first.reads != 0 || second.reads != 0 {
			t.Fatal("failed admission started sampling", err)
		}
		assertTraceClosed(t, first)
		assertTraceClosed(t, second)
	}
}
