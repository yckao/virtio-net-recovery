package supervision_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/supervision"
)

type selector struct {
	mu        sync.Mutex
	inventory supervision.Inventory
	err       error
	calls     atomic.Uint64
}

func (s *selector) Resolve(context.Context) (supervision.Inventory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.Add(1)
	inventory := s.inventory
	inventory.Targets = append([]control.Target(nil), inventory.Targets...)
	return inventory, s.err
}
func (s *selector) set(inventory supervision.Inventory, err error) {
	s.mu.Lock()
	s.inventory, s.err = inventory, err
	s.mu.Unlock()
}

type factory struct {
	mu                          sync.Mutex
	opened, closed              []control.Target
	active                      map[int]control.Target
	cleanupErrors               map[uint64]error
	gate                        chan struct{}
	openedEvents, cleanupEvents chan control.Target
	premature                   atomic.Bool
}

func newFactory() *factory {
	return &factory{active: make(map[int]control.Target), cleanupErrors: make(map[uint64]error), openedEvents: make(chan control.Target, 16), cleanupEvents: make(chan control.Target, 16)}
}
func (f *factory) Open(_ context.Context, target control.Target) (control.Reader, control.Writer, func() error, error) {
	f.mu.Lock()
	if _, exists := f.active[target.PID]; exists {
		f.premature.Store(true)
	}
	f.active[target.PID] = target
	f.opened = append(f.opened, target)
	f.mu.Unlock()
	f.openedEvents <- target
	b := &backend{queue: control.Queue{Slot: 7, Generation: target.StartTime}}
	cleanup := func() error {
		f.cleanupEvents <- target
		if f.gate != nil && target.StartTime == 1 {
			<-f.gate
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.closed = append(f.closed, target)
		delete(f.active, target.PID)
		return f.cleanupErrors[target.StartTime]
	}
	return b, b, cleanup, nil
}
func (f *factory) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.opened), len(f.closed)
}

type backend struct{ queue control.Queue }

func (b *backend) Inventory(context.Context) (control.Inventory, error) {
	return control.Inventory{Queues: []control.Queue{b.queue}, Complete: true}, nil
}
func (b *backend) sample(live bool) control.Sample {
	return control.Sample{Queue: b.queue, At: time.Now(), Num: 256, Avail: 10, Used: 4, Consumed: 5, Live: live}
}
func (b *backend) Sample(context.Context, []control.Queue) ([]control.Sample, error) {
	return []control.Sample{b.sample(false)}, nil
}
func (b *backend) Inspect(context.Context, control.Queue, uint16) control.Result {
	s := b.sample(true)
	return control.Result{Outcome: control.Observed, Decision: control.Pending, Live: &s, CompletedAt: s.At}
}
func (b *backend) Notify(context.Context, control.Queue, uint16) control.Result {
	s := b.sample(true)
	return control.Result{Outcome: control.Accepted, Decision: control.Pending, Live: &s, AcceptedAt: s.At, CompletedAt: s.At}
}
func (*backend) Close() error { return nil }

type rejectingNotices struct{ calls atomic.Uint64 }

func (n *rejectingNotices) TryNotice(string) bool { n.calls.Add(1); return false }

type fixture struct {
	selector   *selector
	factory    *factory
	accounting *control.Accounting
	registry   *control.Registry
	notices    *rejectingNotices
	cancel     context.CancelFunc
	done       chan error
}

func launch(t *testing.T, f *factory) *fixture {
	t.Helper()
	s := &selector{inventory: supervision.Inventory{Targets: []control.Target{{PID: 123, StartTime: 1}}, Complete: true}}
	x := &fixture{selector: s, factory: f, accounting: &control.Accounting{}, registry: &control.Registry{}, notices: &rejectingNotices{}, done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	x.cancel = cancel
	opts := supervision.Options{Refresh: 5 * time.Millisecond, MaxTargets: 2, Worker: control.Options{Recover: true, Cadence: 2 * time.Millisecond, InventoryInterval: 5 * time.Millisecond, VerificationTimeout: time.Second, MaxQueues: 1}}
	go func() {
		x.done <- supervision.Run(ctx, opts, supervision.Dependencies{Selector: s, Factory: f, Accounting: x.accounting, Registry: x.registry, Notices: x.notices, Clock: control.RealClock{}})
	}()
	t.Cleanup(cancel)
	return x
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
func receive(t *testing.T, ch <-chan control.Target) control.Target {
	t.Helper()
	select {
	case target := <-ch:
		return target
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for resource event")
		return control.Target{}
	}
}
func finish(t *testing.T, x *fixture) error {
	t.Helper()
	select {
	case err := <-x.done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not join cleanup")
		return nil
	}
}

func TestPartialDiscoveryRetainsWorkersThenCompleteRemovalClosesThem(t *testing.T) {
	f := newFactory()
	x := launch(t, f)
	receive(t, f.openedEvents)
	wait(t, func() bool { return x.accounting.Snapshot().Accepted > 0 })
	before := x.selector.calls.Load()
	x.selector.set(supervision.Inventory{Complete: false, Problems: 1}, nil)
	wait(t, func() bool { return x.selector.calls.Load() >= before+3 })
	if opened, closed := f.counts(); opened != 1 || closed != 0 {
		t.Fatalf("partial inventory retired a worker: %d/%d", opened, closed)
	}
	x.selector.set(supervision.Inventory{Complete: true}, nil)
	wait(t, func() bool { _, closed := f.counts(); return closed == 1 })
	x.cancel()
	if err := finish(t, x); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if x.accounting.Snapshot().Sampled != 0 {
		t.Fatal("retired worker retained sampled coverage")
	}
}

func TestReplacementWaitsForOldCleanupBeforeOpeningNewGeneration(t *testing.T) {
	f := newFactory()
	f.gate = make(chan struct{})
	x := launch(t, f)
	receive(t, f.openedEvents)
	x.selector.set(supervision.Inventory{Targets: []control.Target{{PID: 123, StartTime: 2}}, Complete: true}, nil)
	if old := receive(t, f.cleanupEvents); old.StartTime != 1 {
		t.Fatal(old)
	}
	select {
	case target := <-f.openedEvents:
		t.Fatalf("opened replacement before cleanup: %+v", target)
	case <-time.After(20 * time.Millisecond):
	}
	close(f.gate)
	if next := receive(t, f.openedEvents); next.StartTime != 2 {
		t.Fatal(next)
	}
	if f.premature.Load() {
		t.Fatal("new resources acquired while old generation remained active")
	}
	x.cancel()
	if err := finish(t, x); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if opened, closed := f.counts(); opened != 2 || closed != 2 {
		t.Fatalf("leaked replacement resources: %d/%d", opened, closed)
	}
}

func TestCancellationJoinsCleanupAndRetiresRegistry(t *testing.T) {
	f := newFactory()
	f.gate = make(chan struct{})
	x := launch(t, f)
	receive(t, f.openedEvents)
	wait(t, func() bool { return x.registry.Active(uint64(1)<<32 | 1) })
	x.cancel()
	receive(t, f.cleanupEvents)
	select {
	case err := <-x.done:
		t.Fatalf("returned before resource cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(f.gate)
	if err := finish(t, x); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if x.registry.Active(uint64(1)<<32 | 1) {
		t.Fatal("cancelled worker's stream remained active")
	}
	if _, closed := f.counts(); closed != 1 {
		t.Fatal("cleanup missing")
	}
}

func TestNoticeRejectionAndDiscoveryFailureDoNotStopRecovery(t *testing.T) {
	f := newFactory()
	x := launch(t, f)
	receive(t, f.openedEvents)
	wait(t, func() bool { return x.accounting.Snapshot().Accepted >= 2 })
	before := x.accounting.Snapshot().Accepted
	x.selector.set(supervision.Inventory{}, errors.New("temporary discovery failure"))
	wait(t, func() bool { return x.notices.calls.Load() > 0 && x.accounting.Snapshot().Accepted >= before+3 })
	if _, closed := f.counts(); closed != 0 {
		t.Fatal("failed optional notice or discovery stopped worker")
	}
	x.cancel()
	if err := finish(t, x); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCleanupFailurePreventsReplacementAcquisition(t *testing.T) {
	f := newFactory()
	cleanupError := errors.New("old generation cleanup failed")
	f.cleanupErrors[1] = cleanupError
	x := launch(t, f)
	receive(t, f.openedEvents)
	x.selector.set(supervision.Inventory{Targets: []control.Target{{PID: 123, StartTime: 2}}, Complete: true}, nil)
	err := finish(t, x)
	if !errors.Is(err, cleanupError) {
		t.Fatalf("cleanup error lost: %v", err)
	}
	if opened, _ := f.counts(); opened != 1 {
		t.Fatal("acquired replacement despite failed old-resource cleanup")
	}
}

func TestShutdownPreservesCleanupError(t *testing.T) {
	f := newFactory()
	cleanupError := errors.New("shutdown cleanup failed")
	f.cleanupErrors[1] = cleanupError
	x := launch(t, f)
	receive(t, f.openedEvents)
	x.cancel()
	err := finish(t, x)
	if !errors.Is(err, cleanupError) {
		t.Fatalf("cleanup failure hidden by cancellation: %v", err)
	}
}
