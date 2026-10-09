package telemetry_test

import (
	"context"
	"errors"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/internal/agent/telemetry"
	"reflect"
	"testing"
	"time"
)

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
	run := func(observer control.Observer, afterSecond func()) (control.Counts, []time.Duration) {
		clock := &deterministicClock{at: time.Unix(100, 0)}
		backend := &recoveringBackend{clock: clock, epoch: clock.Now()}
		accounting := &control.Accounting{}
		worker, err := control.NewWorker(control.Options{Recover: true, Cadence: 100 * time.Millisecond,
			InventoryInterval: 5 * time.Second, VerificationTimeout: time.Second, MaxQueues: 1},
			control.Dependencies{Reader: backend, Writer: backend, Observer: observer, Clock: clock, Accounting: accounting},
			control.Target{PID: 123, StartTime: 456}, clock.Now())
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
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := worker.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		return accounting.Snapshot(), backend.starts
	}
	baseline, expectedStarts := run(nil, nil)
	out := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	value := start(t, out, 1)
	actual, starts := run(value, func() {
		select {
		case <-out.entered:
		case <-time.After(time.Second):
			t.Fatal("writer did not block")
		}
		for range telemetry.OutputCapacity + 16 {
			value.TryNotice("saturate output")
		}
	})
	if actual != baseline || !reflect.DeepEqual(starts, expectedStarts) || actual.Accepted < 2 {
		t.Fatalf("telemetry changed effects: actual=%+v baseline=%+v starts=%v want=%v", actual, baseline, starts, expectedStarts)
	}
	for i := 1; i < len(starts); i++ {
		if starts[i]-starts[i-1] < 150*time.Millisecond {
			t.Fatal("completion pacing violated")
		}
	}
	wait(t, func() bool { return value.Snapshot().Streams == 0 })
	if value.Snapshot().OutputLost == 0 {
		t.Fatal("output did not saturate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := value.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	close(out.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := value.Close(ctx); errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("writer did not join after release")
	}
}
