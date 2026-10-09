package vhost

import (
	"context"
	"errors"
	"testing"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

// Fake only the OS boundary owned by this operation. No other component's
// private state, global clock or implementation is patched.
type notificationFixture struct {
	snapshot     kernel.Snapshot
	snapshots    int
	ringReads    int
	writes       int
	closed       []int
	final        func(*notificationFixture)
	afterWrite   func()
	writeErr     error
	inventoryErr error
	avail, used  uint16
}

func fixture() *notificationFixture {
	return &notificationFixture{snapshot: kernel.Snapshot{Schema: 3, VQ: 1, KickFile: 2, Context: 3, Avail: 4, Used: 5, Backend: 6, Work: 7, Wait: 8, PollWQH: 9, ContextWQH: 9, Num: 256, EventID: 10, LittleEndian: 1, LastAvail: 10}, avail: 12, used: 10}
}
func (f *notificationFixture) Check(ctx context.Context) error { return ctx.Err() }
func (f *notificationFixture) Inventory(context.Context) (kernel.Inventory, error) {
	return kernel.Inventory{Vhosts: []int{42}, Events: map[uint32][]int{10: {43}}, Complete: true}, f.inventoryErr
}
func (f *notificationFixture) Duplicate(int) (int, error) { return 100, nil }
func (f *notificationFixture) Snapshot(context.Context, int) (kernel.Snapshot, error) {
	f.snapshots++
	if f.snapshots == 2 && f.final != nil {
		f.final(f)
	}
	return f.snapshot, nil
}
func (f *notificationFixture) Ring(context.Context, kernel.Snapshot) (uint16, uint16, error) {
	f.ringReads++
	return f.avail, f.used, nil
}
func (f *notificationFixture) PinEvent(uint32, []int) (int, error) { return 101, nil }
func (f *notificationFixture) CloseFD(fd int)                      { f.closed = append(f.closed, fd) }
func (f *notificationFixture) WriteEvent(int) error {
	f.writes++
	if f.afterWrite != nil {
		f.afterWrite()
	}
	return f.writeErr
}
func queueFixture() Queue { return Queue{owner: &queueOwner{}, slot: 42, generation: 1} }
func TestConditionalFinalGuardRechecksProgressAndQueuedWork(t *testing.T) {
	tests := []struct {
		name     string
		final    func(*notificationFixture)
		decision PhysicalDecision
	}{
		{"used advanced", func(f *notificationFixture) { f.used++ }, UsedAdvanced},
		{"work queued", func(f *notificationFixture) { f.snapshot.WorkFlags = 2 }, WorkQueued},
		{"already consumed", func(f *notificationFixture) { f.snapshot.LastAvail = f.avail }, NoPending},
		{"identity replaced", func(f *notificationFixture) { f.snapshot.EventID++ }, IdentityChanged},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := fixture()
			expected := f.snapshot
			f.final = tt.final
			used := uint16(10)
			r := performNotify(context.Background(), queueFixture(), expected, &used, f)
			if r.Decision != tt.decision || r.Write != WriteNotAttempted || f.writes != 0 {
				t.Fatalf("result=%+v writes=%d", r, f.writes)
			}
			if r.CompletedAt.IsZero() || len(f.closed) != 2 {
				t.Fatal("completion or descriptor cleanup missing")
			}
			if tt.decision == IdentityChanged && r.Observation != nil {
				t.Fatal("stale observation escaped identity failure")
			}
		})
	}
}
func TestCancellationImmediatelyBeforeWriteDoesNotWrite(t *testing.T) {
	f := fixture()
	ctx, cancel := context.WithCancel(context.Background())
	f.final = func(*notificationFixture) { cancel() }
	used := uint16(10)
	r := performNotify(ctx, queueFixture(), f.snapshot, &used, f)
	if r.Decision != Cancelled || r.Write != WriteNotAttempted || f.writes != 0 || r.Observation != nil {
		t.Fatalf("result=%+v", r)
	}
}
func TestAcceptedWriteSurvivesLaterCancellation(t *testing.T) {
	f := fixture()
	ctx, cancel := context.WithCancel(context.Background())
	f.afterWrite = cancel
	used := uint16(10)
	r := performNotify(ctx, queueFixture(), f.snapshot, &used, f)
	if r.Write != WriteAccepted || r.Err != nil || r.Receipt == nil || !r.Receipt.HasBaseline || f.writes != 1 {
		t.Fatalf("result=%+v", r)
	}
	if r.Receipt.Used != 10 || r.Receipt.Consumed != 10 || r.Receipt.AcceptedAt.Before(r.Receipt.ObservationAt) || r.CompletedAt.Before(r.Receipt.AcceptedAt) {
		t.Fatal("invalid receipt baseline or time ordering")
	}
}
func TestManualNotificationHasNoRingProgressPrerequisite(t *testing.T) {
	f := fixture()
	f.snapshot.WorkFlags = 2
	f.avail = 0
	f.used = 0
	r := performNotify(context.Background(), queueFixture(), f.snapshot, nil, f)
	if r.Write != WriteAccepted || r.Receipt == nil || r.Receipt.HasBaseline || f.ringReads != 0 {
		t.Fatalf("result=%+v reads=%d", r, f.ringReads)
	}
}
func TestAttemptedFailureDiffersFromObservationFailure(t *testing.T) {
	used := uint16(10)
	f := fixture()
	f.writeErr = errors.New("eventfd full")
	r := performNotify(context.Background(), queueFixture(), f.snapshot, &used, f)
	if r.Write != WriteFailed || r.Err == nil || r.Receipt != nil || f.writes != 1 {
		t.Fatalf("result=%+v", r)
	}
	f = fixture()
	f.inventoryErr = errors.New("unavailable")
	r = performNotify(context.Background(), queueFixture(), f.snapshot, &used, f)
	if r.Write != WriteNotAttempted || r.Decision != Unavailable || f.writes != 0 {
		t.Fatalf("result=%+v", r)
	}
}
