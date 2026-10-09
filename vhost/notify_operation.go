package vhost

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

// notificationIO is the operation's private OS boundary. It lets the complete
// guard/write sequence be tested without a real VM, probes or privileged writes.
type notificationIO interface {
	Check(context.Context) error
	Inventory(context.Context) (kernel.Inventory, error)
	Duplicate(int) (int, error)
	Snapshot(context.Context, int) (kernel.Snapshot, error)
	Ring(context.Context, kernel.Snapshot) (uint16, uint16, error)
	PinEvent(uint32, []int) (int, error)
	CloseFD(int)
	WriteEvent(int) error
}

func performNotify(ctx context.Context, q Queue, expected kernel.Snapshot, expectedUsed *uint16, io notificationIO) (result Result) {
	result.Write = WriteNotAttempted
	result.Decision = Unavailable
	defer func() { result.CompletedAt = time.Now() }()
	fail := func(err error) Result {
		result.Observation = nil
		result.Err = mapError(err)
		result.Decision = decisionFor(result.Err)
		return result
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := io.Check(ctx); err != nil {
		return fail(err)
	}
	inventory, err := io.Inventory(ctx)
	if err != nil {
		return fail(err)
	}
	if !slices.Contains(inventory.Vhosts, q.slot) {
		return fail(ErrIdentityChanged)
	}
	fd, err := io.Duplicate(q.slot)
	if err != nil {
		return fail(err)
	}
	defer io.CloseFD(fd)
	snap, err := io.Snapshot(ctx, fd)
	if err != nil {
		return fail(err)
	}
	if err = snap.Validate(); err != nil {
		return fail(err)
	}
	if snap.Attachment() != expected.Attachment() {
		return fail(ErrIdentityChanged)
	}
	if expectedUsed != nil {
		a, u, err := io.Ring(ctx, snap)
		if err != nil {
			return fail(err)
		}
		obs := observation(q, snap, a, u, time.Now(), true)
		result.Observation = &obs
		result.Decision = Classify(obs, *expectedUsed)
		if result.Decision != Eligible {
			return result
		}
	}
	event, err := io.PinEvent(snap.EventID, inventory.Events[snap.EventID])
	if err != nil {
		return fail(err)
	}
	defer io.CloseFD(event)
	current, err := io.Snapshot(ctx, fd)
	if err != nil {
		return fail(err)
	}
	if err = current.Validate(); err != nil {
		return fail(err)
	}
	if current.Attachment() != snap.Attachment() {
		return fail(ErrIdentityChanged)
	}
	if expectedUsed != nil {
		a, u, err := io.Ring(ctx, current)
		if err != nil {
			return fail(err)
		}
		obs := observation(q, current, a, u, time.Now(), true)
		result.Observation = &obs
		result.Decision = Classify(obs, *expectedUsed)
		if result.Decision != Eligible {
			return result
		}
	}
	if err = io.Check(ctx); err != nil {
		return fail(err)
	}
	// These guards cannot form an atomic transaction with kernel reconfiguration.
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	if err = io.WriteEvent(event); err != nil {
		result.Write = WriteFailed
		result.Err = err
		result.Decision = Eligible
		return result
	}
	receipt := &WriteReceipt{Queue: q, AcceptedAt: time.Now()}
	if result.Observation != nil {
		receipt.HasBaseline = true
		receipt.ObservationAt = result.Observation.At
		receipt.Used = result.Observation.Used
		receipt.Consumed = result.Observation.Consumed
	}
	result.Write = WriteAccepted
	result.Decision = Eligible
	result.Receipt = receipt
	return result
}
func observation(q Queue, s kernel.Snapshot, a, u uint16, at time.Time, live bool) Observation {
	source := SourceCached
	if live {
		source = SourceLive
	}
	return Observation{Queue: q, At: at, Source: source, Live: live, Num: s.Num, Avail: a, Used: u, Consumed: s.LastAvail, Outstanding: a - u, Pending: a - s.LastAvail, WorkQueued: s.WorkFlags&(1<<1) != 0}
}
func mapError(err error) error {
	if errors.Is(err, kernel.ErrUnsupported) {
		return fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	if errors.Is(err, kernel.ErrGeneration) {
		return fmt.Errorf("%w: %v", ErrIdentityChanged, err)
	}
	return err
}
func decisionFor(err error) PhysicalDecision {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Cancelled
	case errors.Is(err, ErrIdentityChanged), errors.Is(err, kernel.ErrGeneration):
		return IdentityChanged
	case errors.Is(err, ErrUnsupported):
		return Unsupported
	case errors.Is(err, ErrBusy):
		return Contended
	default:
		return Unavailable
	}
}
