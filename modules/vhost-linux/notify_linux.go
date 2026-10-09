//go:build linux && amd64

package vhost

import (
	"context"
	"time"

	"github.com/yckao/virtio-net-recovery/modules/vhost-linux/internal/binding"
	"github.com/yckao/virtio-net-recovery/modules/vhost-linux/internal/kernel"
)

type conditional struct{ session *processSession }
type manual struct{ session *processSession }

func (n conditional) Notify(ctx context.Context, q Queue, used uint16) Result {
	return n.session.notify(ctx, q, &used)
}
func (n manual) Notify(ctx context.Context, q Queue) Result { return n.session.notify(ctx, q, nil) }

// notify owns the complete mutation scope. It holds a short cooperative lock,
// refreshes inventory, pins both descriptors, revalidates, and writes once.
func (s *processSession) notify(ctx context.Context, q Queue, expectedUsed *uint16) (result Result) {
	result.Write = WriteNotAttempted
	result.Decision = Unavailable
	defer func() { result.CompletedAt = time.Now() }()
	fail := func(err error) Result {
		result.Err = mapError(err)
		result.Decision = decisionFor(result.Err)
		return result
	}
	done, err := s.enter(ctx)
	if err != nil {
		return fail(err)
	}
	defer done()
	state, err := s.state(q)
	if err != nil {
		return fail(err)
	}
	unlock, err := kernel.MutationLock(s.host.options.StateDir, s.id.PID)
	if err != nil {
		if kernel.IsContention(err) {
			err = ErrBusy
		}
		return fail(err)
	}
	defer unlock()
	return performNotify(ctx, q, state.snapshot, expectedUsed, liveNotificationIO{s})
}

type liveNotificationIO struct{ s *processSession }

func (io liveNotificationIO) Check(ctx context.Context) error { return io.s.process.Check(ctx) }
func (io liveNotificationIO) Inventory(ctx context.Context) (kernel.Inventory, error) {
	return io.s.process.Inventory(ctx)
}
func (io liveNotificationIO) Duplicate(fd int) (int, error) { return io.s.process.Duplicate(fd) }
func (io liveNotificationIO) Snapshot(ctx context.Context, fd int) (kernel.Snapshot, error) {
	return io.s.host.probe.Snapshot(ctx, fd)
}
func (io liveNotificationIO) Ring(ctx context.Context, s kernel.Snapshot) (uint16, uint16, error) {
	return io.s.process.Ring(ctx, s)
}
func (io liveNotificationIO) PinEvent(id uint32, fds []int) (int, error) {
	return kernel.PinEvent(io.s.process, id, fds)
}
func (io liveNotificationIO) CloseFD(fd int)          { kernel.CloseFD(fd) }
func (io liveNotificationIO) WriteEvent(fd int) error { return kernel.WriteEvent(fd) }

// BorrowLostWakeup is discoverable only through the opt-in experimental bridge;
// ReadSession exposes neither this sensitive binding nor a mutation method.
func (s *processSession) BorrowLostWakeup(ctx context.Context, q Queue, load func(binding.Borrowed) binding.LoadResult) (binding.LoadResult, error) {
	done, err := s.enter(ctx)
	if err != nil {
		return binding.LoadResult{}, err
	}
	defer done()
	state, err := s.state(q)
	if err != nil {
		return binding.LoadResult{}, err
	}
	if err = s.process.Check(ctx); err != nil {
		return binding.LoadResult{}, mapError(err)
	}
	fd, err := s.process.Duplicate(q.slot)
	if err != nil {
		return binding.LoadResult{}, err
	}
	defer kernel.CloseFD(fd)
	snap, err := s.host.probe.Snapshot(ctx, fd)
	if err != nil {
		return binding.LoadResult{}, err
	}
	if snap.Attachment() != state.snapshot.Attachment() || snap.Wait == 0 {
		return binding.LoadResult{}, ErrIdentityChanged
	}
	if err = s.process.Check(ctx); err != nil {
		return binding.LoadResult{}, mapError(err)
	}
	if err = ctx.Err(); err != nil {
		return binding.LoadResult{}, err
	}
	return load(binding.Borrowed{FD: fd, Waiter: snap.Wait}), nil
}
