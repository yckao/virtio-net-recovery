// Package lostwakeup provides a trusted, opt-in binding for the experimental
// single-waiter kernel injector. It never loads a module itself.
package lostwakeup

import (
	"context"
	"errors"

	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
	"github.com/yckao/virtio-net-recovery/modules/vhost-linux/internal/binding"
)

// BorrowedBinding values are valid only during the synchronous loader callback.
// Never retain them, load asynchronously, or log the sensitive waiter address.
type BorrowedBinding struct {
	fd     int
	waiter uint64
}

func (b BorrowedBinding) FD() int        { return b.fd }
func (b BorrowedBinding) Waiter() uint64 { return b.waiter }

type LoadResult struct {
	Loaded bool
	Err    error
}

type provider interface {
	BorrowLostWakeup(context.Context, vhost.Queue, func(binding.Borrowed) binding.LoadResult) (binding.LoadResult, error)
}

// WithBinding validates and pins before invoking load. A successful synchronous
// kernel load must retain its own file reference before the callback returns.
// Loaded remains true if cancellation arrives after the kernel accepts loading;
// the caller then owns disarm/unload through an independent cleanup context.
func WithBinding(ctx context.Context, session vhost.ReadSession, queue vhost.Queue, load func(BorrowedBinding) LoadResult) (LoadResult, error) {
	if load == nil {
		return LoadResult{}, errors.New("loader is required")
	}
	p, ok := session.(provider)
	if !ok {
		return LoadResult{}, vhost.ErrUnsupported
	}
	r, err := p.BorrowLostWakeup(ctx, queue, func(b binding.Borrowed) binding.LoadResult {
		r := load(BorrowedBinding{fd: b.FD, waiter: b.Waiter})
		return binding.LoadResult{Loaded: r.Loaded, Err: r.Err}
	})
	return LoadResult{Loaded: r.Loaded, Err: r.Err}, err
}
