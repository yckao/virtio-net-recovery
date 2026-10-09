// Package backend adapts the declared vhost experimental API and the module
// loader to the experiment's own ports. No backend internals cross this adapter.
package backend

import (
	"context"
	"errors"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/experiment"
	"github.com/yckao/virtio-net-recovery/internal/faultlab/module"
	vhost "github.com/yckao/virtio-net-recovery/vhost"
	"github.com/yckao/virtio-net-recovery/vhost/experimental/lostwakeup"
)

type Injector struct {
	Session    vhost.ReadSession
	Queue      vhost.Queue
	ModulePath string
}

func (i Injector) Present(ctx context.Context) (bool, error) {
	state, err := module.State(ctx)
	return state.Loaded, err
}

func (i Injector) Arm(ctx context.Context, plan experiment.Plan) experiment.LoadResult {
	f, err := module.OpenArtifact(i.ModulePath)
	if err != nil {
		return experiment.LoadResult{Err: err}
	}
	defer f.Close()
	result, bindErr := lostwakeup.WithBinding(ctx, i.Session, i.Queue, func(b lostwakeup.BorrowedBinding) lostwakeup.LoadResult {
		err := module.Load(ctx, f, module.Binding{FD: b.FD(), Waiter: b.Waiter()},
			module.Bounds{Delay: plan.Delay, Window: plan.Window, MaxDrops: plan.MaxDrops})
		return lostwakeup.LoadResult{Loaded: err == nil, Err: err}
	})
	return experiment.LoadResult{Loaded: result.Loaded, Err: errors.Join(result.Err, bindErr)}
}

func (i Injector) Statistics(ctx context.Context) (experiment.Statistics, error) {
	s, err := module.State(ctx)
	if err != nil {
		return experiment.Statistics{}, err
	}
	if !s.Loaded {
		return experiment.Statistics{}, errors.New("owned module disappeared before cleanup")
	}
	return experiment.Statistics{Matched: s.Matched, Dropped: s.Dropped, Active: s.Active}, nil
}

func (Injector) Disarm(ctx context.Context) error { return module.Unload(ctx) }

type Restorer struct {
	Notifier vhost.ManualNotifier
	Queue    vhost.Queue
}

func (r Restorer) Restore(ctx context.Context) experiment.RestoreResult {
	outcome := r.Notifier.Notify(ctx, r.Queue)
	result := experiment.RestoreResult{Outcome: experiment.RestoreRefused, CompletedAt: outcome.CompletedAt, Err: outcome.Err}
	switch outcome.Write {
	case vhost.WriteAccepted:
		result.Outcome = experiment.RestoreAccepted
		// Acceptance is a receipt, not permission to discard a later failure.
	case vhost.WriteFailed:
		result.Outcome = experiment.RestoreFailed
	}
	return result
}
