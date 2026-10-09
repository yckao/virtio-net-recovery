package cli

import (
	"context"
	"errors"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/backend"
	"github.com/yckao/virtio-net-recovery/internal/faultlab/experiment"
	"github.com/yckao/virtio-net-recovery/internal/faultlab/module"
	"github.com/yckao/virtio-net-recovery/vhost"
)

func runExperiment(ctx context.Context, c runOptions) (report experiment.Report) {
	report.Restore.Outcome = experiment.RestoreNotAttempted
	unlock, err := module.Lock(c.state)
	if err != nil {
		report.RunError = err
		return
	}
	defer func() { report.CleanupError = errors.Join(report.CleanupError, unlock()) }()
	host, err := vhost.Open(vhost.Options{BPFObject: c.object, StateDir: c.state, MaxQueues: 4096})
	if err != nil {
		report.RunError = err
		return
	}
	defer func() { report.CleanupError = errors.Join(report.CleanupError, host.Close()) }()
	session, err := host.OpenProcess(ctx, vhost.ProcessIdentity{PID: c.pid, StartTime: c.start})
	if err != nil {
		report.RunError = err
		return
	}
	defer func() { report.CleanupError = errors.Join(report.CleanupError, session.Close()) }()
	inventory, err := session.Inventory(ctx)
	if err != nil {
		report.RunError = err
		return
	}
	var queue vhost.Queue
	found := false
	for _, q := range inventory.Queues {
		if q.Slot() == c.slot {
			queue, found = q, true
			break
		}
	}
	if !found {
		report.RunError = errors.New("selected slot has no supported current attachment")
		return
	}
	notifier, err := host.Manual(session)
	if err != nil {
		report.RunError = err
		return
	}
	controller, err := experiment.New(backend.Injector{Session: session, Queue: queue, ModulePath: c.asset},
		backend.Restorer{Notifier: notifier, Queue: queue}, experiment.Timer{})
	if err != nil {
		report.RunError = err
		return
	}
	return controller.Run(ctx, c.plan)
}

func clear(ctx context.Context, dir string, timeout time.Duration) (result error) {
	unlock, err := module.Lock(dir)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, unlock()) }()
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return module.Unload(bounded)
}
