package cli

import (
	"context"
	"errors"
	"io"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/internal/agent/supervision"
)

func runDaemon(ctx context.Context, args []string, stdout, stderr io.Writer, recover bool) (result error) {
	options, err := parseDaemon(args, stderr, recover)
	if err != nil {
		return err
	}
	selector, err := options.selector()
	if err != nil {
		return err
	}
	worker := control.Options{Recover: options.recover, Cadence: options.cadence, InventoryInterval: options.inventory, VerificationTimeout: options.verification, MaxQueues: options.maxQueues}
	if err := worker.Validate(); err != nil {
		return err
	}
	runtime, err := startRuntime(options, stdout, stderr)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, runtime.close()) }()
	host, err := options.open(false)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, host.Close()) }()
	dependencies := supervision.Dependencies{
		Selector: targetSource{selector}, Factory: factory{host, options.recover, options.state},
		Accounting: runtime.accounting, Clock: control.RealClock{},
	}
	if runtime.telemetry != nil {
		dependencies.Observer = runtime.telemetry
		dependencies.Notices = runtime.telemetry
	}
	err = supervision.Run(ctx, supervision.Options{Refresh: options.inventory, MaxTargets: options.maxTargets, RequireInitialTarget: len(options.pids) > 0, Worker: worker}, dependencies)
	return err
}
