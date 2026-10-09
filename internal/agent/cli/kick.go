package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/yckao/virtio-net-recovery/internal/agent/backend"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

func runKick(ctx context.Context, args []string, stdout, stderr io.Writer) (result error) {
	var selection selectionOptions
	var options backendOptions
	f := flags("kick", stderr)
	selection.flags(f)
	options.flags(f, "/vhost-observe.bpf.o")
	if err := parseFlags(f, args); err != nil {
		return err
	}
	selector, err := selection.selector()
	if err != nil {
		return err
	}
	inventory, err := selector.Resolve(ctx)
	if err != nil {
		return err
	}
	host, err := options.open(false)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, host.Close()) }()
	out := newOutput(stdout)
	defer out.Close()
	executionErr := kickTargets(ctx, backend.Commands{Host: host}, inventory.Targets, out)
	if errors.Is(executionErr, errDelivery) {
		return executionErr
	}
	// Effects and cancellation have already been accounted. Receipt delivery has
	// its own short deadline and must survive the operation's cancellation.
	return errors.Join(executionErr, finishCommand(context.WithoutCancel(ctx), out, "kick", inventory, executionErr != nil))
}

func kickTargets(ctx context.Context, source control.KickSource, targets []control.Target, out *commandOutput) error {
	incomplete := false
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		receipt := control.KickTarget(ctx, source, target)
		incomplete = incomplete || !receipt.Complete()
		if err := out.deliver(context.WithoutCancel(ctx), kickDTO(receipt)); err != nil {
			return fmt.Errorf("PID %d: %w", target.PID, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if incomplete {
		return errIncomplete
	}
	return nil
}
