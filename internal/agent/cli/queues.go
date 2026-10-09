package cli

import (
	"context"
	"errors"
	"io"

	"github.com/yckao/virtio-net-recovery/internal/agent/backend"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

func runListQueues(ctx context.Context, args []string, stdout, stderr io.Writer) (result error) {
	var selection selectionOptions
	var options backendOptions
	f := flags("list-queues", stderr)
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
	source := backend.Commands{Host: host}
	incomplete := false
	for _, target := range inventory.Targets {
		listing := control.ListQueues(ctx, source, target)
		incomplete = incomplete || !listing.Complete()
		if err := out.deliver(ctx, queueListDTO(listing)); err != nil {
			return err
		}
	}
	return finishCommand(ctx, out, "list-queues", inventory, incomplete)
}

func queueListDTO(r control.QueueListing) queueListRecord {
	out := queueListRecord{Schema: 1, Command: "list-queues", Target: targetDTO(r.Target), Complete: r.Complete(), Error: message(r.Err), Unavailable: r.Inventory.Unavailable, Truncated: r.Inventory.Truncated}
	for _, q := range r.Inventory.Queues {
		out.Queues = append(out.Queues, queueIdentity{FD: q.Slot, Generation: q.Generation})
	}
	return out
}
