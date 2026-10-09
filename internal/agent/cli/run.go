package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

const deliveryFailed = 3

// Run dispatches once. Command handlers own typed configuration and results.
// Exit 3 means delivery failed, including when a mutation already succeeded.
func Run(ctx context.Context, args []string, out, stderr io.Writer) int {
	name := "observe"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name, args = args[0], args[1:]
	}
	var err error
	switch name {
	case "help":
		fmt.Fprintln(stderr, "vhost-agent [observe|recover|kick|trace|list|list-queues] [options]\nUse COMMAND --help for command-specific flags. Observe is the default.")
	case "list":
		err = runList(ctx, args, out, stderr)
	case "list-queues":
		err = runListQueues(ctx, args, out, stderr)
	case "kick":
		err = runKick(ctx, args, out, stderr)
	case "trace":
		err = runTrace(ctx, args, out, stderr)
	case "observe":
		err = runDaemon(ctx, args, out, stderr, false)
	case "recover":
		err = runDaemon(ctx, args, out, stderr, true)
	default:
		err = fmt.Errorf("unknown command %q", name)
	}
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintln(stderr, err)
	if errors.Is(err, errDelivery) {
		return deliveryFailed
	}
	return 2
}
