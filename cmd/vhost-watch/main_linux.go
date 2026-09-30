//go:build linux && amd64

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vhost-watch/internal/selection"
	"vhost-watch/internal/watch"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := watch.RunSelected(ctx, cfg, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "vhost-watch: %v\n", err)
		os.Exit(1)
	}
}

// parseConfig performs policy validation without opening target or BPF resources.
func parseConfig(args []string) (watch.ManagerConfig, error) {
	fs := flag.NewFlagSet("vhost-watch", flag.ContinueOnError)
	var c watch.Config
	var pids selection.PIDList
	var pattern, libvirtDir string
	var refresh time.Duration
	var once, rescue, traceStages, list, listQueues bool
	fs.Var(&pids, "pid", "QEMU PID; repeat or provide a comma-separated list")
	fs.StringVar(&pattern, "domain-regex", "", "Go regular expression selecting running libvirt domain names; combined with explicit PIDs")
	fs.StringVar(&libvirtDir, "libvirt-state-dir", "/run/libvirt/qemu", "Read-only libvirt QEMU runtime XML directory")
	fs.DurationVar(&refresh, "target-interval", 5*time.Second, "Rediscover selected domains at this interval (observe/recover only)")
	fs.BoolVar(&once, "once", false, "Compatibility alias for --mode kick")
	fs.BoolVar(&rescue, "rescue", false, "Compatibility alias for --mode kick")
	fs.BoolVar(&traceStages, "trace-stages", false, "Compatibility alias for --mode trace; requires --duration")
	fs.BoolVar(&list, "list", false, "List selected running QEMU processes and exit without attaching BPF")
	fs.BoolVar(&listQueues, "list-queues", false, "List validated vhost TX FD slots and exit without recovery")
	fs.StringVar(&c.Mode, "mode", "observe", "observe, recover, kick, or trace")
	fs.StringVar(&c.StateDir, "state-dir", "/state", "Shared per-PID agent lock directory")
	fs.StringVar(&c.BPFObject, "bpf-object", "/watch.bpf.o", "CO-RE BPF ELF object")
	fs.Float64Var(&c.Interval, "interval", 0.1, "Polling interval in seconds; recover also uses it to pace retries")
	fs.Float64Var(&c.InventoryInterval, "inventory-interval", 5, "Full QEMU FD discovery interval in seconds; known queues are sampled every interval")
	fs.Float64Var(&c.VerifyTimeout, "verify-timeout", 5, "Seconds before reporting a write without observed backend progress")
	fs.Float64Var(&c.SummaryInterval, "summary-interval", 5, "JSON queue summary interval in seconds")
	fs.Float64Var(&c.Duration, "duration", 0, "Stop after this many seconds; trace requires an explicit value from 1 to 300")
	fs.IntVar(&c.VhostFD, "vhost-fd", -1, "With --mode kick, restrict to one current QEMU vhost FD")
	// Parse retired flags only to produce an actionable error, even for their
	// former default values. They must never silently change recovery policy.
	fs.Float64("threshold", 3, "Removed; use --mode recover --interval SECONDS")
	fs.Float64("cooldown", 30, "Removed; use --mode recover --interval SECONDS")
	fs.Int("max-recoveries", 3, "Removed; recover retries while its validated candidate persists")
	fs.Float64("kick-interval", 1, "Removed; use --mode recover or explicit --mode kick")
	fs.Bool("batch-rings", false, "Removed; observe/recover automatically batch known queue reads")
	if err := fs.Parse(args); err != nil {
		return watch.ManagerConfig{}, err
	}
	if fs.NArg() != 0 {
		return watch.ManagerConfig{}, errors.New("unexpected positional arguments")
	}
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	for _, name := range []string{"threshold", "cooldown", "max-recoveries", "kick-interval", "batch-rings"} {
		if explicit[name] {
			return watch.ManagerConfig{}, fmt.Errorf("--%s has been removed; use --mode recover with --interval for detection/retry pacing, or --mode kick for a manual write", name)
		}
	}
	if (once || rescue) && traceStages {
		return watch.ManagerConfig{}, errors.New("kick aliases --once/--rescue cannot be combined with --trace-stages")
	}
	if once || rescue {
		if explicit["mode"] && c.Mode != "kick" {
			return watch.ManagerConfig{}, errors.New("--once/--rescue are aliases for --mode kick and conflict with the explicit --mode")
		}
		c.Mode = "kick"
	}
	if traceStages {
		if explicit["mode"] && c.Mode != "trace" {
			return watch.ManagerConfig{}, errors.New("--trace-stages is an alias for --mode trace and conflicts with the explicit --mode")
		}
		c.Mode = "trace"
	}
	if c.Mode == "trace" && !explicit["duration"] {
		return watch.ManagerConfig{}, errors.New("--mode trace requires explicit --duration from 1 to 300 seconds")
	}
	if refresh <= 0 {
		return watch.ManagerConfig{}, errors.New("target refresh interval must be positive")
	}
	c.PID = 1 // Validate before discovering or opening a selected process.
	if err := c.Validate(); err != nil {
		return watch.ManagerConfig{}, err
	}
	c.PID = 0
	selector, err := selection.New(pids, pattern, "/proc", libvirtDir)
	if err != nil {
		return watch.ManagerConfig{}, err
	}
	return watch.ManagerConfig{Agent: c, Selector: selector, Refresh: refresh, List: list, ListQueues: listQueues}, nil
}
