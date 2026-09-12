//go:build linux && amd64

package main

import (
	"context"
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
	var c watch.Config
	var pids selection.PIDList
	var pattern, libvirtDir string
	var refresh time.Duration
	var once, list, listQueues bool
	flag.Var(&pids, "pid", "QEMU PID; repeat or provide a comma-separated list")
	flag.StringVar(&pattern, "domain-regex", "", "Go regular expression selecting running libvirt domain names; combined with explicit PIDs")
	flag.StringVar(&libvirtDir, "libvirt-state-dir", "/run/libvirt/qemu", "Read-only libvirt QEMU runtime XML directory")
	flag.DurationVar(&refresh, "target-interval", 5*time.Second, "Rediscover selected domains at this interval")
	flag.BoolVar(&once, "once", false, "Manually write one verified kick to every selected TX slot, then exit")
	flag.BoolVar(&list, "list", false, "List selected running QEMU processes and exit without attaching BPF")
	flag.BoolVar(&listQueues, "list-queues", false, "List validated vhost TX FD slots and exit without recovery")
	flag.StringVar(&c.Mode, "mode", "observe", "observe, guarded, or periodic")
	flag.StringVar(&c.StateDir, "state-dir", "/state", "Shared per-PID agent lock directory")
	flag.StringVar(&c.BPFObject, "bpf-object", "/watch.bpf.o", "CO-RE BPF ELF object")
	flag.Float64Var(&c.Interval, "interval", 0.25, "Snapshot interval in seconds")
	flag.Float64Var(&c.InventoryInterval, "inventory-interval", 1, "Full QEMU FD discovery interval in seconds; existing queues are sampled every interval")
	flag.BoolVar(&c.TraceStages, "trace-stages", false, "Attach diagnostic probes on the eventfd/vhost data path (adds traffic-dependent overhead)")
	flag.BoolVar(&c.BatchRings, "batch-rings", false, "Guarded mode: batch cached user-ring reads; confirm live vhost state twice before recovery")
	flag.Float64Var(&c.Threshold, "threshold", 3, "Persistent TX no-progress threshold in seconds")
	flag.Float64Var(&c.Cooldown, "cooldown", 30, "Minimum seconds between guarded re-kicks per FD slot")
	flag.IntVar(&c.MaxRecoveries, "max-recoveries", 3, "Guarded recovery budget per FD slot per hour")
	flag.Float64Var(&c.KickInterval, "kick-interval", 1, "Periodic re-kick interval in seconds")
	flag.Float64Var(&c.VerifyTimeout, "verify-timeout", 5, "Seconds to observe progress after a kick")
	flag.Float64Var(&c.SummaryInterval, "summary-interval", 5, "JSON queue summary interval in seconds")
	flag.Float64Var(&c.Duration, "duration", 0, "Stop after this many seconds; zero runs continuously")
	flag.BoolVar(&c.Rescue, "rescue", false, "Write one verified kick per selected TX slot and exit; bypass the observer lock")
	flag.IntVar(&c.VhostFD, "vhost-fd", -1, "With rescue, restrict to one current QEMU vhost FD")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	c.Rescue = c.Rescue || once
	selector, err := selection.New(pids, pattern, "/proc", libvirtDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := watch.RunSelected(ctx, watch.ManagerConfig{Agent: c, Selector: selector, Refresh: refresh, List: list, ListQueues: listQueues}, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "vhost-watch: %v\n", err)
		os.Exit(1)
	}
}
