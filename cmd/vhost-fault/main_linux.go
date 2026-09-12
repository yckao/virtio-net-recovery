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

	"vhost-watch/internal/fault"
	"vhost-watch/internal/selection"
)

func main() {
	var c fault.Config
	var pids selection.PIDList
	var pattern, libvirtDir string
	flag.Var(&pids, "pid", "Host QEMU PID (exactly one VM must match)")
	flag.StringVar(&pattern, "domain-regex", "", "Go regular expression selecting one running libvirt domain")
	flag.StringVar(&libvirtDir, "libvirt-state-dir", "/run/libvirt/qemu", "Read-only libvirt QEMU runtime XML directory")
	flag.IntVar(&c.VhostFD, "vhost-fd", -1, "Required current QEMU vhost FD from vhost-watch --list-queues")
	flag.DurationVar(&c.Delay, "delay", time.Second, "Wait before dropping wakeups (0..60s)")
	flag.DurationVar(&c.Window, "window", 500*time.Millisecond, "Bounded injection window (1ms..60s)")
	flag.UintVar(&c.Drops, "drops", 1, "Maximum matching wakeups to drop (1..1000000)")
	flag.DurationVar(&c.RecoverAfter, "recover-after", 30*time.Second, "Re-kick this TX queue after the window; zero waits for manual recovery or stop")
	flag.StringVar(&c.BPFObject, "bpf-object", "/watch.bpf.o", "CO-RE BPF ELF object")
	flag.StringVar(&c.SourceDir, "source-dir", "/fault-source", "Host fault module source directory")
	flag.StringVar(&c.KernelBuildDir, "kernel-build-dir", "", "Matching Host kernel build directory; defaults to /lib/modules/$(uname -r)/build")
	flag.StringVar(&c.Compiler, "cc", "gcc-12", "Compiler matching the Host kernel toolchain")
	flag.StringVar(&c.StateDir, "state-dir", "/state", "Shared Host controller lock directory")
	flag.BoolVar(&c.Cleanup, "cleanup", false, "Unload a leftover fault module; does not write a recovery kick")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if !c.Cleanup {
		var err error
		c.Selector, err = selection.New(pids, pattern, "/proc", libvirtDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := fault.Run(ctx, c, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "vhost-fault: %v\n", err)
		os.Exit(1)
	}
}
