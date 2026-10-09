// Package cli parses commands, constructs adapters and renders results.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/yckao/virtio-net-recovery/discovery"
	"github.com/yckao/virtio-net-recovery/internal/agent/selection"
	"github.com/yckao/virtio-net-recovery/vhost"
)

type pidList []int

func (p *pidList) String() string { return fmt.Sprint([]int(*p)) }
func (p *pidList) Set(value string) error {
	for _, v := range strings.Split(value, ",") {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return errors.New("PID must be positive")
		}
		*p = append(*p, n)
	}
	return nil
}

type selectionOptions struct {
	pids                  pidList
	domain, proc, runtime string
	maxTargets            int
}

func (o *selectionOptions) flags(f *flag.FlagSet) {
	f.Var(&o.pids, "pid", "QEMU PID (repeat or comma separated; never follows PID reuse)")
	f.StringVar(&o.domain, "domain", "", "libvirt domain name regular expression")
	f.StringVar(&o.proc, "proc", "/proc", "host procfs root for discovery")
	f.StringVar(&o.runtime, "runtime", "/run/libvirt/qemu", "libvirt runtime XML directory")
	f.IntVar(&o.maxTargets, "max-targets", 128, "maximum active target generations (1..4096)")
}
func (o selectionOptions) selector() (*selection.Selector, error) {
	return selection.New(discovery.Options{PIDs: o.pids, DomainPattern: o.domain, ProcRoot: o.proc, RuntimeDir: o.runtime, MaxTargets: o.maxTargets})
}

type backendOptions struct {
	bpf, state string
	maxQueues  int
}

func (o *backendOptions) flags(f *flag.FlagSet, bpf string) {
	f.StringVar(&o.bpf, "bpf", bpf, "matching backend BPF object")
	f.StringVar(&o.state, "state-dir", "/run/vhost-agent", "cooperating mutation locks and automatic leases")
	f.IntVar(&o.maxQueues, "max-queues", 4096, "host-wide admitted queue bound (1..4096)")
}
func (o backendOptions) open(trace bool) (*vhost.Host, error) {
	return vhost.Open(vhost.Options{BPFObject: o.bpf, StateDir: o.state, MaxQueues: o.maxQueues, Trace: trace})
}

type daemonOptions struct {
	selectionOptions
	backendOptions
	cadence, inventory, verification time.Duration
	metrics                          string
	diagnostics                      bool
	recover                          bool
}

func parseDaemon(args []string, stderr io.Writer, recover bool) (daemonOptions, error) {
	o := daemonOptions{recover: recover}
	name := "observe"
	if recover {
		name = "recover"
	}
	f := flags(name, stderr)
	o.selectionOptions.flags(f)
	o.backendOptions.flags(f, "/vhost-observe.bpf.o")
	f.DurationVar(&o.cadence, "cadence", 100*time.Millisecond, "sample and completion-paced retry interval")
	f.DurationVar(&o.inventory, "inventory", 5*time.Second, "target and queue inventory interval")
	f.DurationVar(&o.verification, "verification-timeout", 5*time.Second, "later-progress warning age; does not stop retries")
	f.StringVar(&o.metrics, "metrics", "", "optional HTTP listen address")
	f.BoolVar(&o.diagnostics, "diagnostics", true, "bounded optional JSON diagnostics")
	if err := parseFlags(f, args); err != nil {
		return o, err
	}
	if o.cadence <= 0 || o.inventory <= 0 || o.verification <= 0 {
		return o, errors.New("durations must be positive")
	}
	return o, nil
}

type traceOptions struct {
	selectionOptions
	backendOptions
	cadence, duration time.Duration
}

func parseTrace(args []string, stderr io.Writer) (traceOptions, error) {
	var o traceOptions
	f := flags("trace", stderr)
	o.selectionOptions.flags(f)
	o.backendOptions.flags(f, "/vhost-trace.bpf.o")
	f.DurationVar(&o.cadence, "cadence", 100*time.Millisecond, "trace sample interval")
	f.DurationVar(&o.duration, "duration", 10*time.Second, "capture duration (up to one hour)")
	if err := parseFlags(f, args); err != nil {
		return o, err
	}
	if o.cadence <= 0 || o.duration <= 0 || o.duration > time.Hour {
		return o, errors.New("invalid trace duration or cadence")
	}
	return o, nil
}
func flags(name string, stderr io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet("vhost-agent "+name, flag.ContinueOnError)
	f.SetOutput(stderr)
	return f
}
func parseFlags(f *flag.FlagSet, args []string) error {
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	return nil
}
