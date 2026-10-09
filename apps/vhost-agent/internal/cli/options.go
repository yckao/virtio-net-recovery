// Package cli is the composition root. It is the only package allowed to know
// every concrete adapter; no library or use case imports it.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
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

type options struct {
	command                                    string
	pids                                       pidList
	domain, proc, runtime, bpf, state, metrics string
	cadence, inventory, verification, duration time.Duration
	maxTargets, maxQueues                      int
	diagnostics                                bool
}

func parse(args []string, help io.Writer) (options, error) {
	o := options{command: "observe"}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		o.command = args[0]
		args = args[1:]
	}
	switch o.command {
	case "observe", "recover", "kick", "trace", "list", "list-queues":
	case "help":
		args = []string{"-h"}
	default:
		return o, fmt.Errorf("unknown command %q", o.command)
	}
	flags := flag.NewFlagSet("vhost-agent "+o.command, flag.ContinueOnError)
	flags.SetOutput(help)
	flags.Usage = func() {
		fmt.Fprint(help, "vhost-agent [observe|recover|kick|trace|list|list-queues] [options]\n\nLinux amd64 backend. Observe is the default. Kick writes once per selected queue.\nSelect --pid or --domain; command flags are not compatibility aliases.\n")
		flags.PrintDefaults()
	}
	flags.Var(&o.pids, "pid", "QEMU PID (repeat or comma separated; never follows PID reuse)")
	flags.StringVar(&o.domain, "domain", "", "libvirt domain name regular expression")
	flags.StringVar(&o.proc, "proc", "/proc", "host procfs root for discovery")
	flags.StringVar(&o.runtime, "runtime", "/run/libvirt/qemu", "libvirt runtime XML directory")
	flags.StringVar(&o.bpf, "bpf", "/vhost-observe.bpf.o", "matching backend BPF object (trace requires trace object)")
	flags.StringVar(&o.state, "state-dir", "/run/vhost-agent", "cooperating mutation locks and automatic leases")
	flags.StringVar(&o.metrics, "metrics", "", "optional HTTP listen address (observe/recover only)")
	flags.DurationVar(&o.cadence, "cadence", 100*time.Millisecond, "sample and completion-paced retry interval")
	flags.DurationVar(&o.inventory, "inventory", 5*time.Second, "target and queue inventory interval")
	flags.DurationVar(&o.verification, "verification-timeout", 5*time.Second, "later-progress warning age; does not stop retries")
	flags.DurationVar(&o.duration, "duration", 10*time.Second, "bounded trace duration (up to one hour)")
	flags.IntVar(&o.maxTargets, "max-targets", 128, "maximum active target generations (1..4096)")
	flags.IntVar(&o.maxQueues, "max-queues", 4096, "host-wide admitted queue bound (1..4096)")
	flags.BoolVar(&o.diagnostics, "diagnostics", true, "bounded optional JSON diagnostics (observe/recover only)")
	if err := flags.Parse(args); err != nil {
		return o, err
	}
	if flags.NArg() > 0 {
		return o, errors.New("unexpected positional arguments")
	}
	if len(o.pids) == 0 && o.domain == "" {
		return o, errors.New("select --pid or --domain")
	}
	if o.cadence <= 0 || o.inventory <= 0 || o.verification <= 0 || o.duration <= 0 || o.duration > time.Hour || o.maxTargets < 1 || o.maxTargets > 4096 || o.maxQueues < 1 || o.maxQueues > 4096 {
		return o, errors.New("invalid duration or capacity")
	}
	if o.metrics != "" && o.command != "observe" && o.command != "recover" {
		return o, errors.New("metrics is available for observe/recover only")
	}
	if o.command == "trace" {
		set := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "bpf" {
				set = true
			}
		})
		if !set {
			o.bpf = "/vhost-trace.bpf.o"
		}
	}
	return o, nil
}
