// Package cli is the composition boundary for the experiment executable.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-faultlab/internal/backend"
	"github.com/yckao/virtio-net-recovery/apps/vhost-faultlab/internal/experiment"
	"github.com/yckao/virtio-net-recovery/apps/vhost-faultlab/internal/module"
	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
)

const (
	ExitOK = iota
	ExitOperation
	ExitUsage
	ExitDelivery
)

type options struct {
	pid                  int
	start                uint64
	slot                 int
	object, asset, state string
	plan                 experiment.Plan
}

type output struct {
	Schema          string                    `json:"schema"`
	Command         string                    `json:"command"`
	ModulePath      string                    `json:"module_path,omitempty"`
	Loaded          bool                      `json:"loaded"`
	Disarmed        bool                      `json:"disarmed"`
	StatisticsKnown bool                      `json:"statistics_known"`
	Matched         uint64                    `json:"matched"`
	Dropped         uint64                    `json:"dropped"`
	Active          bool                      `json:"active"`
	Restoration     experiment.RestoreOutcome `json:"restoration"`
	CompletedAt     time.Time                 `json:"restoration_completed_at,omitempty"`
	RunError        string                    `json:"run_error,omitempty"`
	CleanupError    string                    `json:"cleanup_error,omitempty"`
	RestoreError    string                    `json:"restore_error,omitempty"`
}

// Run returns an exit category. Kernel cleanup is finished before any result is
// written; presentation failures can therefore never suppress restoration.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: vhost-faultlab prepare|run|show|clear [options]")
		return ExitUsage
	}
	if args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, "vhost-faultlab prepare|run|show|clear\nBounded Linux amd64 experiment. run always unloads before verified restoration. clear unloads only.")
		return ExitOK
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c options
	var build module.BuildOptions
	var timeout time.Duration
	switch args[0] {
	case "prepare":
		fs.StringVar(&build.KernelBuildDir, "kernel-build-dir", "", "Absolute matching kernel build directory")
		fs.StringVar(&build.Compiler, "cc", "cc", "Kernel-compatible compiler executable")
		fs.StringVar(&build.Output, "output", "", "Absolute new vhost_fault.ko output path; never overwritten")
	case "run":
		fs.IntVar(&c.pid, "pid", 0, "Host QEMU PID")
		fs.Uint64Var(&c.start, "start-time", 0, "Required expected /proc process start generation")
		fs.IntVar(&c.slot, "slot", -1, "Required current vhost FD slot")
		fs.StringVar(&c.asset, "module", "", "Absolute prepared vhost_fault.ko artifact")
		fs.StringVar(&c.object, "bpf-object", "", "Matching vhost-linux observation BPF artifact")
		fs.StringVar(&c.state, "state-dir", "/run/vhost-agent", "Shared cooperating controller and mutation lock directory")
		fs.DurationVar(&c.plan.Delay, "delay", time.Second, "Delay before injection: 0..60s, whole milliseconds")
		fs.DurationVar(&c.plan.Window, "window", 500*time.Millisecond, "Kernel-enforced injection window: 1ms..60s")
		fs.Func("drops", "Maximum dropped wakeups: 1..1000000", func(s string) error {
			count, err := strconv.ParseUint(s, 10, 32)
			if err != nil || count < 1 || count > 1_000_000 {
				return errors.New("drops must be 1..1000000")
			}
			c.plan.MaxDrops = uint32(count)
			return nil
		})
		c.plan.MaxDrops = 1
		fs.DurationVar(&c.plan.CleanupTimeout, "cleanup-timeout", 5*time.Second, "Independent unload/restoration deadline: >0..60s")
	case "show":
	case "clear":
		fs.StringVar(&c.state, "state-dir", "/run/vhost-agent", "Shared controller lock directory")
		fs.DurationVar(&timeout, "timeout", 5*time.Second, "Bounded unload wait: >0..60s")
	default:
		fmt.Fprintln(stderr, "unknown command")
		return ExitUsage
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK
		}
		return ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected positional arguments")
		return ExitUsage
	}
	row := output{Schema: "faultlab.v1", Command: args[0], Restoration: experiment.RestoreNotAttempted}
	var operationErr error
	switch args[0] {
	case "prepare":
		row.ModulePath, operationErr = module.Prepare(ctx, build)
	case "show":
		var state module.Statistics
		state, operationErr = module.State(ctx)
		row.Loaded, row.Active = state.Loaded, state.Active
		row.StatisticsKnown, row.Matched, row.Dropped = state.Loaded, state.Matched, state.Dropped
	case "clear":
		if timeout <= 0 || timeout > time.Minute {
			fmt.Fprintln(stderr, "timeout must be greater than zero and at most 60s")
			return ExitUsage
		}
		operationErr = clear(ctx, c.state, timeout)
		row.Disarmed = operationErr == nil
	case "run":
		if c.pid < 1 || c.start == 0 || c.slot < 0 || !filepath.IsAbs(c.asset) || c.object == "" || !filepath.IsAbs(c.state) {
			fmt.Fprintln(stderr, "run requires pid, start-time, slot, an absolute module path, bpf-object and an absolute state-dir")
			return ExitUsage
		}
		if err := c.plan.Validate(); err != nil {
			fmt.Fprintln(stderr, err)
			return ExitUsage
		}
		report := runExperiment(ctx, c)
		operationErr = report.Err()
		row.Loaded, row.Disarmed, row.StatisticsKnown = report.Loaded, report.Disarmed, report.StatisticsKnown
		row.Matched, row.Dropped, row.Active = report.Statistics.Matched, report.Statistics.Dropped, report.Statistics.Active
		if report.Disarmed {
			row.Active = false
		}
		row.Restoration, row.CompletedAt = report.Restore.Outcome, report.Restore.CompletedAt
		row.RunError, row.CleanupError, row.RestoreError = errorText(report.RunError), errorText(report.CleanupError), errorText(report.Restore.Err)
	}
	if row.RunError == "" && args[0] != "run" {
		row.RunError = errorText(operationErr)
	}
	if err := json.NewEncoder(stdout).Encode(row); err != nil {
		fmt.Fprintln(stderr, "result delivery incomplete; operation may have taken effect:", err)
		return ExitDelivery
	}
	if operationErr != nil {
		return ExitOperation
	}
	return ExitOK
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func runExperiment(ctx context.Context, c options) (report experiment.Report) {
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
