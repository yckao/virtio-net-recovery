package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/experiment"
)

type runOptions struct {
	pid                  int
	start                uint64
	slot                 int
	object, asset, state string
	plan                 experiment.Plan
}

func (c runOptions) validate() error {
	if c.pid < 1 || c.start == 0 || c.slot < 0 || !filepath.IsAbs(c.asset) || c.object == "" || !filepath.IsAbs(c.state) {
		return errors.New("run requires pid, start-time, slot, an absolute module path, bpf-object and an absolute state-dir")
	}
	return c.plan.Validate()
}

func runFault(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("run", stderr)
	var c runOptions
	fs.IntVar(&c.pid, "pid", 0, "Host QEMU PID")
	fs.Uint64Var(&c.start, "start-time", 0, "Required expected /proc process start generation")
	fs.IntVar(&c.slot, "slot", -1, "Required current vhost FD slot")
	fs.StringVar(&c.asset, "module", "", "Absolute prepared vhost_fault.ko artifact")
	fs.StringVar(&c.object, "bpf-object", "", "Matching vhost-linux observation BPF artifact")
	fs.StringVar(&c.state, "state-dir", "/run/vhost-agent", "Shared cooperating controller and mutation lock directory")
	fs.DurationVar(&c.plan.Delay, "delay", time.Second, "Delay before injection: 0..60s, whole milliseconds")
	fs.DurationVar(&c.plan.Window, "window", 500*time.Millisecond, "Kernel-enforced injection window: 1ms..60s")
	fs.DurationVar(&c.plan.CleanupTimeout, "cleanup-timeout", 5*time.Second, "Independent unload/restoration deadline: >0..60s")
	c.plan.MaxDrops = 1
	fs.Func("drops", "Maximum dropped wakeups: 1..1000000", func(s string) error {
		count, err := strconv.ParseUint(s, 10, 32)
		if err != nil || count < 1 || count > 1_000_000 {
			return errors.New("drops must be 1..1000000")
		}
		c.plan.MaxDrops = uint32(count)
		return nil
	})
	if err := parse(fs, args); err != nil {
		return parseExit(err)
	}
	if err := c.validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return ExitUsage
	}
	report := runExperiment(ctx, c)
	return deliver(stdout, stderr, faultResult(report), report.Err())
}

type faultOutput struct {
	Schema          string                    `json:"schema"`
	Command         string                    `json:"command"`
	Loaded          bool                      `json:"loaded"`
	Disarmed        bool                      `json:"disarmed"`
	StatisticsKnown bool                      `json:"statistics_known"`
	Matched         uint64                    `json:"matched"`
	Dropped         uint64                    `json:"dropped"`
	Active          bool                      `json:"active"`
	Restoration     experiment.RestoreOutcome `json:"restoration"`
	CompletedAt     *time.Time                `json:"restoration_completed_at,omitempty"`
	RunError        string                    `json:"run_error,omitempty"`
	CleanupError    string                    `json:"cleanup_error,omitempty"`
	RestoreError    string                    `json:"restore_error,omitempty"`
}

func faultResult(report experiment.Report) faultOutput {
	result := faultOutput{
		Schema: "faultlab.v1", Command: "run", Loaded: report.Loaded, Disarmed: report.Disarmed,
		StatisticsKnown: report.StatisticsKnown, Matched: report.Statistics.Matched, Dropped: report.Statistics.Dropped,
		Active:      report.Statistics.Active && !report.Disarmed,
		Restoration: report.Restore.Outcome,
		RunError:    errorText(report.RunError), CleanupError: errorText(report.CleanupError), RestoreError: errorText(report.Restore.Err),
	}
	if !report.Restore.CompletedAt.IsZero() {
		result.CompletedAt = &report.Restore.CompletedAt
	}
	return result
}
