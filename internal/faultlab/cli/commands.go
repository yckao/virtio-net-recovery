package cli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/module"
)

type prepareOutput struct {
	Schema     string `json:"schema"`
	Command    string `json:"command"`
	ModulePath string `json:"module_path,omitempty"`
	RunError   string `json:"run_error,omitempty"`
}

func runPrepare(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("prepare", stderr)
	var build module.BuildOptions
	fs.StringVar(&build.KernelBuildDir, "kernel-build-dir", "", "Absolute matching kernel build directory")
	fs.StringVar(&build.Compiler, "cc", "cc", "Kernel-compatible compiler executable")
	fs.StringVar(&build.Output, "output", "", "Absolute new vhost_fault.ko output path; never overwritten")
	if err := parse(fs, args); err != nil {
		return parseExit(err)
	}
	path, err := module.Prepare(ctx, build)
	return deliver(stdout, stderr, prepareOutput{Schema: "faultlab.v1", Command: "prepare", ModulePath: path, RunError: errorText(err)}, err)
}

type showOutput struct {
	Schema          string `json:"schema"`
	Command         string `json:"command"`
	Loaded          bool   `json:"loaded"`
	StatisticsKnown bool   `json:"statistics_known"`
	Matched         uint64 `json:"matched"`
	Dropped         uint64 `json:"dropped"`
	Active          bool   `json:"active"`
	RunError        string `json:"run_error,omitempty"`
}

func runShow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if err := parse(flags("show", stderr), args); err != nil {
		return parseExit(err)
	}
	state, err := module.State(ctx)
	row := showOutput{Schema: "faultlab.v1", Command: "show", Loaded: state.Loaded, StatisticsKnown: state.Loaded && err == nil, Matched: state.Matched, Dropped: state.Dropped, Active: state.Active, RunError: errorText(err)}
	return deliver(stdout, stderr, row, err)
}

type clearOptions struct {
	state   string
	timeout time.Duration
}

type clearOutput struct {
	Schema   string `json:"schema"`
	Command  string `json:"command"`
	Disarmed bool   `json:"disarmed"`
	RunError string `json:"run_error,omitempty"`
}

func runClear(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("clear", stderr)
	var c clearOptions
	fs.StringVar(&c.state, "state-dir", "/run/vhost-agent", "Shared controller lock directory")
	fs.DurationVar(&c.timeout, "timeout", 5*time.Second, "Bounded unload wait: >0..60s")
	if err := parse(fs, args); err != nil {
		return parseExit(err)
	}
	if c.timeout <= 0 || c.timeout > time.Minute || !filepath.IsAbs(c.state) {
		fmt.Fprintln(stderr, "clear requires an absolute state-dir and timeout greater than zero and at most 60s")
		return ExitUsage
	}
	err := clear(ctx, c.state, c.timeout)
	return deliver(stdout, stderr, clearOutput{Schema: "faultlab.v1", Command: "clear", Disarmed: err == nil, RunError: errorText(err)}, err)
}
