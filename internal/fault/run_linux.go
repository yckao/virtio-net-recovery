//go:build linux && amd64

// Package fault controls a bounded Host kernel lost-wakeup experiment.
package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"vhost-watch/internal/selection"
	"vhost-watch/internal/watch"
)

type Config struct {
	Selector                                                 *selection.Selector
	VhostFD                                                  int
	Delay, Window, RecoverAfter                              time.Duration
	Drops                                                    uint
	BPFObject, SourceDir, StateDir, KernelBuildDir, Compiler string
	Cleanup                                                  bool
}

func (c Config) validate() error {
	if c.Cleanup {
		return nil
	}
	if c.Selector == nil || c.VhostFD < 0 {
		return errors.New("one selected VM and --vhost-fd are required")
	}
	if c.Delay < 0 || c.Delay > time.Minute || c.Window < time.Millisecond || c.Window > time.Minute || c.Delay%time.Millisecond != 0 || c.Window%time.Millisecond != 0 {
		return errors.New("delay must be 0..60s and window 1ms..60s, in whole milliseconds")
	}
	if c.Drops < 1 || c.Drops > 1000000 || c.RecoverAfter < 0 {
		return errors.New("drops must be 1..1000000 and recover-after must be nonnegative")
	}
	return nil
}

func build(ctx context.Context, c Config) (string, func(), error) {
	dir, err := os.MkdirTemp("", "vhost-fault-")
	if err != nil {
		return "", nil, err
	}
	clean := func() { _ = os.RemoveAll(dir) }
	for _, name := range []string{"Makefile", "vhost_fault.c"} {
		data, err := os.ReadFile(filepath.Join(c.SourceDir, name))
		if err != nil {
			clean()
			return "", nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			clean()
			return "", nil, err
		}
	}
	kdir := c.KernelBuildDir
	if kdir == "" {
		var uts unix.Utsname
		if err := unix.Uname(&uts); err != nil {
			clean()
			return "", nil, err
		}
		kdir = filepath.Join("/lib/modules", unix.ByteSliceToString(uts.Release[:]), "build")
	}
	cmd := exec.CommandContext(ctx, "make", "-C", dir, "KDIR="+kdir, "CC="+c.Compiler)
	output, err := cmd.CombinedOutput()
	if err != nil {
		clean()
		return "", nil, fmt.Errorf("build against Host kernel headers: %w\n%s", err, output)
	}
	return filepath.Join(dir, "vhost_fault.ko"), clean, nil
}

func moduleStats() (map[string]any, error) {
	row := map[string]any{}
	for _, name := range []string{"matched", "dropped", "active"} {
		data, err := os.ReadFile("/sys/module/vhost_fault/parameters/" + name)
		if err != nil {
			return nil, err
		}
		value := strings.TrimSpace(string(data))
		if name == "active" {
			row[name] = value == "Y"
			continue
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, err
		}
		row[name] = n
	}
	return row, nil
}

func unload() error {
	// A worker completing probe unregistration can briefly keep the module busy.
	for i := 0; i < 100; i++ {
		err := unix.DeleteModule("vhost_fault", unix.O_NONBLOCK)
		if err == nil || errors.Is(err, unix.ENOENT) {
			return nil
		}
		if !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EBUSY) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("fault module remained busy")
}

func Run(ctx context.Context, c Config, output io.Writer) (result error) {
	if err := c.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(c.StateDir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(c.StateDir, "fault.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("another fault controller owns this Host lock")
	}
	encoder := json.NewEncoder(output)
	log := func(event string, row map[string]any) {
		if row == nil {
			row = map[string]any{}
		}
		row["event"] = event
		row["time"] = time.Now().UTC()
		_ = encoder.Encode(row)
	}
	if c.Cleanup {
		if err := unload(); err != nil {
			return err
		}
		log("cleaned", map[string]any{"recovery_written": false})
		return nil
	}
	if _, err := os.Stat("/sys/module/vhost_fault"); err == nil {
		return errors.New("vhost_fault is already loaded; stop its controller or use --cleanup first")
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	targets, problems, err := c.Selector.Resolve()
	if err != nil {
		return err
	}
	if len(problems) != 0 {
		return errors.Join(problems...)
	}
	if len(targets) != 1 {
		return fmt.Errorf("fault injection requires exactly one VM; selected %d", len(targets))
	}
	identity := targets[0]
	module, clean, err := build(ctx, c)
	if err != nil {
		return err
	}
	defer clean()
	target, err := watch.OpenTarget(identity.PID)
	if err != nil {
		return err
	}
	defer target.Close()
	if err := c.Selector.Check(identity); err != nil {
		return err
	}
	fd, err := target.Duplicate(c.VhostFD)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	bpf, err := watch.OpenBPF(c.BPFObject, false)
	if err != nil {
		return err
	}
	defer bpf.Close()
	snapshot, err := bpf.Snapshot(fd)
	if err != nil {
		return err
	}
	if snapshot.Wait == 0 {
		return errors.New("TX queue has no identifiable waiter")
	}
	recover := func() error {
		if err := c.Selector.Check(identity); err != nil {
			return err
		}
		currentFD, err := target.Duplicate(c.VhostFD)
		if err != nil {
			return err
		}
		defer unix.Close(currentFD)
		current, err := bpf.Snapshot(currentFD)
		if err != nil {
			return err
		}
		if current.Identity() != snapshot.Identity() {
			return errors.New("TX attachment changed; refusing recovery")
		}
		_, events, err := target.Inventory()
		if err != nil {
			return err
		}
		return watch.Rekick(target, bpf, currentFD, current, events)
	}
	f, err := os.Open(module)
	if err != nil {
		return err
	}
	params := fmt.Sprintf("target_fd=%d target_wait=%d delay_ms=%d window_ms=%d max_drops=%d", fd, snapshot.Wait, c.Delay.Milliseconds(), c.Window.Milliseconds(), c.Drops)
	err = unix.FinitModule(int(f.Fd()), params, 0)
	f.Close()
	if err != nil {
		return fmt.Errorf("load Host fault module: %w", err)
	}
	loaded, recovered := true, false
	defer func() {
		if loaded {
			if err := unload(); err != nil {
				result = errors.Join(result, fmt.Errorf("unload: %w", err))
				return
			}
		}
		if !recovered && target.Alive() {
			if err := recover(); err != nil {
				result = errors.Join(result, fmt.Errorf("cleanup recovery: %w", err))
				log("recovery_failed", map[string]any{"error": err.Error()})
			} else {
				log("recovery_written", map[string]any{"pid": identity.PID, "vhost_fd": c.VhostFD})
			}
		}
	}()
	log("armed", map[string]any{"pid": identity.PID, "domain": identity.Domain, "vhost_fd": c.VhostFD, "delay": c.Delay.String(), "window": c.Window.String(), "max_drops": c.Drops, "recover_after": c.RecoverAfter.String()})
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// The kernel enforces this bound independently of controller scheduling.
	windowEnd := time.Now().Add(c.Delay + c.Window)
	var baselineUsed uint16
	var haveBaseline bool
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if !target.Alive() {
				return errors.New("target QEMU exited")
			}
			if loaded && !now.Before(windowEnd) {
				stats, err := moduleStats()
				if err != nil {
					return err
				}
				if err := unload(); err != nil {
					return err
				}
				loaded = false
				stats["active"] = false
				log("disarmed", stats)
				if stats["dropped"].(uint64) == 0 {
					return errors.New("no matching wakeup was dropped; injection did not occur")
				}
				_, baselineUsed, err = target.Ring(snapshot)
				if err != nil {
					return err
				}
				haveBaseline = true
			}
			if haveBaseline {
				_, used, err := target.Ring(snapshot)
				if err != nil {
					return err
				}
				if used != baselineUsed {
					recovered = true
					log("progress_observed", map[string]any{"pid": identity.PID, "vhost_fd": c.VhostFD})
					return nil
				}
			}
			if !loaded && c.RecoverAfter > 0 && !now.Before(windowEnd.Add(c.RecoverAfter)) {
				log("recovery_deadline", nil)
				return nil
			}
		}
	}
}
