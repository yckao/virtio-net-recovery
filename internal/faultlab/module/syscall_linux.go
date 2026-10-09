//go:build linux && amd64

package module

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

// Linux x86-64 UAPI syscall number. The frozen standard syscall package does
// not export SYS_FINIT_MODULE; this file is never compiled for another ABI.
const sysFinitModule = 313

func Load(ctx context.Context, file *os.File, binding Binding, bounds Bounds) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if file == nil || binding.FD < 0 || binding.Waiter == 0 {
		return errors.New("a pinned binding and open module are required")
	}
	if bounds.Delay < 0 || bounds.Delay > time.Minute || bounds.Delay%time.Millisecond != 0 ||
		bounds.Window < time.Millisecond || bounds.Window > time.Minute || bounds.Window%time.Millisecond != 0 ||
		bounds.MaxDrops < 1 || bounds.MaxDrops > 1_000_000 {
		return errors.New("invalid kernel injection bounds")
	}
	params := fmt.Sprintf("target_fd=%d target_wait=%d delay_ms=%d window_ms=%d max_drops=%d",
		binding.FD, binding.Waiter, bounds.Delay.Milliseconds(), bounds.Window.Milliseconds(), bounds.MaxDrops)
	ptr, err := syscall.BytePtrFromString(params)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(sysFinitModule, file.Fd(), uintptr(unsafe.Pointer(ptr)), 0)
	runtime.KeepAlive(ptr)
	runtime.KeepAlive(file)
	if errno != 0 {
		return errno
	}
	// Do not check cancellation after acceptance; the controller now owns load.
	return nil
}

func Unload(ctx context.Context) error {
	name, _ := syscall.BytePtrFromString(Name)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _, errno := syscall.Syscall(syscall.SYS_DELETE_MODULE, uintptr(unsafe.Pointer(name)), syscall.O_NONBLOCK, 0)
		runtime.KeepAlive(name)
		if errno == 0 || errno == syscall.ENOENT {
			return nil
		}
		if errno != syscall.EBUSY && errno != syscall.EAGAIN {
			return errno
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Lock coordinates cooperating experiment controllers for this host. The
// kernel module's singleton is checked separately; this is not a security lock.
func Lock(dir string) (func() error, error) {
	if !filepath.IsAbs(dir) {
		return nil, errors.New("state directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "faultlab.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.New("another fault controller owns the host lock")
	}
	return f.Close, nil
}
