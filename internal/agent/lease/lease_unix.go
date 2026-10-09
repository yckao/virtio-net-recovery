//go:build linux || darwin

// Package lease coordinates automatic policy ownership among cooperating agents.
// It is independent of the backend's short mutation lock.
package lease

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func Acquire(dir string, pid int) (func() error, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("invalid PID")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("lease directory must not be writable by group/others")
	}
	fd, err := syscall.Open(filepath.Join(dir, fmt.Sprintf("recover-%d.lock", pid)), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "recovery lease")
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("automatic recovery for PID %d already owned: %w", pid, err)
	}
	return f.Close, nil
}
