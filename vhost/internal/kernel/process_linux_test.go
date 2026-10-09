//go:build linux && amd64

package kernel

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestPollInterruptDoesNotMeanExit(t *testing.T) {
	calls := 0
	err := pollAlive(1, func(fds []unix.PollFd, _ int) (int, error) {
		calls++
		if calls == 1 {
			return 0, unix.EINTR
		}
		return 0, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	err = pollAlive(1, func(fds []unix.PollFd, _ int) (int, error) { fds[0].Revents = unix.POLLIN; return 1, nil })
	if !errors.Is(err, ErrGeneration) {
		t.Fatalf("exit=%v", err)
	}
}
func TestRealEventfdWriteAndSaturation(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseFD(fd)
	id, err := EventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	dup, err := unix.Dup(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseFD(dup)
	id2, err := EventID("self", dup)
	if err != nil || id != id2 {
		t.Fatalf("duplicate identity %d %d %v", id, id2, err)
	}
	if err = WriteEvent(dup); err != nil {
		t.Fatal(err)
	}
	var value [8]byte
	if _, err = unix.Read(fd, value[:]); err != nil || binary.NativeEndian.Uint64(value[:]) != 1 {
		t.Fatalf("read %v %v", value, err)
	}
	binary.NativeEndian.PutUint64(value[:], ^uint64(0)-1)
	if _, err = unix.Write(fd, value[:]); err != nil {
		t.Fatal(err)
	}
	if err = WriteEvent(dup); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("full eventfd: %v", err)
	}
}
func TestRealBatchReadAndPartialReadInvalidation(t *testing.T) {
	pid := os.Getpid()
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	start, err := StartTime(stat)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Skipf("pidfd unavailable: %v", err)
	}
	p := &Process{PID: pid, Start: start, pidfd: fd}
	defer p.Close()
	var avail, used [4]byte
	binary.LittleEndian.PutUint16(avail[2:], 7)
	binary.LittleEndian.PutUint16(used[2:], 5)
	s := Snapshot{Schema: 3, VQ: 1, KickFile: 2, Context: 3, Avail: uint64(uintptr(unsafe.Pointer(&avail[0]))), Used: uint64(uintptr(unsafe.Pointer(&used[0]))), Backend: 6, PollWQH: 7, ContextWQH: 7, Num: 256, LittleEndian: 1}
	batch, err := p.NewBatch([]Snapshot{s})
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, u := batch.Indices(0)
	if a != 7 || u != 5 {
		t.Fatalf("indices %d %d", a, u)
	}
	s.Used = 1
	batch, err = p.NewBatch([]Snapshot{s})
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.Read(context.Background()); err == nil {
		t.Fatal("incomplete batch accepted")
	}
	runtime.KeepAlive(&avail)
	runtime.KeepAlive(&used)
}
func TestMutationLockIsSharedAndNonblocking(t *testing.T) {
	root := t.TempDir()
	first, err := MutationLock(root, 42)
	if err != nil {
		t.Fatal(err)
	}
	_, err = MutationLock(root, 42)
	if !IsContention(err) {
		first()
		t.Fatalf("second lock: %v", err)
	}
	first()
	second, err := MutationLock(root, 42)
	if err != nil {
		t.Fatal(err)
	}
	second()
}
