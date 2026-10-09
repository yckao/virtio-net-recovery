//go:build linux && amd64

package kernel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type Process struct {
	PID   int
	Start uint64
	pidfd int
}

func OpenProcess(pid int, start uint64) (*Process, error) {
	if pid <= 0 || start == 0 {
		return nil, errors.New("positive PID and expected start time required")
	}
	// process_vm_readv uses numeric PIDs. Require /proc to describe the caller's
	// PID namespace instead of silently mixing host procfs with a nested namespace.
	self, err := os.Readlink("/proc/self")
	if err != nil {
		return nil, err
	}
	if self != strconv.Itoa(os.Getpid()) {
		return nil, errors.New("procfs and syscall PID namespaces differ")
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, err
	}
	p := &Process{PID: pid, Start: start, pidfd: fd}
	if err = p.Check(context.Background()); err != nil {
		p.Close()
		return nil, err
	}
	comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil || !strings.HasPrefix(string(comm), "qemu-system") {
		p.Close()
		return nil, errors.New("target is not a QEMU system process")
	}
	if err = p.Check(context.Background()); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}
func (p *Process) Close() error {
	if p.pidfd < 0 {
		return nil
	}
	err := unix.Close(p.pidfd)
	p.pidfd = -1
	return err
}
func (p *Process) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := pollAlive(p.pidfd, unix.Poll); err != nil {
		return err
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", p.PID))
	if err != nil {
		return err
	}
	start, err := StartTime(data)
	if err != nil {
		return err
	}
	if start != p.Start {
		return ErrGeneration
	}
	return pollAlive(p.pidfd, unix.Poll)
}
func pollAlive(fd int, poll func([]unix.PollFd, int) (int, error)) error {
	for {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := poll(fds, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return errors.New("pidfd readiness check failed")
		}
		if n != 0 {
			return ErrGeneration
		}
		return nil
	}
}
func (p *Process) Duplicate(fd int) (int, error) { return unix.PidfdGetfd(p.pidfd, fd, 0) }
func CloseFD(fd int)                             { _ = unix.Close(fd) }

// Inventory streams procfs and bounds descriptor work independently of queue
// admission. Capacity exhaustion is explicitly partial, never removal evidence.
func (p *Process) Inventory(ctx context.Context) (Inventory, error) {
	root := fmt.Sprintf("/proc/%d/fd", p.PID)
	directory, err := os.Open(root)
	if err != nil {
		return Inventory{}, err
	}
	defer directory.Close()
	result := Inventory{Events: map[uint32][]int{}, Complete: true}
	const maxDescriptors = 65536
	scanned := 0
	addProblem := func(slot int, err error) {
		result.Complete = false
		if len(result.Problems) < 64 {
			result.Problems = append(result.Problems, InventoryProblem{slot, err})
		} else {
			result.Truncated = true
		}
	}
	for {
		if err = ctx.Err(); err != nil {
			return Inventory{}, err
		}
		entries, readErr := directory.ReadDir(128)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return Inventory{}, readErr
		}
		for _, entry := range entries {
			if scanned == maxDescriptors {
				result.Complete = false
				result.Truncated = true
				slices.Sort(result.Vhosts)
				return result, nil
			}
			scanned++
			fd, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}
			path, err := os.Readlink(filepath.Join(root, entry.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				addProblem(fd, err)
				continue
			}
			switch path {
			case "/dev/vhost-net":
				result.Vhosts = append(result.Vhosts, fd)
			case "anon_inode:[eventfd]":
				id, err := EventID(strconv.Itoa(p.PID), fd)
				// Missing eventfd identity prevents notification later but says nothing
				// about vhost queue removal or read-only ring coverage.
				if err == nil {
					result.Events[id] = append(result.Events[id], fd)
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	slices.Sort(result.Vhosts)
	return result, nil
}
func EventID(pid string, fd int) (uint32, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%s/fdinfo/%d", pid, fd))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && key == "eventfd-id" {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
			return uint32(n), err
		}
	}
	return 0, errors.New("descriptor lacks eventfd identity")
}

// Batch keeps only guest addresses; successful reads never confer write authority.
type Batch struct {
	process *Process
	data    []byte
	local   []unix.Iovec
	remote  []unix.RemoteIovec
}

func (p *Process) NewBatch(snapshots []Snapshot) (*Batch, error) {
	if len(snapshots) < 1 || len(snapshots) > 512 {
		return nil, errors.New("one batch supports 1..512 queues")
	}
	b := &Batch{process: p, data: make([]byte, 4*len(snapshots))}
	for _, s := range snapshots {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		b.remote = append(b.remote, unix.RemoteIovec{Base: uintptr(s.Avail + 2), Len: 2}, unix.RemoteIovec{Base: uintptr(s.Used + 2), Len: 2})
	}
	b.local = []unix.Iovec{{Base: &b.data[0], Len: uint64(len(b.data))}}
	return b, nil
}
func (b *Batch) Read(ctx context.Context) error {
	if err := b.process.Check(ctx); err != nil {
		return err
	}
	n, err := unix.ProcessVMReadv(b.process.PID, b.local, b.remote, 0)
	if err != nil {
		return err
	}
	if n != len(b.data) {
		return errors.New("incomplete ring batch")
	}
	return b.process.Check(ctx)
}
func (b *Batch) Indices(i int) (uint16, uint16) {
	d := b.data[i*4 : i*4+4]
	return binary.LittleEndian.Uint16(d[:2]), binary.LittleEndian.Uint16(d[2:])
}
func (p *Process) Ring(ctx context.Context, s Snapshot) (uint16, uint16, error) {
	b, err := p.NewBatch([]Snapshot{s})
	if err != nil {
		return 0, 0, err
	}
	if err = b.Read(ctx); err != nil {
		return 0, 0, err
	}
	a, u := b.Indices(0)
	return a, u, nil
}

func PinEvent(p *Process, id uint32, candidates []int) (int, error) {
	if len(candidates) == 0 {
		return -1, errors.New("no matching eventfd in inventory")
	}
	fd, err := p.Duplicate(slices.Min(candidates))
	if err != nil {
		return -1, err
	}
	found, err := EventID("self", fd)
	if err != nil {
		CloseFD(fd)
		return -1, err
	}
	if found != id {
		CloseFD(fd)
		return -1, ErrGeneration
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		CloseFD(fd)
		return -1, err
	}
	if flags&unix.O_NONBLOCK == 0 {
		CloseFD(fd)
		return -1, errors.New("refusing blocking eventfd")
	}
	return fd, nil
}
func WriteEvent(fd int) error {
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], 1)
	n, err := unix.Write(fd, data[:])
	if err != nil {
		return err
	}
	if n != 8 {
		return errors.New("short eventfd write")
	}
	return nil
}

// MutationLock takes a short nonblocking lock shared by manual and conditional
// writers. It does not claim exclusion from uncooperative privileged programs.
func MutationLock(root string, pid int) (func(), error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	// O_NOFOLLOW avoids treating a preexisting symlink as a lock file.
	fd, err := unix.Open(filepath.Join(root, fmt.Sprintf("%d.mutation.lock", pid)), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		CloseFD(fd)
		return nil, err
	}
	return func() { _ = unix.Flock(fd, unix.LOCK_UN); CloseFD(fd) }, nil
}
func IsContention(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
