//go:build linux && amd64

package watch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type Target struct{ PID, pidfd int }

// RingBatch caches only user-memory addresses. A failed or short read invalidates
// the whole batch. Cached addresses are never sufficient authority for a write.
type RingBatch struct {
	target *Target
	data   []byte
	local  []unix.Iovec
	remote []unix.RemoteIovec
}

func NewRingBatch(target *Target, snapshots []Snapshot) (*RingBatch, error) {
	if len(snapshots) == 0 || len(snapshots) > 512 {
		return nil, errors.New("ring batch requires 1..512 TX queues")
	}
	b := &RingBatch{target: target, data: make([]byte, 4*len(snapshots))}
	for _, s := range snapshots {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		b.remote = append(b.remote, unix.RemoteIovec{Base: uintptr(s.Avail + 2), Len: 2}, unix.RemoteIovec{Base: uintptr(s.Used + 2), Len: 2})
	}
	b.local = []unix.Iovec{{Base: &b.data[0], Len: uint64(len(b.data))}}
	return b, nil
}

func (b *RingBatch) Read() error {
	n, err := unix.ProcessVMReadv(b.target.PID, b.local, b.remote, 0)
	if err != nil {
		return err
	}
	if n != len(b.data) {
		return errors.New("incomplete batched ring read")
	}
	return nil
}

func (b *RingBatch) Indices(index int) (uint16, uint16) {
	data := b.data[index*4 : index*4+4]
	return binary.LittleEndian.Uint16(data[:2]), binary.LittleEndian.Uint16(data[2:])
}

func OpenTarget(pid int) (*Target, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, err
	}
	t := &Target{PID: pid, pidfd: fd}
	comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil || !strings.HasPrefix(string(comm), "qemu-system") || !t.Alive() {
		t.Close()
		return nil, errors.New("target must be a live QEMU system process")
	}
	return t, nil
}

func (t *Target) Close() { unix.Close(t.pidfd) }
func (t *Target) Alive() bool {
	alive, err := t.CheckAlive()
	return err == nil && alive
}

func (t *Target) CheckAlive() (bool, error) {
	return pollAlive(t.pidfd, unix.Poll)
}

func pollAlive(fd int, poll func([]unix.PollFd, int) (int, error)) (bool, error) {
	for {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := poll(fds, 0)
		// Go's runtime can interrupt syscalls for asynchronous preemption.
		// An interrupted readiness check is not evidence that QEMU exited.
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, err
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return false, errors.New("pidfd readiness check failed")
		}
		return n == 0, nil
	}
}
func (t *Target) Duplicate(fd int) (int, error) { return unix.PidfdGetfd(t.pidfd, fd, 0) }

func eventID(pid string, fd int) (uint32, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%s/fdinfo/%d", pid, fd))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, found := strings.Cut(line, ":")
		if found && key == "eventfd-id" {
			id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
			return uint32(id), err
		}
	}
	return 0, errors.New("FD is not an eventfd with a visible identity")
}

func (t *Target) Inventory() ([]int, map[uint32][]int, error) {
	root := fmt.Sprintf("/proc/%d/fd", t.PID)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, err
	}
	vhosts := []int{}
	events := map[uint32][]int{}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		link, err := os.Readlink(filepath.Join(root, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		switch link {
		case "/dev/vhost-net":
			vhosts = append(vhosts, fd)
		case "anon_inode:[eventfd]":
			id, err := eventID(strconv.Itoa(t.PID), fd)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			events[id] = append(events[id], fd)
		}
	}
	slices.Sort(vhosts)
	return vhosts, events, nil
}

func (t *Target) Ring(s Snapshot) (uint16, uint16, error) {
	var data [4]byte
	local := []unix.Iovec{{Base: &data[0], Len: 2}, {Base: &data[2], Len: 2}}
	remote := []unix.RemoteIovec{{Base: uintptr(s.Avail + 2), Len: 2}, {Base: uintptr(s.Used + 2), Len: 2}}
	n, err := unix.ProcessVMReadv(t.PID, local, remote, 0)
	if err != nil {
		return 0, 0, err
	}
	if n != 4 {
		return 0, 0, errors.New("incomplete ring index read")
	}
	return binary.LittleEndian.Uint16(data[:2]), binary.LittleEndian.Uint16(data[2:]), nil
}

type recoveryTarget interface {
	Duplicate(int) (int, error)
	Alive() bool
}
type snapshotter interface{ Snapshot(int) (Snapshot, error) }

// Rekick pins the exact eventfd and revalidates its current vhost attachment.
// It never changes file flags, so duplicated FDs cannot alter QEMU's flags.
func Rekick(target recoveryTarget, bpf snapshotter, vhostFD int, s Snapshot, events map[uint32][]int) error {
	if err := s.Validate(); err != nil {
		return err
	}
	candidates := events[s.EventID]
	if len(candidates) == 0 {
		return errors.New("live QEMU kick eventfd was not found")
	}
	fd, err := target.Duplicate(slices.Min(candidates))
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	id, err := eventID("self", fd)
	if err != nil {
		return err
	}
	if id != s.EventID {
		return errors.New("duplicated eventfd identity changed")
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	if flags&unix.O_NONBLOCK == 0 {
		return errors.New("refusing a potentially blocking eventfd write")
	}
	current, err := bpf.Snapshot(vhostFD)
	if err != nil {
		return err
	}
	if err = current.Validate(); err != nil {
		return err
	}
	if current.Identity() != s.Identity() || !target.Alive() {
		return errors.New("queue identity changed before recovery")
	}
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
