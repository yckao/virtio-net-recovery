//go:build linux && amd64

package watch

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"runtime"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

type testTarget struct {
	source, duplicate, requested int
	alive                        bool
}

func (t *testTarget) Alive() bool { return t.alive }
func (t *testTarget) Duplicate(fd int) (int, error) {
	t.requested = fd
	duplicate, err := unix.Dup(t.source)
	t.duplicate = duplicate
	return duplicate, err
}

type testBPF struct {
	value Snapshot
	err   error
}

type cancellingBPF struct {
	value  Snapshot
	cancel context.CancelFunc
}

func (b cancellingBPF) Snapshot(int) (Snapshot, error) {
	b.cancel()
	return b.value, nil
}

func TestRekickCancellationAfterLiveSnapshotRefusesWrite(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	id, err := eventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	s := configured()
	s.EventID = id
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := &testTarget{source: fd, duplicate: -1, alive: true}
	err = RekickContext(ctx, target, cancellingBPF{value: s, cancel: cancel}, 33, s, map[uint32][]int{id: {66}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wrong cancellation result: %v", err)
	}
	var writeErr *KickWriteError
	if errors.As(err, &writeErr) {
		t.Fatal("cancelled recovery classified as a write failure")
	}
	if n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0); err != nil || n != 0 {
		t.Fatal("cancelled recovery wrote to eventfd")
	}
}

func (b testBPF) Snapshot(int) (Snapshot, error) { return b.value, b.err }

type manualWriteSnapshotter struct {
	value Snapshot
	calls int
}

func (b *manualWriteSnapshotter) Snapshot(int) (Snapshot, error) {
	b.calls++
	return b.value, nil
}

func TestManualWriteAcceptsEmptyRingAndQueuedWork(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "empty_ring"
		if queued {
			name = "empty_ring_with_queued_work"
		}
		t.Run(name, func(t *testing.T) {
			fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			id, err := eventID("self", fd)
			if err != nil {
				t.Fatal(err)
			}
			// Manual recovery verifies the attachment, without requiring pending
			// descriptors or interpreting the queued bit as worker idleness.
			avail, used := [4]byte{0, 0, 10, 0}, [4]byte{0, 0, 10, 0}
			s := configured()
			s.EventID, s.LastAvail, s.LastUsed = id, 10, 10
			s.Avail = uint64(uintptr(unsafe.Pointer(&avail[0])))
			s.Used = uint64(uintptr(unsafe.Pointer(&used[0])))
			if queued {
				s.WorkFlags = 1 << 1
			}
			target := &testTarget{source: fd, alive: true}
			bpf := &manualWriteSnapshotter{value: s}
			if err := RekickContext(context.Background(), target, bpf, 33, s, map[uint32][]int{id: {66}}); err != nil {
				t.Fatal(err)
			}
			runtime.KeepAlive(&avail)
			runtime.KeepAlive(&used)
			if bpf.calls != 1 {
				t.Fatalf("manual write performed %d snapshots; want only the final pre-write identity check", bpf.calls)
			}
			var data [8]byte
			if n, err := unix.Read(fd, data[:]); err != nil || n != 8 || binary.NativeEndian.Uint64(data[:]) != 1 {
				t.Fatalf("manual recovery did not write exactly one native-u64 kick: %d %v %v", n, data, err)
			}
		})
	}
}

func TestVerifiedRekickAndRefusals(t *testing.T) {
	for _, name := range []string{"valid", "fd_reused", "queue_reconfigured", "process_exited", "blocking", "missing", "snapshot_error", "detached_waiter"} {
		t.Run(name, func(t *testing.T) {
			flags := unix.EFD_CLOEXEC | unix.EFD_NONBLOCK
			if name == "blocking" {
				flags = unix.EFD_CLOEXEC
			}
			fd, err := unix.Eventfd(0, flags)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			id, err := eventID("self", fd)
			if err != nil {
				t.Fatal(err)
			}
			s := configured()
			s.EventID = id
			target := &testTarget{source: fd, duplicate: -1, alive: true}
			bpf := testBPF{value: s}
			events := map[uint32][]int{id: {77, 66}}
			switch name {
			case "fd_reused":
				s.EventID++
				events = map[uint32][]int{s.EventID: {66}}
			case "queue_reconfigured":
				bpf.value.Context++
			case "process_exited":
				target.alive = false
			case "missing":
				events = nil
			case "snapshot_error":
				bpf.err = errors.New("snapshot unavailable")
			case "detached_waiter":
				bpf.value.PollWQH++
			}
			err = Rekick(target, bpf, 33, s, events)
			if (err == nil) != (name == "valid") {
				t.Fatalf("unexpected result: %v", err)
			}
			var writeErr *KickWriteError
			if errors.As(err, &writeErr) {
				t.Fatal("safety refusal classified as a write failure")
			}
			poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			n, err := unix.Poll(poll, 0)
			if err != nil {
				t.Fatal(err)
			}
			if name == "valid" {
				var data [8]byte
				count, err := unix.Read(fd, data[:])
				if err != nil || count != 8 || binary.NativeEndian.Uint64(data[:]) != 1 {
					t.Fatalf("wrong native-u64 kick: %d %v %v", count, data, err)
				}
				if target.requested != 66 {
					t.Fatal("did not select the minimum candidate")
				}
			} else if n != 0 {
				t.Fatal("rejected recovery wrote to the eventfd")
			}
			if target.duplicate >= 0 {
				if _, err = unix.FcntlInt(uintptr(target.duplicate), unix.F_GETFL, 0); err != unix.EBADF {
					t.Fatal("duplicate was not closed")
				}
			}
			currentFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
			if err != nil {
				t.Fatal(err)
			}
			if (currentFlags&unix.O_NONBLOCK == 0) != (name == "blocking") {
				t.Fatal("changed shared file flags")
			}
		})
	}
}

func TestRekickEventfdFullReturnsWriteError(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	id, err := eventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	var data [8]byte
	const full = ^uint64(0) - 1
	binary.NativeEndian.PutUint64(data[:], full)
	if n, err := unix.Write(fd, data[:]); err != nil || n != len(data) {
		t.Fatalf("could not fill eventfd: %d %v", n, err)
	}
	s := configured()
	s.EventID = id
	target := &testTarget{source: fd, duplicate: -1, alive: true}
	err = Rekick(target, testBPF{value: s}, 33, s, map[uint32][]int{id: {66}})
	var writeErr *KickWriteError
	if !errors.As(err, &writeErr) || !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("full eventfd did not return a wrapped write failure: %v", err)
	}
	if n, err := unix.Read(fd, data[:]); err != nil || n != len(data) || binary.NativeEndian.Uint64(data[:]) != full {
		t.Fatalf("failed kick changed the eventfd counter: %d %v %v", n, data, err)
	}
}

func TestRingReadsOnlyIndices(t *testing.T) {
	avail, used := [4]byte{99, 99, 3, 0}, [4]byte{99, 99, 250, 255}
	s := configured()
	s.Avail = uint64(uintptr(unsafe.Pointer(&avail[0])))
	s.Used = uint64(uintptr(unsafe.Pointer(&used[0])))
	target := &Target{PID: os.Getpid()}
	a, u, err := target.Ring(s)
	runtime.KeepAlive(&avail)
	runtime.KeepAlive(&used)
	if err != nil || a != 3 || u != 65530 {
		t.Fatalf("wrong indices: %d %d %v", a, u, err)
	}
	s.Used = 1
	if _, _, err := target.Ring(s); err == nil {
		t.Fatal("accepted a partial/invalid remote read")
	}
}

func TestSnapshotRefusesReusedVhostSlotBeforeIoctl(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reused-vhost")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	event, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(event)
	// No BPF collection exists: rejection must precede map access and ioctl.
	b := &BPF{}
	for _, fd := range []int{int(file.Fd()), event, -1} {
		if _, err := b.Snapshot(fd); err == nil {
			t.Fatal("accepted a non-vhost or closed FD")
		}
	}
}

func TestBatchedRingReadAndPartialFailure(t *testing.T) {
	data := new([4][4]byte)
	var pin runtime.Pinner
	pin.Pin(data)
	defer pin.Unpin()
	for i, value := range []uint16{31, 30, 4, 65530} {
		binary.LittleEndian.PutUint16(data[i][2:], value)
	}
	snapshots := []Snapshot{configured(), configured()}
	for i := range snapshots {
		snapshots[i].Avail = uint64(uintptr(unsafe.Pointer(&data[2*i][0])))
		snapshots[i].Used = uint64(uintptr(unsafe.Pointer(&data[2*i+1][0])))
	}
	target := &Target{PID: os.Getpid()}
	batch, err := NewRingBatch(target, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if err := batch.Read(); err != nil {
		t.Fatal(err)
	}
	for i, want := range [][2]uint16{{31, 30}, {4, 65530}} {
		a, u := batch.Indices(i)
		if a != want[0] || u != want[1] {
			t.Fatalf("queue %d: got %d/%d, want %v", i, a, u, want)
		}
	}
	snapshots[1].Avail = 1
	partial, err := NewRingBatch(target, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Read() == nil {
		t.Fatal("accepted a partial read after the first queue")
	}
}

func TestConfigRejectsUnboundedOrInvalidValues(t *testing.T) {
	c := Config{PID: 1, Mode: "observe", StateDir: "/state", BPFObject: "/watch.bpf.o", Interval: 0.25, InventoryInterval: 1, VerifyTimeout: 5, SummaryInterval: 1, VhostFD: -1}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []float64{0, -1, math.NaN(), math.Inf(1), 1e20, 1e-20} {
		bad := c
		bad.Interval = value
		if bad.Validate() == nil {
			t.Fatalf("accepted interval %v", value)
		}
	}
	bad := c
	bad.VhostFD = 39
	if bad.Validate() == nil {
		t.Fatal("accepted a rescue-only option for the observer")
	}
}

func TestFourModesAndTraceBounds(t *testing.T) {
	c := Config{PID: 1, Mode: "recover", StateDir: "/state", BPFObject: "/watch.bpf.o", Interval: .1, InventoryInterval: 5, VerifyTimeout: 5, SummaryInterval: 5, VhostFD: -1}
	for _, mode := range []string{"observe", "recover", "kick", "trace"} {
		c.Mode, c.Duration = mode, 30
		if err := c.Validate(); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
	}
	for _, value := range []float64{0, .5, 301, math.NaN(), math.Inf(1)} {
		c.Duration = value
		if c.Validate() == nil {
			t.Fatalf("trace accepted duration %v", value)
		}
	}
	c.Mode, c.Duration = "recover", 0
	c.VhostFD = 42
	if c.Validate() == nil {
		t.Fatal("recover allowed a manual-kick FD selector")
	}
	c.Mode = "kick"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.VhostFD = -1
	for _, mode := range []string{"guarded", "periodic", "unknown"} {
		c.Mode = mode
		if c.Validate() == nil {
			t.Fatalf("accepted removed mode %s", mode)
		}
	}
}

func TestInterruptedPollDoesNotMeanQEMUExited(t *testing.T) {
	calls := 0
	alive, err := pollAlive(5, func(fds []unix.PollFd, timeout int) (int, error) {
		calls++
		if fds[0].Fd != 5 || timeout != 0 {
			t.Fatal("wrong readiness check")
		}
		if calls < 3 {
			return -1, unix.EINTR
		}
		return 0, nil
	})
	if err != nil || !alive || calls != 3 {
		t.Fatalf("interrupted poll treated as exit: %v %v %d", alive, err, calls)
	}
	alive, err = pollAlive(5, func(fds []unix.PollFd, _ int) (int, error) { fds[0].Revents = unix.POLLIN; return 1, nil })
	if err != nil || alive {
		t.Fatal("actual pidfd exit readiness ignored")
	}
	_, err = pollAlive(5, func([]unix.PollFd, int) (int, error) { return -1, unix.EBADF })
	if !errors.Is(err, unix.EBADF) {
		t.Fatal("poll failure was hidden")
	}
}
