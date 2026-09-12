//go:build linux && amd64

package watch

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	"golang.org/x/sys/unix"
)

// BPF owns unpinned maps and links. Snapshot calls are serialized across VMs.
type BPF struct {
	snapshotMu  sync.Mutex
	collection  *ebpf.Collection
	links       []link.Link
	traceStages bool
}

func OpenBPF(path string, traceStages bool) (_ *BPF, err error) {
	if err = rlimit.RemoveMemlock(); err != nil {
		return nil, err
	}
	spec, err := ebpf.LoadCollectionSpec(path)
	if err != nil {
		return nil, err
	}
	collection, err := ebpf.NewCollection(spec)
	if err != nil {
		return nil, err
	}
	b := &BPF{collection: collection, traceStages: traceStages}
	defer func() {
		if err != nil {
			b.Close()
		}
	}()
	for name, size := range map[string]uint32{"owner": 4, "sample": 120, "stats": 72} {
		m := collection.Maps[name]
		if m == nil || m.ValueSize() != size {
			return nil, fmt.Errorf("unexpected BPF wire layout for %s", name)
		}
	}
	zero, pid := uint32(0), uint32(os.Getpid())
	if err = collection.Maps["owner"].Update(zero, pid, ebpf.UpdateAny); err != nil {
		return nil, err
	}
	for _, probe := range []struct {
		name, symbol string
		ret          bool
	}{
		{"snapshot_tx", "vhost_net_ioctl", false}, {"signal_event", "eventfd_signal_mask", false},
		{"write_event", "eventfd_write", false}, {"wake_event", "vhost_poll_wakeup", false},
		{"handler_enter", "handle_tx_kick", false}, {"handler_exit", "handle_tx_kick", true},
	} {
		if probe.name != "snapshot_tx" && !traceStages {
			continue
		}
		program := collection.Programs[probe.name]
		if program == nil {
			return nil, fmt.Errorf("missing BPF program %s", probe.name)
		}
		var attached link.Link
		if probe.ret {
			attached, err = link.Kretprobe(probe.symbol, program, nil)
		} else {
			attached, err = link.Kprobe(probe.symbol, program, nil)
		}
		if err != nil {
			return nil, fmt.Errorf("attach %s: %w", probe.name, err)
		}
		b.links = append(b.links, attached)
	}
	return b, nil
}

func (b *BPF) Close() {
	for i := len(b.links) - 1; i >= 0; i-- {
		b.links[i].Close()
	}
	b.collection.Close()
}

func (b *BPF) Snapshot(fd int) (Snapshot, error) {
	b.snapshotMu.Lock()
	defer b.snapshotMu.Unlock()
	var s Snapshot
	// Discovery can outlive a remote FD slot. Check the pinned duplicate before
	// issuing an ioctl so reuse by an unrelated device cannot receive it.
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return s, err
	}
	if path != "/dev/vhost-net" {
		return s, errors.New("snapshot FD is no longer /dev/vhost-net")
	}
	zero := uint32(0)
	if err := b.collection.Maps["sample"].Update(zero, s, ebpf.UpdateAny); err != nil {
		return s, err
	}
	var features uint64
	// VHOST_GET_FEATURES only reads supported features. The attached kprobe
	// records this process's live TX slot during the synchronous ioctl.
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), 0x8008af00, uintptr(unsafe.Pointer(&features)))
	if errno != 0 {
		return s, errno
	}
	if err := b.collection.Maps["sample"].Lookup(zero, &s); err != nil {
		return s, err
	}
	if s.TimestampNS == 0 {
		return s, errors.New("BPF did not observe the snapshot ioctl")
	}
	return s, s.Validate()
}

func (b *BPF) Counters(vq uint64) (Counters, error) {
	var c Counters
	if !b.traceStages {
		return c, nil
	}
	err := b.collection.Maps["stats"].Lookup(vq, &c)
	return c, err
}

func (b *BPF) Forget(s Snapshot) {
	for name, key := range map[string]uint64{"stats": s.VQ, "contexts": s.Context, "works": s.Work, "waits": s.Wait} {
		_ = b.collection.Maps[name].Delete(key)
	}
}
