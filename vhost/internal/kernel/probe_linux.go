//go:build linux && amd64

package kernel

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	"golang.org/x/sys/unix"
)

// Probe owns the serialized mailbox and every map/link. Its owner drains users
// before Close. Admission is cancellable and has a fixed pending-request bound.
type Probe struct {
	gate       chan struct{}
	pending    chan struct{}
	collection *ebpf.Collection
	links      []link.Link
	trace      bool
	refs       map[Attachment]int
	vqs        map[uint64]Attachment
	mailboxErr error
}

func OpenProbe(path string, trace bool, pending int) (_ *Probe, err error) {
	if err = rlimit.RemoveMemlock(); err != nil {
		return nil, err
	}
	spec, err := ebpf.LoadCollectionSpec(path)
	if err != nil {
		return nil, err
	}
	// Normal mode never loads/verifies traffic-path programs, even if a caller
	// supplied a trace object. Normal release assets omit them entirely.
	if !trace {
		for name := range spec.Programs {
			if name != "snapshot_tx" {
				delete(spec.Programs, name)
			}
		}
	}
	collection, err := ebpf.NewCollection(spec)
	if err != nil {
		return nil, err
	}
	p := &Probe{gate: make(chan struct{}, 1), pending: make(chan struct{}, max(1, pending)), collection: collection, trace: trace, refs: map[Attachment]int{}, vqs: map[uint64]Attachment{}}
	defer func() {
		if err != nil {
			p.Close()
		}
	}()
	sizes := map[string]uint32{"owner": 8, "sample": 128}
	if trace {
		sizes["stats"] = 80
	}
	for name, size := range sizes {
		m := collection.Maps[name]
		if m == nil || m.ValueSize() != size {
			return nil, fmt.Errorf("BPF layout mismatch: %s", name)
		}
	}
	for _, probe := range []struct {
		name, symbol string
		ret          bool
	}{
		{"snapshot_tx", "vhost_net_ioctl", false}, {"signal_event", "eventfd_signal_mask", false}, {"write_event", "eventfd_write", false},
		{"wake_event", "vhost_poll_wakeup", false}, {"handler_enter", "handle_tx_kick", false}, {"handler_exit", "handle_tx_kick", true},
	} {
		if probe.name != "snapshot_tx" && !trace {
			continue
		}
		program := collection.Programs[probe.name]
		if program == nil {
			return nil, fmt.Errorf("missing BPF program: %s", probe.name)
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
		p.links = append(p.links, attached)
	}
	return p, nil
}
func (p *Probe) Close() {
	for i := len(p.links) - 1; i >= 0; i-- {
		_ = p.links[i].Close()
	}
	if p.collection != nil {
		p.collection.Close()
	}
}
func (p *Probe) enter(ctx context.Context) (func(), error) {
	select {
	case p.pending <- struct{}{}:
	default:
		return nil, errors.New("snapshot admission capacity exceeded")
	}
	select {
	case p.gate <- struct{}{}:
		return func() { <-p.gate; <-p.pending }, nil
	case <-ctx.Done():
		<-p.pending
		return nil, ctx.Err()
	}
}
func (p *Probe) Snapshot(ctx context.Context, fd int) (Snapshot, error) {
	done, err := p.enter(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer done()
	var s Snapshot
	if p.mailboxErr != nil {
		return s, p.mailboxErr
	}
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		return s, err
	}
	if path != "/dev/vhost-net" {
		return s, errors.New("pinned descriptor is not vhost-net")
	}
	if err = ctx.Err(); err != nil {
		return s, err
	}
	zero := uint32(0)
	err = withSnapshotThread(func(owner uint64) error {
		err := p.collection.Maps["owner"].Update(zero, owner, ebpf.UpdateAny)
		if err != nil {
			// A failed disarm leaves attribution uncertain. This mailbox must not
			// be used again until its owning Host is closed and reopened.
			p.mailboxErr = fmt.Errorf("snapshot request binding failed: %w", err)
		}
		return err
	}, func() error {
		if err := p.collection.Maps["sample"].Update(zero, s, ebpf.UpdateAny); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var features uint64
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), 0x8008af00, uintptr(unsafe.Pointer(&features)))
		if errno != 0 {
			return errno
		}
		return p.collection.Maps["sample"].Lookup(zero, &s)
	})
	if err != nil {
		return s, err
	}
	if s.TimestampNS == 0 {
		return s, errors.New("snapshot ioctl was not observed")
	}
	return s, s.Validate()
}

// A process can own several independent probes. A TGID alone would let each
// probe capture another probe's ioctl. Keep the goroutine on one OS thread for
// the entire armed request, and make the BPF filter match its exact PID/TID.
func withSnapshotThread(bind func(uint64) error, request func() error) (err error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	owner := uint64(uint32(os.Getpid()))<<32 | uint64(uint32(unix.Gettid()))
	if err = bind(owner); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, bind(0)) }()
	return request()
}
func (p *Probe) Counters(ctx context.Context, s Snapshot) (Counters, error) {
	if !p.trace {
		return Counters{}, errors.New("trace capability was not opened")
	}
	done, err := p.enter(ctx)
	if err != nil {
		return Counters{}, err
	}
	defer done()
	var c Counters
	err = p.collection.Maps["stats"].Lookup(s.VQ, &c)
	return c, err
}

// Track owns trace registration leases. Snapshot acquisition alone does not
// register addresses, so a rejected/unadmitted queue cannot consume map capacity.
func (p *Probe) Track(ctx context.Context, s Snapshot) error {
	if !p.trace {
		return nil
	}
	done, err := p.enter(ctx)
	if err != nil {
		return err
	}
	defer done()
	a := s.Attachment()
	if p.refs[a] > 0 {
		p.refs[a]++
		return nil
	}
	if old, ok := p.vqs[s.VQ]; ok && old != a {
		return errors.New("prior trace generation still leased")
	}
	key := struct{ VQ, Epoch uint64 }{s.VQ, s.TimestampNS}
	initial := Counters{Epoch: s.TimestampNS}
	if err = p.collection.Maps["stats"].Update(s.VQ, initial, ebpf.UpdateNoExist); err != nil {
		return err
	}
	written := map[string]uint64{}
	for name, address := range map[string]uint64{"contexts": s.Context, "works": s.Work, "waits": s.Wait} {
		if err = p.collection.Maps[name].Update(address, key, ebpf.UpdateNoExist); err != nil {
			for n, k := range written {
				_ = p.collection.Maps[n].Delete(k)
			}
			_ = p.collection.Maps["stats"].Delete(s.VQ)
			return err
		}
		written[name] = address
	}
	p.refs[a] = 1
	p.vqs[s.VQ] = a
	return nil
}
func (p *Probe) Forget(s Snapshot) {
	if !p.trace {
		return
	}
	// Cleanup cannot be rejected by ordinary request-admission saturation.
	p.gate <- struct{}{}
	defer func() { <-p.gate }()
	a := s.Attachment()
	if p.refs[a] == 0 {
		return
	}
	p.refs[a]--
	if p.refs[a] > 0 {
		return
	}
	delete(p.refs, a)
	delete(p.vqs, s.VQ)
	for name, key := range map[string]uint64{"contexts": s.Context, "works": s.Work, "waits": s.Wait, "stats": s.VQ} {
		_ = p.collection.Maps[name].Delete(key)
	}
	// Inflight entries are removed by kretprobes; their epoch prevents them from
	// decrementing counters belonging to a newly allocated queue at the same address.
}
