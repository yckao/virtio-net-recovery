//go:build linux && amd64

package watch

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

// These opt-in, read-only benchmarks use an existing lab VM. Stop the resident
// observer first so a second set of global probes does not distort the result.
// No benchmark writes eventfds, changes queue configuration or injects a fault.
func liveTarget(b *testing.B) *Target {
	b.Helper()
	value := os.Getenv("VHOST_WATCH_BENCH_PID")
	if value == "" {
		b.Skip("set VHOST_WATCH_BENCH_PID to an authorized lab QEMU PID")
	}
	pid, err := strconv.Atoi(value)
	if err != nil {
		b.Fatal(err)
	}
	target, err := OpenTarget(pid)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(target.Close)
	return target
}

func cpuNS(b *testing.B) int64 {
	b.Helper()
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return usage.Utime.Nano() + usage.Stime.Nano()
}

func measured(b *testing.B, run func()) {
	b.Helper()
	b.ResetTimer()
	start := cpuNS(b)
	for i := 0; i < b.N; i++ {
		run()
	}
	used := cpuNS(b) - start
	b.StopTimer()
	b.ReportMetric(float64(used)/float64(b.N), "cpu-ns/op")
}

func BenchmarkLiveInventory(b *testing.B) {
	target := liveTarget(b)
	measured(b, func() {
		if _, _, err := target.Inventory(); err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkLiveFDReadlink(b *testing.B) {
	target := liveTarget(b)
	root := fmt.Sprintf("/proc/%d/fd", target.PID)
	entries, err := os.ReadDir(root)
	if err != nil {
		b.Fatal(err)
	}
	i := 0
	measured(b, func() {
		if _, err := os.Readlink(filepath.Join(root, entries[i%len(entries)].Name())); err != nil {
			b.Fatal(err)
		}
		i++
	})
}

func BenchmarkLiveEventID(b *testing.B) {
	target := liveTarget(b)
	_, events, err := target.Inventory()
	if err != nil || len(events) == 0 {
		b.Fatalf("eventfd inventory: %v", err)
	}
	fds := []int{}
	for _, values := range events {
		fds = append(fds, values...)
	}
	i := 0
	measured(b, func() {
		if _, err := eventID(strconv.Itoa(target.PID), fds[i%len(fds)]); err != nil {
			b.Fatal(err)
		}
		i++
	})
}

func readLiveQueue(b *testing.B, target *Target, bpf *BPF, remoteFD int, d *Detector) {
	b.Helper()
	fd, err := target.Duplicate(remoteFD)
	if err != nil {
		b.Fatal(err)
	}
	defer unix.Close(fd)
	s, err := bpf.Snapshot(fd)
	if err != nil {
		b.Fatal(err)
	}
	avail, used, err := target.Ring(s)
	if err != nil {
		b.Fatal(err)
	}
	counters, err := bpf.Counters(s.VQ)
	if err != nil {
		b.Fatal(err)
	}
	now := monotonic()
	d.Observe(s, avail, used, s.WorkFlags&(1<<1) != 0 || counters.Active != 0 || now-float64(counters.LastHandlerNS)/1e9 < 0.02, now)
}

func liveBPF(b *testing.B) *BPF {
	b.Helper()
	path := os.Getenv("VHOST_WATCH_BENCH_BPF")
	if path == "" {
		b.Fatal("set VHOST_WATCH_BENCH_BPF to the matching watch.bpf.o")
	}
	probe, err := OpenBPF(path, os.Getenv("VHOST_WATCH_BENCH_TRACE_STAGES") == "1")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(probe.Close)
	return probe
}

func BenchmarkLiveQueueRead(b *testing.B) {
	target := liveTarget(b)
	bpf := liveBPF(b)
	vhosts, _, err := target.Inventory()
	if err != nil || len(vhosts) != 4 {
		b.Fatalf("four-queue inventory: %v", err)
	}
	states := []*Detector{}
	for range vhosts {
		states = append(states, NewDetector(3, 30, 3))
	}
	i := 0
	measured(b, func() {
		index := i % len(vhosts)
		readLiveQueue(b, target, bpf, vhosts[index], states[index])
		i++
	})
}

func BenchmarkLiveReadCycle(b *testing.B) {
	for _, count := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("queues-%d", count), func(b *testing.B) {
			target := liveTarget(b)
			bpf := liveBPF(b)
			states := map[int]*Detector{}
			measured(b, func() {
				alive, err := target.CheckAlive()
				if err != nil || !alive {
					b.Fatalf("target liveness: %v", err)
				}
				vhosts, _, err := target.Inventory()
				if err != nil || len(vhosts) < count {
					b.Fatalf("queue inventory: %v", err)
				}
				for _, remoteFD := range vhosts[:count] {
					d := states[remoteFD]
					if d == nil {
						d = NewDetector(3, 30, 3)
						states[remoteFD] = d
					}
					readLiveQueue(b, target, bpf, remoteFD, d)
				}
			})
		})
	}
}
