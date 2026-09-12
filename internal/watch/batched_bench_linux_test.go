//go:build linux && amd64

package watch

import (
	"fmt"
	"testing"
	"time"
)

// Retain the real 20 ms sleep, one-second discovery, and one batched memory
// read. The subsets are real queues; this is not a synthetic 60-queue VM.
func BenchmarkBatchedCadenced(b *testing.B) {
	for _, count := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("queues-%d", count), func(b *testing.B) {
			target, probe := liveTarget(b), liveBPF(b)
			var batch *RingBatch
			var snapshots []Snapshot
			refresh := func() {
				vhosts, _, err := target.Inventory()
				if err != nil || len(vhosts) != 4 {
					b.Fatalf("four-queue inventory: %v", err)
				}
				snapshots = nil
				for _, fd := range vhosts[:count] {
					s, _, err := liveRow(target, probe, fd)
					if err != nil {
						b.Fatal(err)
					}
					snapshots = append(snapshots, s)
				}
				batch, err = NewRingBatch(target, snapshots)
				if err != nil {
					b.Fatal(err)
				}
			}
			refresh()
			progress := make([]RingProgress, count)
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			iteration := 0
			measured(b, func() {
				<-ticker.C
				alive, err := target.CheckAlive()
				if err != nil || !alive {
					b.Fatalf("target unavailable: %v", err)
				}
				now := monotonic()
				if iteration%50 == 0 {
					refresh()
				}
				if err := batch.Read(); err != nil {
					b.Fatal(err)
				}
				for i := range progress {
					a, u := batch.Indices(i)
					if progress[i].Observe(a, u, snapshots[i].Num, now, .04) {
						b.Fatal("unexpected stalled-ring candidate in idle component test")
					}
				}
				iteration++
			})
		})
	}
}
