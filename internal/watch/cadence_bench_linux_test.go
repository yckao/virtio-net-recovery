//go:build linux && amd64

package watch

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Unlike the continuous microbenchmarks, these measurements retain the polling
// sleep and its cold-cache/scheduler costs. They read subsets of the four real
// queues; extrapolating the slope does not constitute a 60-queue VM test.
func BenchmarkLiveCadenced(b *testing.B) {
	period := 20 * time.Millisecond
	if value := os.Getenv("VHOST_WATCH_BENCH_INTERVAL"); value != "" {
		var err error
		period, err = time.ParseDuration(value)
		if err != nil || period <= 0 || period > time.Second {
			b.Fatal("benchmark interval must be in (0, 1s]")
		}
	}
	for _, count := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("queues-%d", count), func(b *testing.B) {
			target := liveTarget(b)
			probe := liveBPF(b)
			vhosts, _, err := target.Inventory()
			if err != nil || len(vhosts) != 4 {
				b.Fatalf("four-queue inventory: %v", err)
			}
			states := make([]*Detector, count)
			for i := range states {
				states[i] = NewDetector(0.06, 30, 3)
			}
			ticker := time.NewTicker(period)
			defer ticker.Stop()
			iteration := 0
			measured(b, func() {
				<-ticker.C
				alive, err := target.CheckAlive()
				if err != nil || !alive {
					b.Fatalf("target unavailable: %v", err)
				}
				if iteration%int(time.Second/period) == 0 {
					current, _, err := target.Inventory()
					if err != nil || len(current) != 4 {
						b.Fatalf("inventory changed: %v", err)
					}
					vhosts = current
				}
				for i := range states {
					readLiveQueue(b, target, probe, vhosts[i], states[i])
				}
				iteration++
			})
		})
	}
}
