// Command replay demonstrates the public policy contract without a host.
package main

import (
	"fmt"
	"time"

	recovery "github.com/yckao/virtio-net-recovery/modules/recovery-core"
)

func main() {
	p, err := recovery.New(recovery.Config{Mode: recovery.Recover,
		Cadence: 100 * time.Millisecond, VerificationTimeout: time.Second}, 1)
	must(err)
	_, err = p.Observe(recovery.CachedObservation{Generation: 1, At: 0, Avail: 20, Used: 10, Size: 256})
	must(err)
	u, err := p.Observe(recovery.CachedObservation{Generation: 1, At: 100 * time.Millisecond, Avail: 20, Used: 10, Size: 256})
	must(err)
	op := u.Operation
	// A real application executes a generation-validated backend operation.
	// This replay supplies a recorded accepted receipt instead.
	_, err = p.Complete(recovery.OperationResult{ID: op.ID, Generation: op.Generation,
		CompletedAt: 110 * time.Millisecond, AcceptedAt: 110 * time.Millisecond,
		Outcome: recovery.Accepted, Reason: recovery.ReasonPending,
		Live: &recovery.LiveObservation{Generation: 1, At: 105 * time.Millisecond, Used: 10, Consumed: 10, Valid: true}})
	must(err)
	u, err = p.Observe(recovery.CachedObservation{Generation: 1, At: 220 * time.Millisecond, Avail: 20, Used: 11, Size: 256})
	must(err)
	_, err = p.Complete(recovery.OperationResult{ID: u.Operation.ID, Generation: 1,
		CompletedAt: 225 * time.Millisecond, Outcome: recovery.Observed, Reason: recovery.ReasonUsedProgress,
		Live: &recovery.LiveObservation{Generation: 1, At: 225 * time.Millisecond, Used: 11, Consumed: 12, Valid: true}})
	must(err)
	s := p.Status()
	fmt.Printf("attempts=%d accepted=%d later_progress=%d pending=%t\n",
		s.Totals.Attempts, s.Totals.Writes, s.Totals.Verified, s.VerificationPending)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
