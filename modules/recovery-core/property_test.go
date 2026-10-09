package recovery_test

import (
	"testing"
	"time"

	recovery "github.com/yckao/virtio-net-recovery/modules/recovery-core"
)

func FuzzRingCandidate(f *testing.F) {
	f.Add(uint16(20), uint16(10), uint32(256))
	f.Add(uint16(3), uint16(65530), uint32(256))
	f.Add(uint16(500), uint16(10), uint32(256))
	f.Add(uint16(1), uint16(0), uint32(0))
	f.Fuzz(func(t *testing.T, avail, used uint16, size uint32) {
		p := policy(t, recovery.Recover)
		s := recovery.CachedObservation{Generation: 1, Avail: avail, Used: used, Size: size}
		first, err := p.Observe(s)
		if err != nil || first.Operation != nil || first.Candidate {
			t.Fatal("first sample can never be a candidate")
		}
		s.At = cadence
		u, err := p.Observe(s)
		if err != nil {
			t.Fatal(err)
		}
		valid := size >= 1 && size <= 32768 && size&(size-1) == 0
		want := valid && avail-used != 0 && uint32(avail-used) <= size
		if u.Candidate != want || (u.Operation != nil) != want {
			t.Fatalf("ring predicate: avail=%d used=%d size=%d update=%+v", avail, used, size, u)
		}
		bounded(t, first)
		bounded(t, u)
	})
}

func FuzzSerialOutcomeAccountingAndPacing(f *testing.F) {
	f.Add([]byte{4, 4, 4, 4, 4, 4, 4, 4}, false)
	f.Add([]byte{4, 4, 4, 4, 4, 4, 4, 4}, true)
	f.Add([]byte{4, 4, 5, 7, 1, 4, 4, 8, 9, 10, 11, 12, 4}, false)
	f.Fuzz(func(t *testing.T, input []byte, observing bool) {
		if len(input) > 256 {
			input = input[:256]
		}
		mode := recovery.Recover
		if observing {
			mode = recovery.Observe
		}
		p := policy(t, mode)
		var now, lastCompletion time.Duration
		var used uint16
		haveCompletion := false
		for _, b := range input {
			now += time.Duration(b%9) * 25 * time.Millisecond
			if b&16 != 0 {
				used += uint16(b)
			}
			if b&32 != 0 {
				if _, err := p.ReplaceGeneration(p.Status().Generation+1, now); err != nil {
					t.Fatal(err)
				}
			}
			u := observe(t, p, now, used+5, used)
			if u.Operation == nil {
				continue
			}
			op := u.Operation
			if haveCompletion && op.StartedAt-lastCompletion < cadence {
				t.Fatal("operation bypassed completion pacing")
			}
			if observing && op.Kind != recovery.Inspect {
				t.Fatal("observe acquired a notification effect")
			}
			now += time.Duration(b%5) * 30 * time.Millisecond
			r := recovery.OperationResult{ID: op.ID, Generation: op.Generation, CompletedAt: now}
			if op.Kind == recovery.Inspect {
				r.Outcome, r.Reason = recovery.Observed, recovery.ReasonPending
				r.Live = &recovery.LiveObservation{Generation: op.Generation, At: now, Used: used, Consumed: used, Valid: true}
			} else {
				switch b % 5 {
				case 0:
					r = accepted(op, now)
				case 1:
					r.Outcome, r.Reason = recovery.Refused, recovery.ReasonWorkQueued
				case 2:
					r.Outcome, r.Reason = recovery.Unavailable, recovery.ReasonUnavailable
				case 3:
					r.Outcome, r.Reason = recovery.Cancelled, recovery.ReasonCancelled
				case 4:
					r.Outcome, r.Reason = recovery.WriteFailed, recovery.ReasonWriteFailed
				}
			}
			complete(t, p, r)
			haveCompletion, lastCompletion = true, now
			s := p.Status()
			totals := s.Totals
			if totals.Attempts != totals.Writes+totals.Refusals+totals.Unavailable+totals.Cancellations+totals.WriteFailures {
				t.Fatal("completed outcomes do not partition attempts")
			}
			before := p.Status()
			if _, err := p.Complete(r); err == nil || p.Status() != before {
				t.Fatal("duplicate receipt changed state")
			}
		}
	})
}

func BenchmarkHealthyObservation(b *testing.B) {
	p := policy(b, recovery.Recover)
	b.ReportAllocs()
	for i := range b.N {
		used := uint16(i)
		_, err := p.Observe(recovery.CachedObservation{Generation: 1, At: time.Duration(i) * cadence,
			Avail: used + 5, Used: used, Size: 256})
		if err != nil {
			b.Fatal(err)
		}
	}
}
