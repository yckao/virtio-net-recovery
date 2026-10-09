package recovery_test

import (
	"errors"
	"testing"
	"time"

	recovery "github.com/yckao/virtio-net-recovery/recovery"
)

const cadence = 100 * time.Millisecond

func policy(t testing.TB, mode recovery.Mode) *recovery.Policy {
	t.Helper()
	p, err := recovery.New(recovery.Config{Mode: mode, Cadence: cadence, VerificationTimeout: time.Second}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func observe(t testing.TB, p *recovery.Policy, at time.Duration, avail, used uint16) recovery.Update {
	t.Helper()
	u, err := p.Observe(recovery.CachedObservation{Generation: p.Status().Generation, At: at, Avail: avail, Used: used, Size: 256})
	if err != nil {
		t.Fatal(err)
	}
	bounded(t, u)
	return u
}

func bounded(t testing.TB, u recovery.Update) {
	t.Helper()
	if len(u.Facts) > recovery.MaxFactsPerUpdate {
		t.Fatalf("unbounded update: %d facts", len(u.Facts))
	}
}

func complete(t testing.TB, p *recovery.Policy, r recovery.OperationResult) recovery.Update {
	t.Helper()
	u, err := p.Complete(r)
	if err != nil {
		t.Fatal(err)
	}
	bounded(t, u)
	return u
}

func accepted(op *recovery.Operation, at time.Duration) recovery.OperationResult {
	return recovery.OperationResult{ID: op.ID, Generation: op.Generation, CompletedAt: at,
		Outcome: recovery.Accepted, Reason: recovery.ReasonPending, AcceptedAt: at,
		Live: &recovery.LiveObservation{Generation: op.Generation, At: op.StartedAt,
			Used: op.ExpectedUsed, Consumed: op.ExpectedUsed, Valid: true}}
}

func candidate(t testing.TB, p *recovery.Policy, at time.Duration) *recovery.Operation {
	t.Helper()
	if u := observe(t, p, at, 20, 10); u.Candidate || u.Operation != nil {
		t.Fatal("first observation inherited age")
	}
	u := observe(t, p, at+cadence, 20, 10)
	if !u.Candidate || u.Operation == nil {
		t.Fatal("stable outstanding work did not become a candidate")
	}
	return u.Operation
}

func factCount(u recovery.Update, kind recovery.FactKind) int {
	n := 0
	for _, f := range u.Facts {
		if f.Kind == kind {
			n++
		}
	}
	return n
}

func TestCandidateUsesObservedBacklogNotElapsedIdleTime(t *testing.T) {
	p := policy(t, recovery.Recover)
	observe(t, p, 0, 10, 10)
	if u := observe(t, p, time.Hour, 20, 10); u.Candidate || u.Operation != nil {
		t.Fatal("idle age became backlog age")
	}
	if u := observe(t, p, time.Hour+cadence/2, 30, 10); u.Candidate {
		t.Fatal("candidate before cadence")
	}
	u := observe(t, p, time.Hour+cadence, 40, 10)
	if !u.Candidate || u.Operation.Kind != recovery.Notify || u.Operation.AttemptID != 1 {
		t.Fatalf("unexpected candidate: %+v", u)
	}
}

func TestObserveCannotCreateAutomaticAttemptAndPacesInspections(t *testing.T) {
	p := policy(t, recovery.Observe)
	op := candidate(t, p, 0)
	if op.Kind != recovery.Inspect || op.AttemptID != 0 {
		t.Fatal("observe issued a notification")
	}
	complete(t, p, recovery.OperationResult{ID: op.ID, Generation: 1, CompletedAt: 2 * cadence,
		Outcome: recovery.Observed, Reason: recovery.ReasonPending,
		Live: &recovery.LiveObservation{Generation: 1, At: cadence, Used: 10, Consumed: 10, Valid: true}})
	for _, at := range []time.Duration{2 * cadence, 2*cadence + cadence/2} {
		if u := observe(t, p, at, 20, 10); u.Operation != nil {
			t.Fatal("inspection was not paced from completion")
		}
	}
	if u := observe(t, p, 3*cadence, 20, 10); u.Operation == nil || u.Operation.Kind != recovery.Inspect {
		t.Fatal("next inspection was omitted")
	}
	if s := p.Status(); s.Totals.Attempts != 0 || s.Totals.Writes != 0 {
		t.Fatalf("observe charged attempts: %+v", s)
	}
}

func TestSlowAttemptCompletionControlsEveryOutcomeCadence(t *testing.T) {
	for _, tc := range []struct {
		outcome recovery.Outcome
		reason  recovery.Reason
	}{
		{recovery.Accepted, recovery.ReasonPending},
		{recovery.Refused, recovery.ReasonWorkQueued},
		{recovery.Unavailable, recovery.ReasonUnavailable},
		{recovery.Cancelled, recovery.ReasonCancelled},
		{recovery.WriteFailed, recovery.ReasonWriteFailed},
	} {
		t.Run(string(tc.outcome), func(t *testing.T) {
			p := policy(t, recovery.Recover)
			op := candidate(t, p, 0)
			r := accepted(op, time.Second)
			r.Outcome, r.Reason = tc.outcome, tc.reason
			if tc.outcome != recovery.Accepted {
				r.AcceptedAt, r.Live = 0, nil
			}
			complete(t, p, r)
			observe(t, p, time.Second, 20, 10)
			if u := observe(t, p, time.Second+cadence-time.Nanosecond, 20, 10); u.Operation != nil {
				t.Fatal("slow attempt enabled an immediate retry")
			}
			if u := observe(t, p, time.Second+cadence, 20, 10); u.Operation == nil || u.Operation.Kind != recovery.Notify {
				t.Fatal("refusal or failure disabled retry")
			}
		})
	}
}

func TestVerificationRetainsFirstWriteAndRequiresBothLiveIndices(t *testing.T) {
	p := policy(t, recovery.Recover)
	first := candidate(t, p, 0)
	complete(t, p, accepted(first, cadence))
	retry := observe(t, p, 2*cadence, 20, 10).Operation
	complete(t, p, accepted(retry, 2*cadence))
	if s := p.Status(); s.OriginAttemptID != first.AttemptID || s.VerificationStartedAt != cadence {
		t.Fatal("retry replaced verification origin")
	}
	// Cached completion progress only requests a live inspection.
	u := observe(t, p, 3*cadence, 20, 11)
	if u.Operation == nil || u.Operation.Kind != recovery.Inspect || !u.Operation.ForVerification || u.Operation.ForCandidate {
		t.Fatal("cached progress did not request verification-only inspection")
	}
	complete(t, p, recovery.OperationResult{ID: u.Operation.ID, Generation: 1, CompletedAt: 3 * cadence,
		Outcome: recovery.Observed, Reason: recovery.ReasonUsedProgress,
		Live: &recovery.LiveObservation{Generation: 1, At: 3 * cadence, Used: 11, Consumed: 10, Valid: true}})
	if !p.Status().VerificationPending {
		t.Fatal("used progress alone confirmed recovery")
	}
	u = observe(t, p, 4*cadence, 20, 11)
	u = complete(t, p, recovery.OperationResult{ID: u.Operation.ID, Generation: 1, CompletedAt: 4 * cadence,
		Outcome: recovery.Observed, Reason: recovery.ReasonUsedProgress,
		Live: &recovery.LiveObservation{Generation: 1, At: 4 * cadence, Used: 11, Consumed: 12, Valid: true}})
	if factCount(u, recovery.VerificationConfirmed) != 1 || p.Status().VerificationPending || p.Status().Totals.Verified != 1 {
		t.Fatal("both live indices did not confirm exactly once")
	}
	if s := p.Status(); s.Totals.Attempts != 2 || s.Totals.Writes != 2 {
		t.Fatal("verification inspection was charged as an attempt")
	}
}

func TestUnavailableTimeoutAndLateVerification(t *testing.T) {
	p := policy(t, recovery.Recover)
	op := candidate(t, p, 0)
	r := accepted(op, 5*cadence)
	r.Live.At = 2 * cadence // Snapshot time is not write time.
	complete(t, p, r)
	if _, err := p.Unavailable(time.Second); err != nil {
		t.Fatal(err)
	}
	u, err := p.Advance(15 * cadence)
	if err != nil || factCount(u, recovery.VerificationOverdue) != 1 {
		t.Fatal("timeout did not use accepted-write time")
	}
	u, _ = p.Advance(2 * time.Second)
	if factCount(u, recovery.VerificationOverdue) != 0 || !p.Status().VerificationPending {
		t.Fatal("timeout repeated or cleared baseline")
	}
	u = observe(t, p, 3*time.Second, 20, 11)
	if u.Operation == nil || u.Operation.Kind != recovery.Inspect {
		t.Fatal("unavailability erased verification")
	}
	u = complete(t, p, recovery.OperationResult{ID: u.Operation.ID, Generation: 1, CompletedAt: 3 * time.Second,
		Outcome: recovery.Observed, Reason: recovery.ReasonUsedProgress,
		Live: &recovery.LiveObservation{Generation: 1, At: 3 * time.Second, Used: 11, Consumed: 12, Valid: true}})
	if factCount(u, recovery.VerificationConfirmed) != 1 || p.Status().Totals.Verified != 1 {
		t.Fatal("late progress did not confirm original baseline")
	}
}

func TestGenerationReplacementRetainsSlotAccountingOnly(t *testing.T) {
	p := policy(t, recovery.Recover)
	op := candidate(t, p, 0)
	complete(t, p, accepted(op, time.Second))
	u, err := p.ReplaceGeneration(2, time.Second)
	if err != nil || factCount(u, recovery.VerificationDiscarded) != 1 {
		t.Fatal("replacement did not discard old verification")
	}
	s := p.Status()
	if s.Candidate || s.VerificationPending || s.Totals.Writes != 1 || s.LastAttemptCompletedAt != time.Second || !s.HasCompletedAttempt {
		t.Fatalf("wrong replacement state: %+v", s)
	}
	if u := observe(t, p, time.Second, 20, 10); u.Operation != nil {
		t.Fatal("new attachment inherited candidate age")
	}
	if u := observe(t, p, time.Second+cadence, 20, 10); u.Operation == nil || u.Operation.Generation != 2 || u.Operation.AttemptID != 2 {
		t.Fatal("replacement lost slot sequencing")
	}
}

func TestRefusedAttemptCanVerifyEarlierWrite(t *testing.T) {
	p := policy(t, recovery.Recover)
	op := candidate(t, p, 0)
	complete(t, p, accepted(op, cadence))
	op = observe(t, p, 2*cadence, 20, 10).Operation
	u := complete(t, p, recovery.OperationResult{ID: op.ID, Generation: 1, CompletedAt: 2 * cadence,
		Outcome: recovery.Refused, Reason: recovery.ReasonUsedProgress,
		Live: &recovery.LiveObservation{Generation: 1, At: 2 * cadence, Used: 11, Consumed: 12, Valid: true}})
	if factCount(u, recovery.VerificationConfirmed) != 1 || p.Status().Totals.Refusals != 1 || u.Candidate {
		t.Fatal("valid progress was lost behind a physical refusal")
	}
}

func TestInvalidCachedSamplesResetAgeAndCannotVerify(t *testing.T) {
	p := policy(t, recovery.Recover)
	observe(t, p, 0, 20, 10)
	for _, size := range []uint32{0, 255, 65536, 256} {
		u, err := p.Observe(recovery.CachedObservation{Generation: 1, At: cadence, Avail: 500, Used: 10, Size: size})
		if err != nil || u.Operation != nil || u.Candidate || factCount(u, recovery.ObservationRejected) != 1 {
			t.Fatal("invalid sample became work")
		}
	}
	if u := observe(t, p, time.Second, 20, 10); u.Candidate {
		t.Fatal("valid sample inherited invalid observation age")
	}
}

func TestNoHistoryOrQuotaAccumulatesAcrossPersistentIncident(t *testing.T) {
	p := policy(t, recovery.Recover)
	observe(t, p, 0, 20, 10)
	for i := 1; i <= 5000; i++ {
		at := time.Duration(i) * cadence
		op := observe(t, p, at, 20, 10).Operation
		if op == nil {
			t.Fatalf("retry exhausted at %d", i)
		}
		complete(t, p, accepted(op, at))
	}
	s := p.Status()
	if s.Totals.Attempts != 5000 || s.Totals.Writes != 5000 || s.OriginAttemptID != 1 || !s.VerificationTimedOut {
		t.Fatalf("wrong persistent incident accounting: %+v", s)
	}
}

func TestRingWrapAndHealthyCompletion(t *testing.T) {
	p := policy(t, recovery.Recover)
	observe(t, p, 0, 3, 65530)
	u := observe(t, p, cadence, 3, 65530)
	if u.Operation == nil {
		t.Fatal("valid 16-bit ring wrap was rejected")
	}
	p = policy(t, recovery.Recover)
	for i := 0; i < 10000; i++ {
		used := uint16(i * 1000)
		if u := observe(t, p, time.Duration(i)*cadence, used+5, used); u.Candidate || u.Operation != nil {
			t.Fatal("healthy completing queue became a candidate")
		}
	}
}

func TestInvalidConfigurationAndInputsLeaveStateUnchanged(t *testing.T) {
	for _, c := range []recovery.Config{
		{}, {Mode: recovery.Recover, Cadence: cadence},
		{Mode: 9, Cadence: cadence, VerificationTimeout: time.Second},
		{Mode: recovery.Recover, Cadence: -1, VerificationTimeout: time.Second},
	} {
		if _, err := recovery.New(c, 1); !errors.Is(err, recovery.ErrInvalidConfig) {
			t.Fatal("invalid config accepted")
		}
	}
	p := policy(t, recovery.Recover)
	observe(t, p, cadence, 20, 10)
	before := p.Status()
	if _, err := p.Observe(recovery.CachedObservation{Generation: 1, At: 0}); !errors.Is(err, recovery.ErrInvalidTime) {
		t.Fatal("backward observation accepted")
	}
	if _, err := p.Observe(recovery.CachedObservation{Generation: 2, At: 2 * cadence}); !errors.Is(err, recovery.ErrGeneration) {
		t.Fatal("foreign generation accepted")
	}
	if _, err := p.ReplaceGeneration(0, 2*cadence); !errors.Is(err, recovery.ErrGeneration) {
		t.Fatal("zero generation accepted")
	}
	if p.Status() != before {
		t.Fatal("invalid input mutated state")
	}
	if u := observe(t, p, 2*cadence, 20, 10); u.Operation == nil {
		t.Fatal("invalid input silently changed temporal state")
	}
}

func TestOperationResultValidationIsTransactionalAndExactlyOnce(t *testing.T) {
	mutations := map[string]func(*recovery.OperationResult){
		"wrong operation":        func(r *recovery.OperationResult) { r.ID++ },
		"wrong generation":       func(r *recovery.OperationResult) { r.Generation++ },
		"backward completion":    func(r *recovery.OperationResult) { r.CompletedAt = 0 },
		"unknown outcome":        func(r *recovery.OperationResult) { r.Outcome = "invented" },
		"unknown reason":         func(r *recovery.OperationResult) { r.Reason = "invented" },
		"missing baseline":       func(r *recovery.OperationResult) { r.Live = nil },
		"invalid baseline":       func(r *recovery.OperationResult) { r.Live.Valid = false },
		"foreign baseline":       func(r *recovery.OperationResult) { r.Live.Generation++ },
		"future baseline":        func(r *recovery.OperationResult) { r.Live.At = time.Hour },
		"old baseline":           func(r *recovery.OperationResult) { r.Live.At = 0 },
		"stale used":             func(r *recovery.OperationResult) { r.Live.Used++ },
		"write before baseline":  func(r *recovery.OperationResult) { r.AcceptedAt = 0 },
		"write after completion": func(r *recovery.OperationResult) { r.AcceptedAt = time.Hour },
		"contradictory reason":   func(r *recovery.OperationResult) { r.Reason = recovery.ReasonWorkQueued },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p := policy(t, recovery.Recover)
			op := candidate(t, p, 0)
			before := p.Status()
			r := accepted(op, 2*cadence)
			mutate(&r)
			if _, err := p.Complete(r); err == nil || p.Status() != before {
				t.Fatal("invalid result accepted or partially applied")
			}
			r = accepted(op, 2*cadence)
			complete(t, p, r)
			after := p.Status()
			if _, err := p.Complete(r); !errors.Is(err, recovery.ErrUnexpectedResult) || p.Status() != after {
				t.Fatal("duplicate result changed accounting")
			}
		})
	}
}

func TestInFlightProtocolAndReturnedValuesHaveNoAliasing(t *testing.T) {
	p := policy(t, recovery.Recover)
	op := candidate(t, p, 0)
	original := *op
	op.ID = 999
	if p.Status().Operation != original {
		t.Fatal("returned operation aliases policy state")
	}
	for _, change := range []func() error{
		func() error { _, e := p.Advance(time.Second); return e },
		func() error { _, e := p.Unavailable(time.Second); return e },
		func() error { _, e := p.ReplaceGeneration(2, time.Second); return e },
		func() error { _, e := p.Stop(time.Second); return e },
	} {
		if err := change(); !errors.Is(err, recovery.ErrOperationInFlight) {
			t.Fatal("mutation bypassed in-flight result accounting")
		}
	}
	r := accepted(&original, 2*cadence)
	u := complete(t, p, r)
	r.Live.Used = 999
	u.Facts[0].Outcome = recovery.Refused
	if s := p.Status(); s.Totals.Writes != 1 || s.OriginAttemptID != original.AttemptID {
		t.Fatal("caller changed retained receipt state")
	}
	if _, err := p.Stop(3 * cadence); err != nil {
		t.Fatal(err)
	}
	if !p.Status().VerificationPending {
		t.Fatal("stop hid unresolved verification from final status")
	}
	if _, err := p.Advance(time.Hour); !errors.Is(err, recovery.ErrStopped) {
		t.Fatal("stopped policy resumed")
	}
}

func TestTimeAdvanceNeverManufacturesWork(t *testing.T) {
	p := policy(t, recovery.Recover)
	observe(t, p, 0, 20, 10)
	u, err := p.Advance(time.Hour)
	if err != nil || u.Operation != nil || u.Candidate {
		t.Fatal("elapsed time without a later sample created a notification")
	}
}

func TestMissingLiveIdentityFailureDiscardsVerification(t *testing.T) {
	for _, inspect := range []bool{false, true} {
		p := policy(t, recovery.Recover)
		first := candidate(t, p, 0)
		complete(t, p, accepted(first, cadence))
		used := uint16(10)
		if inspect {
			used++
		}
		op := observe(t, p, 2*cadence, 20, used).Operation
		if (op.Kind == recovery.Inspect) != inspect {
			t.Fatal("wrong test operation")
		}
		u := complete(t, p, recovery.OperationResult{ID: op.ID, Generation: op.Generation,
			CompletedAt: 2 * cadence, Outcome: recovery.Unavailable, Reason: recovery.ReasonIdentityChanged})
		if p.Status().VerificationPending || factCount(u, recovery.VerificationDiscarded) != 1 || u.Candidate {
			t.Fatal("identity failure retained the old generation's verification")
		}
	}
}

func TestUnavailableReceiptCannotInventVerification(t *testing.T) {
	p := policy(t, recovery.Recover)
	first := candidate(t, p, 0)
	complete(t, p, accepted(first, cadence))
	op := observe(t, p, 2*cadence, 20, 11).Operation
	before := p.Status()
	r := recovery.OperationResult{ID: op.ID, Generation: 1, CompletedAt: 2 * cadence,
		Outcome: recovery.Unavailable, Reason: recovery.ReasonUnavailable,
		Live: &recovery.LiveObservation{Generation: 1, At: 2 * cadence, Used: 11, Consumed: 12, Valid: true}}
	if _, err := p.Complete(r); !errors.Is(err, recovery.ErrInvalidResult) || p.Status() != before {
		t.Fatal("contradictory unavailable result mutated verification")
	}
	r.Live.Valid = false
	complete(t, p, r)
	if !p.Status().VerificationPending || p.Status().Totals.Verified != 0 {
		t.Fatal("invalid observation verified a write")
	}
}
