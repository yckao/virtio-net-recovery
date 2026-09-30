package watch

import "testing"

func TestRecurringRecoveryDoesNotExhaustAndPacesRefusals(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	if p.Observe(11, 10, 256, 0) || !p.Observe(12, 10, 256, .101) {
		t.Fatal("candidate must require two observations separated by a cadence")
	}
	for i := 0; i < 3000; i++ {
		now := .101 + float64(i)*.11
		if !p.Attempt(now) {
			t.Fatalf("persistent incident exhausted at attempt %d", i)
		}
		// Alternate successful writes and refused validation for over five
		// minutes; neither may bypass pacing or permanently disable retry.
		if i%2 == 0 {
			p.Written(now, 10, 10)
		} else {
			p.Refused(true)
		}
		p.FinishAttempt(now + .005)
		if p.Attempt(now+.09) || p.Attempt(now+.104) {
			t.Fatal("validation/write completion did not bound retry frequency")
		}
	}
	if p.Attempts != 3000 || p.Writes != 1500 || p.Refusals != 1500 {
		t.Fatalf("wrong aggregate counts: %+v", p)
	}
	if p.MaxAttemptGap < .109 || p.MaxAttemptGap > .111 {
		t.Fatalf("incorrect measured attempt gap: %v", p.MaxAttemptGap)
	}
}

func TestVerificationKeepsFirstWriteAndRequiresConsumptionAndCompletion(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	p.Written(1, 10, 11)
	p.Written(1.2, 10, 11)
	if p.PendingAge(1.3) < .29 {
		t.Fatal("repeat write overwrote the initial verification time")
	}
	if _, ok := p.Verify(1.3, 10, 12); ok {
		t.Fatal("consumption alone confirmed recovery")
	}
	if _, ok := p.Verify(1.3, 11, 11); ok {
		t.Fatal("completion alone confirmed recovery")
	}
	if !p.Unconfirmed(2, 1) || p.Unconfirmed(2.1, 1) {
		t.Fatal("unconfirmed report should occur once without clearing verification")
	}
	p.Written(2.2, 10, 11)
	latency, ok := p.Verify(2.3, 11, 12)
	if !ok || latency < 1.29 || latency > 1.31 || p.Confirmed != 1 || p.verification != nil {
		t.Fatalf("incorrect delayed verification: %v %v %+v", latency, ok, p)
	}
}

func TestPartialReadInvalidationDoesNotConfirmOrPostponeRecovery(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	p.Observe(20, 10, 256, 0)
	p.Written(.11, 10, 10)
	p.Invalidate(.12)
	if p.Observe(20, 10, 256, .3) || !p.Observe(20, 10, 256, .41) {
		t.Fatal("read failure did not require a new observation interval")
	}
	if p.PendingAge(.5) < .38 || p.Confirmed != 0 {
		t.Fatal("read failure erased or confirmed the outstanding recovery")
	}
	if p.Observe(500, 10, 256, .6) {
		t.Fatal("inconsistent ring indices became a recovery candidate")
	}
}

func TestBackdatedSummaryAndVerificationFailureTelemetry(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	p.Written(1.1, 10, 10)
	if p.PendingAge(1) != 0 {
		t.Fatal("a loop timestamp before the write emitted negative pending age")
	}
	if p.Refused(false) || p.Refusals != 0 || p.Attempts != 0 {
		t.Fatal("verification-only failure was charged as a recovery refusal")
	}
	if !p.Attempt(1.2) || !p.Refused(true) || p.Refusals != 1 || p.Attempts != 1 {
		t.Fatal("charged recovery refusal was omitted from attempt telemetry")
	}
}

func TestUnsupportedDiscoveryDoesNotForceHealthyCadenceOrRepeatLogs(t *testing.T) {
	failures := map[int]string{}
	for i := 0; i < 100; i++ {
		report, refresh := noteDiscoveryFailure(failures, 7, "unsupported ring", false)
		if refresh || report != (i == 0) {
			t.Fatal("never-supported slot forced cadence-rate inventory or repeated logs")
		}
	}
	delete(failures, 7) // Successful discovery established a supported attachment.
	if report, refresh := noteDiscoveryFailure(failures, 7, "attachment unavailable", true); !report || !refresh {
		t.Fatal("previously supported attachment did not request prompt rediscovery")
	}
	for i := 0; i < 100; i++ {
		if report, refresh := noteDiscoveryFailure(failures, 7, "attachment unavailable", true); report || refresh {
			t.Fatal("persistent supported-slot failure forced cadence-rate inventory or repeated logs")
		}
	}
	if report, refresh := noteDiscoveryFailure(failures, 7, "different error", true); !report || refresh {
		t.Fatal("changing error text restarted the fast discovery burst")
	}
	delete(failures, 7) // Successful discovery or disappearance resets the report.
	if report, _ := noteDiscoveryFailure(failures, 7, "unsupported ring", false); !report {
		t.Fatal("new slot generation suppressed its first failure report")
	}
}

func TestHealthyCompletionRemainsNonCandidate(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	for i := 0; i < 10000; i++ {
		used := uint16(i * 1000) // Exercise natural 16-bit wrap.
		if p.Observe(used+5, used, 256, float64(i)*.11) {
			t.Fatal("continuously completing healthy queue became a candidate")
		}
	}
	if p.Attempts != 0 || p.Writes != 0 {
		t.Fatal("healthy observation initiated recovery")
	}
}

func TestDisappearingSlotsDoNotAccumulatePoliciesOrResetActivePacing(t *testing.T) {
	active := NewRecoveryPolicy(.1)
	active.Attempt(1)
	active.FinishAttempt(1.05)
	active.Written(1.05, 10, 11)
	policies := map[int]*RecoveryPolicy{7: active}
	for fd := 8; fd < 10008; fd++ {
		policies[fd] = NewRecoveryPolicy(.1)
		pruneRecoveryPolicies(policies, []int{7})
		if len(policies) != 1 || policies[7] != active {
			t.Fatal("slot churn retained historical state or replaced the active policy")
		}
	}
	if active.Attempt(1.14) || active.PendingAge(1.15) < .099 {
		t.Fatal("pruning historical slots reset active pacing or verification")
	}
	pruneRecoveryPolicies(policies, nil)
	if len(policies) != 0 {
		t.Fatal("an empty inventory retained policies")
	}
}

func TestRecoveryProgressTotalsSurvivePolicyPruningAndNewSlots(t *testing.T) {
	var totals recoveryProgressTotals
	p := NewRecoveryPolicy(.1)
	policies := map[int]*RecoveryPolicy{7: p}
	beforeWrites, beforeConfirmed := p.Writes, p.Confirmed
	p.Written(1, 10, 11)
	p.Written(1.2, 10, 11)
	totals.add(p, beforeWrites, beforeConfirmed)
	beforeWrites, beforeConfirmed = p.Writes, p.Confirmed
	if _, ok := p.Verify(1.3, 11, 12); !ok {
		t.Fatal("later live progress did not confirm the first slot")
	}
	totals.add(p, beforeWrites, beforeConfirmed)
	pruneRecoveryPolicies(policies, nil)
	if len(policies) != 0 || totals.writes != 2 || totals.confirmed != 1 {
		t.Fatalf("pruning lost lifetime progress totals: %+v", totals)
	}

	p = NewRecoveryPolicy(.1)
	policies[8] = p
	beforeWrites, beforeConfirmed = p.Writes, p.Confirmed
	p.Written(2, 20, 21)
	if _, ok := p.Verify(2.1, 20, 22); ok {
		t.Fatal("consumption alone confirmed the new slot")
	}
	totals.add(p, beforeWrites, beforeConfirmed)
	if totals.writes != 3 || totals.confirmed != 1 {
		t.Fatalf("partial progress changed lifetime confirmations: %+v", totals)
	}
	beforeWrites, beforeConfirmed = p.Writes, p.Confirmed
	if _, ok := p.Verify(2.2, 21, 22); !ok {
		t.Fatal("later live progress did not confirm the new slot")
	}
	totals.add(p, beforeWrites, beforeConfirmed)
	if totals.writes != 3 || totals.confirmed != 2 {
		t.Fatalf("new slot replaced lifetime progress totals: %+v", totals)
	}
	beforeWrites, beforeConfirmed = p.Writes, p.Confirmed
	p.Verify(2.3, 22, 23)
	totals.add(p, beforeWrites, beforeConfirmed)
	pruneRecoveryPolicies(policies, nil)
	if totals.writes != 3 || totals.confirmed != 2 {
		t.Fatalf("verification without a pending write duplicated confirmations: %+v", totals)
	}
}

func TestCoverageIncludesUnsupportedSlotsWithoutDoubleCountingKnownFailures(t *testing.T) {
	failures := map[int]string{8: "temporarily unavailable", 9: "unsupported ring"}
	fds := unavailableRecoveryFDs([]int{7, 8}, []int{7}, failures)
	if len(fds) != 2 || fds[0] != 8 || fds[1] != 9 {
		t.Fatalf("incomplete or duplicated unavailable coverage: %v", fds)
	}
	fds = unavailableRecoveryFDs([]int{7, 8}, nil, failures)
	if len(fds) != 3 || fds[0] != 7 || fds[1] != 8 || fds[2] != 9 {
		t.Fatalf("failed batch omitted previously healthy coverage: %v", fds)
	}
}

func TestLostCoverageStillReportsTimeoutAndPreservesProgressVerification(t *testing.T) {
	p := NewRecoveryPolicy(.1)
	p.Written(1, 10, 11)
	p.Invalidate(2)
	if !p.Unconfirmed(6, 5) || p.Unconfirmed(7, 5) || p.PendingAge(7) != 6 {
		t.Fatal("lost coverage suspended, repeated or erased the original timeout")
	}
	if p.Observe(20, 10, 256, 7) || !p.Observe(20, 10, 256, 7.11) || !p.Attempt(7.11) {
		t.Fatal("timeout disabled recovery after coverage resumed")
	}
	p.Written(7.11, 10, 11)
	latency, confirmed := p.Verify(8, 11, 12)
	if !confirmed || latency != 7 {
		t.Fatal("timeout or repeated write lost the original progress baseline")
	}
}
