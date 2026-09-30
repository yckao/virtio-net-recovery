//go:build linux && amd64

package watch

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type observationFaultTarget struct {
	*candidateTarget
	duplicateAt, duplicates             int
	duplicateErr, ringErr, inventoryErr error
	missingEvents                       bool
}

func (t *observationFaultTarget) Duplicate(fd int) (int, error) {
	t.duplicates++
	if t.duplicates == t.duplicateAt {
		return -1, t.duplicateErr
	}
	return t.candidateTarget.Duplicate(fd)
}

func (t *observationFaultTarget) Ring(s Snapshot) (uint16, uint16, error) {
	if t.ringErr != nil {
		return 0, 0, t.ringErr
	}
	return t.candidateTarget.Ring(s)
}

func (t *observationFaultTarget) Inventory() ([]int, map[uint32][]int, error) {
	if t.inventoryErr != nil {
		return nil, nil, t.inventoryErr
	}
	vhosts, events, err := t.candidateTarget.Inventory()
	if t.missingEvents {
		events = nil
	}
	return vhosts, events, err
}

type observationFaultSnapshot struct {
	value         Snapshot
	failAt, calls int
	err           error
}

func (b *observationFaultSnapshot) Snapshot(int) (Snapshot, error) {
	b.calls++
	if b.calls == b.failAt {
		return Snapshot{}, b.err
	}
	return b.value, nil
}

// The harness supplies the cycle boundary; errors and classification come from
// recoverLive and the same production predicate used by runRecover.
func accumulateObservationFailure(failed bool, ctx context.Context, l *logger, err error) bool {
	return failed || l.err == nil && ctx.Err() == nil && isObservationError(err)
}

func TestMetricsRecoveryObservationFailuresReachCompletedPoll(t *testing.T) {
	for _, kind := range []string{"initial_duplicate", "initial_snapshot", "ring", "inventory", "vhost_duplicate", "eventfd_duplicate", "eventfd_read", "final_snapshot"} {
		t.Run(kind, func(t *testing.T) {
			m, q, target, _, l, _, _ := metricRecoveryFixture(t)
			w := newMetricWorker(t, m)
			defer w.Retire()
			injected := fmt.Errorf("observation syscall: %w", unix.EIO)
			fault := &observationFaultTarget{candidateTarget: target, duplicateErr: injected}
			bpf := &observationFaultSnapshot{value: q.snapshot, err: injected}
			cause := error(unix.EIO)
			switch kind {
			case "initial_duplicate":
				fault.duplicateAt = 1
			case "initial_snapshot":
				bpf.failAt = 1
			case "ring":
				fault.ringErr = injected
			case "inventory":
				fault.inventoryErr = injected
			case "vhost_duplicate":
				fault.duplicateAt = 2
			case "eventfd_duplicate":
				fault.duplicateAt = 3
			case "eventfd_read":
				fault.duplicateAt, fault.duplicateErr, cause = 3, nil, unix.ENOENT
			case "final_snapshot":
				bpf.failAt = 2
			}
			ctx := context.Background()
			err := recoverLive(ctx, Config{Mode: "recover", Metrics: m}, fault, bpf, l, q, true)
			if !errors.Is(err, cause) || l.err != nil {
				t.Fatalf("read failure lost its cause or became stdout failure: %v / %v", err, l.err)
			}
			if kind != "eventfd_read" && err.Error() != injected.Error() {
				t.Fatalf("read failure message changed: %v", err)
			}
			if !q.policy.Refused(true) || q.policy.Refusals != 1 || q.policy.Writes != 0 {
				t.Fatal("legacy charged refusal accounting changed")
			}
			failed := accumulateObservationFailure(false, ctx, l, fmt.Errorf("outer: %w", err))
			recordRecoveryMetrics(w, map[int]*recoveryQueue{q.fd: q}, nil, 1, .1, failed)
			values := scrapeMetrics(t, m)
			requireMetric(t, values, "poll_errors_total", 1)
			requireMetric(t, values, `writes_total{result="refused"}`, 1)
			requireMetric(t, values, `writes_total{result="error"}`, 0)
			requireMetric(t, values, `decisions_total{reason="unavailable"}`, 1)
		})
	}
}

func TestMetricsRecoverySafetyWriteAndCancellationAreNotObservationFailures(t *testing.T) {
	for _, kind := range []string{"missing_eventfd", "blocking", "write_error", "wrapped_write", "cancel", "deadline", "wrapped_cancel", "initial_identity", "final_identity", "invalid_final_snapshot"} {
		t.Run(kind, func(t *testing.T) {
			m, q, target, _, l, _, fd := metricRecoveryFixture(t)
			w := newMetricWorker(t, m)
			defer w.Retire()
			fault := &observationFaultTarget{candidateTarget: target}
			ctx := context.Background()
			var bpf snapshotter = testBPF{value: q.snapshot}
			result := "refused"
			switch kind {
			case "missing_eventfd":
				fault.missingEvents = true
			case "blocking":
				flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags&^unix.O_NONBLOCK); err != nil {
					t.Fatal(err)
				}
			case "write_error":
				var data [8]byte
				binary.NativeEndian.PutUint64(data[:], ^uint64(0)-1)
				if _, err := unix.Write(fd, data[:]); err != nil {
					t.Fatal(err)
				}
				result = "error"
			case "wrapped_write":
				bpf = testBPF{err: fmt.Errorf("snapshot failure: %w", &KickWriteError{Err: unix.EIO})}
				result = "error"
			case "cancel":
				child, cancel := context.WithCancel(ctx)
				cancel()
				ctx = child
			case "deadline":
				child, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx = child
			case "wrapped_cancel":
				bpf = testBPF{err: fmt.Errorf("snapshot cancelled: %w", context.Canceled)}
			default:
				changed := q.snapshot
				changed.Context++
				if kind == "initial_identity" {
					bpf = testBPF{value: changed}
				} else {
					if kind == "invalid_final_snapshot" {
						changed.PollWQH++
					}
					bpf = &sequenceSnapshotter{snapshots: []Snapshot{q.snapshot, changed}}
				}
			}
			err := recoverLive(ctx, Config{Mode: "recover", Metrics: m}, fault, bpf, l, q, true)
			if err == nil || l.err != nil || isObservationError(fmt.Errorf("outer: %w", err)) {
				t.Fatalf("guard/write/cancellation became an observation error: %v", err)
			}
			recordRecoveryMetrics(w, map[int]*recoveryQueue{q.fd: q}, nil, 1, .1, accumulateObservationFailure(false, ctx, l, err))
			values := scrapeMetrics(t, m)
			requireMetric(t, values, "poll_errors_total", 0)
			requireMetric(t, values, `writes_total{result="`+result+`"}`, 1)
			requireMetric(t, values, `writes_total{result="success"}`, 0)
		})
	}
}

type observationFailedOutput struct{ err error }

func (w observationFailedOutput) Write([]byte) (int, error) { return 0, w.err }

func TestMetricsObservationPipelineExcludesAnyStdoutError(t *testing.T) {
	for _, outputErr := range []error{errors.New("output failed"), fmt.Errorf("output: %w", context.Canceled), &KickWriteError{Err: unix.EIO}, fmt.Errorf("output: %w", ErrKickIdentityChanged)} {
		for _, before := range []bool{true, false} {
			m, q, target, _, l, _, _ := metricRecoveryFixture(t)
			w := newMetricWorker(t, m)
			l.encoder = json.NewEncoder(observationFailedOutput{err: outputErr})
			if before {
				l.emit("output_test", map[string]any{})
			}
			ctx := context.Background()
			err := recoverLive(ctx, Config{Mode: "recover", Metrics: m}, target, testBPF{value: q.snapshot}, l, q, true)
			if !errors.Is(err, outputErr) || !errors.Is(l.err, outputErr) {
				t.Fatalf("stdout origin lost: %v / %v", err, l.err)
			}
			recordRecoveryMetrics(w, map[int]*recoveryQueue{q.fd: q}, nil, 1, .1, accumulateObservationFailure(false, ctx, l, err))
			values := scrapeMetrics(t, m)
			want := float64(1)
			if before {
				want = 0
			}
			requireMetric(t, values, "poll_errors_total", 0)
			requireMetric(t, values, `writes_total{result="success"}`, want)
			requireMetric(t, values, `writes_total{result="error"}`, 0)
			requireMetric(t, values, `writes_total{result="refused"}`, 0)
			w.Retire()
		}
	}
}

func TestMetricsObservationCycleDeduplicatesOverlapAndResets(t *testing.T) {
	m, q, target, _, l, _, _ := metricRecoveryFixture(t)
	w := newMetricWorker(t, m)
	defer w.Retire()
	ctx := context.Background()
	liveFailed := false
	for range 2 {
		err := recoverLive(ctx, Config{Mode: "recover", Metrics: m}, target, testBPF{err: unix.EIO}, l, q, true)
		if !errors.Is(err, unix.EIO) {
			t.Fatalf("missing actual read failure: %v", err)
		}
		liveFailed = accumulateObservationFailure(liveFailed, ctx, l, err)
		q.policy.Refused(true)
	}
	legacyBefore, legacyAfter := uint64(4), uint64(4)
	known := map[int]*recoveryQueue{q.fd: q}
	recordRecoveryMetrics(w, known, nil, 1, .1, legacyAfter > legacyBefore || liveFailed)
	requireMetric(t, scrapeMetrics(t, m), "poll_errors_total", 1)
	if q.policy.Refusals != 2 {
		t.Fatal("legacy refusals were deduplicated with metric cycles")
	}
	liveFailed = false
	err := recoverLive(ctx, Config{Mode: "recover", Metrics: m}, target, testBPF{err: unix.EIO}, l, q, true)
	if !errors.Is(err, unix.EIO) {
		t.Fatalf("missing overlapping read failure: %v", err)
	}
	liveFailed = accumulateObservationFailure(liveFailed, ctx, l, err)
	legacyAfter = 6
	recordRecoveryMetrics(w, known, nil, 1, .1, legacyAfter > legacyBefore || liveFailed)
	requireMetric(t, scrapeMetrics(t, m), "poll_errors_total", 2)
	liveFailed, legacyBefore = false, legacyAfter
	err = recoverLive(ctx, Config{Mode: "recover", Metrics: m}, target, testBPF{value: q.snapshot}, l, q, true)
	if err != nil {
		t.Fatal(err)
	}
	liveFailed = accumulateObservationFailure(liveFailed, ctx, l, err)
	recordRecoveryMetrics(w, known, nil, 1, .1, legacyAfter > legacyBefore || liveFailed)
	requireMetric(t, scrapeMetrics(t, m), "poll_errors_total", 2)
	requireMetric(t, scrapeMetrics(t, m), `writes_total{result="success"}`, 1)
}
