//go:build linux && amd64

package watch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func metricRecoveryFixture(t *testing.T) (*Metrics, *recoveryQueue, *candidateTarget, *float64, *logger, *bytes.Buffer, int) {
	t.Helper()
	q, target, clock, l, output, fd := recoveryJournalFixture(t)
	m := NewMetrics()
	q.metrics, q.journal = m, nil
	output.Reset()
	q.ensureJournal(l)
	q.journal.now = func() float64 { return *clock }
	q.observeCached(l, *clock, true)
	return m, q, target, clock, l, output, fd
}

func TestMetricsFirstCachedCandidateCoverageAndRetirement(t *testing.T) {
	m, q, _, clock, l, _, _ := metricRecoveryFixture(t)
	q.observeCached(l, *clock, true)
	requireMetric(t, scrapeMetrics(t, m), "candidates_total", 1)
	w := newMetricWorker(t, m)
	recordRecoveryMetrics(w, map[int]*recoveryQueue{q.fd: q}, map[int]string{q.fd: "known", 99: "unsupported"}, 1, .1, true)
	values := scrapeMetrics(t, m)
	for name, want := range map[string]float64{"open_episodes": 1, "sampled_queues": 1, "unavailable_queues": 1, "poll_errors_total": 1} {
		requireMetric(t, values, name, want)
	}
	q.closeEpisode(EpisodeOutcomeStopped)
	w.Retire()
	values = scrapeMetrics(t, m)
	for _, name := range []string{"open_episodes", "sampled_queues", "unavailable_queues", "poll_gap_seconds", "poll_max_gap_seconds"} {
		requireMetric(t, values, name, 0)
	}
	requireMetric(t, values, "poll_errors_total", 1)
}

func TestMetricsEveryAcceptedWriteSurvivesJournalRateLimit(t *testing.T) {
	m, q, target, clock, l, output, fd := metricRecoveryFixture(t)
	for range 5 {
		if err := recoverLive(context.Background(), Config{Mode: "recover", Metrics: m}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
			t.Fatal(err)
		}
		*clock += .01
	}
	if len(recoveryJournalRecords(t, output)) != 2 {
		t.Fatal("fixture did not exercise suppressed journal write records")
	}
	values := scrapeMetrics(t, m)
	requireMetric(t, values, `writes_total{result="success"}`, 5)
	requireMetric(t, values, `decisions_total{reason="unconsumed"}`, 5)
	requireMetric(t, values, "candidates_total", 1)
	var data [8]byte
	if _, err := unix.Read(fd, data[:]); err != nil || binary.NativeEndian.Uint64(data[:]) != 5 {
		t.Fatalf("actual eventfd writes were not observed: %v", err)
	}
}

type failFinalSnapshot struct {
	value Snapshot
	calls int
}

func (b *failFinalSnapshot) Snapshot(int) (Snapshot, error) {
	b.calls++
	if b.calls > 1 {
		return Snapshot{}, errors.New("final live snapshot unavailable")
	}
	return b.value, nil
}

func TestMetricsWriteErrorIdentityAndFinalValidationRefusal(t *testing.T) {
	for _, kind := range []string{"write_error", "identity", "final_unavailable"} {
		t.Run(kind, func(t *testing.T) {
			m, q, target, _, l, _, fd := metricRecoveryFixture(t)
			s := q.snapshot
			var bpf snapshotter = testBPF{value: s}
			result, reason := "refused", "identity_change"
			switch kind {
			case "write_error":
				var full [8]byte
				binary.NativeEndian.PutUint64(full[:], ^uint64(0)-1)
				if _, err := unix.Write(fd, full[:]); err != nil {
					t.Fatal(err)
				}
				result, reason = "error", "unconsumed"
			case "identity":
				s.Context++
				bpf = testBPF{value: s}
			case "final_unavailable":
				bpf = &failFinalSnapshot{value: s}
				reason = "unavailable"
			}
			if err := recoverLive(context.Background(), Config{Mode: "recover", Metrics: m}, target, bpf, l, q, true); err == nil {
				t.Fatal("expected failed or refused kick")
			}
			values := scrapeMetrics(t, m)
			requireMetric(t, values, `writes_total{result="`+result+`"}`, 1)
			requireMetric(t, values, `writes_total{result="success"}`, 0)
			requireMetric(t, values, `decisions_total{reason="`+reason+`"}`, 1)
			var decisions float64
			for _, label := range decisionLabels {
				decisions += values[`vhost_watch_decisions_total{reason="`+label+`"}`]
			}
			if decisions != 1 {
				t.Fatalf("one live call produced %v decisions", decisions)
			}
		})
	}
}

func TestMetricsStdoutFailureBeforeAndAfterAcceptedWrite(t *testing.T) {
	for _, before := range []bool{true, false} {
		m, q, target, _, l, _, _ := metricRecoveryFixture(t)
		l.encoder = json.NewEncoder(journalFailedOutput{})
		if before {
			l.emit("test_output", map[string]any{})
		}
		err := recoverLive(context.Background(), Config{Mode: "recover", Metrics: m}, target, testBPF{value: q.snapshot}, l, q, true)
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("stdout error was lost: %v", err)
		}
		values := scrapeMetrics(t, m)
		want := float64(1)
		if before {
			want = 0
		}
		requireMetric(t, values, `writes_total{result="success"}`, want)
		requireMetric(t, values, `writes_total{result="error"}`, 0)
		requireMetric(t, values, `writes_total{result="refused"}`, 0)
		requireMetric(t, values, "poll_errors_total", 0)
	}
}

func TestMetricsManualKickCountsAcceptedWriteDespiteStdoutFailure(t *testing.T) {
	m, q, target, _, l, _, _ := metricRecoveryFixture(t)
	l.encoder = json.NewEncoder(journalFailedOutput{})
	err := kick(context.Background(), Config{Mode: "kick", VhostFD: -1, Metrics: m}, target, testBPF{value: q.snapshot}, l)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("stdout error was lost: %v", err)
	}
	values := scrapeMetrics(t, m)
	requireMetric(t, values, `writes_total{result="success"}`, 1)
	requireMetric(t, values, `writes_total{result="refused"}`, 0)
	requireMetric(t, values, `writes_total{result="error"}`, 0)
}

func TestMetricsEarlyTargetFailureRetiresRegisteredWorker(t *testing.T) {
	m := NewMetrics()
	c := Config{PID: os.Getpid(), Mode: "observe", StateDir: t.TempDir(), BPFObject: "/missing.bpf.o",
		Interval: .1, InventoryInterval: 5, VerifyTimeout: 5, SummaryInterval: 5, VhostFD: -1, Metrics: m}
	if err := Run(context.Background(), c, io.Discard); err == nil {
		t.Fatal("test process must not be accepted as QEMU")
	}
	if len(m.workers) != 0 {
		t.Fatal("early target failure retained a metrics worker")
	}
	requireMetric(t, scrapeMetrics(t, m), "poll_errors_total", 0)
}
