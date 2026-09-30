//go:build linux && amd64

package watch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"
)

func TestObserveDoesNotWriteWhileRecoverUsesSameLiveCandidate(t *testing.T) {
	for _, mode := range []string{"observe", "recover"} {
		t.Run(mode, func(t *testing.T) {
			fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			id, err := eventID("self", fd)
			if err != nil {
				t.Fatal(err)
			}
			s := configured()
			s.EventID = id
			target := &candidateTarget{testTarget: testTarget{source: fd, alive: true}, snapshot: s}
			q := &recoveryQueue{fd: 42, snapshot: s, policy: NewRecoveryPolicy(.1), avail: 20, used: 10}
			var output bytes.Buffer
			l := &logger{encoder: json.NewEncoder(&output)}
			if err := recoverLive(context.Background(), Config{Mode: mode}, target, testBPF{value: s}, l, q, true); err != nil {
				t.Fatal(err)
			}
			n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if (n > 0) != (mode == "recover") {
				t.Fatalf("%s wrote incorrectly: %d", mode, n)
			}
			if !strings.Contains(output.String(), `"event":"candidate"`) {
				t.Fatal("shared live candidate was not reported")
			}
			if (q.policy.Writes > 0) != (mode == "recover") {
				t.Fatal("write accounting conflated observe and recover")
			}
			if q.lastDecision != EpisodeReasonUnconsumed || q.summary(monotonic())["last_decision"] != EpisodeReasonUnconsumed {
				t.Fatal("successful live confirmation lost its unconsumed decision")
			}
		})
	}
}

type candidateTarget struct {
	testTarget
	snapshot    Snapshot
	avail, used uint16
	haveIndices bool
}

func (t *candidateTarget) Inventory() ([]int, map[uint32][]int, error) {
	return []int{42}, map[uint32][]int{t.snapshot.EventID: {66}}, nil
}
func (t *candidateTarget) Ring(Snapshot) (uint16, uint16, error) {
	if t.haveIndices {
		return t.avail, t.used, nil
	}
	return 20, 10, nil
}

func TestQueuedWorkRefusesRecoveryWithoutClaimingWorkerIdle(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	id, err := eventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	s := configured()
	s.EventID, s.WorkFlags = id, 1<<1
	target := &candidateTarget{testTarget: testTarget{source: fd, alive: true}, snapshot: s}
	q := &recoveryQueue{fd: 42, snapshot: s, policy: NewRecoveryPolicy(.1), avail: 20, used: 10}
	var output bytes.Buffer
	l := &logger{encoder: json.NewEncoder(&output)}
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: s}, l, q, true); err != nil {
		t.Fatal(err)
	}
	if q.policy.Writes != 0 || q.lastDecision != EpisodeReasonBusy || q.policy.Refusals != 1 {
		t.Fatal("queued work was treated as a write candidate")
	}
	if !strings.Contains(output.String(), `"event":"candidate"`) || !strings.Contains(output.String(), `"last_write_reason":"work_queued"`) {
		t.Fatal("queued work refusal lost its candidate episode or fixed reason")
	}
}

func recoveryJournalFixture(t *testing.T) (*recoveryQueue, *candidateTarget, *float64, *logger, *bytes.Buffer, int) {
	t.Helper()
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(fd) })
	id, err := eventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	s := configured()
	s.EventID = id
	q := &recoveryQueue{fd: 42, snapshot: s, policy: NewRecoveryPolicy(.1), avail: 20, used: 10}
	target := &candidateTarget{testTarget: testTarget{source: fd, alive: true}, snapshot: s}
	output := new(bytes.Buffer)
	l := &logger{encoder: json.NewEncoder(output)}
	clock := new(float64)
	q.ensureJournal(l)
	q.journal.now = func() float64 { return *clock }
	q.observeCached(l, *clock, true)
	return q, target, clock, l, output, fd
}

func recoveryJournalRecords(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	var rows []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	for {
		var fields map[string]any
		if err := decoder.Decode(&fields); err != nil {
			if err == io.EOF {
				return rows
			}
			t.Fatal(err)
		}
		rows = append(rows, fields)
	}
}

func TestRecoveryJournalVerificationRetainsOriginAcrossTimeoutAndRetry(t *testing.T) {
	q, target, clock, l, output, _ := recoveryJournalFixture(t)
	origin := q.journal.EventID()
	if !q.policy.Attempt(*clock) {
		t.Fatal("first recovery was not eligible")
	}
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
		t.Fatal(err)
	}
	if q.verificationEventID != origin {
		t.Fatal("first accepted write did not retain its active episode ID")
	}
	*clock = 31
	q.tickDiagnostics(l, *clock, 5)
	if q.journal.Active() || q.verificationEventID != origin || !q.policy.verification.unconfirmed {
		t.Fatal("diagnostic timeout changed the recovery verification baseline")
	}
	*clock = 32
	q.observeCached(l, *clock, true)
	newID := q.journal.EventID()
	if newID == origin || !q.policy.Attempt(*clock) {
		t.Fatal("diagnostic timeout blocked a new episode or paced recovery")
	}
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
		t.Fatal(err)
	}
	if q.policy.Writes != 2 || q.policy.verification.at != 0 || q.verificationEventID != origin {
		t.Fatal("retry replaced the first write's verification origin")
	}
	*clock = 32.1
	target.haveIndices, target.avail, target.used = true, 20, 11
	s := q.snapshot
	s.LastAvail = 11
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: s}, l, q, false); err != nil {
		t.Fatal(err)
	}
	if q.policy.verification != nil || q.verificationEventID != "" {
		t.Fatal("confirmed progress did not clear its pending origin")
	}
	var progress, unconfirmed bool
	for _, r := range recoveryJournalRecords(t, output) {
		switch r["event"] {
		case "recovery_unconfirmed":
			unconfirmed = r["event_id"] == origin && r["verification_timed_out"] == true
		case "progress_after_kick":
			progress = r["event_id"] == origin && r["event_id"] != newID && r["verification_timed_out"] == true
		}
	}
	if !progress || !unconfirmed {
		t.Fatal("verification records did not correlate with the original write")
	}
}

func TestRecoveryJournalFreshWriteOpensDuringDiagnosticReopenDelay(t *testing.T) {
	for _, reopenDelay := range []bool{false, true} {
		name := "active_episode"
		if reopenDelay {
			name = "reopen_delay"
		}
		t.Run(name, func(t *testing.T) {
			q, target, clock, l, output, fd := recoveryJournalFixture(t)
			oldID := q.journal.EventID()
			if reopenDelay {
				q.closeEpisode(EpisodeOutcomeQuiet)
			}
			*clock = .1
			q.observeCached(l, *clock, true)
			if q.journal.Active() == reopenDelay {
				t.Fatal("ordinary candidate changed the episode's reopen eligibility")
			}
			if !q.policy.Attempt(*clock) {
				t.Fatal("first recovery was not eligible")
			}
			if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
				t.Fatal(err)
			}
			id := q.journal.EventID()
			if !q.journal.Active() || id == "" || q.verificationEventID != id || (id == oldID) == reopenDelay {
				t.Fatal("a new verification borrowed a closed origin or replaced an active origin")
			}
			summary := q.summary(*clock)
			if summary["event_id"] != id || summary["verification_event_id"] != id || summary["last_decision"] != EpisodeReasonUnconsumed || summary["writes"] != uint64(1) {
				t.Fatalf("first-write summary lost its live decision or origin: %+v", summary)
			}
			writes := 0
			for _, r := range recoveryJournalRecords(t, output) {
				if r["event"] != "episode_write" {
					continue
				}
				writes++
				if r["event_id"] != id || r["decision"] != string(EpisodeReasonUnconsumed) || r["last_write_reason"] != string(EpisodeReasonUnconsumed) || r["last_write"] != string(EpisodeWriteSuccess) || r["write_accepted"] != true || r["write_successes"] != float64(1) || r["write_errors"] != float64(0) || r["write_refusals"] != float64(0) {
					t.Fatalf("first-write JSON lost its live decision or accepted write: %+v", r)
				}
			}
			if writes != 1 {
				t.Fatalf("expected one first-write record, got %d", writes)
			}
			var data [8]byte
			if n, err := unix.Read(fd, data[:]); err != nil || n != 8 || binary.NativeEndian.Uint64(data[:]) != 1 {
				t.Fatal("the first accepted eventfd write was not observed")
			}
		})
	}
}

func TestRecoveryJournalWriteFailureDiffersFromIdentityRefusal(t *testing.T) {
	for _, kind := range []string{"write_error", "identity_refused"} {
		t.Run(kind, func(t *testing.T) {
			q, target, _, l, output, fd := recoveryJournalFixture(t)
			s := q.snapshot
			if kind == "write_error" {
				var full [8]byte
				binary.NativeEndian.PutUint64(full[:], ^uint64(0)-1)
				if _, err := unix.Write(fd, full[:]); err != nil {
					t.Fatal(err)
				}
			} else {
				s.Context++
			}
			err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: s}, l, q, true)
			if err == nil {
				t.Fatal("expected a failed/refused attempt")
			}
			var writeErr *KickWriteError
			if errors.As(err, &writeErr) != (kind == "write_error") {
				t.Fatal("validation refusal was conflated with an actual write error")
			}
			if q.policy.Writes != 0 || q.policy.verification != nil {
				t.Fatal("failed/refused attempt became accepted verification")
			}
			var observed bool
			for _, r := range recoveryJournalRecords(t, output) {
				if r["event"] != "episode_write" {
					continue
				}
				if kind == "write_error" {
					observed = r["write_errors"] == float64(1) && r["write_refusals"] == float64(0) && r["last_write_reason"] == string(EpisodeReasonWriteFailed)
				} else {
					observed = r["write_refusals"] == float64(1) && r["write_errors"] == float64(0) && r["last_write_reason"] == string(EpisodeReasonIdentityChange)
				}
			}
			if !observed {
				t.Fatal("typed failure/refusal lost its fixed journal result")
			}
		})
	}
}

type journalFailedOutput struct{}

func (journalFailedOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestRecoveryJournalOutputFailureDoesNotReclassifyAcceptedWrite(t *testing.T) {
	q, target, _, l, _, fd := recoveryJournalFixture(t)
	// Candidate output was accepted. Fail the subsequent first-write record.
	l.encoder = json.NewEncoder(journalFailedOutput{})
	err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("stdout failure was not returned: %v", err)
	}
	if q.policy.Writes != 1 || q.policy.Refusals != 0 || q.policy.WriteErrors != 0 {
		t.Fatal("accepted write was reclassified as a refusal/write failure")
	}
	if q.journal.active.writes != 1 || q.journal.active.writeErrors != 0 || q.journal.active.writeRefusals != 0 {
		t.Fatal("journal accounting conflated stdout failure with kick failure")
	}
	var data [8]byte
	if n, err := unix.Read(fd, data[:]); err != nil || n != 8 || binary.NativeEndian.Uint64(data[:]) != 1 {
		t.Fatal("the accepted eventfd write was not observed")
	}
}

func TestInterruptedRefreshStoppedSummaryRetainsClosedEpisodeVerification(t *testing.T) {
	q, target, clock, l, _, _ := recoveryJournalFixture(t)
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
		t.Fatal(err)
	}
	origin := q.verificationEventID
	q.closeEpisode(EpisodeOutcomeUnavailable)
	*clock = 8
	q.policy.Unconfirmed(*clock, 5)
	known := map[int]*recoveryQueue{}
	restoreUnvisitedRecoveryQueues(known, map[int]*recoveryQueue{q.fd: q})
	known[q.fd].closeEpisode(EpisodeOutcomeStopped)
	row := known[q.fd].summary(*clock)
	if row["verification_event_id"] != origin || row["verification_pending"] != true || row["verification_timed_out"] != true || row["unconfirmed_age"] != float64(8) {
		t.Fatalf("stopped summary lost an unvisited queue's original verification: %+v", row)
	}
	if row["episode_open"] != false {
		t.Fatal("stopped summary reopened the closed diagnostic episode")
	}
	current := &recoveryQueue{fd: q.fd, policy: NewRecoveryPolicy(.1)}
	known[q.fd] = current
	restoreUnvisitedRecoveryQueues(known, map[int]*recoveryQueue{q.fd: q})
	if known[q.fd] != current {
		t.Fatal("stopped summary replaced a current attachment with pending history")
	}
}

func TestRecoveryJournalIdentityChangeClosesAndClearsVerification(t *testing.T) {
	q, target, clock, l, output, _ := recoveryJournalFixture(t)
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
		t.Fatal(err)
	}
	origin := q.verificationEventID
	*clock = .2
	s := q.snapshot
	s.Context++
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: s}, l, q, true); err == nil {
		t.Fatal("identity change was accepted")
	}
	if q.journal.Active() || q.journal.count != 0 || q.policy.verification != nil || q.verificationEventID != "" {
		t.Fatal("identity change retained old snapshots or verification")
	}
	rows := recoveryJournalRecords(t, output)
	closing := rows[len(rows)-1]
	if closing["event"] != "episode_closed" || closing["event_id"] != origin || closing["outcome"] != string(EpisodeOutcomeIdentityChange) {
		t.Fatal("identity change lost the closed episode's correlation")
	}
}

type sequenceSnapshotter struct {
	snapshots []Snapshot
	index     int
}

func (b *sequenceSnapshotter) Snapshot(int) (Snapshot, error) {
	s := b.snapshots[b.index]
	b.index++
	return s, nil
}

func TestRecoveryJournalFinalPreWriteIdentityRefusalClearsOldOrigin(t *testing.T) {
	q, target, clock, l, output, fd := recoveryJournalFixture(t)
	q.verificationEventID = q.journal.EventID()
	q.policy.Written(*clock, q.used, q.snapshot.LastAvail)
	*clock = .2
	changed := q.snapshot
	changed.Context++
	bpf := &sequenceSnapshotter{snapshots: []Snapshot{q.snapshot, changed}}
	err := recoverLive(context.Background(), Config{Mode: "recover"}, target, bpf, l, q, true)
	if !errors.Is(err, ErrKickIdentityChanged) || bpf.index != 2 {
		t.Fatalf("final live identity refusal was not distinguished: %v", err)
	}
	if q.journal.Active() || q.policy.verification != nil || q.verificationEventID != "" || q.lastDecision != EpisodeReasonIdentityChange {
		t.Fatal("final pre-write identity change retained the old verification")
	}
	if n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0); err != nil || n != 0 {
		t.Fatal("final pre-write identity refusal wrote to eventfd")
	}
	rows := recoveryJournalRecords(t, output)
	if rows[len(rows)-1]["outcome"] != string(EpisodeOutcomeIdentityChange) {
		t.Fatal("final identity refusal was hidden as unavailable")
	}
}

func TestRecoveryJournalInvalidLiveRingClosesWithoutWrite(t *testing.T) {
	q, target, _, l, output, fd := recoveryJournalFixture(t)
	target.haveIndices, target.avail, target.used = true, 400, 11
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: q.snapshot}, l, q, true); err != nil {
		t.Fatal(err)
	}
	if q.journal.Active() || q.lastDecision != EpisodeReasonInvalidRing || q.policy.Writes != 0 {
		t.Fatal("inconsistent live indices remained a write candidate")
	}
	if n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0); err != nil || n != 0 {
		t.Fatal("invalid ring wrote to eventfd")
	}
	rows := recoveryJournalRecords(t, output)
	if rows[len(rows)-1]["outcome"] != string(EpisodeOutcomeInvalidRing) {
		t.Fatal("invalid ring was hidden as quiet/unavailable")
	}
	if rows[len(rows)-1]["used_progress"] != false {
		t.Fatal("inconsistent live indices were accepted as valid progress")
	}
}

func TestStageProgramsAreAbsentOutsideTrace(t *testing.T) {
	for _, trace := range []bool{false, true} {
		spec := &ebpf.CollectionSpec{Programs: map[string]*ebpf.ProgramSpec{"snapshot_tx": {}, "signal_event": {}, "handler_exit": {}}}
		selectBPFPrograms(spec, trace)
		if spec.Programs["snapshot_tx"] == nil {
			t.Fatal("snapshot primitive removed")
		}
		if (spec.Programs["signal_event"] != nil) != trace || (spec.Programs["handler_exit"] != nil) != trace {
			t.Fatal("traffic stages loaded outside trace")
		}
	}
}
