package evidence_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/evidence"
)

func newRecorder(t *testing.T, options evidence.Options) *evidence.Recorder {
	t.Helper()
	r, err := evidence.New(options, 1)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func apply(t *testing.T, r *evidence.Recorder, in evidence.Input) []evidence.Record {
	t.Helper()
	records, err := r.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > evidence.MaxRecordsPerInput {
		t.Fatalf("unbounded transition: %d records", len(records))
	}
	return records
}

func sample(at time.Duration, used uint16) evidence.Observation {
	return evidence.Observation{At: at, HasSample: true, Sample: evidence.Sample{
		At: at, Avail: used + 10, Used: used, Valid: true, Source: evidence.SourceCached,
	}}
}

func start(t *testing.T, r *evidence.Recorder, at time.Duration, used uint16) evidence.Record {
	t.Helper()
	observation := sample(at, used)
	observation.CandidateKnown, observation.Candidate = true, true
	records := apply(t, r, observation)
	if len(records) != 1 || records[0].Kind != evidence.EpisodeOpened {
		t.Fatalf("expected opening: %+v", records)
	}
	return records[0]
}

func TestAtomicObservationCancelsQuietBeforeSampleExpiry(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, 0, 10)
	apply(t, r, evidence.Observation{At: time.Second, CandidateKnown: true})
	in := sample(2*time.Second, 11)
	in.CandidateKnown, in.Candidate = true, true
	for _, record := range apply(t, r, in) {
		if record.Kind == evidence.EpisodeClosed || record.Kind == evidence.EpisodeOpened || record.EpisodeID != 1 {
			t.Fatalf("atomic positive observation expired current episode: %+v", record)
		}
	}
	closed := apply(t, r, evidence.Retired{At: 2 * time.Second})[0]
	if closed.EpisodeID != 1 || !closed.UsedProgress {
		t.Fatalf("observation lost episode continuity: %+v", closed)
	}
}

func TestObservationPreservesAcquisitionTimeAndRejectsReorderedSamples(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	in := sample(time.Second, 10)
	in.At = 2 * time.Second
	in.CandidateKnown, in.Candidate = true, true
	opened := apply(t, r, in)[0]
	if opened.At != 2*time.Second || opened.Samples[0].At != time.Second {
		t.Fatal("acquisition time replaced by presentation time", opened)
	}
	older := sample(time.Millisecond, 20)
	older.At = 3 * time.Second
	if _, err := r.Apply(older); !errors.Is(err, evidence.ErrBackwardTime) {
		t.Fatal("out of order sample accepted", err)
	}
	closed := apply(t, r, evidence.Retired{At: 2 * time.Second})[0]
	if closed.UsedProgress || closed.Samples[0].Used != 10 {
		t.Fatal("rejected observation changed evidence", closed)
	}
}

func TestBoundedHistoryChronologyAndOwnership(t *testing.T) {
	opts := evidence.DefaultOptions()
	opts.HistoryLimit = 3
	r := newRecorder(t, opts)
	for i := range 100 {
		apply(t, r, sample(time.Duration(i)*time.Millisecond, uint16(i)))
	}
	opened := apply(t, r, evidence.Observation{CandidateKnown: true, At: time.Second, Candidate: true})[0]
	if len(opened.Samples) != 3 {
		t.Fatalf("history length: %d", len(opened.Samples))
	}
	for i, s := range opened.Samples {
		if s.Used != uint16(97+i) {
			t.Fatalf("history not chronological: %+v", opened.Samples)
		}
	}
	opened.Samples[2].Used = 555 // A consumer is allowed to modify its owned record.
	closed := apply(t, r, evidence.Retired{At: 2 * time.Second})[0]
	if closed.Samples[0].Used != 99 || closed.UsedProgress {
		t.Fatal("returned samples alias recorder state")
	}
	closed.Samples[0].Used = 123
	if opened.Samples[0].Used != 97 || opened.Samples[2].Used != 555 {
		t.Fatal("returned records alias each other")
	}
}

func TestCandidatesAreExplicitAndQuietRequiresNegativeState(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	// No sample means no opening; an action also must not invent a candidate.
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, Candidate: true}); len(got) != 0 {
		t.Fatal(got)
	}
	action := apply(t, r, evidence.ActionObserved{AttemptID: 1, Outcome: evidence.ActionAccepted})
	if len(action) != 1 || action[0].EpisodeID != 0 || action[0].Kind != evidence.ActionRecorded {
		t.Fatal("action before candidate opened an episode")
	}
	start(t, r, 0, 10)
	if got := apply(t, r, evidence.TimeAdvanced{At: 5 * time.Second}); len(got) != 0 {
		t.Fatal("missing candidate messages were interpreted as quiet")
	}
	apply(t, r, evidence.Observation{CandidateKnown: true, At: 5 * time.Second, Candidate: false})
	// A currently observed candidate cancels quiet, even if no intervening tick
	// was delivered at the quiet deadline.
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, At: 7 * time.Second, Candidate: true}); len(got) != 0 {
		t.Fatal("fresh candidate failed to cancel quiet")
	}
	apply(t, r, evidence.Observation{CandidateKnown: true, At: 8 * time.Second, Candidate: false})
	if got := apply(t, r, evidence.TimeAdvanced{At: 9*time.Second - time.Nanosecond}); len(got) != 0 {
		t.Fatal("quiet closed early")
	}
	closed := apply(t, r, evidence.TimeAdvanced{At: 9 * time.Second})
	if len(closed) != 1 || closed[0].Kind != evidence.EpisodeClosed || closed[0].CloseReason != evidence.CloseQuiet || closed[0].Actions.Accepted != 0 {
		t.Fatalf("unexpected quiet close: %+v", closed)
	}
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, At: 9 * time.Second, Candidate: true}); len(got) != 0 {
		t.Fatal("reopen delay not enforced")
	}
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, At: 10 * time.Second, Candidate: true}); len(got) != 1 || got[0].EpisodeID != 2 {
		t.Fatalf("reopen failed: %+v", got)
	}
}

func TestActionTotalsSurviveRecordSuppression(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, 0, 10)
	var actionRecords int
	for i := range 100 {
		records := apply(t, r, evidence.ActionObserved{At: time.Duration(i) * time.Millisecond,
			AttemptID: evidence.AttemptID(i + 1), Outcome: evidence.ActionOutcome(i%5 + 1)})
		actionRecords += len(records)
	}
	if actionRecords != 1 {
		t.Fatalf("first action should report immediately, subsequent ones coalesce: %d", actionRecords)
	}
	closed := apply(t, r, evidence.Retired{At: 100 * time.Millisecond})[0]
	want := evidence.ActionTotals{Accepted: 20, Refused: 20, Failed: 20, Cancelled: 20, Unavailable: 20}
	if closed.Actions != want || closed.AttemptID != 100 || closed.Action != evidence.ActionUnavailable {
		t.Fatalf("lost suppressed accounting: %+v", closed)
	}
}

func TestProgressUsesValidityAndFreshnessNotActionTiming(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, 0, 10)
	invalid := sample(time.Second, 11)
	invalid.Sample.Valid = false
	if records := apply(t, r, invalid); records[0].UsedProgress {
		t.Fatal("invalid sample counted as progress")
	}
	fresh := sample(2*time.Second, 11)
	fresh.Sample.Source = evidence.SourceLive
	fresh.Sample.Consumed, fresh.Sample.ConsumedFresh = 12, true
	records := apply(t, r, fresh)
	if !records[0].UsedProgress || records[0].ConsumedProgress {
		t.Fatal("first fresh consumed sample should establish, not advance, baseline")
	}
	apply(t, r, evidence.ActionObserved{At: 2 * time.Second, AttemptID: 1, Outcome: evidence.ActionAccepted})
	fresh.At, fresh.Sample.At, fresh.Sample.Consumed = 3*time.Second, 3*time.Second, 13
	records = apply(t, r, fresh)
	if !records[0].UsedProgress || !records[0].ConsumedProgress {
		t.Fatal("later valid consumed change was not recorded")
	}
	// The record says episode-relative progress. It supplies no causal verdict.
	if records[0].Verification != 0 {
		t.Fatal("sample progress inferred verification")
	}
}

func TestGapClosesIncompleteAndResetsHistory(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, 0, 10)
	gap := apply(t, r, evidence.GapObserved{At: time.Second, Lost: 3})
	if len(gap) != 2 || gap[0].CloseReason != evidence.CloseGap || !gap[0].Incomplete || gap[1].Kind != evidence.GapRecorded || gap[1].LostInputs != 3 {
		t.Fatalf("loss not represented: %+v", gap)
	}
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, At: time.Second, Candidate: true}); len(got) != 0 {
		t.Fatal("gap retained an old sample baseline")
	}
	opened := start(t, r, time.Second, 20)
	if opened.EpisodeID != 2 || len(opened.Samples) != 1 || opened.Samples[0].Used != 20 || opened.UsedProgress || opened.ConsumedProgress {
		t.Fatalf("new episode inherited pre-gap facts: %+v", opened)
	}
}

func TestLateVerificationDoesNotReopenOrClaimCurrentEpisode(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, 0, 10)
	apply(t, r, evidence.ActionObserved{AttemptID: 7, Outcome: evidence.ActionAccepted})
	closed := apply(t, r, evidence.TimeAdvanced{At: 30 * time.Second})
	if len(closed) != 1 || closed[0].CloseReason != evidence.CloseTimeout {
		t.Fatal("episode did not time out")
	}
	start(t, r, 31*time.Second, 20)
	verdict := apply(t, r, evidence.VerificationObserved{At: 32 * time.Second,
		AttemptID: 7, Outcome: evidence.VerificationProgress})
	if len(verdict) != 1 || verdict[0].Kind != evidence.VerificationRecorded || verdict[0].EpisodeID != 0 || verdict[0].AttemptID != 7 {
		t.Fatalf("late verdict was misattributed: %+v", verdict)
	}
	final := apply(t, r, evidence.Retired{At: 32 * time.Second})
	if final[0].EpisodeID != 2 || final[0].Actions.Accepted != 0 {
		t.Fatal("verification mutated the newer episode")
	}
}

// Embedding the sealed interface is possible in Go; it must not let a consumer
// add a variant or cause Apply to call through the embedded nil interface.
type unsupportedInput struct{ evidence.Input }

func TestInvalidInputNeverPartiallyMutates(t *testing.T) {
	invalidInputs := []evidence.Input{
		nil, unsupportedInput{}, &evidence.TimeAdvanced{},
		evidence.TimeAdvanced{At: -1},
		evidence.ActionObserved{At: 2 * time.Second, AttemptID: 0, Outcome: evidence.ActionAccepted},
		evidence.ActionObserved{At: 2 * time.Second, AttemptID: 1, Outcome: evidence.ActionOutcome(99)},
		evidence.VerificationObserved{At: 2 * time.Second, AttemptID: 0, Outcome: evidence.VerificationProgress},
		evidence.VerificationObserved{At: 2 * time.Second, AttemptID: 1, Outcome: evidence.VerificationOutcome(99)},
		evidence.GapObserved{At: 2 * time.Second, Lost: 0},
		evidence.Observation{At: 2 * time.Second, HasSample: true, Sample: evidence.Sample{At: 2 * time.Second, Source: evidence.Source(99)}},
		evidence.Observation{At: 2 * time.Second, HasSample: true, Sample: evidence.Sample{At: 2 * time.Second, Consumed: 5}},
		evidence.Observation{At: 2 * time.Second, HasSample: true, Sample: evidence.Sample{At: 2 * time.Second, WorkQueued: true}},
	}
	for i, invalid := range invalidInputs {
		r, control := newRecorder(t, evidence.DefaultOptions()), newRecorder(t, evidence.DefaultOptions())
		start(t, r, time.Second, 10)
		start(t, control, time.Second, 10)
		if records, err := r.Apply(invalid); !errors.Is(err, evidence.ErrInvalidInput) || len(records) != 0 {
			t.Fatalf("case %d: records=%+v err=%v", i, records, err)
		}
		good := evidence.Retired{At: time.Second}
		if got, want := apply(t, r, good), apply(t, control, good); !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d mutated recorder", i)
		}
	}
}

func TestBackwardTimeAndRetirement(t *testing.T) {
	r := newRecorder(t, evidence.DefaultOptions())
	start(t, r, time.Second, 10)
	if _, err := r.Apply(evidence.TimeAdvanced{}); !errors.Is(err, evidence.ErrBackwardTime) {
		t.Fatal(err)
	}
	retired := apply(t, r, evidence.Retired{At: time.Second})
	if len(retired) != 2 || retired[0].CloseReason != evidence.CloseRetired || retired[1].Kind != evidence.StreamRetired {
		t.Fatal(retired)
	}
	if again := apply(t, r, evidence.Retired{At: time.Second}); len(again) != 0 {
		t.Fatal("retirement emitted twice")
	}
	if _, err := r.Apply(sample(2*time.Second, 20)); !errors.Is(err, evidence.ErrRetired) {
		t.Fatal(err)
	}
}

func TestOptionsAndZeroRecorder(t *testing.T) {
	for _, limit := range []int{-1, 0, evidence.MaxHistoryLimit + 1} {
		opts := evidence.DefaultOptions()
		opts.HistoryLimit = limit
		if _, err := evidence.New(opts, 1); !errors.Is(err, evidence.ErrInvalidOptions) {
			t.Fatal(err)
		}
	}
	for _, duration := range []string{"lifetime", "record", "quiet", "reopen"} {
		opts := evidence.DefaultOptions()
		switch duration {
		case "lifetime":
			opts.Lifetime = 0
		case "record":
			opts.RecordInterval = 0
		case "quiet":
			opts.QuietPeriod = 0
		case "reopen":
			opts.ReopenDelay = -1
		}
		if _, err := evidence.New(opts, 1); !errors.Is(err, evidence.ErrInvalidOptions) {
			t.Fatal(err)
		}
	}
	if _, err := evidence.New(evidence.DefaultOptions(), 0); !errors.Is(err, evidence.ErrInvalidOptions) {
		t.Fatal(err)
	}
	var zero evidence.Recorder
	if _, err := zero.Apply(evidence.TimeAdvanced{}); !errors.Is(err, evidence.ErrUninitialized) {
		t.Fatal(err)
	}
}

func TestLongDeterministicStreamHasBoundedResults(t *testing.T) {
	opts := evidence.DefaultOptions()
	opts.HistoryLimit, opts.Lifetime, opts.ReopenDelay = 4, time.Second, 0
	a, b := newRecorder(t, opts), newRecorder(t, opts)
	for i := range 10000 {
		at := time.Duration(i) * 10 * time.Millisecond
		inputs := []evidence.Input{sample(at, uint16(i)), evidence.Observation{CandidateKnown: true, At: at, Candidate: i%9 != 0},
			evidence.ActionObserved{At: at, AttemptID: evidence.AttemptID(i + 1), Outcome: evidence.ActionAccepted}}
		if i%123 == 0 {
			inputs = append(inputs, evidence.GapObserved{At: at, Lost: 1})
		}
		for _, in := range inputs {
			left, right := apply(t, a, in), apply(t, b, in)
			if !reflect.DeepEqual(left, right) {
				t.Fatal("identical streams were not deterministic")
			}
			for _, record := range left {
				if len(record.Samples) > opts.HistoryLimit {
					t.Fatal("history limit exceeded")
				}
			}
		}
	}
}

func TestTimeoutAndReopenDoNotOverflowTime(t *testing.T) {
	opts := evidence.DefaultOptions()
	opts.Lifetime, opts.ReopenDelay = time.Nanosecond, time.Duration(1<<63-1)
	r := newRecorder(t, opts)
	start(t, r, time.Duration(1<<63-3), 10)
	closed := apply(t, r, evidence.TimeAdvanced{At: time.Duration(1<<63 - 2)})
	if len(closed) != 1 || closed[0].CloseReason != evidence.CloseTimeout {
		t.Fatal(closed)
	}
	if got := apply(t, r, evidence.Observation{CandidateKnown: true, At: time.Duration(1<<63 - 1), Candidate: true}); len(got) != 0 {
		t.Fatal("time addition overflow bypassed reopen delay")
	}
}
