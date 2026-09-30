package watch

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

type episodeRecord struct {
	event  string
	at     float64
	fields map[string]any
}

func episodeFixture() (*EpisodeJournal, *float64, *[]episodeRecord) {
	now := new(float64)
	records := new([]episodeRecord)
	j := NewEpisodeJournal(func(event string, fields map[string]any) {
		*records = append(*records, episodeRecord{event, *now, fields})
	}, func() float64 { return *now })
	return j, now, records
}

func episodeSample(at float64, used uint16) EpisodeSample {
	return EpisodeSample{At: at, Avail: 100, Used: used, Source: EpisodeSourceUser}
}

func TestEpisodePreHistoryBoundedChronologicalAndImmutable(t *testing.T) {
	j, now, records := episodeFixture()
	if j.Open(EpisodeReasonCandidate) {
		t.Fatal("opened an episode without a sample")
	}
	for i := range 100 {
		*now = float64(i) / 10
		j.Observe(episodeSample(*now, uint16(i)), false)
	}
	if !j.Open(EpisodeReasonCandidate) || j.Open(EpisodeReasonCandidate) {
		t.Fatal("expected exactly one episode per queue")
	}
	first := (*records)[0]
	samples := first.fields["snapshots"].([]EpisodeSample)
	if first.event != "candidate" || len(samples) != EpisodeSnapshotLimit {
		t.Fatalf("unexpected candidate record: %+v", first)
	}
	for i, s := range samples {
		if s.Used != uint16(84+i) {
			t.Fatalf("snapshot %d was not chronological: %+v", i, s)
		}
	}
	for i := range 100 {
		*now += .025
		j.Observe(episodeSample(*now, uint16(200+i)), true)
	}
	if samples[0].Used != 84 || samples[15].Used != 99 {
		t.Fatal("later ring writes mutated an emitted pre-history")
	}
	for _, r := range *records {
		if len(r.fields["snapshots"].([]EpisodeSample)) > EpisodeSnapshotLimit {
			t.Fatalf("unbounded snapshots in %s", r.event)
		}
		if r.fields["event_id"] != first.fields["event_id"] {
			t.Fatal("candidate and subsequent snapshots have different IDs")
		}
	}
}

func TestEpisodeRateLimitAggregatesEveryWriteAndKeepsLatest(t *testing.T) {
	j, now, records := episodeFixture()
	j.Observe(episodeSample(0, 20), true)
	j.Open(EpisodeReasonCandidate)
	results := []EpisodeWrite{EpisodeWriteSuccess, EpisodeWriteError, EpisodeWriteRefused}
	for i := range 120 {
		*now = float64(i) / 40
		j.Observe(episodeSample(*now, uint16(20+i)), true)
		j.RecordWrite(results[i%len(results)], EpisodeReasonLivePending)
	}
	*now = 2.99
	j.Observe(episodeSample(*now, 499), true)
	j.Close(EpisodeOutcomeStopped)
	if len(*records) > 6 {
		t.Fatalf("120 attempts generated %d records", len(*records))
	}
	var lastAt float64
	for _, r := range (*records)[1 : len(*records)-1] {
		// Only the first write is an immediate, fixed-size exception.
		if r.event == "episode_write" && r.at == 0 {
			continue
		}
		if r.at-lastAt < EpisodeRecordInterval {
			t.Fatalf("follow-up records exceeded one/second: %+v", *records)
		}
		lastAt = r.at
	}
	closing := (*records)[len(*records)-1]
	for _, field := range []string{"write_successes", "write_errors", "write_refusals"} {
		if closing.fields[field] != uint64(40) {
			t.Fatalf("lost suppressed %s: %+v", field, closing.fields)
		}
	}
	if closing.fields["snapshots"].([]EpisodeSample)[0].Used != 499 {
		t.Fatal("closing record did not include the latest suppressed sample")
	}
	if !closing.fields["after_write"].(bool) || closing.fields["last_write"] != EpisodeWriteRefused {
		t.Fatal("write chronology/results were lost")
	}
	if _, exists := closing.fields["lost_notification"]; exists {
		t.Fatal("later progress was presented as a lost-notification verdict")
	}
}

func TestEpisodeProgressUsesFreshConsumptionAndSeparatesOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		used, consumed uint16
		fresh          bool
		outcome        EpisodeOutcome
	}{
		{"quiet", 20, 30, true, EpisodeOutcomeQuiet},
		{"used", 21, 30, true, EpisodeOutcomeUsed},
		{"consumed", 20, 31, true, EpisodeOutcomeConsumed},
		{"both", 21, 31, true, EpisodeOutcomeBoth},
		{"stale_consumed", 20, 31, false, EpisodeOutcomeQuiet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j, now, records := episodeFixture()
			initial := episodeSample(0, 20)
			initial.Source, initial.Consumed, initial.ConsumedFresh = EpisodeSourceLive, 30, true
			j.Observe(initial, true)
			j.Open(EpisodeReasonCandidate)
			j.RecordWrite(EpisodeWriteSuccess, EpisodeReasonNone)
			*now = .1
			s := episodeSample(*now, tc.used)
			s.Consumed, s.ConsumedFresh = tc.consumed, tc.fresh
			j.Observe(s, false)
			*now = 1.2
			s.At = *now
			j.Observe(s, false)
			closing := (*records)[len(*records)-1]
			if j.Active() || closing.event != "episode_closed" || closing.fields["outcome"] != tc.outcome {
				t.Fatalf("incorrect progress outcome: %+v", closing)
			}
		})
	}
}

func TestEpisodeFirstFreshConsumptionEstablishesBaseline(t *testing.T) {
	j, now, records := episodeFixture()
	s := episodeSample(0, 20)
	s.Consumed = 10 // An old inventory value is not a live baseline.
	j.Observe(s, true)
	j.Open(EpisodeReasonCandidate)
	*now = 1
	s.Source, s.Consumed, s.ConsumedFresh = EpisodeSourceLive, 30, true
	j.Observe(s, true)
	if (*records)[len(*records)-1].fields["consumption_progress"].(bool) {
		t.Fatal("first live consumption was compared to a stale inventory value")
	}
	*now = 2
	s.Consumed = 31
	j.Observe(s, true)
	if !j.Active() || !(*records)[len(*records)-1].fields["consumption_progress"].(bool) {
		t.Fatal("consumption-only progress must be reported without prematurely closing")
	}
}

func TestEpisodeWriteChronologyDoesNotRelabelPreWriteProgress(t *testing.T) {
	j, now, records := episodeFixture()
	j.Observe(episodeSample(0, 20), true)
	j.Open(EpisodeReasonCandidate)
	*now = .5
	j.Observe(episodeSample(*now, 21), true)
	*now = 1
	j.RecordWrite(EpisodeWriteSuccess, EpisodeReasonNone)
	written := (*records)[len(*records)-1]
	if !written.fields["write_accepted"].(bool) || !written.fields["used_progress"].(bool) || written.fields["after_write"].(bool) {
		t.Fatal("pre-write progress or sample was relabeled as post-write")
	}
	if written.fields["first_write_at"] != float64(1) {
		t.Fatal("first successful write time was not retained")
	}
	// A sample at the same time as the write is not known to be subsequent.
	j.Observe(episodeSample(1, 21), true)
	j.Close(EpisodeOutcomeUsed)
	if (*records)[len(*records)-1].fields["after_write"].(bool) {
		t.Fatal("a simultaneous sample was treated as a later sample")
	}
}

func TestEpisodeCandidateCancelsQuietAndQueuedZeroDoesNotMeanIdle(t *testing.T) {
	j, now, _ := episodeFixture()
	s := episodeSample(0, 20)
	s.WorkQueuedFresh = true
	j.Observe(s, true)
	j.Open(EpisodeReasonCandidate)
	*now = 2
	j.Observe(s, true)
	if !j.Active() {
		t.Fatal("work_queued=0 was treated as worker idle")
	}
	*now = 2.1
	j.Observe(s, false)
	*now = 2.9
	j.Observe(s, true)
	*now = 3.2
	j.Observe(s, false)
	if !j.Active() {
		t.Fatal("a new candidate did not cancel the old quiet period")
	}
	*now = 4.3
	j.Observe(s, false)
	if j.Active() {
		t.Fatal("continuous quiet period did not close the episode")
	}
}

func TestEpisodeDeadlineAndReopenBoundDoNotDependOnPollAvailability(t *testing.T) {
	j, now, records := episodeFixture()
	j.Observe(episodeSample(0, 20), true)
	j.Open(EpisodeReasonCandidate)
	id := j.EventID()
	*now = .1
	j.Observe(episodeSample(*now, 20), false)
	*now = 2
	j.Tick()
	if !j.Active() {
		t.Fatal("unavailable polls were treated as continuing quiet")
	}
	*now = EpisodeLifetime
	j.Tick()
	closing := (*records)[len(*records)-1]
	if j.Active() || closing.fields["outcome"] != EpisodeOutcomeTimeout {
		t.Fatalf("unavailable polls prevented the deadline: %+v", closing)
	}
	j.Tick()
	j.Close(EpisodeOutcomeStopped)
	j.RecordWrite(EpisodeWriteSuccess, EpisodeReasonNone)
	if len(*records) != 2 || j.Open(EpisodeReasonCandidate) {
		t.Fatal("closed episodes emitted duplicates or reopened without pacing")
	}
	*now += EpisodeReopenDelay
	j.Observe(episodeSample(*now, 20), true)
	if !j.Open(EpisodeReasonCandidate) || j.EventID() == id {
		t.Fatal("persistent candidate could not reopen with a fresh ID after pacing")
	}
}

func TestEpisodeTimeoutObserveIncludesCurrentSample(t *testing.T) {
	j, now, records := episodeFixture()
	j.Observe(episodeSample(0, 20), true)
	j.Open(EpisodeReasonCandidate)
	*now = 31
	j.Observe(episodeSample(*now, 21), false)
	closing := (*records)[len(*records)-1]
	if closing.fields["outcome"] != EpisodeOutcomeTimeout || closing.fields["snapshots"].([]EpisodeSample)[0].Used != 21 {
		t.Fatalf("timeout record lost the deadline's current sample: %+v", closing)
	}
}

func TestEpisodeIdentityChangeClearsOldPreHistory(t *testing.T) {
	j, now, records := episodeFixture()
	j.Observe(episodeSample(0, 20), true)
	j.Open(EpisodeReasonCandidate)
	j.Close(EpisodeOutcomeIdentityChange)
	*now = 1
	if j.Open(EpisodeReasonCandidate) {
		t.Fatal("replacement queue reused old identity's history")
	}
	j.Observe(episodeSample(*now, 500), true)
	if !j.Open(EpisodeReasonCandidate) {
		t.Fatal("replacement queue could not open after sampling")
	}
	samples := (*records)[len(*records)-1].fields["snapshots"].([]EpisodeSample)
	if len(samples) != 1 || samples[0].Used != 500 {
		t.Fatalf("replacement record retained the old queue: %+v", samples)
	}
	j.Close(EpisodeOutcomeStopped)
	*now = 2
	j.Observe(episodeSample(*now, 600), false)
	j.Close(EpisodeOutcomeIdentityChange) // No candidate was active.
	if j.Open(EpisodeReasonCandidate) {
		t.Fatal("identity change while no episode was active retained old history")
	}
}

func TestEpisodeInvalidSnapshotRetainedWithoutProgressVerdict(t *testing.T) {
	j, now, records := episodeFixture()
	s := episodeSample(0, 20)
	s.Consumed, s.ConsumedFresh = 30, true
	j.Observe(s, true)
	j.Open(EpisodeReasonCandidate)
	*now = .1
	s.At, s.Used, s.Consumed, s.Invalid = *now, 21, 31, true
	j.RecordDecision(EpisodeReasonInvalidRing)
	j.Observe(s, false)
	j.Close(EpisodeOutcomeInvalidRing)
	closing := (*records)[len(*records)-1]
	if closing.fields["used_progress"] != false || closing.fields["consumption_progress"] != false {
		t.Fatal("invalid indices were accepted as observed progress")
	}
	if !closing.fields["snapshots"].([]EpisodeSample)[0].Invalid || closing.fields["decision"] != EpisodeReasonInvalidRing {
		t.Fatal("invalid current snapshot or its classification was lost")
	}
}

func TestEpisodeRejectsArbitraryReasonAndSourceAndIsJSONSafe(t *testing.T) {
	j, _, records := episodeFixture()
	s := episodeSample(0, 20)
	s.Source = "raw address 0xffff1234"
	j.Observe(s, true)
	j.Open("raw error containing secret")
	j.RecordWrite(EpisodeWriteError, "raw write error containing secret")
	j.Close(EpisodeOutcomeUnavailable)
	for _, r := range *records {
		data, err := json.Marshal(r.fields)
		if err != nil {
			t.Fatalf("record is not JSON serializable: %v", err)
		}
		if strings.Contains(string(data), "raw") || strings.Contains(string(data), "secret") || strings.Contains(string(data), "0xffff") {
			t.Fatalf("arbitrary reason/source escaped normalization: %s", data)
		}
		if r.fields["reason"] != EpisodeReasonUnknown {
			t.Fatal("unbounded candidate reason")
		}
	}
}

func TestEpisodeIDsUniqueAcrossConcurrentQueueLoops(t *testing.T) {
	ids := make(chan string, 64)
	var workers sync.WaitGroup
	for range cap(ids) {
		workers.Go(func() {
			j := NewEpisodeJournal(nil, func() float64 { return 0 })
			j.Observe(episodeSample(0, 20), true)
			j.Open(EpisodeReasonCandidate)
			ids <- j.EventID()
		})
	}
	workers.Wait()
	close(ids)
	seen := make(map[string]bool)
	for id := range ids {
		if id == "" || seen[id] || !strings.HasPrefix(id, episodePrefix+"-") {
			t.Fatalf("duplicate or malformed event ID %q", id)
		}
		seen[id] = true
	}
}
