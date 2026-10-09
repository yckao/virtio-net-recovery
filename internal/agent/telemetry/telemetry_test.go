package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/internal/agent/telemetry"
)

type capture struct {
	mu sync.Mutex
	bytes.Buffer
}

func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Buffer.Write(p)
}
func (c *capture) data() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.Buffer.Bytes())
}

type line struct {
	InputLost          uint64 `json:"input_lost"`
	OutputLost         uint64 `json:"output_lost"`
	DeliveryIncomplete bool   `json:"delivery_incomplete"`
	Record             struct {
		Event                              string `json:"event"`
		Stream, Sequence, Episode, Attempt uint64
		Outcome, Reason, Message           string
		EvidenceIncomplete                 bool   `json:"evidence_incomplete"`
		LostInputs                         uint64 `json:"gap_inputs"`
		UsedProgress                       bool   `json:"used_progress"`
		Samples                            []struct {
			At    int64 `json:"at_ns"`
			Used  uint16
			Valid bool
		} `json:"samples"`
	} `json:"record"`
}

func lines(t *testing.T, c *capture) []line {
	t.Helper()
	var result []line
	for _, raw := range bytes.Split(bytes.TrimSpace(c.data()), []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		var item line
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatal(err)
		}
		if len(raw)+1 > telemetry.MaxRecordBytes {
			t.Fatal("record bound exceeded")
		}
		result = append(result, item)
	}
	return result
}
func start(t *testing.T, out io.Writer, max int) *telemetry.Telemetry {
	t.Helper()
	value, err := telemetry.Start(out, max)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func stop(t *testing.T, value *telemetry.Telemetry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := value.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
func wait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func frame(at time.Duration, used uint16) control.Frame {
	return control.Frame{At: at, SampleAt: at, HasSample: true, SampleValid: true, CandidateKnown: true, Candidate: true,
		Sample: control.Sample{At: time.Unix(100, 0).Add(at), Num: 256, Avail: used + 10, Used: used}}
}
func open(value *telemetry.Telemetry) control.QueueObserver {
	return value.Open(control.Target{PID: 12, StartTime: 34, Name: "guest"}, control.Queue{Slot: 3, Generation: 7})
}

func TestScopeCloseDrainsAcceptedOutcomesBeforeRetirement(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	scope.Observe(frame(0, 10))
	f := frame(time.Second, 10)
	f.Events[0] = control.Event{Kind: control.EventAction, Outcome: control.Accepted, AttemptID: 19, Decision: control.Pending}
	f.EventCount = 1
	scope.Observe(f)
	scope.Close(time.Second)
	stop(t, value)
	var accepted, retired bool
	for _, item := range lines(t, out) {
		if item.Record.Event == "action" && item.Record.Attempt == 19 && item.Record.Outcome == "accepted" {
			accepted = true
		}
		if item.Record.Event == "stream_retired" {
			if !accepted {
				t.Fatal("retired before admitted outcome")
			}
			retired = true
		}
	}
	if !accepted || !retired || value.Snapshot().Streams != 0 || value.Snapshot().InputLost != 0 {
		t.Fatal(value.Snapshot(), accepted, retired)
	}
}

func TestAtomicSampleAndCandidatePreserveEpisodeAtQuietDeadline(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	scope.Observe(frame(0, 10))
	quiet := frame(time.Second, 10)
	quiet.Candidate = false
	scope.Observe(quiet)
	scope.Observe(frame(2*time.Second, 11))
	scope.Close(2 * time.Second)
	stop(t, value)
	var opens int
	for _, item := range lines(t, out) {
		if item.Record.Event == "candidate" {
			opens++
		}
		if item.Record.Event == "episode_closed" && (item.Record.Reason != "retired" || !item.Record.UsedProgress) {
			t.Fatal(item)
		}
	}
	if opens != 1 || value.Snapshot().RecorderErrors != 0 {
		t.Fatal(opens, value.Snapshot())
	}
}

func TestInputLossHasOneCounterAndResetsEvidenceBeforeNextSample(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	scope.Observe(frame(0, 10))
	invalid := frame(-time.Second, 20)
	scope.Observe(invalid)
	scope.Observe(frame(time.Second, 30))
	scope.Close(time.Second)
	stop(t, value)
	var gap, fresh bool
	for _, item := range lines(t, out) {
		if item.Record.Event == "evidence_gap" && item.Record.LostInputs == 1 {
			gap = true
		}
		if item.Record.Event == "candidate" && item.Record.Episode == 2 {
			fresh = len(item.Record.Samples) == 1 && item.Record.Samples[0].Used == 30 && !item.Record.UsedProgress
		}
	}
	if !gap || !fresh || value.Snapshot().InputLost != 1 {
		t.Fatal(gap, fresh, value.Snapshot())
	}
}

func TestTrailingInputLossClosesEpisodeIncomplete(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	scope.Observe(frame(0, 10))
	scope.Observe(frame(-time.Second, 11))
	scope.Close(time.Second)
	stop(t, value)
	var incomplete bool
	for _, item := range lines(t, out) {
		if item.Record.Event == "episode_closed" && item.Record.EvidenceIncomplete {
			incomplete = true
		}
	}
	if !incomplete || value.Snapshot().InputLost != 1 {
		t.Fatal(incomplete, value.Snapshot())
	}
}

func TestControlOwnsSampleValidityAndOldSamplesAreNotReplayed(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	f := frame(0, 10)
	f.SampleValid = false
	scope.Observe(f)
	f = frame(time.Second, 20)
	f.Sample.Num = 0 // The bridge must use the supplied validity verdict.
	scope.Observe(f)
	f.At = 2 * time.Second
	f.Sample.Used = 30 // Same underlying sample timestamp.
	scope.Observe(f)
	scope.Close(2 * time.Second)
	stop(t, value)
	var opened, closed bool
	for _, item := range lines(t, out) {
		if item.Record.Event == "candidate" {
			opened = true
			if item.Record.Episode != 1 {
				t.Fatal(item)
			}
		}
		if item.Record.Event == "episode_closed" {
			closed = true
			if item.Record.UsedProgress || item.Record.Samples[0].Used != 20 {
				t.Fatal(item)
			}
		}
	}
	if !opened || !closed {
		t.Fatal(opened, closed)
	}
}

type blockedWriter struct {
	out              capture
	entered, release chan struct{}
	once             sync.Once
}

func (b *blockedWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.out.Write(p)
}

func TestBlockedOutputDoesNotStopReductionAndNextLineReportsOutputLoss(t *testing.T) {
	out := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	value := start(t, out, 1)
	value.TryNotice("first")
	select {
	case <-out.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter")
	}
	for i := 0; i < telemetry.OutputCapacity+10; i++ {
		if !value.TryNotice("pending") {
			t.Fatal("fixture unexpectedly lost input")
		}
		wait(t, func() bool { return value.Snapshot().Processed == uint64(i+2) })
	}
	scope := open(value)
	scope.Observe(frame(0, 10))
	scope.Close(0)
	wait(t, func() bool { return value.Snapshot().Streams == 0 })
	if value.Snapshot().InputLost != 0 || value.Snapshot().OutputLost == 0 {
		t.Fatal(value.Snapshot())
	}
	close(out.release)
	stop(t, value)
	var reported bool
	for _, item := range lines(t, &out.out) {
		if item.OutputLost > 0 && item.DeliveryIncomplete && !item.Record.EvidenceIncomplete {
			reported = true
		}
	}
	if !reported {
		t.Fatal("queued record concealed delivery loss")
	}
}

func TestBlockedWriterCannotDelayClosePastDeadline(t *testing.T) {
	out := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	value := start(t, out, 1)
	value.TryNotice("blocked")
	<-out.entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := value.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	if !value.Snapshot().Incomplete {
		t.Fatal("incomplete drain hidden")
	}
	close(out.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := value.Close(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("interrupted drain status lost", err)
	}
}

type failedWriter struct{ short bool }

func (f failedWriter) Write(p []byte) (int, error) {
	if f.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("failed")
}
func TestDestinationFailureCountsEveryUndeliveredRecordOnce(t *testing.T) {
	for _, short := range []bool{false, true} {
		value := start(t, failedWriter{short}, 1)
		for range 3 {
			if !value.TryNotice("message") {
				t.Fatal("rejected fixture")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := value.Close(ctx); err == nil {
			t.Fatal("destination failure hidden")
		}
		cancel()
		if stats := value.Snapshot(); !stats.Failed || stats.InputLost != 0 || stats.OutputLost != 3 {
			t.Fatal(stats)
		}
	}
}

func TestInputCapacityAndScopeChurnRemainBounded(t *testing.T) {
	value := start(t, io.Discard, 1)
	scope := open(value)
	for i := 0; i < 100000; i++ {
		scope.Observe(frame(time.Duration(i), 10))
	}
	scope.Close(time.Millisecond)
	stop(t, value)
	stats := value.Snapshot()
	if stats.InputLost == 0 || stats.Processed+stats.InputLost != 100000 || stats.Streams != 0 {
		t.Fatal(stats)
	}
}

func TestConcurrentScopesDrainWithoutSharingRecorderState(t *testing.T) {
	value := start(t, io.Discard, 8)
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for generation := range 50 {
				scope := value.Open(control.Target{PID: worker + 1}, control.Queue{Slot: 1, Generation: uint64(generation + 1)})
				scope.Observe(frame(0, 10))
				scope.Observe(frame(time.Second, 11))
				scope.Close(time.Second)
			}
		}()
	}
	workers.Wait()
	stop(t, value)
	if stats := value.Snapshot(); stats.Streams != 0 || stats.Processed+stats.InputLost != 800 || stats.RecorderErrors != 0 {
		t.Fatal(stats)
	}
}

func TestSampleAcquisitionTimeIsNotReplacedByFrameTime(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := open(value)
	f := frame(time.Second, 10)
	f.At = 2 * time.Second
	scope.Observe(f)
	scope.Close(2 * time.Second)
	stop(t, value)
	for _, item := range lines(t, out) {
		if item.Record.Event == "candidate" {
			if item.Record.Samples[0].At != int64(time.Second) {
				t.Fatal("acquisition time was relabeled", item)
			}
			return
		}
	}
	t.Fatal("candidate record missing")
}

func TestBoundedTextAndClosedScopeCannotRetainOrReopen(t *testing.T) {
	out := &capture{}
	value := start(t, out, 1)
	scope := value.Open(control.Target{PID: 1, Name: strings.Repeat("<", 10000)}, control.Queue{Slot: 1, Generation: 1})
	f := frame(0, 10)
	f.Events[0] = control.Event{Kind: control.EventAction, AttemptID: 1, Outcome: control.WriteFailed, Error: strings.Repeat("\x00", 10000)}
	f.EventCount = 1
	scope.Observe(f)
	scope.Close(0)
	scope.Observe(frame(time.Second, 20))
	stop(t, value)
	if value.Snapshot().InputLost != 1 {
		t.Fatal(value.Snapshot())
	}
	var actions int
	for _, item := range lines(t, out) {
		if item.Record.Event == "action" {
			actions++
			if len(item.Record.Message) != 256 {
				t.Fatal("text was not bounded")
			}
		}
	}
	if actions != 1 {
		t.Fatal(actions)
	}
}
