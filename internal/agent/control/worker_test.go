package control_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

type clock struct{ at time.Time }

func (c *clock) Now() time.Time { return c.at }
func (c *clock) Wait(ctx context.Context, d time.Duration) error {
	c.at = c.at.Add(d)
	return ctx.Err()
}

type fixture struct {
	num, liveNum          uint32
	noLive                bool
	operationError        error
	samplesOverride       []control.Sample
	inventoryTruncated    bool
	inventoryOverride     *control.Inventory
	inventoryCalls        int
	inventoryError        bool
	noQueues              bool
	clock                 *clock
	queue                 control.Queue
	avail, used, consumed uint16
	missing, partial      bool
	writes, inspects      int
	outcome               control.Outcome
	decision              control.Decision
	cost                  time.Duration
	attempts              []time.Time
	unavailable           []int
}

func (f *fixture) Inventory(context.Context) (control.Inventory, error) {
	f.inventoryCalls++
	if f.inventoryOverride != nil {
		return *f.inventoryOverride, nil
	}
	if f.inventoryError {
		return control.Inventory{}, errors.New("inventory unavailable")
	}
	var queues []control.Queue
	if !f.noQueues {
		queues = []control.Queue{f.queue}
	}
	return control.Inventory{Queues: queues, Unavailable: f.unavailable, Complete: !f.partial, Truncated: f.inventoryTruncated}, nil
}
func (f *fixture) Sample(context.Context, []control.Queue) ([]control.Sample, error) {
	if f.samplesOverride != nil {
		return f.samplesOverride, nil
	}
	if f.missing {
		return nil, nil
	}
	return []control.Sample{f.sample(false)}, nil
}
func (f *fixture) sample(live bool) control.Sample {
	num := f.num
	if num == 0 {
		num = 256
	}
	if live && f.liveNum != 0 {
		num = f.liveNum
	}
	return control.Sample{Queue: f.queue, At: f.clock.Now(), Num: num, Avail: f.avail, Used: f.used, Consumed: f.consumed, Live: live}
}
func (f *fixture) Inspect(context.Context, control.Queue, uint16) control.Result {
	f.inspects++
	s := f.sample(true)
	return control.Result{Outcome: control.Observed, Decision: control.UsedProgress, CompletedAt: f.clock.Now(), Live: &s}
}
func (f *fixture) Notify(context.Context, control.Queue, uint16) control.Result {
	f.writes++
	f.attempts = append(f.attempts, f.clock.Now())
	f.clock.at = f.clock.at.Add(f.cost)
	s := f.sample(true)
	result := control.Result{Outcome: f.outcome, Decision: f.decision, CompletedAt: f.clock.Now(), Live: &s, Err: f.operationError}
	if f.outcome == control.Accepted {
		result.AcceptedAt = f.clock.Now()
	}
	if f.decision == control.Changed || f.noLive {
		result.Live = nil
	}
	return result
}
func (f *fixture) Close() error { return nil }
func (f *fixture) Kick(ctx context.Context, q control.Queue) control.Result {
	return f.Notify(ctx, q, 0)
}

type dropper struct{ frames int }

func (d *dropper) Open(control.Target, control.Queue) control.QueueObserver { return d }
func (d *dropper) Observe(control.Frame)                                    { d.frames++ }
func (d *dropper) Close(time.Duration)                                      {}
func newFixture(t *testing.T, recover bool, observer control.Observer) (*control.Worker, *fixture, *control.Accounting) {
	t.Helper()
	c := &clock{at: time.Unix(100, 0)}
	f := &fixture{clock: c, queue: control.Queue{Slot: 7, Generation: 1}, avail: 10, used: 5, consumed: 6, outcome: control.Accepted, decision: control.Pending}
	a := &control.Accounting{}
	w, err := control.NewWorker(control.Options{Recover: recover, Cadence: 100 * time.Millisecond, InventoryInterval: time.Millisecond, VerificationTimeout: time.Second, MaxQueues: 16}, control.Dependencies{Reader: f, Writer: f, Clock: c, Accounting: a, Observer: observer}, control.Target{PID: 1, StartTime: 2}, c.at)
	if err != nil {
		t.Fatal(err)
	}
	return w, f, a
}
func poll(t *testing.T, w *control.Worker, f *fixture, after time.Duration) {
	t.Helper()
	f.clock.at = f.clock.at.Add(after)
	if err := w.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestCompletionPacingAndAuthoritativeAccountingSurviveDroppedFrames(t *testing.T) {
	d := &dropper{}
	w, f, a := newFixture(t, true, d)
	f.cost = 50 * time.Millisecond
	poll(t, w, f, 0)
	poll(t, w, f, 100*time.Millisecond)
	poll(t, w, f, 99*time.Millisecond)
	if f.writes != 1 {
		t.Fatal("retried before completion cadence", f.writes)
	}
	poll(t, w, f, time.Millisecond)
	if f.writes != 2 {
		t.Fatal(f.writes)
	}
	if f.attempts[1].Sub(f.attempts[0]) != 150*time.Millisecond {
		t.Fatal(f.attempts)
	}
	c := a.Snapshot()
	if c.Accepted != 2 || c.Attempts != 2 || d.frames == 0 {
		t.Fatal(c)
	}
}
func TestGenerationReplacementCannotBypassPacing(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	poll(t, w, f, 0)
	poll(t, w, f, 100*time.Millisecond)
	f.queue.Generation = 2
	poll(t, w, f, time.Millisecond)
	poll(t, w, f, 99*time.Millisecond)
	if f.writes != 1 {
		t.Fatal(f.writes)
	}
	poll(t, w, f, time.Millisecond)
	if f.writes != 2 || a.Snapshot().Verified != 0 {
		t.Fatal(a.Snapshot())
	}
}
func TestUnavailableResetsCandidateWithoutInventingProgress(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	poll(t, w, f, 0)
	f.missing = true
	poll(t, w, f, 100*time.Millisecond)
	f.missing = false
	poll(t, w, f, 0)
	if f.writes != 0 {
		t.Fatal("unavailable time produced a candidate")
	}
	poll(t, w, f, 100*time.Millisecond)
	if a.Snapshot().Accepted != 1 {
		t.Fatal(a.Snapshot())
	}
	f.used = 7
	f.consumed = 8
	poll(t, w, f, 100*time.Millisecond)
	if a.Snapshot().Verified != 1 {
		t.Fatal(a.Snapshot())
	}
}
func TestObservationNeverRequestsWriter(t *testing.T) {
	w, f, a := newFixture(t, false, nil)
	poll(t, w, f, 0)
	poll(t, w, f, 100*time.Millisecond)
	if f.writes != 0 || f.inspects != 1 || a.Snapshot().Accepted != 0 {
		t.Fatal(a.Snapshot(), f.writes, f.inspects)
	}
}
func TestIdentityFailureReceiptIsValidAndNeverConfirmsOldGeneration(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	poll(t, w, f, 0)
	poll(t, w, f, 100*time.Millisecond)
	f.outcome = control.ReadFailed
	f.decision = control.Changed
	poll(t, w, f, 100*time.Millisecond)
	f.queue.Generation = 2
	f.used = 8
	f.consumed = 9
	poll(t, w, f, 100*time.Millisecond)
	if a.Snapshot().Verified != 0 {
		t.Fatal(a.Snapshot())
	}
}
func TestManualHasNoCandidateRequirementAndKeepsActualReceipt(t *testing.T) {
	_, f, _ := newFixture(t, true, nil)
	f.avail = f.used
	batch, err := control.ExecuteManual(context.Background(), f, f)
	if err != nil || !batch.Complete || f.writes != 1 || batch.Receipts[0].Result.Outcome != control.Accepted {
		t.Fatal(batch, err, f.writes)
	}
}

func TestInventoryFailureBreaksCandidateContinuity(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	poll(t, w, f, 0)
	f.inventoryError = true
	poll(t, w, f, 100*time.Millisecond)
	if f.writes != 0 || a.Snapshot().PollErrors != 1 {
		t.Fatal(f.writes, a.Snapshot())
	}
	f.inventoryError = false
	poll(t, w, f, time.Millisecond)
	poll(t, w, f, 99*time.Millisecond)
	if f.writes != 0 {
		t.Fatal("candidate aged through missing inventory")
	}
	poll(t, w, f, time.Millisecond)
	if f.writes != 1 {
		t.Fatal(f.writes)
	}
}
func TestUnsupportedAndMissingQueuesHaveVisibleCoverage(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	f.noQueues = true
	f.unavailable = []int{7, 8}
	poll(t, w, f, 0)
	if c := a.Snapshot(); c.Unavailable != 2 || c.Sampled != 0 || c.PollErrors != 1 {
		t.Fatal(c)
	}
	f.noQueues = true
	f.unavailable = []int{7}
	poll(t, w, f, time.Millisecond)
	if c := a.Snapshot(); c.Unavailable != 1 || c.Sampled != 0 {
		t.Fatal(c)
	}
	f.unavailable = nil
	f.noQueues = false
	f.missing = true
	poll(t, w, f, time.Millisecond)
	if c := a.Snapshot(); c.Unavailable != 1 || c.PollErrors != 3 {
		t.Fatal(c)
	}
}

func TestOnePromptRediscoveryDoesNotBecomeAnInventoryLoop(t *testing.T) {
	c := &clock{at: time.Unix(100, 0)}
	f := &fixture{clock: c, queue: control.Queue{Slot: 7, Generation: 1}, avail: 10, used: 5}
	w, err := control.NewWorker(control.Options{Cadence: 100 * time.Millisecond, InventoryInterval: 5 * time.Second, VerificationTimeout: time.Second, MaxQueues: 1}, control.Dependencies{Reader: f, Clock: c, Accounting: &control.Accounting{}}, control.Target{PID: 1, StartTime: 1}, c.at)
	if err != nil {
		t.Fatal(err)
	}
	poll(t, w, f, 0)
	f.missing = true
	poll(t, w, f, 100*time.Millisecond)
	poll(t, w, f, 100*time.Millisecond)
	if f.inventoryCalls != 2 {
		t.Fatal("missing prompt retry", f.inventoryCalls)
	}
	for i := 0; i < 10; i++ {
		poll(t, w, f, 100*time.Millisecond)
	}
	if f.inventoryCalls != 2 {
		t.Fatal("repeated failed prompt rediscovery", f.inventoryCalls)
	}
	poll(t, w, f, 4*time.Second)
	if f.inventoryCalls != 3 {
		t.Fatal("normal inventory not resumed", f.inventoryCalls)
	}
}

type scopeLog struct {
	scopes      []*observedQueue
	transitions []string
}
type observedQueue struct {
	owner  *scopeLog
	queue  control.Queue
	frames []control.Frame
	closed int
}

func (l *scopeLog) Open(_ control.Target, q control.Queue) control.QueueObserver {
	s := &observedQueue{owner: l, queue: q}
	l.scopes = append(l.scopes, s)
	l.transitions = append(l.transitions, "open")
	return s
}
func (s *observedQueue) Observe(f control.Frame) { s.frames = append(s.frames, f) }
func (s *observedQueue) Close(time.Duration) {
	s.closed++
	s.owner.transitions = append(s.owner.transitions, "close")
}
func TestObserverScopeFollowsAttachmentLifetime(t *testing.T) {
	log := &scopeLog{}
	w, f, _ := newFixture(t, true, log)
	poll(t, w, f, 0)
	f.queue.Generation++
	poll(t, w, f, time.Millisecond)
	if len(log.scopes) != 2 || log.scopes[0].closed != 1 || log.scopes[1].closed != 0 || log.transitions[1] != "close" {
		t.Fatal("replacement did not transfer observer lifetime", log.transitions)
	}
	f.noQueues = true
	poll(t, w, f, time.Millisecond)
	if log.scopes[1].closed != 1 {
		t.Fatal("removed queue observer retained")
	}
}
func TestObserverValidityComesFromPolicyAndCompletion(t *testing.T) {
	log := &scopeLog{}
	w, f, _ := newFixture(t, true, log)
	f.num = 255
	poll(t, w, f, 0)
	frame := log.scopes[0].frames[0]
	if !frame.HasSample || frame.SampleValid || frame.CandidateKnown {
		t.Fatal("invalid cached geometry became valid evidence", frame)
	}
	f.num = 256
	poll(t, w, f, time.Millisecond)
	f.outcome = control.Refused
	f.decision = control.Invalid
	f.liveNum = 255
	poll(t, w, f, 100*time.Millisecond)
	frames := log.scopes[0].frames
	frame = frames[len(frames)-1]
	if !frame.HasSample || !frame.Sample.Live || frame.SampleValid || frame.CandidateKnown {
		t.Fatal("invalid live receipt became valid evidence", frame)
	}
}
func TestCompletionWithoutLiveDataDoesNotRepeatCachedSample(t *testing.T) {
	log := &scopeLog{}
	w, f, _ := newFixture(t, true, log)
	f.outcome = control.Refused
	f.decision = control.Busy
	f.noLive = true
	f.operationError = errors.New("mutation already owned")
	poll(t, w, f, 0)
	poll(t, w, f, 100*time.Millisecond)
	frames := log.scopes[0].frames
	frame := frames[len(frames)-1]
	if frame.HasSample || frame.CandidateKnown || frame.EventCount != 1 || frame.Events[0].Error != "mutation already owned" {
		t.Fatal("completion invented fresh observation or lost failure", frame)
	}
}
func TestCompletedAttemptCountersPartitionUnavailableAndCancelled(t *testing.T) {
	for _, outcome := range []control.Outcome{control.ReadFailed, control.Aborted} {
		w, f, a := newFixture(t, true, nil)
		f.outcome = outcome
		f.decision = control.Unavailable
		f.noLive = true
		if outcome == control.Aborted {
			f.decision = control.Cancelled
		}
		poll(t, w, f, 0)
		poll(t, w, f, 100*time.Millisecond)
		c := a.Snapshot()
		if c.Attempts != 1 || c.Accepted+c.Refused+c.ReadErrors+c.WriteErrors+c.Cancelled != c.Attempts {
			t.Fatal("authoritative outcome partition lost failure", c)
		}
	}
}
func TestAmbiguousBatchLosesAllCoverageAndCandidateAge(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	poll(t, w, f, 0)
	f.samplesOverride = []control.Sample{f.sample(false), f.sample(false)}
	poll(t, w, f, 100*time.Millisecond)
	if c := a.Snapshot(); f.writes != 0 || c.Sampled != 0 || c.Unavailable != 1 || c.PollErrors != 1 {
		t.Fatal("duplicate batch created coverage", c)
	}
	f.samplesOverride = nil
	poll(t, w, f, time.Millisecond)
	if f.writes != 0 {
		t.Fatal("candidate aged across invalid batch")
	}
}
func TestTruncatedInventoryRemainsVisibleWithoutFakeQueue(t *testing.T) {
	w, f, a := newFixture(t, true, nil)
	f.partial = true
	f.inventoryTruncated = true
	poll(t, w, f, 0)
	if c := a.Snapshot(); c.InventoryTruncated != 1 || c.PollErrors != 1 {
		t.Fatal("truncation disappeared", c)
	}
	f.partial = false
	f.inventoryTruncated = false
	poll(t, w, f, time.Millisecond)
	if c := a.Snapshot(); c.InventoryTruncated != 0 {
		t.Fatal("resolved truncation remained", c)
	}
}

func TestInvalidInventoryCannotRetireOrReplaceExistingScope(t *testing.T) {
	for _, invalid := range []control.Inventory{
		{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 2}, {Slot: 8, Generation: 3}}},
		{Complete: true, Queues: []control.Queue{{Slot: 8}}},
		{Complete: true, Unavailable: []int{-1}},
		{Complete: true, Truncated: true},
	} {
		observer := &scopeLog{}
		w, f, a := newFixture(t, true, observer)
		poll(t, w, f, 0)
		f.inventoryOverride = &invalid
		f.clock.at = f.clock.at.Add(time.Millisecond)
		if err := w.Poll(context.Background()); err == nil {
			t.Fatal("ambiguous inventory accepted", invalid)
		}
		if len(observer.scopes) != 1 || observer.scopes[0].closed != 0 || a.Snapshot().InventoryTruncated != 0 {
			t.Fatal("invalid inventory mutated existing scope", observer, a.Snapshot())
		}
	}
}
