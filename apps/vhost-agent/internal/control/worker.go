package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	recovery "github.com/yckao/virtio-net-recovery/modules/recovery-core"
)

type Options struct {
	Recover                                         bool
	Cadence, InventoryInterval, VerificationTimeout time.Duration
	MaxQueues                                       int
}

func (o Options) Validate() error {
	if o.Cadence <= 0 || o.InventoryInterval <= 0 || o.VerificationTimeout <= 0 || o.MaxQueues < 1 || o.MaxQueues > 4096 {
		return errors.New("positive durations and max queues in 1..4096 required")
	}
	return nil
}

type Dependencies struct {
	Reader     Reader
	Writer     Writer
	Reporter   Reporter
	Clock      Clock
	Accounting *Accounting
	Registry   *Registry
}
type Worker struct {
	options                Options
	deps                   Dependencies
	target                 Target
	epoch                  time.Time
	prefix, streamSequence uint64
	queues                 map[int]*queueState
	lastInventory          time.Time
	forceInventory         bool
	rediscoveryUsed        bool
	sampled, unavailable   int
	lastPoll               time.Time
	inventoryUnavailable   map[int]bool
}
type queueState struct {
	queue            Queue
	policy           *recovery.Policy
	stream, sequence uint64
	sample           Sample
	hasSample        bool
}

func NewWorker(o Options, d Dependencies, t Target, epoch time.Time, workerID uint32) (*Worker, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if d.Reader == nil || d.Clock == nil || d.Accounting == nil || d.Registry == nil || workerID == 0 || (o.Recover && d.Writer == nil) {
		return nil, errors.New("missing worker dependencies")
	}
	return &Worker{options: o, deps: d, target: t, epoch: epoch, prefix: uint64(workerID) << 32, queues: map[int]*queueState{}, forceInventory: true}, nil
}

// Run owns no backend lifetime; the supervisor closes the reader after Run returns.
func (w *Worker) Run(ctx context.Context) error {
	defer w.retire()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		start := w.deps.Clock.Now()
		if err := w.Poll(ctx); err != nil {
			return err
		}
		delay := w.options.Cadence - w.deps.Clock.Now().Sub(start)
		if delay < 0 {
			delay = 0
		}
		if err := w.deps.Clock.Wait(ctx, delay); err != nil {
			return err
		}
	}
}

// Poll is one deterministic application cycle. Observation errors lose coverage,
// not the worker's policy identity. Core contract errors are fatal.
func (w *Worker) Poll(ctx context.Context) error {
	now := w.deps.Clock.Now()
	failed := false
	inventoryFailed := false
	inventorySucceeded := false
	if w.forceInventory || now.Sub(w.lastInventory) >= w.options.InventoryInterval {
		inv, err := w.deps.Reader.Inventory(ctx)
		w.lastInventory = now
		w.forceInventory = false
		if err != nil {
			failed = true
			inventoryFailed = true
		} else {
			if err = w.reconcile(inv, now); err != nil {
				return err
			}
			failed = len(inv.Unavailable) > 0
			inventorySucceeded = true
		}
	}
	keys := make([]int, 0, len(w.queues))
	for slot := range w.queues {
		keys = append(keys, slot)
	}
	slices.Sort(keys)
	handles := make([]Queue, 0, len(keys))
	for _, slot := range keys {
		if !w.inventoryUnavailable[slot] {
			handles = append(handles, w.queues[slot].queue)
		}
	}
	var samples []Sample
	var err error
	if !inventoryFailed {
		samples, err = w.deps.Reader.Sample(ctx, handles)
	}
	if err != nil {
		failed = true
		samples = nil
	}
	bySlot := make(map[int]Sample, len(samples))
	for _, s := range samples {
		bySlot[s.Queue.Slot] = s
	}
	sampled := 0
	for _, slot := range keys {
		q := w.queues[slot]
		s, ok := bySlot[slot]
		if !ok || s.Queue != q.queue || w.inventoryUnavailable[slot] {
			failed = true
			w.requestRediscovery()
			u, e := q.policy.Unavailable(w.elapsed(w.deps.Clock.Now()))
			if e != nil {
				return e
			}
			q.hasSample = false
			w.account(u)
			w.report(q, u)
			continue
		}
		q.sample, q.hasSample = s, true
		sampled++
		u, e := q.policy.Observe(recovery.CachedObservation{Generation: q.queue.Generation, At: w.elapsed(s.At), Avail: s.Avail, Used: s.Used, Size: s.Num})
		if e != nil {
			return fmt.Errorf("observe policy: %w", e)
		}
		w.account(u)
		w.report(q, u)
		if u.Operation != nil {
			op := *u.Operation
			var result Result
			if op.Kind == recovery.Notify {
				result = w.deps.Writer.Notify(ctx, q.queue, op.ExpectedUsed)
			} else {
				result = w.deps.Reader.Inspect(ctx, q.queue, op.ExpectedUsed)
			}
			if result.Outcome == ReadFailed {
				failed = true
			}
			if result.Decision == Changed {
				w.requestRediscovery()
			}
			if result.Live != nil {
				q.sample = *result.Live
				q.hasSample = true
			}
			completed, e := q.policy.Complete(toCoreResult(op, result, w.epoch))
			if e != nil {
				return fmt.Errorf("complete policy: %w", e)
			}
			w.account(completed)
			w.report(q, completed)
		}
		advanced, e := q.policy.Advance(w.elapsed(w.deps.Clock.Now()))
		if e != nil {
			return e
		}
		w.account(advanced)
		if len(advanced.Facts) > 0 {
			w.report(q, advanced)
		}
	}
	missing := len(w.queues) - sampled
	for slot := range w.inventoryUnavailable {
		if _, known := w.queues[slot]; !known {
			missing++
		}
	}
	w.deps.Accounting.Sampled.Add(int64(sampled - w.sampled))
	w.deps.Accounting.Unavailable.Add(int64(missing - w.unavailable))
	w.sampled, w.unavailable = sampled, missing
	gap := int64(0)
	if !w.lastPoll.IsZero() {
		gap = now.Sub(w.lastPoll).Nanoseconds()
	}
	w.lastPoll = now
	if inventorySucceeded && !failed {
		w.rediscoveryUsed = false
	}
	w.deps.Accounting.recordPoll(gap, failed)
	return nil
}
func (w *Worker) elapsed(t time.Time) time.Duration { return t.Sub(w.epoch) }
func (w *Worker) reconcile(inv Inventory, now time.Time) error {
	if len(inv.Queues) > w.options.MaxQueues {
		return errors.New("worker inventory exceeds queue bound")
	}
	present := map[int]bool{}
	w.inventoryUnavailable = map[int]bool{}
	for _, slot := range inv.Unavailable {
		present[slot] = true
		w.inventoryUnavailable[slot] = true
	}
	for _, handle := range inv.Queues {
		present[handle.Slot] = true
	}
	if inv.Complete {
		for slot, q := range w.queues {
			if !present[slot] {
				w.deps.Registry.Remove(q.stream)
				delete(w.queues, slot)
			}
		}
	}
	for _, handle := range inv.Queues {
		present[handle.Slot] = true
		q, exists := w.queues[handle.Slot]
		if exists && q.queue == handle {
			continue
		}
		if exists {
			w.deps.Registry.Remove(q.stream)
			if _, err := q.policy.ReplaceGeneration(handle.Generation, w.elapsed(now)); err != nil {
				return err
			}
		} else {
			if len(w.queues) >= w.options.MaxQueues {
				return errors.New("worker queue state capacity exceeded")
			}
			mode := recovery.Observe
			if w.options.Recover {
				mode = recovery.Recover
			}
			p, err := recovery.New(recovery.Config{Mode: mode, Cadence: w.options.Cadence, VerificationTimeout: w.options.VerificationTimeout}, handle.Generation)
			if err != nil {
				return err
			}
			q = &queueState{policy: p}
			w.queues[handle.Slot] = q
		}
		w.streamSequence++
		if w.streamSequence > uint64(^uint32(0)) {
			return errors.New("stream identity exhausted")
		}
		q.queue, q.stream, q.sequence, q.hasSample = handle, w.prefix|w.streamSequence, 0, false
		w.deps.Registry.Add(q.stream)
	}
	return nil
}
func (w *Worker) retire() {
	for _, q := range w.queues {
		w.deps.Registry.Remove(q.stream)
	}
	w.deps.Accounting.Sampled.Add(-int64(w.sampled))
	w.deps.Accounting.Unavailable.Add(-int64(w.unavailable))
	w.sampled, w.unavailable = 0, 0
}
func (w *Worker) account(u recovery.Update) {
	for _, f := range u.Facts {
		switch f.Kind {
		case recovery.OperationStarted:
			if f.AttemptID != 0 {
				w.deps.Accounting.Attempts.Add(1)
			}
		case recovery.OperationCompleted:
			if f.AttemptID != 0 {
				switch f.Outcome {
				case recovery.Accepted:
					w.deps.Accounting.Accepted.Add(1)
				case recovery.Refused:
					w.deps.Accounting.Refused.Add(1)
				case recovery.WriteFailed:
					w.deps.Accounting.WriteErrors.Add(1)
				}
			}
		case recovery.VerificationConfirmed:
			w.deps.Accounting.Verified.Add(1)
		}
	}
}
func (w *Worker) report(q *queueState, u recovery.Update) {
	if w.deps.Reporter == nil {
		return
	}
	q.sequence++
	frame := Frame{Stream: q.stream, Sequence: q.sequence, Target: w.target, Queue: q.queue, At: w.elapsed(w.deps.Clock.Now()), Sample: q.sample, HasSample: q.hasSample, CandidateKnown: q.hasSample, Candidate: u.Candidate}
	for _, f := range u.Facts {
		event, ok := toEvent(f)
		if ok && frame.EventCount < len(frame.Events) {
			frame.Events[frame.EventCount] = event
			frame.EventCount++
		}
	}
	if !w.deps.Reporter.TryOffer(frame) {
		w.deps.Accounting.InputLost.Add(1)
	}
}

// One prompt refresh follows loss of an admitted queue. A failed prompt cannot
// turn discovery into a per-poll loop; normal inventory cadence restores budget.
func (w *Worker) requestRediscovery() {
	if !w.rediscoveryUsed {
		w.rediscoveryUsed = true
		w.forceInventory = true
	}
}
