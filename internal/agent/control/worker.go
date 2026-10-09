package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	recovery "github.com/yckao/virtio-net-recovery/recovery"
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
	Observer   Observer
	Clock      Clock
	Accounting *Accounting
}
type Worker struct {
	options              Options
	deps                 Dependencies
	target               Target
	epoch                time.Time
	queues               map[int]*queueState
	lastInventory        time.Time
	forceInventory       bool
	rediscoveryUsed      bool
	sampled, unavailable int
	lastPoll             time.Time
	inventoryUnavailable map[int]bool
	inventoryTruncated   bool
	inventoryIncomplete  bool
}
type queueState struct {
	queue       Queue
	policy      *recovery.Policy
	observer    QueueObserver
	sampleValid bool
	sample      Sample
	hasSample   bool
}

func NewWorker(o Options, d Dependencies, t Target, epoch time.Time) (*Worker, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if d.Reader == nil || d.Clock == nil || d.Accounting == nil || (o.Recover && d.Writer == nil) {
		return nil, errors.New("missing worker dependencies")
	}
	return &Worker{options: o, deps: d, target: t, epoch: epoch, queues: map[int]*queueState{}, forceInventory: true}, nil
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
	refresh, err := w.refreshInventory(ctx, now)
	if err != nil {
		return err
	}
	failed := refresh == inventoryMissing || w.inventoryIncomplete || len(w.inventoryUnavailable) > 0 || w.inventoryTruncated
	keys := w.queueSlots()
	samples := map[int]Sample{}
	if refresh != inventoryMissing {
		var err error
		samples, err = w.readSamples(ctx, keys)
		failed = failed || err != nil
	}
	sampled := 0
	for _, slot := range keys {
		q := w.queues[slot]
		sample, ok := samples[slot]
		if !ok || w.inventoryUnavailable[slot] {
			failed = true
			if err := w.loseObservation(q); err != nil {
				return err
			}
			continue
		}
		sampled++
		unavailable, err := w.observeQueue(ctx, q, sample)
		if err != nil {
			return err
		}
		failed = failed || unavailable
	}
	if refresh == inventoryReady && !failed {
		w.rediscoveryUsed = false
	}
	w.updateCoverage(now, sampled, failed)
	return nil
}

type inventoryRefresh uint8

const (
	inventoryUnchanged inventoryRefresh = iota
	inventoryReady
	inventoryMissing
)

func (w *Worker) refreshInventory(ctx context.Context, now time.Time) (inventoryRefresh, error) {
	if !w.forceInventory && now.Sub(w.lastInventory) < w.options.InventoryInterval {
		return inventoryUnchanged, nil
	}
	w.lastInventory, w.forceInventory = now, false
	inventory, err := w.deps.Reader.Inventory(ctx)
	if err != nil {
		return inventoryMissing, nil
	}
	if err = w.reconcile(inventory, now); err != nil {
		return inventoryMissing, err
	}
	w.inventoryIncomplete = !inventory.Complete
	return inventoryReady, nil
}
func (w *Worker) queueSlots() []int {
	keys := make([]int, 0, len(w.queues))
	for slot := range w.queues {
		keys = append(keys, slot)
	}
	slices.Sort(keys)
	return keys
}
func (w *Worker) readSamples(ctx context.Context, slots []int) (map[int]Sample, error) {
	handles := make([]Queue, 0, len(slots))
	expected := make(map[Queue]bool, len(slots))
	for _, slot := range slots {
		if !w.inventoryUnavailable[slot] {
			q := w.queues[slot].queue
			handles = append(handles, q)
			expected[q] = true
		}
	}
	samples, err := w.deps.Reader.Sample(ctx, handles)
	if err != nil {
		return nil, err
	}
	if len(samples) != len(handles) {
		return nil, errors.New("incomplete observation batch")
	}
	observed := make(map[int]Sample, len(samples))
	for _, sample := range samples {
		if !expected[sample.Queue] {
			return nil, errors.New("unexpected or duplicate observation in batch")
		}
		delete(expected, sample.Queue)
		observed[sample.Queue.Slot] = sample
	}
	return observed, nil
}
func (w *Worker) loseObservation(q *queueState) error {
	w.requestRediscovery()
	update, err := q.policy.Unavailable(w.elapsed(w.deps.Clock.Now()))
	if err != nil {
		return err
	}
	q.hasSample, q.sampleValid = false, false
	w.account(update)
	w.report(q, update)
	return nil
}
func (w *Worker) observeQueue(ctx context.Context, q *queueState, sample Sample) (bool, error) {
	update, err := q.policy.Observe(recovery.CachedObservation{Generation: q.queue.Generation, At: w.elapsed(sample.At), Avail: sample.Avail, Used: sample.Used, Size: sample.Num})
	if err != nil {
		return false, fmt.Errorf("observe policy: %w", err)
	}
	q.sample, q.hasSample, q.sampleValid = sample, true, true
	for _, fact := range update.Facts {
		if fact.Kind == recovery.ObservationRejected {
			q.sampleValid = false
		}
	}
	failed := !q.sampleValid
	w.account(update)
	w.report(q, update)
	if update.Operation != nil {
		unavailable, err := w.performOperation(ctx, q, *update.Operation)
		if err != nil {
			return false, err
		}
		failed = failed || unavailable
	}
	advanced, err := q.policy.Advance(w.elapsed(w.deps.Clock.Now()))
	if err != nil {
		return false, err
	}
	w.account(advanced)
	if len(advanced.Facts) > 0 {
		w.report(q, advanced)
	}
	return failed, nil
}
func (w *Worker) performOperation(ctx context.Context, q *queueState, operation recovery.Operation) (bool, error) {
	var result Result
	if operation.Kind == recovery.Notify {
		result = w.deps.Writer.Notify(ctx, q.queue, operation.ExpectedUsed)
	} else {
		result = w.deps.Reader.Inspect(ctx, q.queue, operation.ExpectedUsed)
	}
	receipt := toCoreResult(operation, result, w.epoch)
	update, err := q.policy.Complete(receipt)
	if err != nil {
		return false, fmt.Errorf("complete policy: %w", err)
	}
	// A completion never presents an earlier cached observation as a new live
	// sample. If the effect returned no observation, its frame contains none.
	q.hasSample, q.sampleValid = false, false
	if result.Live != nil {
		q.sample = *result.Live
		q.hasSample = true
		q.sampleValid = receipt.Live.Valid
	}
	if result.Outcome == ReadFailed || result.Decision == Changed {
		q.hasSample, q.sampleValid = false, false
	}
	if result.Decision == Changed {
		w.requestRediscovery()
	}
	w.account(update)
	w.reportResult(q, update, result.Err)
	return result.Outcome == ReadFailed, nil
}
func (w *Worker) updateCoverage(now time.Time, sampled int, failed bool) {
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
	w.deps.Accounting.recordPoll(gap, failed)
}
func (w *Worker) elapsed(t time.Time) time.Duration { return t.Sub(w.epoch) }
func (w *Worker) reconcile(inv Inventory, now time.Time) error {
	if len(inv.Queues) > w.options.MaxQueues || len(inv.Unavailable) > w.options.MaxQueues {
		return errors.New("worker inventory exceeds queue bound")
	}
	if err := validateInventory(inv); err != nil {
		return err
	}
	if inv.Truncated != w.inventoryTruncated {
		if inv.Truncated {
			w.deps.Accounting.InventoryTruncated.Add(1)
		} else {
			w.deps.Accounting.InventoryTruncated.Add(-1)
		}
		w.inventoryTruncated = inv.Truncated
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
				w.closeObserver(q)
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
			w.closeObserver(q)
			if _, err := q.policy.ReplaceGeneration(handle.Generation, w.elapsed(now)); err != nil {
				return err
			}
		} else {
			if len(w.queues) >= w.options.MaxQueues {
				w.inventoryUnavailable[handle.Slot] = true
				continue
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
		q.queue, q.hasSample = handle, false
		if w.deps.Observer != nil {
			q.observer = w.deps.Observer.Open(w.target, handle)
		}

	}
	return nil
}

func (w *Worker) retire() {
	for _, q := range w.queues {
		w.closeObserver(q)
	}
	w.deps.Accounting.Sampled.Add(-int64(w.sampled))
	w.deps.Accounting.Unavailable.Add(-int64(w.unavailable))
	w.sampled, w.unavailable = 0, 0
	if w.inventoryTruncated {
		w.deps.Accounting.InventoryTruncated.Add(-1)
		w.inventoryTruncated = false
	}
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
				case recovery.Unavailable:
					w.deps.Accounting.ReadErrors.Add(1)
				case recovery.Cancelled:
					w.deps.Accounting.Cancelled.Add(1)
				}
			}
		case recovery.VerificationConfirmed:
			w.deps.Accounting.Verified.Add(1)
		}
	}
}
func (w *Worker) report(q *queueState, u recovery.Update) { w.reportResult(q, u, nil) }
func (w *Worker) reportResult(q *queueState, u recovery.Update, operationError error) {
	if q.observer == nil {
		return
	}
	frame := Frame{At: w.elapsed(w.deps.Clock.Now()), Sample: q.sample, HasSample: q.hasSample, SampleValid: q.sampleValid, CandidateKnown: q.hasSample && q.sampleValid, Candidate: u.Candidate}
	if q.hasSample {
		frame.SampleAt = w.elapsed(q.sample.At)
	}
	// A later fact-only update must not replay this observation as a new sample.
	q.hasSample, q.sampleValid = false, false
	for _, f := range u.Facts {
		if event, ok := toEvent(f); ok {
			if operationError != nil && (event.Kind == EventAction || event.Kind == EventDecision) {
				event.Error = operationError.Error()
				if len(event.Error) > 256 {
					event.Error = event.Error[:256]
				}
			}
			frame.Events[frame.EventCount] = event
			frame.EventCount++
		}
	}
	q.observer.Observe(frame)
}
func (w *Worker) closeObserver(q *queueState) {
	if q.observer != nil {
		q.observer.Close(w.elapsed(w.deps.Clock.Now()))
		q.observer = nil
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
