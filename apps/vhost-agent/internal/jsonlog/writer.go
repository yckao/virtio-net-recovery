// Package jsonlog encodes presentation values and delivers them through one
// bounded writer worker. A slow destination never runs in a producer goroutine.
package jsonlog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
)

var (
	ErrInvalidOptions = errors.New("invalid JSON output options")
	ErrStarted        = errors.New("JSON output already started")
	ErrClosed         = errors.New("JSON output closed")
)

type Options struct{ MaxRecords, MaxBytes, MaxRecordBytes int }

func DefaultOptions() Options {
	return Options{MaxRecords: 256, MaxBytes: 4 << 20, MaxRecordBytes: 64 << 10}
}

type Stats struct {
	Enqueued, Written, Dropped, Oversized, Errors uint64
	PendingRecords, PendingBytes                  int
	Closed, Failed, Incomplete                    bool
}

type Writer struct {
	options                                         Options
	output                                          io.Writer
	mu                                              sync.Mutex
	queue                                           []queuedRecord
	head, count, pending, bytes                     int
	started, closed, failed, incomplete             bool
	notify, done                                    chan struct{}
	abort                                           atomic.Bool
	enqueued, written, dropped, oversized, failures atomic.Uint64
}

type queuedRecord struct {
	data      []byte
	budget    int
	important bool
	kind      report.Kind
	stream    uint64
}

func New(options Options, output io.Writer) (*Writer, error) {
	if output == nil || options.MaxRecords < 1 || options.MaxRecords > 65536 || options.MaxBytes < 1 || options.MaxBytes > 256<<20 ||
		options.MaxRecordBytes < 1 || options.MaxRecordBytes > 1<<20 || options.MaxRecordBytes > options.MaxBytes {
		return nil, ErrInvalidOptions
	}
	return &Writer{options: options, output: output, queue: make([]queuedRecord, options.MaxRecords), notify: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

// Start launches exactly one destination worker. Cancellation stops further
// work but cannot interrupt an arbitrary io.Writer already inside Write.
func (w *Writer) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.started {
		return ErrStarted
	}
	w.started = true
	go w.run(ctx)
	return nil
}

// TryWrite encodes a bounded value and immediately admits or drops it. No call
// into the supplied io.Writer occurs here. Count and byte budgets include the
// in-flight write. Lock contention also drops rather than delaying a producer.
func (w *Writer) TryWrite(record report.Record) bool {
	if !validRecord(record) {
		w.dropped.Add(1)
		w.oversized.Add(1)
		return false
	}
	data, err := json.Marshal(toWire(record))
	budget := len(data) + deliveryOverhead
	if err != nil || budget > w.options.MaxRecordBytes {
		w.dropped.Add(1)
		w.oversized.Add(1)
		return false
	}
	if !w.mu.TryLock() {
		w.dropped.Add(1)
		return false
	}
	defer w.mu.Unlock()
	if w.closed || w.failed {
		w.dropped.Add(1)
		return false
	}
	important := importantRecord(record.Kind)
	if !important {
		// Coalesce a stream's queued routine snapshot, never an in-flight
		// write or an action/terminal record. Each replaced record is lost.
		for i := 0; i < w.count; i++ {
			old := w.queue[(w.head+i)%len(w.queue)]
			if !old.important && old.stream == record.Stream && old.kind == record.Kind {
				w.evict(i)
				break
			}
		}
		reserveRecords := min(w.options.MaxRecords-1, max(1, w.options.MaxRecords/4))
		reserveBytes := w.options.MaxBytes / 4
		if w.pending >= w.options.MaxRecords-reserveRecords || budget > w.options.MaxBytes-reserveBytes-w.bytes {
			w.dropped.Add(1)
			return false
		}
	} else {
		for w.count > 0 && (w.pending >= w.options.MaxRecords || budget > w.options.MaxBytes-w.bytes) {
			// Prefer the oldest routine record; if only outcomes remain,
			// retain the most recent bounded set of outcomes.
			victim := 0
			for i := 0; i < w.count; i++ {
				if !w.queue[(w.head+i)%len(w.queue)].important {
					victim = i
					break
				}
			}
			w.evict(victim)
		}
		if w.pending >= w.options.MaxRecords || budget > w.options.MaxBytes-w.bytes {
			w.dropped.Add(1)
			return false
		}
	}
	w.queue[(w.head+w.count)%len(w.queue)] = queuedRecord{data: data, budget: budget, important: important, kind: record.Kind, stream: record.Stream}
	w.count++
	w.pending++
	w.bytes += budget
	w.enqueued.Add(1)
	w.wake()
	return true
}

func (w *Writer) wake() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *Writer) run(ctx context.Context) {
	defer close(w.done)
	for {
		w.mu.Lock()
		if ctx.Err() != nil || w.abort.Load() {
			w.closed, w.incomplete = true, true
			w.discardQueued()
			w.mu.Unlock()
			return
		}
		if w.count == 0 {
			closed := w.closed
			w.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-ctx.Done():
			case <-w.notify:
			}
			continue
		}
		record := w.queue[w.head]
		w.queue[w.head] = queuedRecord{}
		w.head = (w.head + 1) % len(w.queue)
		w.count--
		w.mu.Unlock()
		lost := w.dropped.Load()
		data, err := json.Marshal(deliveryDTO{Version: 1, Incomplete: lost != 0, Lost: lost, Record: record.data})
		if err == nil {
			data = append(data, '\n')
			var n int
			n, err = w.output.Write(data)
			if err == nil && n != len(data) {
				err = io.ErrShortWrite
			}
		}
		w.mu.Lock()
		w.pending--
		w.bytes -= record.budget
		if err != nil {
			w.failed, w.closed, w.incomplete = true, true, true
			w.failures.Add(1)
			w.dropped.Add(1)
			w.discardQueued()
			w.mu.Unlock()
			return
		}
		w.written.Add(1)
		w.mu.Unlock()
	}
}

func (w *Writer) discardQueued() {
	for w.count > 0 {
		budget := w.queue[w.head].budget
		w.queue[w.head] = queuedRecord{}
		w.head = (w.head + 1) % len(w.queue)
		w.count--
		w.pending--
		w.bytes -= budget
		w.dropped.Add(1)
	}
}

func (w *Writer) evict(offset int) {
	index := (w.head + offset) % len(w.queue)
	w.bytes -= w.queue[index].budget
	for i := offset; i < w.count-1; i++ {
		w.queue[(w.head+i)%len(w.queue)] = w.queue[(w.head+i+1)%len(w.queue)]
	}
	w.queue[(w.head+w.count-1)%len(w.queue)] = queuedRecord{}
	w.count--
	w.pending--
	w.dropped.Add(1)
}

func importantRecord(kind report.Kind) bool {
	switch kind {
	case report.Candidate, report.EpisodeAction, report.EpisodeClosed, report.Gap, report.Retired, report.Action, report.Verification, report.Status, report.Manual:
		return true
	default:
		return false
	}
}

// Shutdown stops admission and drains admitted records until ctx expires. On
// expiry it discards queued records and returns ctx.Err without waiting for a
// blocked destination. One bounded in-flight buffer/goroutine can remain until
// that Write returns; reusable callers must supply an independently cancellable
// writer. The CLI may exit after its separate backend cleanup completes.
func (w *Writer) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	if !w.started {
		w.started = true
		w.incomplete = w.pending != 0
		w.discardQueued()
		close(w.done)
	}
	w.wake()
	w.mu.Unlock()
	select {
	case <-w.done:
		if w.Snapshot().Failed {
			return errors.New("JSON destination failed")
		}
		return nil
	case <-ctx.Done():
		w.abort.Store(true)
		w.mu.Lock()
		w.incomplete = true
		w.discardQueued()
		w.mu.Unlock()
		w.wake()
		return ctx.Err()
	}
}

func (w *Writer) Snapshot() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Stats{Enqueued: w.enqueued.Load(), Written: w.written.Load(), Dropped: w.dropped.Load(), Oversized: w.oversized.Load(), Errors: w.failures.Load(),
		PendingRecords: w.pending, PendingBytes: w.bytes, Closed: w.closed, Failed: w.failed, Incomplete: w.incomplete || w.dropped.Load() != 0}
}
