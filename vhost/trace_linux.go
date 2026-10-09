//go:build linux && amd64

package vhost

import (
	"context"
	"sync"
)

// Trace views share the process lease and host-owned optional probe set. They
// own no raw map keys and cannot notify. Closing a view does not close its reader.
type traceSession struct {
	mu      sync.Mutex
	session *processSession
	closed  bool
}

func (t *traceSession) Close() error { t.mu.Lock(); t.closed = true; t.mu.Unlock(); return nil }
func (t *traceSession) Sample(ctx context.Context, queues []Queue) ([]TraceObservation, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	done, err := t.session.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if len(queues) > t.session.host.options.MaxQueues {
		return nil, ErrQueueLimit
	}
	out := make([]TraceObservation, 0, len(queues))
	for _, q := range queues {
		snapshot, obs, err := t.session.inspect(ctx, q)
		if err != nil {
			return nil, err
		}
		c, err := t.session.host.probe.Counters(ctx, snapshot)
		if err != nil {
			return nil, err
		}
		out = append(out, TraceObservation{Observation: obs, Counters: Counters{Signals: c.Signals, Writes: c.Writes, Wakeups: c.Wakeups, Handlers: c.Handlers, LastSignalNS: c.LastSignalNS, LastWriteNS: c.LastWriteNS, LastWakeupNS: c.LastWakeupNS, LastHandlerNS: c.LastHandlerNS, Active: c.Active}})
	}
	return out, nil
}
