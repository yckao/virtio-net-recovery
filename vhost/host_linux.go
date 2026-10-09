//go:build linux && amd64

package vhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

// Host owns the probe and bounded process/queue leases. Close drains sessions
// before freeing BPF resources; it is idempotent and safe with concurrent calls.
type Host struct {
	mu         sync.Mutex
	options    Options
	probe      *kernel.Probe
	sessions   map[*processSession]struct{}
	queueCount int
	closed     bool
	done       chan struct{}
}

func Open(o Options) (*Host, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	p, err := kernel.OpenProbe(o.BPFObject, o.Trace, o.MaxQueues)
	if err != nil {
		return nil, fmt.Errorf("open BPF observation: %w", err)
	}
	return &Host{options: o, probe: p, sessions: map[*processSession]struct{}{}, done: make(chan struct{})}, nil
}
func (h *Host) OpenProcess(ctx context.Context, id ProcessIdentity) (ReadSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(h.sessions) >= h.options.MaxQueues {
		return nil, ErrQueueLimit
	}
	p, err := kernel.OpenProcess(id.PID, id.StartTime)
	if err != nil {
		return nil, mapError(err)
	}
	owner := &queueOwner{}
	if _, err = rand.Read(owner.token[:]); err != nil {
		_ = p.Close()
		return nil, err
	}
	s := &processSession{host: h, process: p, id: id, owner: owner, gate: make(chan struct{}, 1), queues: map[int]*queueState{}}
	h.sessions[s] = struct{}{}
	return s, nil
}
func (h *Host) session(r ReadSession) (*processSession, error) {
	s, ok := r.(*processSession)
	if !ok || s.host != h {
		return nil, ErrIdentityChanged
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	return s, nil
}
func (h *Host) Conditional(r ReadSession) (ConditionalNotifier, error) {
	s, err := h.session(r)
	if err != nil {
		return nil, err
	}
	return conditional{s}, nil
}
func (h *Host) Manual(r ReadSession) (ManualNotifier, error) {
	s, err := h.session(r)
	if err != nil {
		return nil, err
	}
	return manual{s}, nil
}
func (h *Host) Trace(r ReadSession) (TraceSession, error) {
	if !h.options.Trace {
		return nil, ErrUnsupported
	}
	s, err := h.session(r)
	if err != nil {
		return nil, err
	}
	return &traceSession{session: s}, nil
}
func (h *Host) reserveQueue() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.queueCount >= h.options.MaxQueues {
		return false
	}
	h.queueCount++
	return true
}
func (h *Host) releaseQueue() { h.mu.Lock(); h.queueCount--; h.mu.Unlock() }
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		done := h.done
		h.mu.Unlock()
		<-done
		return nil
	}
	h.closed = true
	sessions := make([]*processSession, 0, len(h.sessions))
	for s := range h.sessions {
		sessions = append(sessions, s)
	}
	h.mu.Unlock()
	var result error
	for _, s := range sessions {
		result = errors.Join(result, s.Close())
	}
	h.probe.Close()
	close(h.done)
	return result
}
