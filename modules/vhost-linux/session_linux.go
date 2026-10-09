//go:build linux && amd64

package vhost

import (
	"context"
	"slices"
	"time"

	"github.com/yckao/virtio-net-recovery/modules/vhost-linux/internal/kernel"
)

type queueState struct {
	handle   Queue
	snapshot kernel.Snapshot
}
type processSession struct {
	host         *Host
	process      *kernel.Process
	id           ProcessIdentity
	owner        *queueOwner
	gate         chan struct{}
	closed       bool
	generation   uint64
	queues       map[int]*queueState
	batches      []*kernel.Batch
	batchHandles []Queue
}

func (s *processSession) Identity() ProcessIdentity { return s.id }
func (s *processSession) enter(ctx context.Context) (func(), error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.closed {
		<-s.gate
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		<-s.gate
		return nil, err
	}
	return func() { <-s.gate }, nil
}
func (s *processSession) state(q Queue) (*queueState, error) {
	state := s.queues[q.slot]
	if q.owner != s.owner || q.owner == nil || state == nil || state.handle != q {
		return nil, ErrIdentityChanged
	}
	return state, nil
}
func (s *processSession) forget(slot int) {
	if q := s.queues[slot]; q != nil {
		s.host.probe.Forget(q.snapshot)
		delete(s.queues, slot)
		s.host.releaseQueue()
	}
	s.batches = nil
	s.batchHandles = nil
}
func (s *processSession) Close() error {
	s.gate <- struct{}{}
	defer func() { <-s.gate }()
	if s.closed {
		return nil
	}
	s.closed = true
	for slot := range s.queues {
		s.forget(slot)
	}
	err := s.process.Close()
	s.host.mu.Lock()
	delete(s.host.sessions, s)
	s.host.mu.Unlock()
	return err
}
func (s *processSession) Inventory(ctx context.Context) (Inventory, error) {
	done, err := s.enter(ctx)
	if err != nil {
		return Inventory{}, err
	}
	defer done()
	if err = s.process.Check(ctx); err != nil {
		return Inventory{}, mapError(err)
	}
	inv, err := s.process.Inventory(ctx)
	if err != nil {
		return Inventory{}, err
	}
	out := Inventory{Complete: inv.Complete}
	for _, p := range inv.Problems {
		if len(out.Problems) >= s.host.options.MaxQueues {
			out.Complete = false
			break
		}
		out.Problems = append(out.Problems, QueueProblem{p.Slot, p.Err})
	}
	seen := map[int]bool{}
	for _, slot := range inv.Vhosts {
		if len(out.Queues)+len(out.Problems) >= s.host.options.MaxQueues {
			out.Problems = append(out.Problems, QueueProblem{-1, ErrQueueLimit})
			out.Complete = false
			break
		}
		seen[slot] = true
		snap, err := s.freshSnapshot(ctx, slot)
		if err != nil {
			out.Problems = append(out.Problems, QueueProblem{slot, mapError(err)})
			continue
		}
		old := s.queues[slot]
		if old != nil && old.snapshot.Attachment() != snap.Attachment() {
			s.forget(slot)
			old = nil
		}
		if old == nil {
			if !s.host.reserveQueue() {
				out.Problems = append(out.Problems, QueueProblem{slot, ErrQueueLimit})
				continue
			}
			if err = s.host.probe.Track(ctx, snap); err != nil {
				s.host.releaseQueue()
				out.Problems = append(out.Problems, QueueProblem{slot, err})
				continue
			}
			s.generation++
			old = &queueState{handle: Queue{owner: s.owner, generation: s.generation, slot: slot}, snapshot: snap}
			s.queues[slot] = old
			s.batches = nil
		} else {
			old.snapshot = snap
		}
		out.Queues = append(out.Queues, old.handle)
	}
	if out.Complete {
		for slot := range s.queues {
			if !seen[slot] {
				s.forget(slot)
			}
		}
	}
	if err = s.process.Check(ctx); err != nil {
		return Inventory{}, mapError(err)
	}
	return out, nil
}
func (s *processSession) freshSnapshot(ctx context.Context, slot int) (kernel.Snapshot, error) {
	fd, err := s.process.Duplicate(slot)
	if err != nil {
		return kernel.Snapshot{}, err
	}
	defer kernel.CloseFD(fd)
	return s.host.probe.Snapshot(ctx, fd)
}
func (s *processSession) Sample(ctx context.Context, queues []Queue) ([]Observation, error) {
	done, err := s.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if len(queues) == 0 {
		return nil, nil
	}
	if len(queues) > s.host.options.MaxQueues {
		return nil, ErrQueueLimit
	}
	states := make([]*queueState, len(queues))
	for i, q := range queues {
		states[i], err = s.state(q)
		if err != nil {
			return nil, err
		}
	}
	if s.batches == nil || !slices.Equal(s.batchHandles, queues) {
		s.batches = nil
		s.batchHandles = nil
		for start := 0; start < len(states); start += 512 {
			end := min(start+512, len(states))
			snapshots := make([]kernel.Snapshot, end-start)
			for i, state := range states[start:end] {
				snapshots[i] = state.snapshot
			}
			batch, err := s.process.NewBatch(snapshots)
			if err != nil {
				s.batches = nil
				return nil, err
			}
			s.batches = append(s.batches, batch)
		}
		s.batchHandles = slices.Clone(queues)
	}
	// Return no observations if any private syscall batch fails.
	out := make([]Observation, 0, len(queues))
	index := 0
	for _, batch := range s.batches {
		if err = batch.Read(ctx); err != nil {
			s.batches = nil
			s.batchHandles = nil
			return nil, mapError(err)
		}
		at := time.Now()
		count := min(512, len(states)-index)
		for i := 0; i < count; i++ {
			a, u := batch.Indices(i)
			state := states[index+i]
			out = append(out, observation(state.handle, state.snapshot, a, u, at, false))
		}
		index += count
	}
	return out, nil
}

func (s *processSession) Inspect(ctx context.Context, q Queue) (Observation, error) {
	done, err := s.enter(ctx)
	if err != nil {
		return Observation{}, err
	}
	defer done()
	_, obs, err := s.inspect(ctx, q)
	return obs, err
}
func (s *processSession) inspect(ctx context.Context, q Queue) (kernel.Snapshot, Observation, error) {
	state, err := s.state(q)
	if err != nil {
		return kernel.Snapshot{}, Observation{}, err
	}
	if err = s.process.Check(ctx); err != nil {
		return kernel.Snapshot{}, Observation{}, mapError(err)
	}
	snap, err := s.freshSnapshot(ctx, q.slot)
	if err != nil {
		return snap, Observation{}, err
	}
	if snap.Attachment() != state.snapshot.Attachment() {
		return snap, Observation{}, ErrIdentityChanged
	}
	a, u, err := s.process.Ring(ctx, snap)
	if err != nil {
		return snap, Observation{}, mapError(err)
	}
	return snap, observation(q, snap, a, u, time.Now(), true), nil
}
