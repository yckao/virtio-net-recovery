//go:build linux && amd64

package vhost

import (
	"context"
	"errors"
	"testing"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

func inventorySession() *processSession {
	owner := &queueOwner{}
	return &processSession{
		host:  &Host{options: Options{MaxQueues: 1}, queueCount: 1, probe: &kernel.Probe{}},
		owner: owner, generation: 1,
		queues: map[int]*queueState{100: {handle: Queue{owner: owner, slot: 100, generation: 1}, snapshot: kernel.Snapshot{VQ: 100}}},
	}
}

func inventorySnapshot(_ context.Context, slot int) (kernel.Snapshot, error) {
	return kernel.Snapshot{VQ: uint64(slot)}, nil
}

func TestInventoryCapacityCannotStarveExistingQueue(t *testing.T) {
	s := inventorySession()
	old := s.queues[100].handle
	for range 3 {
		out := s.refreshInventory(context.Background(), kernel.Inventory{Vhosts: []int{50, 100}, Complete: true}, inventorySnapshot)
		if len(out.Queues) != 1 || out.Queues[0] != old || len(out.Problems) != 0 || out.Complete || !out.Truncated {
			t.Fatalf("existing queue displaced: %+v", out)
		}
		if s.host.queueCount != 1 {
			t.Fatalf("reservation count: %d", s.host.queueCount)
		}
	}
}

func TestInventoryReleasesAbsentQueueBeforeAdmission(t *testing.T) {
	s := inventorySession()
	old := s.queues[100].handle
	out := s.refreshInventory(context.Background(), kernel.Inventory{Vhosts: []int{50}, Complete: true}, inventorySnapshot)
	if len(out.Queues) != 1 || out.Queues[0].Slot() != 50 || !out.Complete || out.Truncated || len(out.Problems) != 0 || s.host.queueCount != 1 {
		t.Fatalf("replacement could not claim released capacity: %+v", out)
	}
	if _, err := s.state(old); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("retired handle is still usable: %v", err)
	}
}

func TestInventoryPartialScanRetainsAbsentQueue(t *testing.T) {
	s := inventorySession()
	old := s.queues[100].handle
	out := s.refreshInventory(context.Background(), kernel.Inventory{Vhosts: []int{50}}, inventorySnapshot)
	if out.Complete || !out.Truncated || len(out.Queues) != 0 || len(out.Problems) != 1 || out.Problems[0].Slot != 50 || !errors.Is(out.Problems[0].Err, ErrQueueLimit) {
		t.Fatalf("unexpected partial result: %+v", out)
	}
	if _, err := s.state(old); err != nil {
		t.Fatalf("partial absence retired a handle: %v", err)
	}
}

func TestInventoryProblemsDoNotDisplaceKnownQueues(t *testing.T) {
	s := inventorySession()
	out := s.refreshInventory(context.Background(), kernel.Inventory{
		Vhosts: []int{100}, Problems: []kernel.InventoryProblem{{Slot: 10, Err: errors.New("readlink")}},
	}, inventorySnapshot)
	if len(out.Queues) != 1 || out.Queues[0].Slot() != 100 || len(out.Problems) != 0 || out.Complete || !out.Truncated {
		t.Fatalf("unrelated problem displaced known queue: %+v", out)
	}
}
