package vhost

import (
	"errors"
	"reflect"
	"testing"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

func TestInventoryPlanPreservesKnownBeforeNewSlotsAndProblems(t *testing.T) {
	failure := errors.New("readlink")
	entries, retired := planInventory([]int{100, 70}, kernel.Inventory{
		Vhosts: []int{50, 100}, Problems: []kernel.InventoryProblem{{Slot: 10, Err: failure}, {Slot: 70, Err: failure}},
	})
	want := []inventoryEntry{{70, failure}, {100, nil}, {50, nil}, {10, failure}}
	if !reflect.DeepEqual(entries, want) || len(retired) != 0 {
		t.Fatalf("entries %v, retired %v", entries, retired)
	}
}

func TestInventoryPlanRetiresOnlyConfirmedAbsence(t *testing.T) {
	for _, tc := range []struct {
		name                string
		complete, truncated bool
		retired             []int
	}{
		{"complete", true, false, []int{100}},
		{"partial", false, false, nil},
		{"capacity", false, true, nil},
		{"contradictory", true, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, retired := planInventory([]int{100}, kernel.Inventory{Vhosts: []int{50}, Complete: tc.complete, Truncated: tc.truncated})
			if !reflect.DeepEqual(retired, tc.retired) {
				t.Fatalf("retired %v, want %v", retired, tc.retired)
			}
		})
	}
}
