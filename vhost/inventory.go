package vhost

import (
	"slices"

	"github.com/yckao/virtio-net-recovery/vhost/internal/kernel"
)

type inventoryEntry struct {
	slot int
	err  error
}

// Existing slots get the first claim on bounded observation capacity. Directory
// problems do not prove removal, and partial scans cannot retire absent slots.
func planInventory(known []int, inv kernel.Inventory) (entries []inventoryEntry, retired []int) {
	found := make(map[int]bool, len(inv.Vhosts))
	problems := make(map[int]error, len(inv.Problems))
	for _, slot := range inv.Vhosts {
		found[slot] = true
	}
	for _, problem := range inv.Problems {
		problems[problem.Slot] = problem.Err
	}
	slices.Sort(known)
	existing := make(map[int]bool, len(known))
	for _, slot := range known {
		existing[slot] = true
		if found[slot] {
			entries = append(entries, inventoryEntry{slot: slot})
		} else if err, ok := problems[slot]; ok {
			entries = append(entries, inventoryEntry{slot: slot, err: err})
		} else if inv.Complete && !inv.Truncated {
			retired = append(retired, slot)
		}
	}
	for _, slot := range inv.Vhosts {
		if !existing[slot] {
			entries = append(entries, inventoryEntry{slot: slot})
		}
	}
	for _, problem := range inv.Problems {
		if !existing[problem.Slot] {
			entries = append(entries, inventoryEntry{slot: problem.Slot, err: problem.Err})
		}
	}
	return entries, retired
}
