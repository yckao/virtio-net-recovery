package control

import "errors"

func validateInventory(inv Inventory) error {
	if len(inv.Queues)+len(inv.Unavailable) > 4096 {
		return errors.New("queue inventory exceeds supported capacity")
	}
	if inv.Complete && inv.Truncated {
		return errors.New("truncated inventory cannot be complete")
	}
	seen := make(map[int]bool, len(inv.Queues))
	for _, queue := range inv.Queues {
		if queue.Slot < 0 || queue.Generation == 0 || seen[queue.Slot] {
			return errors.New("inventory contains invalid or duplicate queue slots")
		}
		seen[queue.Slot] = true
	}
	for _, slot := range inv.Unavailable {
		if slot < 0 || seen[slot] {
			return errors.New("inventory contains invalid or duplicate unavailable slots")
		}
		seen[slot] = true
	}
	return nil
}
