package control

import (
	"context"
	"errors"
)

type Receipt struct {
	Queue  Queue
	Result Result
}
type ManualBatch struct {
	Receipts []Receipt
	Complete bool
}

// ExecuteManual performs each selected slot at most once and retains receipts
// before presentation. Delivery failure cannot initiate another write.
func ExecuteManual(ctx context.Context, r QueueSession, w ManualWriter) (ManualBatch, error) {
	inv, err := r.Inventory(ctx)
	if err != nil {
		return ManualBatch{}, err
	}
	if err := validateInventory(inv); err != nil {
		return ManualBatch{}, err
	}
	batch := ManualBatch{Complete: inv.Complete && !inv.Truncated && len(inv.Unavailable) == 0}
	for _, slot := range inv.Unavailable {
		batch.Receipts = append(batch.Receipts, Receipt{Queue: Queue{Slot: slot}, Result: Result{Outcome: ReadFailed, Decision: Unavailable, Err: errors.New("queue unavailable")}})
	}
	for _, q := range inv.Queues {
		if err := ctx.Err(); err != nil {
			return batch, err
		}
		result := w.Kick(ctx, q)
		batch.Receipts = append(batch.Receipts, Receipt{q, result})
		if result.Outcome != Accepted || result.Err != nil {
			batch.Complete = false
		}
	}
	if len(batch.Receipts) == 0 {
		batch.Complete = false
		return batch, errors.New("no supported queues selected")
	}
	return batch, nil
}
