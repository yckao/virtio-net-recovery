package control

import (
	"context"
	"errors"
)

// QueueSession and KickSession keep resource ownership with the use case. A
// read-only command never acquires a mutation capability.
type QueueSession interface {
	Inventory(context.Context) (Inventory, error)
	Close() error
}
type KickSession interface {
	QueueSession
	ManualWriter
}
type QueueSource interface {
	OpenQueues(context.Context, Target) (QueueSession, error)
}
type KickSource interface {
	OpenKick(context.Context, Target) (KickSession, error)
}

type QueueListing struct {
	Target    Target
	Inventory Inventory
	Err       error
}

func (r QueueListing) Complete() bool {
	return r.Err == nil && r.Inventory.Complete && !r.Inventory.Truncated && len(r.Inventory.Unavailable) == 0
}

func ListQueues(ctx context.Context, source QueueSource, target Target) (result QueueListing) {
	result.Target = target
	session, err := source.OpenQueues(ctx, target)
	if err != nil {
		result.Err = err
		return result
	}
	defer func() { result.Err = errors.Join(result.Err, session.Close()) }()
	result.Inventory, result.Err = session.Inventory(ctx)
	if result.Err == nil {
		result.Err = validateInventory(result.Inventory)
	}
	return result
}

type KickReport struct {
	Target Target
	Batch  ManualBatch
	Err    error
}

func (r KickReport) Complete() bool { return r.Err == nil && r.Batch.Complete }

// KickTarget acquires one capability and attempts each inventoried queue once.
// Its receipts survive a later cleanup or presentation failure.
func KickTarget(ctx context.Context, source KickSource, target Target) (result KickReport) {
	result.Target = target
	session, err := source.OpenKick(ctx, target)
	if err != nil {
		result.Err = err
		return result
	}
	defer func() { result.Err = errors.Join(result.Err, session.Close()) }()
	result.Batch, result.Err = ExecuteManual(ctx, session, session)
	return result
}
