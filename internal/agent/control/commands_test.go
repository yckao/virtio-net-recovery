package control_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

type commandSession struct {
	inventory              control.Inventory
	inventoryErr, closeErr error
	calls                  []control.Queue
	closed                 int
	results                []control.Result
	cancel                 context.CancelFunc
}

func (s *commandSession) Inventory(context.Context) (control.Inventory, error) {
	return s.inventory, s.inventoryErr
}
func (s *commandSession) Close() error { s.closed++; return s.closeErr }
func (s *commandSession) Kick(_ context.Context, q control.Queue) control.Result {
	s.calls = append(s.calls, q)
	result := s.results[len(s.calls)-1]
	if s.cancel != nil {
		s.cancel()
	}
	return result
}

type commandSource struct {
	session *commandSession
	err     error
}

func (s commandSource) OpenQueues(context.Context, control.Target) (control.QueueSession, error) {
	return s.session, s.err
}
func (s commandSource) OpenKick(context.Context, control.Target) (control.KickSession, error) {
	return s.session, s.err
}

func TestKickRetainsAcceptedReceiptOnCancellationAndCleanupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanup := errors.New("close failed")
	session := &commandSession{
		inventory: control.Inventory{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 1}, {Slot: 9, Generation: 1}}},
		results:   []control.Result{{Outcome: control.Accepted}}, cancel: cancel, closeErr: cleanup,
	}
	report := control.KickTarget(ctx, commandSource{session: session}, control.Target{PID: 42, StartTime: 1})
	if report.Complete() || !errors.Is(report.Err, context.Canceled) || !errors.Is(report.Err, cleanup) {
		t.Fatalf("report: %+v", report)
	}
	if session.closed != 1 || len(session.calls) != 1 || len(report.Batch.Receipts) != 1 || report.Batch.Receipts[0].Result.Outcome != control.Accepted {
		t.Fatalf("lost or repeated effect: session=%+v report=%+v", session, report)
	}
}
func TestKickContinuesAfterLocalRefusalWithoutRetry(t *testing.T) {
	session := &commandSession{
		inventory: control.Inventory{Complete: false, Unavailable: []int{7}, Queues: []control.Queue{{Slot: 8, Generation: 1}, {Slot: 9, Generation: 1}}},
		results:   []control.Result{{Outcome: control.Refused, Decision: control.Busy}, {Outcome: control.Accepted}},
	}
	report := control.KickTarget(context.Background(), commandSource{session: session}, control.Target{PID: 42, StartTime: 1})
	if report.Complete() || report.Err != nil || session.closed != 1 || len(session.calls) != 2 || len(report.Batch.Receipts) != 3 {
		t.Fatalf("session=%+v report=%+v", session, report)
	}
	if report.Batch.Receipts[0].Result.Outcome != control.ReadFailed || report.Batch.Receipts[2].Result.Outcome != control.Accepted {
		t.Fatal(report.Batch)
	}
}
func TestQueueListingClosesAfterInventoryFailure(t *testing.T) {
	readErr := errors.New("inventory unavailable")
	closeErr := errors.New("cleanup failed")
	session := &commandSession{inventoryErr: readErr, closeErr: closeErr}
	report := control.ListQueues(context.Background(), commandSource{session: session}, control.Target{PID: 42, StartTime: 1})
	if report.Complete() || session.closed != 1 || !errors.Is(report.Err, readErr) || !errors.Is(report.Err, closeErr) {
		t.Fatalf("%+v closes=%d", report, session.closed)
	}
}
func TestAcquisitionFailureHasNoEffects(t *testing.T) {
	failure := errors.New("permission denied")
	session := &commandSession{}
	report := control.KickTarget(context.Background(), commandSource{session: session, err: failure}, control.Target{})
	if report.Complete() || !errors.Is(report.Err, failure) || session.closed != 0 || len(session.calls) != 0 {
		t.Fatalf("%+v", report)
	}
}

func TestMalformedInventoryCannotRepeatAManualWrite(t *testing.T) {
	for _, inventory := range []control.Inventory{
		{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 1}, {Slot: 8, Generation: 1}}},
		{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 1}}, Unavailable: []int{8}},
		{Complete: true, Truncated: true, Queues: []control.Queue{{Slot: 8, Generation: 1}}},
	} {
		session := &commandSession{inventory: inventory}
		report := control.KickTarget(context.Background(), commandSource{session: session}, control.Target{PID: 42, StartTime: 1})
		if report.Err == nil || report.Complete() || len(session.calls) != 0 || session.closed != 1 {
			t.Fatalf("invalid inventory caused effects: %+v %+v", report, session)
		}
	}
}
func TestAcceptedWriteWithPostWriteErrorRemainsAcceptedButIncomplete(t *testing.T) {
	failure := errors.New("descriptor cleanup failed")
	session := &commandSession{
		inventory: control.Inventory{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 1}}},
		results:   []control.Result{{Outcome: control.Accepted, Err: failure}},
	}
	report := control.KickTarget(context.Background(), commandSource{session: session}, control.Target{PID: 42, StartTime: 1})
	receipt := report.Batch.Receipts[0].Result
	if report.Complete() || receipt.Outcome != control.Accepted || !errors.Is(receipt.Err, failure) {
		t.Fatalf("%+v", report)
	}
}
