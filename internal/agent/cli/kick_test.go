package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
)

type kickFixture struct {
	opened, writes, closed int
	cancel                 context.CancelFunc
}

func (f *kickFixture) OpenKick(context.Context, control.Target) (control.KickSession, error) {
	f.opened++
	return f, nil
}
func (*kickFixture) Inventory(context.Context) (control.Inventory, error) {
	return control.Inventory{Complete: true, Queues: []control.Queue{{Slot: 8, Generation: 1}}}, nil
}
func (f *kickFixture) Close() error { f.closed++; return nil }
func (f *kickFixture) Kick(context.Context, control.Queue) control.Result {
	f.writes++
	if f.cancel != nil {
		f.cancel()
	}
	return control.Result{Outcome: control.Accepted, Decision: control.Pending}
}
func TestKickCancellationDeliversReceiptAndStopsNextTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &kickFixture{cancel: cancel}
	var data bytes.Buffer
	out := newOutput(&data)
	defer out.Close()
	err := kickTargets(ctx, source, []control.Target{{PID: 42, StartTime: 1}, {PID: 43, StartTime: 1}}, out)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errDelivery) {
		t.Fatal(err)
	}
	if source.opened != 1 || source.writes != 1 || source.closed != 1 {
		t.Fatalf("effects: %+v", source)
	}
	var receipt kickRecord
	if err := json.Unmarshal(bytes.TrimSpace(data.Bytes()), &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Queues) != 1 || receipt.Queues[0].Outcome != "accepted" {
		t.Fatalf("receipt lost: %+v", receipt)
	}
}

type rejectReceipt struct{}

func (rejectReceipt) Write([]byte) (int, error) { return 0, errors.New("destination failed") }
func TestKickDeliveryFailureStopsBeforeNextTarget(t *testing.T) {
	source := &kickFixture{}
	out := newOutput(rejectReceipt{})
	defer out.Close()
	err := kickTargets(context.Background(), source, []control.Target{{PID: 42, StartTime: 1}, {PID: 43, StartTime: 1}}, out)
	if !errors.Is(err, errDelivery) || source.opened != 1 || source.writes != 1 || source.closed != 1 {
		t.Fatalf("err=%v effects=%+v", err, source)
	}
}
