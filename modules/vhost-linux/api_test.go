package vhost_test

import (
	"context"
	"testing"

	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
	"github.com/yckao/virtio-net-recovery/modules/vhost-linux/experimental/lostwakeup"
)

func TestPhysicalClassificationUsesFreshEvidence(t *testing.T) {
	good := vhost.Observation{Source: vhost.SourceLive, Live: true, Num: 256, Avail: 12, Used: 10, Consumed: 10, Outstanding: 2, Pending: 2}
	tests := []struct {
		name   string
		change func(*vhost.Observation)
		want   vhost.PhysicalDecision
	}{
		{"eligible", func(*vhost.Observation) {}, vhost.Eligible},
		{"cached", func(o *vhost.Observation) { o.Live = false; o.Source = vhost.SourceCached }, vhost.Unavailable},
		{"invalid", func(o *vhost.Observation) { o.Outstanding = 257 }, vhost.InvalidRing},
		{"progress", func(o *vhost.Observation) { o.Used = 11 }, vhost.UsedAdvanced},
		{"drained", func(o *vhost.Observation) { o.Outstanding = 0 }, vhost.Drained},
		{"queued", func(o *vhost.Observation) { o.WorkQueued = true }, vhost.WorkQueued},
		{"consumed", func(o *vhost.Observation) { o.Pending = 0 }, vhost.NoPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := good
			tt.change(&o)
			if got := vhost.Classify(o, 10); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}
func TestInvalidOptionsAreRejectedBeforePlatformAccess(t *testing.T) {
	for _, o := range []vhost.Options{{}, {BPFObject: "x", StateDir: "x", MaxQueues: 4097}} {
		if h, err := vhost.Open(o); err == nil || h != nil {
			t.Fatalf("accepted invalid options: %+v", o)
		}
	}
}
func TestExperimentalBindingRejectsMissingCapability(t *testing.T) {
	called := false
	_, err := lostwakeup.WithBinding(context.Background(), nil, vhost.Queue{}, func(lostwakeup.BorrowedBinding) lostwakeup.LoadResult { called = true; return lostwakeup.LoadResult{} })
	if err == nil || called {
		t.Fatal("missing capability invoked loader")
	}
}
func TestOpaqueZeroQueueIsOnlyDiagnostic(t *testing.T) {
	var q vhost.Queue
	if q.Generation() != 0 || q.String() != "queue:invalid" {
		t.Fatal("unexpected zero queue")
	}
}
