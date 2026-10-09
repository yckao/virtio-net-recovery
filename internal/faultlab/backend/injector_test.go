package backend_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/backend"
	"github.com/yckao/virtio-net-recovery/internal/faultlab/experiment"
	vhost "github.com/yckao/virtio-net-recovery/vhost"
)

type notifier struct {
	result vhost.Result
	calls  int
}

func (n *notifier) Notify(context.Context, vhost.Queue) vhost.Result {
	n.calls++
	return n.result
}

func TestRestorationPreservesActualAcceptedOutcome(t *testing.T) {
	at := time.Unix(1, 0)
	postWrite := errors.New("post-write cleanup failed")
	n := &notifier{result: vhost.Result{Write: vhost.WriteAccepted, CompletedAt: at, Err: postWrite}}
	r := (backend.Restorer{Notifier: n}).Restore(context.Background())
	if r.Outcome != experiment.RestoreAccepted || !errors.Is(r.Err, postWrite) || r.CompletedAt != at || n.calls != 1 {
		t.Fatal("accepted write was reclassified, repeated, or its later error lost")
	}
}

func TestRestorationDistinguishesRefusalAndWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		in  vhost.WriteOutcome
		out experiment.RestoreOutcome
	}{{vhost.WriteNotAttempted, experiment.RestoreRefused}, {vhost.WriteFailed, experiment.RestoreFailed}} {
		n := &notifier{result: vhost.Result{Write: tc.in, Err: errors.New("failed")}}
		r := (backend.Restorer{Notifier: n}).Restore(context.Background())
		if r.Outcome != tc.out || r.Err == nil || n.calls != 1 {
			t.Fatal("wrong restoration classification")
		}
	}
}
