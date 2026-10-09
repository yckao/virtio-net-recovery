package backend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
)

// These tests exercise only the pure translation seam. The zero queue value is
// deliberately not a usable backend capability; no handle is reconstructed and
// no test opens a Host, process, BPF object or notification descriptor.
func liveObservation() vhost.Observation {
	return vhost.Observation{At: time.Unix(100, 0), Source: vhost.SourceLive,
		Live: true, Num: 256, Avail: 20, Used: 10, Consumed: 11,
		Outstanding: 10, Pending: 9}
}

func TestAcceptedReceiptSurvivesPostWriteFailureAndOwnsItsProjection(t *testing.T) {
	live := liveObservation()
	wrote := live.At.Add(time.Millisecond)
	completed := wrote.Add(time.Millisecond)
	in := vhost.Result{Decision: vhost.Eligible, Write: vhost.WriteAccepted,
		CompletedAt: completed, Observation: &live, Err: context.Canceled,
		Receipt: &vhost.WriteReceipt{AcceptedAt: wrote, ObservationAt: live.At,
			HasBaseline: true, Used: live.Used, Consumed: live.Consumed}}
	got := result(in)
	if got.Outcome != control.Accepted || got.Decision != control.Pending ||
		got.AcceptedAt != wrote || got.CompletedAt != completed || !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("accepted write was relabeled or lost timestamps: %+v", got)
	}
	if got.Live == nil || !got.Live.Live || got.Live.At != live.At || got.Live.Used != 10 || got.Live.Consumed != 11 {
		t.Fatalf("accepted baseline lost: %+v", got.Live)
	}
	live.Used, in.Receipt.AcceptedAt = 100, time.Time{}
	if got.Live.Used != 10 || got.AcceptedAt != wrote {
		t.Fatal("source mutation changed translated receipt")
	}
}

func TestRefusedProgressRemainsAvailableForEarlierVerification(t *testing.T) {
	live := liveObservation()
	live.Used, live.Consumed = 12, 13
	completed := live.At.Add(time.Millisecond)
	got := result(vhost.Result{Decision: vhost.UsedAdvanced, Write: vhost.WriteNotAttempted,
		CompletedAt: completed, Observation: &live})
	if got.Outcome != control.Refused || got.Decision != control.UsedProgress ||
		got.Live == nil || !got.Live.Live || got.Live.Used != 12 || got.Live.Consumed != 13 ||
		got.CompletedAt != completed || !got.AcceptedAt.IsZero() {
		t.Fatalf("physical refusal discarded valid progress or invented a write: %+v", got)
	}
}

func TestFailureCategoriesDoNotInventAcceptedWrites(t *testing.T) {
	completed := time.Unix(100, 0)
	for _, tc := range []struct {
		name     string
		physical vhost.PhysicalDecision
		write    vhost.WriteOutcome
		outcome  control.Outcome
		decision control.Decision
	}{
		{"cancelled", vhost.Cancelled, vhost.WriteNotAttempted, control.Aborted, control.Cancelled},
		{"changed", vhost.IdentityChanged, vhost.WriteNotAttempted, control.ReadFailed, control.Changed},
		{"unavailable", vhost.Unavailable, vhost.WriteNotAttempted, control.ReadFailed, control.Unavailable},
		{"unsupported", vhost.Unsupported, vhost.WriteNotAttempted, control.ReadFailed, control.Unavailable},
		{"contended", vhost.Contended, vhost.WriteNotAttempted, control.Refused, control.Busy},
		{"write failed", vhost.Eligible, vhost.WriteFailed, control.WriteFailed, control.Pending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := result(vhost.Result{Decision: tc.physical, Write: tc.write, CompletedAt: completed})
			if got.Outcome != tc.outcome || got.Decision != tc.decision || got.CompletedAt != completed ||
				got.Live != nil || !got.AcceptedAt.IsZero() {
				t.Fatalf("wrong result category: %+v", got)
			}
		})
	}
}

func TestIdentityChangeDropsAnyStaleObservation(t *testing.T) {
	live := liveObservation()
	got := result(vhost.Result{Decision: vhost.IdentityChanged, Write: vhost.WriteNotAttempted,
		Observation: &live, CompletedAt: live.At, Err: vhost.ErrIdentityChanged})
	if got.Outcome != control.ReadFailed || got.Decision != control.Changed || got.Live != nil || !errors.Is(got.Err, vhost.ErrIdentityChanged) {
		t.Fatalf("identity refusal retained stale live evidence: %+v", got)
	}
	before := time.Now()
	got = missing()
	if got.Outcome != control.ReadFailed || got.Decision != control.Changed || got.Live != nil ||
		got.CompletedAt.Before(before) || got.CompletedAt.After(time.Now()) || !errors.Is(got.Err, vhost.ErrIdentityChanged) {
		t.Fatalf("missing adapter handle has no correlated failure timestamp: %+v", got)
	}
}

func TestInspectionErrorsPreserveCancellationAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		err      error
		outcome  control.Outcome
		decision control.Decision
	}{
		{fmt.Errorf("inspect: %w", context.Canceled), control.Aborted, control.Cancelled},
		{fmt.Errorf("inspect: %w", context.DeadlineExceeded), control.Aborted, control.Cancelled},
		{fmt.Errorf("inspect: %w", vhost.ErrIdentityChanged), control.ReadFailed, control.Changed},
		{errors.New("read unavailable"), control.ReadFailed, control.Unavailable},
	} {
		if outcome, classified := failureOutcome(tc.err), failureDecision(tc.err); outcome != tc.outcome || classified != tc.decision {
			t.Fatalf("inspection error %v became %v/%v", tc.err, outcome, classified)
		}
	}
}

func TestInspectionPhysicalClassificationReachesApplicationMeaning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*vhost.Observation)
		want   control.Decision
	}{
		{"pending", func(*vhost.Observation) {}, control.Pending},
		{"invalid geometry", func(o *vhost.Observation) { o.Num = 255 }, control.Invalid},
		{"used progress", func(o *vhost.Observation) { o.Used++ }, control.UsedProgress},
		{"drained", func(o *vhost.Observation) { o.Outstanding = 0 }, control.Drained},
		{"work queued", func(o *vhost.Observation) { o.WorkQueued = true }, control.Queued},
		{"already consumed", func(o *vhost.Observation) { o.Pending = 0 }, control.Consumed},
		{"cached is not live", func(o *vhost.Observation) { o.Live = false; o.Source = vhost.SourceCached }, control.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := liveObservation()
			tc.change(&live)
			if got := decision(vhost.Classify(live, 10)); got != tc.want {
				t.Fatalf("wrong application meaning: got %v want %v", got, tc.want)
			}
		})
	}
}
