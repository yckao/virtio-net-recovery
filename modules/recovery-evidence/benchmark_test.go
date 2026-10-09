package evidence_test

import (
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/modules/recovery-evidence"
)

func BenchmarkHealthySamples(b *testing.B) {
	r, _ := evidence.New(evidence.DefaultOptions(), 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_, _ = r.Apply(sample(time.Duration(i), uint16(i)))
	}
}

func BenchmarkCandidateActionChurn(b *testing.B) {
	opts := evidence.DefaultOptions()
	opts.Lifetime, opts.ReopenDelay = time.Nanosecond, 0
	r, _ := evidence.New(opts, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		at := time.Duration(i)
		_, _ = r.Apply(sample(at, uint16(i)))
		_, _ = r.Apply(evidence.CandidateStateObserved{At: at, Active: true})
		_, _ = r.Apply(evidence.ActionObserved{At: at, AttemptID: evidence.AttemptID(i + 1), Outcome: evidence.ActionAccepted})
	}
}

func BenchmarkStreamRetirement(b *testing.B) {
	b.ReportAllocs()
	for i := range b.N {
		r, _ := evidence.New(evidence.DefaultOptions(), evidence.StreamID(i+1))
		_, _ = r.Apply(sample(0, 0))
		_, _ = r.Apply(evidence.CandidateStateObserved{Active: true})
		_, _ = r.Apply(evidence.Retired{})
	}
}
