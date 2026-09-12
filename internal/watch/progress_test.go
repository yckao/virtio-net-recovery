package watch

import "testing"

func TestRingCandidateRequiresObservedBacklogAndNoCompletions(t *testing.T) {
	p := &RingProgress{}
	if p.Observe(10, 10, 256, 0, .04) || p.Observe(11, 10, 256, 1, .04) {
		t.Fatal("idle time or a newly published buffer counted as a persistent backlog")
	}
	if p.Observe(30, 10, 256, 1.02, .04) || !p.Observe(50, 10, 256, 1.05, .04) {
		t.Fatal("uncompleted backlog did not become a candidate")
	}
	if p.Observe(50, 11, 256, 1.06, .04) || p.Observe(50, 11, 256, 1.08, .04) {
		t.Fatal("completion did not restart the candidate timer")
	}
	p.Reset(2)
	if p.Observe(50, 11, 256, 3, .04) {
		t.Fatal("a failed live confirmation did not restart observation")
	}
}

func TestRingCandidateWrapAndInvalidReads(t *testing.T) {
	p := &RingProgress{}
	if p.Observe(3, 65530, 256, 0, .04) || !p.Observe(3, 65530, 256, .05, .04) {
		t.Fatal("valid ring wrap was rejected")
	}
	if p.Observe(1000, 3, 256, .1, .04) || p.Observe(20, 3, 256, .2, .04) {
		t.Fatal("an inconsistent sample contributed to stall age")
	}
}
