package evidence_test

import (
	"fmt"
	"time"

	"github.com/yckao/virtio-net-recovery/evidence"
)

func ExampleRecorder() {
	recorder, _ := evidence.New(evidence.DefaultOptions(), 42)
	opened, _ := recorder.Apply(evidence.Observation{HasSample: true, CandidateKnown: true, Candidate: true, Sample: evidence.Sample{
		Avail: 10, Used: 4, Source: evidence.SourceCached, Valid: true,
	}})
	fmt.Println("episode", opened[0].EpisodeID)
	_, _ = recorder.Apply(evidence.ActionObserved{At: time.Millisecond,
		AttemptID: 7, Outcome: evidence.ActionAccepted})
	_, _ = recorder.Apply(evidence.Observation{CandidateKnown: true, At: time.Second, Candidate: false})
	closed, _ := recorder.Apply(evidence.TimeAdvanced{At: 2 * time.Second})
	fmt.Println("accepted actions", closed[0].Actions.Accepted)
	// Output:
	// episode 1
	// accepted actions 1
}
