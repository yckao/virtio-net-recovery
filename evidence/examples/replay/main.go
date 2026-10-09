// Command replay demonstrates the public evidence API without another component.
package main

import (
	"fmt"
	"time"

	"github.com/yckao/virtio-net-recovery/evidence"
)

func main() {
	r, err := evidence.New(evidence.DefaultOptions(), 1)
	if err != nil {
		panic(err)
	}
	inputs := []evidence.Input{
		evidence.Observation{HasSample: true, CandidateKnown: true, Candidate: true, Sample: evidence.Sample{Avail: 10, Used: 4,
			Source: evidence.SourceCached, Valid: true}},
		evidence.ActionObserved{At: time.Millisecond, AttemptID: 1, Outcome: evidence.ActionAccepted},
		evidence.GapObserved{At: time.Second, Lost: 2},
		evidence.VerificationObserved{At: 2 * time.Second, AttemptID: 1, Outcome: evidence.VerificationProgress},
		evidence.Retired{At: 2 * time.Second},
	}
	for _, input := range inputs {
		records, err := r.Apply(input)
		if err != nil {
			panic(err)
		}
		for _, record := range records {
			fmt.Printf("kind=%d stream=%d episode=%d attempt=%d incomplete=%t lost=%d\n",
				record.Kind, record.StreamID, record.EpisodeID, record.AttemptID, record.Incomplete, record.LostInputs)
		}
	}
}
