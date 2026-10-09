package telemetry

import (
	"encoding/json"
	"io"
	"time"
)

// These are owned presentation values, private to the pipeline. Queue entries
// contain at most 16 samples and bounded strings; no borrowed component values
// or open-ended payloads reach the writer.
type sample struct {
	At              time.Duration `json:"at_ns"`
	Avail           uint16        `json:"avail"`
	Used            uint16        `json:"used"`
	Consumed        uint16        `json:"consumed"`
	ConsumedFresh   bool          `json:"consumed_fresh"`
	WorkQueued      bool          `json:"work_queued"`
	WorkQueuedFresh bool          `json:"work_queued_fresh"`
	Valid           bool          `json:"valid"`
	Source          string        `json:"source"`
}
type totals struct {
	Accepted    uint64 `json:"accepted"`
	Refused     uint64 `json:"refused"`
	Failed      uint64 `json:"failed"`
	Cancelled   uint64 `json:"cancelled"`
	Unavailable uint64 `json:"unavailable"`
}
type record struct {
	Event            string        `json:"event"`
	Stream           uint64        `json:"stream,omitempty"`
	Sequence         uint64        `json:"sequence,omitempty"`
	PID              int           `json:"pid,omitempty"`
	StartTime        uint64        `json:"start_time,omitempty"`
	Name             string        `json:"name,omitempty"`
	Slot             int           `json:"slot"`
	Generation       uint64        `json:"generation,omitempty"`
	Episode          uint64        `json:"episode,omitempty"`
	Attempt          uint64        `json:"attempt,omitempty"`
	At               time.Duration `json:"at_ns"`
	StartedAt        time.Duration `json:"started_at_ns,omitempty"`
	ObservedAt       time.Duration `json:"observed_at_ns,omitempty"`
	AcceptedAt       time.Duration `json:"accepted_at_ns,omitempty"`
	Latency          time.Duration `json:"latency_ns,omitempty"`
	Outcome          string        `json:"outcome,omitempty"`
	Reason           string        `json:"reason,omitempty"`
	Message          string        `json:"message,omitempty"`
	Samples          []sample      `json:"samples,omitempty"`
	Actions          totals        `json:"actions"`
	UsedProgress     bool          `json:"used_progress,omitempty"`
	ConsumedProgress bool          `json:"consumed_progress,omitempty"`
	Incomplete       bool          `json:"evidence_incomplete"`
	LostInputs       uint64        `json:"gap_inputs,omitempty"`
}
type envelope struct {
	Version    int    `json:"schema_version"`
	InputLost  uint64 `json:"input_lost"`
	OutputLost uint64 `json:"output_lost"`
	Incomplete bool   `json:"delivery_incomplete"`
	Record     record `json:"record"`
}

func (t *Telemetry) write(out io.Writer) {
	defer close(t.done)
	for value := range t.output {
		if t.failed.Load() || t.abort.Load() {
			t.outputLost.Add(1)
			continue
		}
		lost := t.outputLost.Load()
		data, err := json.Marshal(envelope{Version: 1, InputLost: t.inputLost.Load(), OutputLost: lost, Incomplete: lost != 0, Record: value})
		if err == nil && len(data)+1 > MaxRecordBytes {
			err = io.ErrShortBuffer
		}
		if err == nil {
			data = append(data, '\n')
			var n int
			n, err = out.Write(data)
			if err == nil && n != len(data) {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			t.failed.Store(true)
			t.outputLost.Add(1)
		}
	}
}
