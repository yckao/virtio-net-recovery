package jsonlog

import (
	"encoding/json"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
)

// Every admission reserves the longest delivery metadata plus a newline. The
// worker fills the cumulative loss count only when the record reaches Write.
const deliveryOverhead = len(`{"schema_version":1,"delivery_incomplete":false,"output_lost":18446744073709551615,"record":}`) + 1

type deliveryDTO struct {
	Version    int             `json:"schema_version"`
	Incomplete bool            `json:"delivery_incomplete"`
	Lost       uint64          `json:"output_lost"`
	Record     json.RawMessage `json:"record"`
}

type sampleDTO struct {
	AtNS            int64  `json:"at_ns"`
	Avail           uint16 `json:"avail"`
	Used            uint16 `json:"used"`
	Consumed        uint16 `json:"consumed"`
	ConsumedFresh   bool   `json:"consumed_fresh"`
	WorkQueued      bool   `json:"work_queued"`
	WorkQueuedFresh bool   `json:"work_queued_fresh"`
	Valid           bool   `json:"valid"`
	Source          string `json:"source"`
}
type actionsDTO struct {
	Accepted    uint64 `json:"accepted"`
	Refused     uint64 `json:"refused"`
	Failed      uint64 `json:"failed"`
	Cancelled   uint64 `json:"cancelled"`
	Unavailable uint64 `json:"unavailable"`
}
type stagesDTO struct {
	Signals  uint64 `json:"signals"`
	Writes   uint64 `json:"writes"`
	Wakeups  uint64 `json:"wakeups"`
	Handlers uint64 `json:"handlers"`
}
type recordDTO struct {
	Version          int         `json:"schema_version"`
	Event            string      `json:"event"`
	Stream           uint64      `json:"stream"`
	Sequence         uint64      `json:"sequence"`
	Episode          uint64      `json:"episode"`
	Attempt          uint64      `json:"attempt"`
	PID              int         `json:"pid"`
	Name             string      `json:"name,omitempty"`
	StartTime        uint64      `json:"start_time"`
	Slot             int         `json:"slot"`
	Generation       uint64      `json:"generation"`
	AtNS             int64       `json:"at_ns"`
	StartedAtNS      int64       `json:"started_at_ns"`
	LatencyNS        int64       `json:"latency_ns"`
	Samples          []sampleDTO `json:"samples,omitempty"`
	Actions          actionsDTO  `json:"actions"`
	Stages           stagesDTO   `json:"stages"`
	Outcome          string      `json:"outcome,omitempty"`
	Reason           string      `json:"reason,omitempty"`
	Message          string      `json:"message,omitempty"`
	UsedProgress     bool        `json:"used_progress"`
	ConsumedProgress bool        `json:"consumed_progress"`
	Incomplete       bool        `json:"incomplete"`
	LostInputs       uint64      `json:"lost_inputs"`
}

func validRecord(r report.Record) bool {
	if len(r.Kind) > 64 || len(r.Name) > report.MaxNameBytes || len(r.Message) > report.MaxMessageBytes || len(r.Outcome) > 64 || len(r.Reason) > 64 || len(r.Samples) > report.MaxSamples {
		return false
	}
	for _, s := range r.Samples {
		if len(s.Source) > 32 {
			return false
		}
	}
	return true
}

func toWire(r report.Record) recordDTO {
	d := recordDTO{Version: 1, Event: string(r.Kind), Stream: r.Stream, Sequence: r.Sequence, Episode: r.Episode, Attempt: r.Attempt,
		PID: r.PID, Name: r.Name, StartTime: r.StartTime, Slot: r.Slot, Generation: r.Generation,
		AtNS: int64(r.At), StartedAtNS: int64(r.StartedAt), LatencyNS: int64(r.Latency), Outcome: r.Outcome, Reason: r.Reason, Message: r.Message,
		UsedProgress: r.UsedProgress, ConsumedProgress: r.ConsumedProgress, Incomplete: r.Incomplete, LostInputs: r.LostInputs,
		Actions: actionsDTO{r.Actions.Accepted, r.Actions.Refused, r.Actions.Failed, r.Actions.Cancelled, r.Actions.Unavailable},
		Stages:  stagesDTO{r.Stages.Signals, r.Stages.Writes, r.Stages.Wakeups, r.Stages.Handlers}}
	if len(r.Samples) > 0 {
		d.Samples = make([]sampleDTO, len(r.Samples))
	}
	for i, s := range r.Samples {
		d.Samples[i] = sampleDTO{int64(s.At), s.Avail, s.Used, s.Consumed, s.ConsumedFresh, s.WorkQueued, s.WorkQueuedFresh, s.Valid, s.Source}
	}
	return d
}
