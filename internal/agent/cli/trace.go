package cli

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/backend"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	vhost "github.com/yckao/virtio-net-recovery/vhost"
)

type traceRecord struct {
	Schema     int          `json:"schema_version"`
	Kind       string       `json:"event"`
	Target     targetRecord `json:"target"`
	FD         int          `json:"fd"`
	Generation uint64       `json:"generation"`
	At         time.Time    `json:"at"`
	Avail      uint16       `json:"avail"`
	Used       uint16       `json:"used"`
	Consumed   uint16       `json:"consumed"`
	Signals    uint64       `json:"signals"`
	Writes     uint64       `json:"writes"`
	Wakeups    uint64       `json:"wakeups"`
	Handlers   uint64       `json:"handlers"`
}

func runTrace(ctx context.Context, args []string, stdout, stderr io.Writer) (result error) {
	o, err := parseTrace(args, stderr)
	if err != nil {
		return err
	}
	selector, err := o.selector()
	if err != nil {
		return err
	}
	inv, err := selector.Resolve(ctx)
	if err != nil {
		return err
	}
	if !inv.Complete || len(inv.Problems) > 0 || inv.ProblemsOmitted > 0 || len(inv.Targets) == 0 {
		return errors.New("trace requires a complete nonempty target inventory")
	}
	host, err := o.open(true)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, host.Close()) }()
	targets, err := openTraceTargets(ctx, host, inv.Targets)
	if err != nil {
		return err
	}
	out := newOutput(stdout)
	defer out.Close()
	return control.RunTrace(ctx, control.TraceOptions{Duration: o.duration, Cadence: o.cadence, MaxTargets: o.maxTargets, MaxQueues: o.maxQueues}, targets, control.RealClock{}, traceOutput{out})
}

// Construction owns partial resources until the complete acquired set is
// transferred to the read-only control workflow.
func openTraceTargets(ctx context.Context, host *vhost.Host, targets []control.Target) (acquired []control.TraceTarget, result error) {
	defer func() {
		if result != nil {
			for _, t := range acquired {
				result = errors.Join(result, t.Tracer.Close(), t.Reader.Close())
			}
			acquired = nil
		}
	}()
	for _, target := range targets {
		session, err := backend.Open(ctx, host, target)
		if err != nil {
			return acquired, err
		}
		reader := session.Reader()
		tracer, err := session.Trace()
		if err != nil {
			return acquired, errors.Join(err, reader.Close())
		}
		acquired = append(acquired, control.TraceTarget{Target: target, Reader: reader, Tracer: tracer})
	}
	return acquired, nil
}

type traceOutput struct{ output *commandOutput }

func (s traceOutput) Deliver(ctx context.Context, frame control.TraceFrame) error {
	records := make([]traceRecord, 0, len(frame.Samples))
	for _, sample := range frame.Samples {
		records = append(records, traceRecord{Schema: 1, Kind: "trace_sample", Target: targetDTO(frame.Target), FD: sample.Sample.Queue.Slot, Generation: sample.Sample.Queue.Generation, At: sample.Sample.At, Avail: sample.Sample.Avail, Used: sample.Sample.Used, Consumed: sample.Sample.Consumed, Signals: sample.Signals, Writes: sample.Writes, Wakeups: sample.Wakeups, Handlers: sample.Handlers})
	}
	return s.output.deliver(ctx, records)
}
