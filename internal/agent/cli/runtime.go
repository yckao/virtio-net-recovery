package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/internal/agent/metrics"
	"github.com/yckao/virtio-net-recovery/internal/agent/telemetry"
)

// daemonRuntime owns optional consumers. All control producers have joined
// before close drains admitted observations and waits for output delivery.
type daemonRuntime struct {
	accounting *control.Accounting
	telemetry  *telemetry.Telemetry
	metrics    *metrics.Server
	serveDone  chan error
	stderr     io.Writer
}

func startRuntime(o daemonOptions, out, stderr io.Writer) (*daemonRuntime, error) {
	r := &daemonRuntime{accounting: &control.Accounting{}, stderr: stderr}
	if o.diagnostics {
		t, err := telemetry.Start(out, o.maxQueues)
		if err != nil {
			return nil, err
		}
		r.telemetry = t
	}
	if o.metrics != "" {
		server, err := metrics.Listen(o.metrics, r.snapshot)
		if err != nil {
			return nil, errors.Join(err, r.close())
		}
		r.metrics = server
		r.serveDone = make(chan error, 1)
		go r.serveMetrics()
	}
	return r, nil
}
func (r *daemonRuntime) serveMetrics() {
	err := r.metrics.Serve()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	r.serveDone <- err
	if err != nil {
		// Failure reporting is independent of the recovery workers and optional
		// diagnostics. Even a blocked stderr cannot stop their sampling or effects.
		fmt.Fprintln(r.stderr, "metrics exporter stopped; recovery continues:", err)
	}
}
func (r *daemonRuntime) close() error {
	// Each independent consumer gets its own drain deadline.
	return errors.Join(r.closeMetrics(), r.closeTelemetry())
}
func (r *daemonRuntime) closeMetrics() error {
	if r.metrics == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := r.metrics.Close(ctx)
	select {
	case err := <-r.serveDone:
		return errors.Join(result, err)
	case <-ctx.Done():
		return errors.Join(result, ctx.Err())
	}
}
func (r *daemonRuntime) closeTelemetry() error {
	if r.telemetry == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return r.telemetry.Close(ctx)
}
func (r *daemonRuntime) snapshot() metrics.Snapshot {
	c := r.accounting.Snapshot()
	var t telemetry.Stats
	if r.telemetry != nil {
		t = r.telemetry.Snapshot()
	}
	return metrics.Snapshot{
		Attempts: c.Attempts, Accepted: c.Accepted, Refused: c.Refused, ReadErrors: c.ReadErrors, WriteErrors: c.WriteErrors, Cancelled: c.Cancelled, Verified: c.Verified,
		Polls: c.Polls, PollErrors: c.PollErrors, DiscoveryErrors: c.DiscoveryErrors, AdmissionErrors: c.AdmissionErrors,
		SelectedTargets: c.SelectedTargets, ActiveTargets: c.ActiveTargets, UnavailableTargets: c.UnavailableTargets, DiscoveryProblems: c.DiscoveryProblems,
		InputLost: t.InputLost, OutputLost: t.OutputLost, EvidenceErrors: t.RecorderErrors,
		Sampled: c.Sampled, Unavailable: c.Unavailable, InventoryTruncated: c.InventoryTruncated, MaxPollGapNS: c.MaxPollGapNS,
	}
}
