package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/diagnostics"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/jsonlog"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/metrics"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/selection"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/supervision"
	discovery "github.com/yckao/virtio-net-recovery/modules/qemu-discovery"
	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
)

// Run returns 0 for successful execution, 2 for incomplete execution and 3 when
// delivery failed. In particular, exit 3 never means a kick did not happen.
func Run(ctx context.Context, args []string, out, stderr io.Writer) (status int) {
	o, err := parse(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	selector, err := selection.New(discovery.Options{PIDs: o.pids, DomainPattern: o.domain, ProcRoot: o.proc, RuntimeDir: o.runtime, MaxTargets: o.maxTargets})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if o.command == "observe" || o.command == "recover" {
		return daemon(ctx, o, selector, out, stderr)
	}
	var host *vhost.Host
	if o.command != "list" {
		host, err = openHost(o)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		defer func() {
			if err := host.Close(); err != nil {
				fmt.Fprintln(stderr, "backend cleanup:", err)
				if status == 0 {
					status = 2
				}
			}
		}()
	}
	if o.command == "trace" {
		return trace(ctx, o, selector, host, out, stderr)
	}
	return oneShot(ctx, o, selector, host, out, stderr)
}
func openHost(o options) (*vhost.Host, error) {
	return vhost.Open(vhost.Options{BPFObject: o.bpf, Trace: o.command == "trace", StateDir: o.state, MaxQueues: o.maxQueues})
}
func daemon(ctx context.Context, o options, selector *selection.Selector, out, stderr io.Writer) int {
	accounting := &control.Accounting{}
	registry := &control.Registry{}
	output, _ := jsonlog.New(jsonlog.DefaultOptions(), out)
	reporting, cancelReporting := context.WithCancel(context.Background())
	defer cancelReporting()
	if err := output.Start(reporting); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer func() {
		deadline, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := output.Shutdown(deadline); err != nil {
			fmt.Fprintln(stderr, "diagnostic delivery incomplete:", err)
		}
	}()
	var reporter control.Reporter
	var diagnostic *diagnostics.Worker
	var notice supervision.Notices
	if o.diagnostics {
		cfg := diagnostics.DefaultOptions()
		cfg.MaxStreams = o.maxQueues
		var err error
		diagnostic, err = diagnostics.New(cfg, registry, output)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		if err = diagnostic.Start(reporting); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		reporter = diagnostic
		notice = notices{output}
		defer func() {
			deadline, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := diagnostic.Shutdown(deadline); err != nil {
				fmt.Fprintln(stderr, "diagnostics shutdown:", err)
			}
		}()
	}
	running, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErrors := make(chan error, 1)
	if o.metrics != "" {
		server, err := metrics.Listen(o.metrics, func() metrics.Snapshot {
			c := accounting.Snapshot()
			outputStats := output.Snapshot()
			var evidenceErrors uint64
			if diagnostic != nil {
				evidenceErrors = diagnostic.Snapshot().RecorderErrors
			}
			return metrics.Snapshot{Attempts: c.Attempts, Accepted: c.Accepted, Refused: c.Refused, WriteErrors: c.WriteErrors, Verified: c.Verified, Polls: c.Polls, PollErrors: c.PollErrors, InputLost: c.InputLost, OutputLost: outputStats.Dropped, EvidenceErrors: evidenceErrors, Sampled: c.Sampled, Unavailable: c.Unavailable, MaxPollGapNS: c.MaxPollGapNS}
		})
		if err != nil {
			fmt.Fprintln(stderr, "metrics listener:", err)
			return 2
		}
		defer func() {
			deadline, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			server.Close(deadline)
		}()
		go func() {
			if err := server.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErrors <- err
				output.TryWrite(report.Record{Kind: report.Status, Message: "metrics exporter stopped; recovery continues", Incomplete: true})
			}
		}()
	}
	host, err := openHost(o)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer host.Close()
	workerOptions := control.Options{Recover: o.command == "recover", Cadence: o.cadence, InventoryInterval: o.inventory, VerificationTimeout: o.verification, MaxQueues: o.maxQueues}
	err = supervision.Run(running, supervision.Options{Refresh: o.inventory, MaxTargets: o.maxTargets, Worker: workerOptions}, supervision.Dependencies{Selector: targetSource{selector}, Factory: factory{host, workerOptions.Recover, o.state}, Reporter: reporter, Notices: notice, Accounting: accounting, Registry: registry, Clock: control.RealClock{}})
	counts := accounting.Snapshot()
	if o.diagnostics {
		output.TryWrite(report.Record{Kind: report.Status, Message: fmt.Sprintf("stopped: attempts=%d accepted=%d later_progress=%d input_lost=%d", counts.Attempts, counts.Accepted, counts.Verified, counts.InputLost)})
	}
	select {
	case serverError := <-serveErrors:
		fmt.Fprintln(stderr, serverError)
		return 2
	default:
	}
	if err != nil && err != context.Canceled {
		fmt.Fprintln(stderr, err)
		return 2
	}
	return 0
}
