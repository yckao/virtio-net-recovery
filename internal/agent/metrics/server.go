// Package metrics exposes a fixed, unlabelled presentation snapshot. Scrapes do
// not acquire control locks, invoke evidence or read kernel state.
package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Snapshot struct {
	ReadErrors, Cancelled, DiscoveryErrors, AdmissionErrors                                                      uint64
	SelectedTargets, ActiveTargets, UnavailableTargets, DiscoveryProblems, InventoryTruncated                    int64
	Attempts, Accepted, Refused, WriteErrors, Verified, Polls, PollErrors, InputLost, OutputLost, EvidenceErrors uint64
	Sampled, Unavailable, MaxPollGapNS                                                                           int64
}
type Server struct {
	server   *http.Server
	listener net.Listener
}

func Listen(address string, snapshot func() Snapshot) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		s := snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, metric := range []struct {
			name  string
			value uint64
		}{
			{"attempts_total", s.Attempts},
			{"writes_accepted_total", s.Accepted},
			{"refusals_total", s.Refused},
			{"read_errors_total", s.ReadErrors},
			{"write_errors_total", s.WriteErrors},
			{"cancelled_attempts_total", s.Cancelled},
			{"later_progress_total", s.Verified},
			{"polls_total", s.Polls},
			{"poll_errors_total", s.PollErrors},
			{"discovery_errors_total", s.DiscoveryErrors},
			{"admission_errors_total", s.AdmissionErrors},
			{"diagnostic_input_lost_total", s.InputLost},
			{"output_lost_total", s.OutputLost},
			{"evidence_errors_total", s.EvidenceErrors},
		} {
			fmt.Fprintf(w, "vhost_%s %d\n", metric.name, metric.value)
		}
		for _, metric := range []struct {
			name  string
			value int64
		}{
			{"sampled_queues", s.Sampled},
			{"unavailable_queues", s.Unavailable},
			{"selected_targets", s.SelectedTargets},
			{"active_targets", s.ActiveTargets},
			{"unavailable_targets", s.UnavailableTargets},
			{"discovery_problems", s.DiscoveryProblems},
			{"truncated_inventories", s.InventoryTruncated},
		} {
			fmt.Fprintf(w, "vhost_%s %d\n", metric.name, metric.value)
		}
		fmt.Fprintf(w, "vhost_max_poll_gap_seconds %g\n", float64(s.MaxPollGapNS)/1e9)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
	return &Server{server, boundedListener(listener)}, nil
}
func (s *Server) Serve() error                    { return s.server.Serve(s.listener) }
func (s *Server) Close(ctx context.Context) error { return s.server.Shutdown(ctx) }
func (s *Server) Address() string                 { return s.listener.Addr().String() }
