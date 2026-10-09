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
		fmt.Fprintf(w, "vhost_attempts_total %d\nvhost_writes_accepted_total %d\nvhost_refusals_total %d\nvhost_write_errors_total %d\nvhost_later_progress_total %d\nvhost_polls_total %d\nvhost_poll_errors_total %d\nvhost_diagnostic_input_lost_total %d\nvhost_output_lost_total %d\nvhost_evidence_errors_total %d\nvhost_sampled_queues %d\nvhost_unavailable_queues %d\nvhost_max_poll_gap_seconds %g\n", s.Attempts, s.Accepted, s.Refused, s.WriteErrors, s.Verified, s.Polls, s.PollErrors, s.InputLost, s.OutputLost, s.EvidenceErrors, s.Sampled, s.Unavailable, float64(s.MaxPollGapNS)/1e9)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
	return &Server{server, boundedListener(listener)}, nil
}
func (s *Server) Serve() error                    { return s.server.Serve(s.listener) }
func (s *Server) Close(ctx context.Context) error { return s.server.Shutdown(ctx) }
func (s *Server) Address() string                 { return s.listener.Addr().String() }
