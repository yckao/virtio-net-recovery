package watch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type DecisionReason string
type WriteResult string
type ProgressOutcome string

const (
	ReasonUnconsumed       DecisionReason  = "unconsumed"
	ReasonUsedProgress     DecisionReason  = "used_progress"
	ReasonDrained          DecisionReason  = "drained"
	ReasonNoPending        DecisionReason  = "no_pending"
	ReasonWorkQueued       DecisionReason  = "work_queued"
	ReasonInvalidRing      DecisionReason  = "invalid_ring"
	ReasonIdentityChange   DecisionReason  = "identity_change"
	ReasonUnavailable      DecisionReason  = "unavailable"
	WriteSuccess           WriteResult     = "success"
	WriteError             WriteResult     = "error"
	WriteRefused           WriteResult     = "refused"
	ProgressConsumption    ProgressOutcome = "consumption"
	ProgressUsed           ProgressOutcome = "used"
	ProgressBoth           ProgressOutcome = "both"
	ProgressTimeout        ProgressOutcome = "timeout"
	ProgressIdentityChange ProgressOutcome = "identity_change"
	MaxMetricWorkers                       = 1024
)

var decisionLabels = [...]string{string(ReasonUnconsumed), string(ReasonUsedProgress), string(ReasonDrained), string(ReasonNoPending), string(ReasonWorkQueued), string(ReasonInvalidRing), string(ReasonIdentityChange), string(ReasonUnavailable)}
var writeLabels = [...]string{string(WriteSuccess), string(WriteError), string(WriteRefused)}
var progressLabels = [...]string{string(ProgressConsumption), string(ProgressUsed), string(ProgressBoth), string(ProgressTimeout), string(ProgressIdentityChange)}

type metricCounters struct {
	candidates, pollErrors uint64
	decisions              [len(decisionLabels)]uint64
	writes                 [len(writeLabels)]uint64
	progress               [len(progressLabels)]uint64
}

type workerGauges struct {
	open, sampled, unavailable int
	lastGap, maxGap            time.Duration
}

// Metrics exports fixed series only. Its zero value is ready for use.
// Successful writes and subsequent progress are separate observations; neither
// is evidence that a notification was lost.
type Metrics struct {
	mu       sync.Mutex
	counters metricCounters
	workers  map[*WorkerMetrics]workerGauges
}

// WorkerMetrics is an internal stable key, never a Prometheus label. Call Retire
// when a selected process exits or its observer stops.
type WorkerMetrics struct{ metrics *Metrics }

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) RegisterWorker() (*WorkerMetrics, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.workers) >= MaxMetricWorkers {
		return nil, errors.New("metrics active worker limit reached")
	}
	if m.workers == nil {
		m.workers = make(map[*WorkerMetrics]workerGauges)
	}
	w := &WorkerMetrics{metrics: m}
	m.workers[w] = workerGauges{}
	return w, nil
}

// SetGauges replaces this worker's current values. Invalid and retired updates
// are rejected; the exporter never retains retired keys.
func (w *WorkerMetrics) SetGauges(open, sampled, unavailable int) bool {
	if open < 0 || sampled < 0 || unavailable < 0 {
		return false
	}
	m := w.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.workers[w]
	if ok {
		g.open, g.sampled, g.unavailable = open, sampled, unavailable
		m.workers[w] = g
	}
	return ok
}

// ObservePoll records elapsed time between cycles. A worker's first cycle can
// use zero. Last/max gap gauges report the largest value among active workers.
func (w *WorkerMetrics) ObservePoll(gap time.Duration, failed bool) {
	m := w.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.workers[w]
	if !ok || gap < 0 {
		return
	}
	g.lastGap, g.maxGap = gap, max(g.maxGap, gap)
	m.workers[w] = g
	if failed {
		m.counters.pollErrors++
	}
}

func (w *WorkerMetrics) Retire() {
	m := w.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, w)
}

func (m *Metrics) Candidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters.candidates++
}

func (m *Metrics) increment(value string, labels []string, counters []uint64) bool {
	for i, label := range labels {
		if value == label {
			m.mu.Lock()
			defer m.mu.Unlock()
			counters[i]++
			return true
		}
	}
	return false
}

func (m *Metrics) Decision(reason DecisionReason) bool {
	return m.increment(string(reason), decisionLabels[:], m.counters.decisions[:])
}

func (m *Metrics) Write(result WriteResult) bool {
	return m.increment(string(result), writeLabels[:], m.counters.writes[:])
}

func (m *Metrics) Progress(outcome ProgressOutcome) bool {
	return m.increment(string(outcome), progressLabels[:], m.counters.progress[:])
}

type metricSnapshot struct {
	metricCounters
	workerGauges
}

func (m *Metrics) snapshot() metricSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := metricSnapshot{metricCounters: m.counters}
	for _, g := range m.workers {
		s.open += g.open
		s.sampled += g.sampled
		s.unavailable += g.unavailable
		s.lastGap = max(s.lastGap, g.lastGap)
		s.maxGap = max(s.maxGap, g.maxGap)
	}
	return s
}

func metricHeader(w io.Writer, name, help, kind string) {
	fmt.Fprintf(w, "# HELP vhost_watch_%s %s\n# TYPE vhost_watch_%s %s\n", name, help, name, kind)
}

func metricValue(w io.Writer, name, help, kind string, value any) {
	metricHeader(w, name, help, kind)
	fmt.Fprintf(w, "vhost_watch_%s %v\n", name, value)
}

func metricLabels(w io.Writer, name, help, label string, labels []string, values []uint64) {
	metricHeader(w, name, help, "counter")
	for i, value := range values {
		fmt.Fprintf(w, "vhost_watch_%s{%s=%q} %d\n", name, label, labels[i], value)
	}
}

// Handler serves only /metrics using Prometheus text format. It snapshots under
// the lock, then writes outside it so a slow scrape cannot delay queue polling.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if r.Method == http.MethodHead {
			return
		}
		s := m.snapshot()
		metricValue(w, "candidates_total", "Candidate episodes observed; not a lost notification verdict.", "counter", s.candidates)
		metricLabels(w, "decisions_total", "Live confirmation decisions by fixed reason.", "reason", decisionLabels[:], s.decisions[:])
		metricLabels(w, "writes_total", "Verified kick write attempts by result.", "result", writeLabels[:], s.writes[:])
		metricLabels(w, "progress_total", "Episode progress outcomes; not proof of causation.", "outcome", progressLabels[:], s.progress[:])
		metricValue(w, "open_episodes", "Currently open episodes across active workers.", "gauge", s.open)
		metricValue(w, "poll_errors_total", "Failed polling cycles across all workers.", "counter", s.pollErrors)
		metricValue(w, "poll_gap_seconds", "Largest last polling gap among active workers.", "gauge", s.lastGap.Seconds())
		metricValue(w, "poll_max_gap_seconds", "Largest polling gap since an active worker registered.", "gauge", s.maxGap.Seconds())
		metricValue(w, "sampled_queues", "Queues sampled in active workers' latest cycles.", "gauge", s.sampled)
		metricValue(w, "unavailable_queues", "Known queues unavailable in active workers' latest cycles.", "gauge", s.unavailable)
	})
}

// Start binds synchronously so a missing exporter can fail before queue work
// starts. An empty address binds to loopback. Cancellation closes the server.
func (m *Metrics) Start(ctx context.Context, address string) (stop func() error, err error) {
	if address == "" {
		address = "127.0.0.1:9475"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &http.Server{Addr: address, Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Serve(listener)
	}()
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-done:
		}
	}()
	return s.Close, nil
}
