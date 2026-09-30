package watch

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func scrapeMetrics(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if r.Code != http.StatusOK || r.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("scrape status/content type: %d %q", r.Code, r.Header().Get("Content-Type"))
	}
	values := make(map[string]float64)
	for _, line := range strings.Split(r.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("malformed metric: %q", line)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("invalid value in %q: %v", line, err)
		}
		if _, duplicate := values[fields[0]]; duplicate {
			t.Fatalf("duplicate series: %q", fields[0])
		}
		values[fields[0]] = value
	}
	return values
}

func newMetricWorker(t *testing.T, m *Metrics) *WorkerMetrics {
	t.Helper()
	w, err := m.RegisterWorker()
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func requireMetric(t *testing.T, values map[string]float64, name string, want float64) {
	t.Helper()
	got, ok := values["vhost_watch_"+name]
	if !ok || got != want {
		t.Errorf("%s: got %v (present %v), want %v", name, got, ok, want)
	}
}

func TestMetricsFixedSeriesRejectLabelInjection(t *testing.T) {
	m := NewMetrics()
	m.Candidate()
	for _, reason := range []DecisionReason{ReasonUnconsumed, ReasonUsedProgress, ReasonDrained, ReasonNoPending, ReasonWorkQueued, ReasonInvalidRing, ReasonIdentityChange, ReasonUnavailable} {
		if !m.Decision(reason) {
			t.Fatalf("rejected known reason %q", reason)
		}
	}
	for _, result := range []WriteResult{WriteSuccess, WriteError, WriteRefused} {
		if !m.Write(result) {
			t.Fatalf("rejected known result %q", result)
		}
	}
	for _, outcome := range []ProgressOutcome{ProgressConsumption, ProgressUsed, ProgressBoth, ProgressTimeout, ProgressIdentityChange} {
		if !m.Progress(outcome) {
			t.Fatalf("rejected known outcome %q", outcome)
		}
	}
	for _, unknown := range []string{"", "other", "pid=123", "unconsumed\"}\ninjected 1\n#"} {
		if m.Decision(DecisionReason(unknown)) || m.Write(WriteResult(unknown)) || m.Progress(ProgressOutcome(unknown)) {
			t.Fatalf("accepted unknown enum %q", unknown)
		}
	}
	values := scrapeMetrics(t, m)
	if len(values) != 23 {
		t.Fatalf("got %d series, want fixed 23", len(values))
	}
	for _, group := range []struct {
		name, label string
		values      []string
	}{
		{"decisions_total", "reason", []string{"unconsumed", "used_progress", "drained", "no_pending", "work_queued", "invalid_ring", "identity_change", "unavailable"}},
		{"writes_total", "result", []string{"success", "error", "refused"}},
		{"progress_total", "outcome", []string{"consumption", "used", "both", "timeout", "identity_change"}},
	} {
		for _, label := range group.values {
			requireMetric(t, values, fmt.Sprintf("%s{%s=%q}", group.name, group.label, label), 1)
		}
	}
	requireMetric(t, values, "candidates_total", 1)
	for name := range values {
		if strings.Contains(name, "injected") || strings.Contains(name, "pid") || strings.Contains(name, "event_id") || strings.Contains(name, "identity=") {
			t.Errorf("unexpected identity or input label: %s", name)
		}
	}
}

func TestMetricsAggregateAndRetireWorkers(t *testing.T) {
	var m Metrics // Zero value works without NewMetrics.
	a, b := newMetricWorker(t, &m), newMetricWorker(t, &m)
	if !a.SetGauges(2, 3, 1) || !b.SetGauges(1, 7, 2) {
		t.Fatal("active worker update rejected")
	}
	a.ObservePoll(time.Second, true)
	a.ObservePoll(200*time.Millisecond, false)
	b.ObservePoll(500*time.Millisecond, true)
	values := scrapeMetrics(t, &m)
	for name, want := range map[string]float64{"open_episodes": 3, "sampled_queues": 10, "unavailable_queues": 3, "poll_gap_seconds": 0.5, "poll_max_gap_seconds": 1, "poll_errors_total": 2} {
		requireMetric(t, values, name, want)
	}
	if a.SetGauges(-1, 0, 0) || a.SetGauges(0, -1, 0) || a.SetGauges(0, 0, -1) {
		t.Fatal("negative gauges accepted")
	}
	b.ObservePoll(-time.Second, true)
	a.Retire()
	a.Retire()
	if a.SetGauges(100, 100, 100) {
		t.Fatal("retired worker update accepted")
	}
	a.ObservePoll(10*time.Second, true)
	values = scrapeMetrics(t, &m)
	for name, want := range map[string]float64{"open_episodes": 1, "sampled_queues": 7, "unavailable_queues": 2, "poll_gap_seconds": 0.5, "poll_max_gap_seconds": 0.5, "poll_errors_total": 2} {
		requireMetric(t, values, name, want)
	}
	b.Retire()
	values = scrapeMetrics(t, &m)
	for _, name := range []string{"open_episodes", "sampled_queues", "unavailable_queues", "poll_gap_seconds", "poll_max_gap_seconds"} {
		requireMetric(t, values, name, 0)
	}
	if len(m.workers) != 0 {
		t.Fatalf("retired keys retained: %d", len(m.workers))
	}
}

func TestMetricsWorkerMemoryBoundAndChurn(t *testing.T) {
	m := NewMetrics()
	workers := make([]*WorkerMetrics, MaxMetricWorkers)
	for i := range workers {
		workers[i] = newMetricWorker(t, m)
	}
	if w, err := m.RegisterWorker(); err == nil || w != nil {
		t.Fatal("active worker bound not enforced")
	}
	for _, w := range workers {
		w.Retire()
	}
	for range MaxMetricWorkers * 2 {
		newMetricWorker(t, m).Retire()
	}
	if len(m.workers) != 0 {
		t.Fatalf("retired churn grew registry to %d", len(m.workers))
	}
}

func TestMetricsConcurrentUpdatesAndScrapes(t *testing.T) {
	const workers, cycles = 32, 200
	m := NewMetrics()
	var wg sync.WaitGroup
	for range workers {
		w := newMetricWorker(t, m)
		wg.Go(func() {
			defer w.Retire()
			for i := range cycles {
				m.Candidate()
				m.Decision(ReasonUnconsumed)
				m.Write(WriteSuccess)
				m.Progress(ProgressBoth)
				w.SetGauges(1, 2, 1)
				w.ObservePoll(time.Duration(i)*time.Millisecond, i%10 == 0)
			}
		})
	}
	for range cycles {
		scrapeMetrics(t, m)
	}
	wg.Wait()
	values := scrapeMetrics(t, m)
	for _, name := range []string{"candidates_total", `decisions_total{reason="unconsumed"}`, `writes_total{result="success"}`, `progress_total{outcome="both"}`} {
		requireMetric(t, values, name, workers*cycles)
	}
	requireMetric(t, values, "poll_errors_total", workers*cycles/10)
	requireMetric(t, values, "open_episodes", 0)
}

func TestMetricsHandlerRoutesAndMethods(t *testing.T) {
	m := NewMetrics()
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/metrics", http.StatusOK},
		{http.MethodHead, "/metrics", http.StatusOK},
		{http.MethodGet, "/metrics/", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusNotFound},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			r := httptest.NewRecorder()
			m.Handler().ServeHTTP(r, httptest.NewRequest(test.method, test.path, nil))
			if r.Code != test.status {
				t.Fatalf("got status %d, want %d", r.Code, test.status)
			}
			if test.method == http.MethodHead && r.Body.Len() != 0 {
				t.Fatal("HEAD returned a body")
			}
		})
	}
}

func TestMetricsStartBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stop, err := NewMetrics().Start(context.Background(), listener.Addr().String())
	if err == nil || stop != nil {
		t.Fatal("Start did not report occupied address synchronously")
	}
}

func TestMetricsStartStopsOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := NewMetrics().Start(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: %d", response.StatusCode)
	}
	cancel()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return
		}
		connection.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("metrics listener did not close on cancellation")
}
