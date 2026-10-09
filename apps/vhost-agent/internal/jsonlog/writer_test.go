package jsonlog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/jsonlog"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
)

func writer(t *testing.T, o jsonlog.Options, sink io.Writer) *jsonlog.Writer {
	t.Helper()
	w, err := jsonlog.New(o, sink)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func wait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func stop(t *testing.T, w *jsonlog.Writer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedEncodingAndFIFO(t *testing.T) {
	var output bytes.Buffer
	w := writer(t, jsonlog.DefaultOptions(), &output)
	r := report.Record{Kind: report.Action, Attempt: 7, Samples: []report.Sample{{Used: 12, Source: "cached"}}}
	if !w.TryWrite(r) {
		t.Fatal("record rejected")
	}
	r.Samples[0].Used = 999
	if output.Len() != 0 {
		t.Fatal("producer called destination")
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop(t, w)
	var got struct {
		Version    int  `json:"schema_version"`
		Incomplete bool `json:"delivery_incomplete"`
		Record     struct {
			Version int `json:"schema_version"`
			Samples []struct {
				Used uint16 `json:"used"`
			} `json:"samples"`
		} `json:"record"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Record.Version != 1 || got.Incomplete || got.Record.Samples[0].Used != 12 {
		t.Fatalf("borrowed input escaped: %+v", got)
	}
}

func TestCountByteAndRecordBounds(t *testing.T) {
	o := jsonlog.DefaultOptions()
	o.MaxRecords = 1
	w := writer(t, o, io.Discard)
	if !w.TryWrite(report.Record{Kind: report.EpisodeSnapshot, Stream: 1}) || w.TryWrite(report.Record{Kind: report.EpisodeSnapshot, Stream: 2}) {
		t.Fatal("record-count admission failed")
	}
	stats := w.Snapshot()
	if stats.PendingRecords != 1 || stats.Dropped != 1 {
		t.Fatal(stats)
	}
	stop(t, w) // A never-started writer discards its bounded queue.
	o = jsonlog.DefaultOptions()
	o.MaxBytes = 1000
	o.MaxRecordBytes = 1000
	w = writer(t, o, io.Discard)
	for range 10 {
		w.TryWrite(report.Record{Kind: report.Status})
	}
	stats = w.Snapshot()
	if stats.PendingBytes > o.MaxBytes || stats.Dropped == 0 || stats.PendingRecords == 0 {
		t.Fatal(stats)
	}
	stop(t, w)
	o = jsonlog.DefaultOptions()
	o.MaxRecordBytes = 64
	w = writer(t, o, io.Discard)
	if w.TryWrite(report.Record{Kind: report.Status}) {
		t.Fatal("oversized record admitted")
	}
	if w.TryWrite(report.Record{Samples: make([]report.Sample, report.MaxSamples+1)}) {
		t.Fatal("unbounded sample input admitted")
	}
	if w.Snapshot().Oversized != 2 {
		t.Fatal(w.Snapshot())
	}
	stop(t, w)
}

type blockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockedWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return len(p), nil
}

func TestBlockedSinkCannotBlockAdmissionOrShutdown(t *testing.T) {
	sink := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	o := jsonlog.DefaultOptions()
	o.MaxRecords = 2
	w := writer(t, o, sink)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !w.TryWrite(report.Record{Kind: report.Action}) {
		t.Fatal("first rejected")
	}
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("writer not started")
	}
	if !w.TryWrite(report.Record{Kind: report.Action}) {
		t.Fatal("second rejected")
	}
	if !w.TryWrite(report.Record{Kind: report.Action}) {
		t.Fatal("recent action did not replace an older queued action")
	}
	if stats := w.Snapshot(); stats.PendingRecords != 2 || stats.Dropped != 1 {
		t.Fatal("in-flight and queued records exceeded bound", stats)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := w.Shutdown(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	stats := w.Snapshot()
	if !stats.Incomplete || stats.PendingRecords != 1 || stats.Dropped != 2 {
		t.Fatal(stats)
	}
	close(sink.release)
	stop(t, w)
}

type failedWriter struct{ short bool }

func (f failedWriter) Write(p []byte) (int, error) {
	if f.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("destination failed")
}

func TestDestinationFailureDisablesSinkAndCountsDiscardedRecords(t *testing.T) {
	for _, short := range []bool{false, true} {
		w := writer(t, jsonlog.DefaultOptions(), failedWriter{short: short})
		w.TryWrite(report.Record{Kind: report.Action})
		w.TryWrite(report.Record{Kind: report.Action})
		if err := w.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		wait(t, func() bool { return w.Snapshot().Failed })
		stats := w.Snapshot()
		if stats.Errors != 1 || stats.Dropped != 2 || stats.PendingRecords != 0 || stats.PendingBytes != 0 {
			t.Fatal(stats)
		}
		if w.TryWrite(report.Record{}) {
			t.Fatal("failed sink accepted another record")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := w.Shutdown(ctx); err == nil {
			t.Fatal("failure hidden")
		}
		cancel()
	}
}

func TestConcurrentProducersHaveCompleteLinesAndCountedDrops(t *testing.T) {
	var output bytes.Buffer
	w := writer(t, jsonlog.DefaultOptions(), &output)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				w.TryWrite(report.Record{Kind: report.Action})
			}
		}()
	}
	wg.Wait()
	stop(t, w)
	stats := w.Snapshot()
	if stats.Written+stats.Dropped != 400 {
		t.Fatal(stats)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
		if len(line) > 0 && !json.Valid(line) {
			t.Fatal("interleaved output")
		}
	}
}

func TestQueuedRoutineSnapshotsCoalesceToLatestValue(t *testing.T) {
	var output bytes.Buffer
	w := writer(t, jsonlog.DefaultOptions(), &output)
	for sequence := uint64(1); sequence <= 3; sequence++ {
		if !w.TryWrite(report.Record{Kind: report.EpisodeSnapshot, Stream: 7, Sequence: sequence}) {
			t.Fatal("replacement snapshot rejected")
		}
	}
	if stats := w.Snapshot(); stats.PendingRecords != 1 || stats.Dropped != 2 {
		t.Fatal(stats)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop(t, w)
	var got struct {
		Incomplete bool   `json:"delivery_incomplete"`
		Lost       uint64 `json:"output_lost"`
		Record     struct {
			Sequence   uint64 `json:"sequence"`
			Incomplete bool   `json:"incomplete"`
		} `json:"record"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &got); err != nil {
		t.Fatal(err)
	}
	if got.Record.Sequence != 3 {
		t.Fatalf("stale snapshot retained: %+v", got)
	}
	if !got.Incomplete || got.Lost != 2 || got.Record.Incomplete {
		t.Fatalf("delivery loss must be visible without changing evidence completeness: %+v", got)
	}
}

func TestOutcomesUseReservedRoomAndEvictRoutineBeforeOlderOutcomes(t *testing.T) {
	var output bytes.Buffer
	opts := jsonlog.DefaultOptions()
	opts.MaxRecords = 4
	w := writer(t, opts, &output)
	for stream := uint64(1); stream <= 3; stream++ {
		if !w.TryWrite(report.Record{Kind: report.EpisodeSnapshot, Stream: stream}) {
			t.Fatal("routine budget rejected a snapshot")
		}
	}
	if w.TryWrite(report.Record{Kind: report.EpisodeSnapshot, Stream: 4}) {
		t.Fatal("routine snapshot consumed reserved outcome room")
	}
	for attempt := uint64(1); attempt <= 5; attempt++ {
		if !w.TryWrite(report.Record{Kind: report.Action, Attempt: attempt}) {
			t.Fatal("recent outcome rejected despite evictable queued records")
		}
	}
	if stats := w.Snapshot(); stats.PendingRecords != 4 || stats.Dropped != 5 {
		t.Fatal(stats)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop(t, w)
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 4 {
		t.Fatalf("wrong retained outcome count: %d", len(lines))
	}
	for i, line := range lines {
		var got struct {
			Incomplete bool   `json:"delivery_incomplete"`
			Lost       uint64 `json:"output_lost"`
			Record     struct {
				Kind    report.Kind `json:"event"`
				Attempt uint64      `json:"attempt"`
			} `json:"record"`
		}
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatal(err)
		}
		if got.Record.Kind != report.Action || got.Record.Attempt != uint64(i+2) || !got.Incomplete || got.Lost != 5 {
			t.Fatalf("retained wrong record: %+v", got)
		}
	}
}

func TestDeliveryMetadataAndEscapingStayWithinAdmittedByteBudget(t *testing.T) {
	var output bytes.Buffer
	opts := jsonlog.DefaultOptions()
	opts.MaxRecordBytes = 1024
	opts.MaxBytes = 1024
	w := writer(t, opts, &output)
	r := report.Record{Kind: report.Action, Message: "<script>\"<&>\n\t\x00"}
	if !w.TryWrite(r) {
		t.Fatal("bounded escaped record rejected")
	}
	budget := w.Snapshot().PendingBytes
	if budget > opts.MaxBytes || budget > opts.MaxRecordBytes {
		t.Fatal("metadata exceeded admission budget", budget)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop(t, w)
	if output.Len() > budget || !json.Valid(bytes.TrimSpace(output.Bytes())) {
		t.Fatal("wire output exceeded reserved bytes", output.Len(), budget)
	}
}
