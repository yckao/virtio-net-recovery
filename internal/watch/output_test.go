package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestTraceOutputCapPreservesCompleteLinesAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	w := newTraceWriter(&output, cancel)
	w.remaining = len("{\"a\":1}\n")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(map[string]int{"a": 2}); !errors.Is(err, errTraceOutputLimit) {
		t.Fatalf("wrong cap error: %v", err)
	}
	if output.String() != "{\"a\":1}\n" {
		t.Fatalf("partial or over-limit output: %q", output.String())
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("output exhaustion did not stop trace workers")
	}
}

func TestTraceOutputCapIsSharedByConcurrentEncoders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	w := newTraceWriter(&output, cancel)
	const line = "{\"a\":1}\n"
	w.remaining = len(line) * 10
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { _ = json.NewEncoder(w).Encode(map[string]int{"a": 1}) })
	}
	wg.Wait()
	if output.Len() != len(line)*10 || bytes.Count(output.Bytes(), []byte{'\n'}) != 10 {
		t.Fatal("per-worker budget or partial output")
	}
	for _, l := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
		if !json.Valid(l) {
			t.Fatal("corrupt concurrent JSON line")
		}
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("shared cap did not cancel")
	}
}

type shortOutput struct{}

func (shortOutput) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestTraceOutputFailureCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newTraceWriter(shortOutput{}, cancel)
	if _, err := fmt.Fprintln(w, "{}"); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("wrong short-write result: %v", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("output error left trace running")
	}
}
