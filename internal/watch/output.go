package watch

import (
	"context"
	"errors"
	"io"
	"sync"
)

const traceOutputLimit = 8 << 20

var errTraceOutputLimit = errors.New("trace stopped at the shared 8 MiB JSON output limit")

// JSON Encoder writes a complete encoded line in one Write. Admission happens
// before touching stdout, so reaching the cap cannot leave a partial JSON line.
// This bound is shared by all selected targets, not reset for each VM.
type traceWriter struct {
	mu        sync.Mutex
	output    io.Writer
	remaining int
	cancel    context.CancelFunc
	err       error
}

func newTraceWriter(output io.Writer, cancel context.CancelFunc) *traceWriter {
	return &traceWriter{output: output, remaining: traceOutputLimit, cancel: cancel}
}

func (w *traceWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if len(p) > w.remaining {
		w.err = errTraceOutputLimit
		w.cancel()
		return 0, w.err
	}
	n, err := w.output.Write(p)
	w.remaining -= n
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		w.cancel()
	}
	return n, err
}

func (w *traceWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}
