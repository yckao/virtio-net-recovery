package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

var errDelivery = errors.New("delivery=failed; completed writes must not be retried")
var errIncomplete = errors.New("execution incomplete")

type writeRequest struct {
	data []byte
	done chan error
}

// commandOutput owns one writer for the invocation. There is no backlog or
// retry. A blocked Writer can outlive its deadline; it cannot spawn more work.
type commandOutput struct{ requests chan writeRequest }

func newOutput(w io.Writer) *commandOutput {
	o := &commandOutput{requests: make(chan writeRequest)}
	go func() {
		for r := range o.requests {
			n, err := w.Write(r.data)
			if err == nil && n != len(r.data) {
				err = io.ErrShortWrite
			}
			r.done <- err
		}
	}()
	return o
}
func (o *commandOutput) Close() { close(o.requests) }
func (o *commandOutput) deliver(ctx context.Context, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return errors.Join(errDelivery, err)
	}
	if len(data) > 4<<20 {
		return errors.Join(errDelivery, errors.New("record exceeds 4 MiB"))
	}
	request := writeRequest{data: append(data, '\n'), done: make(chan error, 1)}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case o.requests <- request:
	case <-bounded.Done():
		return errors.Join(errDelivery, bounded.Err())
	}
	select {
	case err := <-request.done:
		if err != nil {
			return errors.Join(errDelivery, err)
		}
		return nil
	case <-bounded.Done():
		return errors.Join(errDelivery, bounded.Err())
	}
}
func message(err error) string {
	if err == nil {
		return ""
	}
	return bounded(err.Error(), 256)
}
func bounded(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
