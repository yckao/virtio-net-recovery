package cli

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockedOutput struct{ entered, release chan struct{} }

func (w blockedOutput) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}
func TestCommandDeliveryDeadlineDoesNotWaitForBlockedWriter(t *testing.T) {
	w := blockedOutput{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(w.release)
	out := newOutput(w)
	defer out.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- out.deliver(ctx, struct{ Value int }{42}) }()
	<-w.entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errDelivery) || !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery waited for an uncancellable writer")
	}
}
