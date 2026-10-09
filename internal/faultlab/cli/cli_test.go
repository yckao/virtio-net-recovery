package cli_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/cli"
)

func TestHelpAndInvalidInvocationsNeedNoKernel(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, cli.ExitUsage}, {[]string{"help"}, cli.ExitOK}, {[]string{"run", "-h"}, cli.ExitOK},
		{[]string{"run"}, cli.ExitUsage}, {[]string{"run", "--drops", "0"}, cli.ExitUsage},
		{[]string{"clear", "--timeout", "0"}, cli.ExitUsage}, {[]string{"unknown"}, cli.ExitUsage},
		{[]string{"clear", "--state-dir", "relative"}, cli.ExitUsage},
		{[]string{"show", "unexpected"}, cli.ExitUsage},
	} {
		var out, errOut bytes.Buffer
		if code := cli.Run(context.Background(), tc.args, &out, &errOut); code != tc.code {
			t.Fatalf("%v: got %d want %d: %s", tc.args, code, tc.code, errOut.String())
		}
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("sink unavailable") }

func TestPresentationFailureHasItsOwnExitCategory(t *testing.T) {
	var errOut bytes.Buffer
	// Invalid prepare options fail before any build or kernel action.
	code := cli.Run(context.Background(), []string{"prepare"}, failedWriter{}, &errOut)
	if code != cli.ExitDelivery {
		t.Fatalf("reporting failure was conflated with operation status: %d", code)
	}
}

func TestHelpDeliveryFailureCannotClaimSuccess(t *testing.T) {
	var errOut bytes.Buffer
	if code := cli.Run(context.Background(), []string{"help"}, failedWriter{}, &errOut); code != cli.ExitDelivery {
		t.Fatal("help output failure claimed success", code)
	}
}
