package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/cli"
)

func process(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "42")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[19] = "101"
	for name, value := range map[string]string{"stat": "42 (qemu-system-x86) " + strings.Join(fields, " "), "comm": "qemu-system-x86\n", "cmdline": "qemu-system-x86_64\x00-uuid\x0012345678-1234-1234-1234-123456789abc\x00"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
func TestListUsesVersionedPresentationAndAnchoredIdentity(t *testing.T) {
	root := process(t)
	var out, stderr bytes.Buffer
	status := cli.Run(context.Background(), []string{"list", "--pid", "42", "--proc", root}, &out, &stderr)
	if status != 0 {
		t.Fatal(status, stderr.String())
	}
	var result struct {
		Schema   int `json:"schema_version"`
		Complete bool
		Target   struct {
			PID       int
			StartTime uint64 `json:"start_time"`
		}
	}
	decoder := json.NewDecoder(&out)
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Schema != 1 || !result.Complete || result.Target.StartTime != 101 {
		t.Fatal(result)
	}
	var summary struct {
		Event, Execution string
		Targets          int
	}
	if err := decoder.Decode(&summary); err != nil {
		t.Fatal(err)
	}
	if summary.Event != "command_summary" || summary.Execution != "complete" || summary.Targets != 1 {
		t.Fatal(summary)
	}
}

type failedWriter struct{ calls int }

func (w *failedWriter) Write([]byte) (int, error) { w.calls++; return 0, errors.New("sink failed") }
func TestDeliveryFailureHasDistinctExitCodeAndDoesNotRetry(t *testing.T) {
	root := process(t)
	w := &failedWriter{}
	var stderr bytes.Buffer
	status := cli.Run(context.Background(), []string{"list", "--pid", "42", "--proc", root}, w, &stderr)
	if status != 3 || w.calls != 1 || !strings.Contains(stderr.String(), "delivery=failed") {
		t.Fatal(status, w.calls, stderr.String())
	}
}
func TestInvalidAndRetiredFlagsNeverStartEffects(t *testing.T) {
	for _, args := range [][]string{{"recover", "--pid", "42", "--cadence", "0s"}, {"kick", "--pid", "42", "--once"}, {"trace", "--pid", "42", "--duration", "2h"}, {"unknown"}, {"kick", "--pid", "42", "--metrics", ":9090"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			if code := cli.Run(context.Background(), args, io.Discard, io.Discard); code != 2 {
				t.Fatal(code)
			}
		})
	}
}
func TestHelpDoesNotRequireBackend(t *testing.T) {
	if code := cli.Run(context.Background(), []string{"help"}, io.Discard, io.Discard); code != 0 {
		t.Fatal(code)
	}
}
