//go:build linux && amd64

package watch

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"vhost-watch/internal/selection"
)

func TestMetricsBindFailurePrecedesTargetDiscoveryAndBPF(t *testing.T) {
	selector, err := selection.New([]int{1234}, "", t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := ManagerConfig{
		Agent: Config{Mode: "observe", StateDir: t.TempDir(), BPFObject: "/missing.bpf.o",
			Interval: .1, InventoryInterval: 5, VerifyTimeout: 5, SummaryInterval: 5, VhostFD: -1},
		Selector: selector, Refresh: time.Second, MetricsAddress: "127.0.0.1:not-a-port",
	}
	var output bytes.Buffer
	err = RunSelected(context.Background(), cfg, &output)
	if err == nil || !strings.HasPrefix(err.Error(), "metrics: ") {
		t.Fatalf("metrics bind did not fail first: %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("target discovery ran before metrics bind failure: %s", output.String())
	}
}
