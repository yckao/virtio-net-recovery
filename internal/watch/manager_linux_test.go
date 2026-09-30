//go:build linux && amd64

package watch

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"vhost-watch/internal/selection"
)

func TestConcurrentTraceFailuresKeepTheirSourceTargets(t *testing.T) {
	targets := []selection.Target{{PID: 101, Domain: "vm-a"}, {PID: 202, Domain: "vm-b"}, {PID: 303, Domain: "vm-ok"}}
	var output bytes.Buffer
	log := &logger{encoder: json.NewEncoder(&output)}
	failures := runTraceTargets(targets, func(target selection.Target) error {
		// These initialization errors occur before any started record. Equal
		// messages must still be attributable when completions race.
		if target.PID == 303 {
			return nil
		}
		return errors.New("target permission denied")
	}, log)
	if failures != 2 || log.err != nil {
		t.Fatalf("wrong aggregate result: failures=%d error=%v", failures, log.err)
	}
	seen := map[int]string{}
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
		var record struct {
			Event  string `json:"event"`
			PID    int    `json:"pid"`
			Domain string `json:"domain"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Event != "target_failed" || record.Error != "target permission denied" {
			t.Fatalf("wrong failure record: %+v", record)
		}
		if _, duplicate := seen[record.PID]; duplicate {
			t.Fatal("one failure overwrote another target")
		}
		seen[record.PID] = record.Domain
	}
	if len(seen) != 2 || seen[101] != "vm-a" || seen[202] != "vm-b" {
		t.Fatalf("failure target attribution lost: %v", seen)
	}
}
