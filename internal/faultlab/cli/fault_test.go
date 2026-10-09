package cli

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yckao/virtio-net-recovery/internal/faultlab/experiment"
)

func TestFaultReportKeepsReceiptAndPostWriteFailureDistinct(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	postWrite := errors.New("restoration cleanup failed")
	report := experiment.Report{
		Loaded: true, Disarmed: true, StatisticsKnown: true,
		Statistics: experiment.Statistics{Active: true, Dropped: 1},
		Restore:    experiment.RestoreResult{Outcome: experiment.RestoreAccepted, CompletedAt: at, Err: postWrite},
	}
	row := faultResult(report)
	if row.Active || row.Restoration != experiment.RestoreAccepted || row.RestoreError != postWrite.Error() || row.CompletedAt == nil || *row.CompletedAt != at {
		t.Fatal("presentation changed the actual receipt or suppressed failure", row)
	}
}

func TestUnattemptedRestorationHasNoInventedCompletionTime(t *testing.T) {
	data, err := json.Marshal(faultResult(experiment.Report{Restore: experiment.RestoreResult{Outcome: experiment.RestoreNotAttempted}}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "restoration_completed_at") {
		t.Fatal("unattempted operation acquired a timestamp", string(data))
	}
}
