package watch

import (
	"strconv"
	"testing"
)

func TestRetirementStaysBoundedAcrossDomainProcessChurn(t *testing.T) {
	retired := map[string]bool{"current-explicit-pid": true}
	for i := range 10000 {
		retired[strconv.Itoa(i)] = true
		pruneRetiredTargets(retired, map[string]bool{"current-explicit-pid": true})
		if len(retired) != 1 || !retired["current-explicit-pid"] {
			t.Fatal("retirement leaked old generations or restarted the current one")
		}
	}
	pruneRetiredTargets(retired, nil)
	if len(retired) != 0 {
		t.Fatal("empty selection retained retired processes")
	}
}

func TestInterruptedRefreshForgetsTransferredAndUnvisitedAttachments(t *testing.T) {
	old := Snapshot{VQ: 1, Context: 1}
	replaced := Snapshot{VQ: 2, Context: 2}
	newAttachment := Snapshot{VQ: 2, Context: 3}
	unvisited := Snapshot{VQ: 3, Context: 4}
	current := map[int]Snapshot{7: old, 8: newAttachment}
	pending := map[int]Snapshot{7: old, 8: replaced, 9: unvisited}
	forgotten := map[Snapshot]int{}
	forgetSnapshotSets(current, pending, func(s Snapshot) { forgotten[s.Identity()]++ })
	for _, s := range []Snapshot{old, replaced, newAttachment, unvisited} {
		if forgotten[s.Identity()] != 1 {
			t.Fatalf("attachment not released exactly once after partial refresh: %+v", forgotten)
		}
	}
	if len(forgotten) != 4 {
		t.Fatal("cleanup visited an unowned attachment")
	}
}
