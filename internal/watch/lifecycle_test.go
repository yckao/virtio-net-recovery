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
