//go:build linux && amd64

package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"
)

func TestObserveDoesNotWriteWhileRecoverUsesSameLiveCandidate(t *testing.T) {
	for _, mode := range []string{"observe", "recover"} {
		t.Run(mode, func(t *testing.T) {
			fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			id, err := eventID("self", fd)
			if err != nil {
				t.Fatal(err)
			}
			s := configured()
			s.EventID = id
			target := &candidateTarget{testTarget: testTarget{source: fd, alive: true}, snapshot: s}
			q := &recoveryQueue{fd: 42, snapshot: s, policy: NewRecoveryPolicy(.1), avail: 20, used: 10}
			var output bytes.Buffer
			l := &logger{encoder: json.NewEncoder(&output)}
			if err := recoverLive(context.Background(), Config{Mode: mode}, target, testBPF{value: s}, l, q, true); err != nil {
				t.Fatal(err)
			}
			n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if (n > 0) != (mode == "recover") {
				t.Fatalf("%s wrote incorrectly: %d", mode, n)
			}
			if !strings.Contains(output.String(), `"event":"candidate"`) {
				t.Fatal("shared live candidate was not reported")
			}
			if (q.policy.Writes > 0) != (mode == "recover") {
				t.Fatal("write accounting conflated observe and recover")
			}
		})
	}
}

type candidateTarget struct {
	testTarget
	snapshot Snapshot
}

func (t *candidateTarget) Inventory() ([]int, map[uint32][]int, error) {
	return []int{42}, map[uint32][]int{t.snapshot.EventID: {66}}, nil
}
func (t *candidateTarget) Ring(Snapshot) (uint16, uint16, error) { return 20, 10, nil }

func TestQueuedWorkRefusesRecoveryWithoutClaimingWorkerIdle(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	id, err := eventID("self", fd)
	if err != nil {
		t.Fatal(err)
	}
	s := configured()
	s.EventID, s.WorkFlags = id, 1<<1
	target := &candidateTarget{testTarget: testTarget{source: fd, alive: true}, snapshot: s}
	q := &recoveryQueue{fd: 42, snapshot: s, policy: NewRecoveryPolicy(.1), avail: 20, used: 10}
	var output bytes.Buffer
	l := &logger{encoder: json.NewEncoder(&output)}
	if err := recoverLive(context.Background(), Config{Mode: "recover"}, target, testBPF{value: s}, l, q, true); err != nil {
		t.Fatal(err)
	}
	if q.policy.Writes != 0 || output.Len() != 0 {
		t.Fatal("queued work was treated as a write candidate")
	}
}

func TestStageProgramsAreAbsentOutsideTrace(t *testing.T) {
	for _, trace := range []bool{false, true} {
		spec := &ebpf.CollectionSpec{Programs: map[string]*ebpf.ProgramSpec{"snapshot_tx": {}, "signal_event": {}, "handler_exit": {}}}
		selectBPFPrograms(spec, trace)
		if spec.Programs["snapshot_tx"] == nil {
			t.Fatal("snapshot primitive removed")
		}
		if (spec.Programs["signal_event"] != nil) != trace || (spec.Programs["handler_exit"] != nil) != trace {
			t.Fatal("traffic stages loaded outside trace")
		}
	}
}
