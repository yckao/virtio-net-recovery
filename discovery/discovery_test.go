package discovery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	discovery "github.com/yckao/virtio-net-recovery/discovery"
)

func process(t *testing.T, root string, pid int, start int, uuid string) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprint(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fields := append([]string{"S"}, strings.Fields(strings.Repeat("0 ", 18))...)
	fields = append(fields, fmt.Sprint(start))
	for name, data := range map[string]string{"stat": fmt.Sprintf("%d (qemu (guest)) %s", pid, strings.Join(fields, " ")), "comm": "qemu-system-x86\n", "cmdline": "qemu-system-x86_64\x00-uuid\x00" + uuid + "\x00"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func domain(t *testing.T, root string, pid int) {
	t.Helper()
	data := fmt.Sprintf(`<domstatus state="running" pid="%d"><domain><name>worker-a</name><uuid>id-a</uuid></domain></domstatus>`, pid)
	if err := os.WriteFile(filepath.Join(root, "a.xml"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestExplicitIdentityDoesNotFollowReuseButDomainCan(t *testing.T) {
	proc, dir := t.TempDir(), t.TempDir()
	process(t, proc, 101, 1000, "id-a")
	domain(t, dir, 101)
	s, err := discovery.New(discovery.Options{PIDs: []int{101}, DomainPattern: "^worker-", ProcRoot: proc, RuntimeDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Resolve(context.Background())
	if err != nil || len(r.Targets) != 1 || len(r.Problems) != 0 || !r.Complete {
		t.Fatalf("%+v %v", r, err)
	}
	process(t, proc, 101, 1001, "id-other")
	r, err = s.Resolve(context.Background())
	if err != nil || len(r.Targets) != 0 || len(r.Problems) != 2 {
		t.Fatalf("reuse: %+v %v", r, err)
	}
	process(t, proc, 202, 2000, "id-a")
	domain(t, dir, 202)
	r, err = s.Resolve(context.Background())
	if err != nil || len(r.Targets) != 1 || r.Targets[0].PID != 202 {
		t.Fatalf("replacement: %+v %v", r, err)
	}
}
func TestIncompleteInventoryPreservesDiscoveredExplicitTarget(t *testing.T) {
	proc := t.TempDir()
	process(t, proc, 101, 1000, "id-a")
	s, err := discovery.New(discovery.Options{PIDs: []int{101}, DomainPattern: "worker", ProcRoot: proc, RuntimeDir: filepath.Join(t.TempDir(), "missing")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Resolve(context.Background())
	if err == nil || r.Complete || len(r.Targets) != 1 {
		t.Fatalf("%+v %v", r, err)
	}
}
func TestLimitsAndOptionsAreOwned(t *testing.T) {
	proc := t.TempDir()
	process(t, proc, 101, 1000, "id-a")
	pids := []int{101}
	s, err := discovery.New(discovery.Options{PIDs: pids, ProcRoot: proc})
	if err != nil {
		t.Fatal(err)
	}
	pids[0] = 202
	r, err := s.Resolve(context.Background())
	if err != nil || len(r.Targets) != 1 || r.Targets[0].PID != 101 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err = discovery.New(discovery.Options{PIDs: []int{1, 2}, MaxTargets: 1}); err == nil {
		t.Fatal("unbounded targets")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = s.Resolve(ctx)
	if err == nil || r.Complete {
		t.Fatal("cancelled inventory is complete")
	}
}
