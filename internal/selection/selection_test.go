package selection

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, root string, pid int, start, uuid string) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprint(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fields := append([]string{"S"}, strings.Fields(strings.Repeat("0 ", 18))...)
	fields = append(fields, start)
	for name, data := range map[string]string{"comm": "qemu-system-x86\n", "stat": fmt.Sprintf("%d (qemu (VM)) %s", pid, strings.Join(fields, " ")), "cmdline": "qemu-system-x86_64\x00-uuid\x00" + uuid + "\x00"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func domain(t *testing.T, root, name, state string, pid int, uuid string) {
	t.Helper()
	data := fmt.Sprintf(`<domstatus state="%s" pid="%d"><domain><name>%s</name><uuid>%s</uuid></domain></domstatus>`, state, pid, name, uuid)
	if err := os.WriteFile(filepath.Join(root, name+".xml"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSelectionLifecycle(t *testing.T) {
	proc, runtime := t.TempDir(), t.TempDir()
	fixture(t, proc, 101, "1000", "uuid-a")
	fixture(t, proc, 202, "2000", "uuid-b")
	fixture(t, proc, 303, "3000", "uuid-c")
	domain(t, runtime, "worker-a", "running", 101, "uuid-a")
	domain(t, runtime, "worker-b", "paused", 202, "uuid-b")
	domain(t, runtime, "unselected", "running", 303, "uuid-c")
	s, err := New([]int{101}, "^worker-", proc, runtime)
	if err != nil {
		t.Fatal(err)
	}
	ts, problems, err := s.Resolve()
	if err != nil || len(problems) != 0 || len(ts) != 1 || ts[0].Domain != "worker-a" {
		t.Fatalf("%+v %v %v", ts, problems, err)
	}
	old := ts[0]
	fixture(t, proc, 101, "1001", "uuid-x")
	if s.Check(old) == nil {
		t.Fatal("PID reuse accepted")
	}
	ts, problems, err = s.Resolve()
	if err != nil || len(ts) != 0 || len(problems) != 2 {
		t.Fatalf("reuse: %+v %v %v", ts, problems, err)
	}
	fixture(t, proc, 404, "4000", "uuid-a")
	domain(t, runtime, "worker-a", "running", 404, "uuid-a")
	ts, _, err = s.Resolve()
	if err != nil || len(ts) != 1 || ts[0].PID != 404 {
		t.Fatalf("domain restart: %+v %v", ts, err)
	}
}

func TestPIDListAndValidation(t *testing.T) {
	var p PIDList
	if p.Set("12, 34") != nil || p.Set("34") != nil || p.String() != "12,34" {
		t.Fatal(p)
	}
	for _, v := range []string{"", "0", "-1", "12,no"} {
		if p.Set(v) == nil {
			t.Fatalf("accepted %q", v)
		}
	}
	if p.String() != "12,34" {
		t.Fatal("partially applied invalid list")
	}
	if _, err := New(nil, "", "/proc", "/run/libvirt/qemu"); err == nil {
		t.Fatal("empty selection accepted")
	}
	if _, err := New(nil, "[", "/proc", "/run/libvirt/qemu"); err == nil {
		t.Fatal("invalid regex accepted")
	}
}
