package discovery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	discovery "github.com/yckao/virtio-net-recovery/modules/qemu-discovery"
)

func TestUncertainProcessObservationCannotDeclareRemoval(t *testing.T) {
	for _, kind := range []string{"malformed stat", "missing stat", "unreadable comm", "oversized UUID"} {
		t.Run(kind, func(t *testing.T) {
			proc := t.TempDir()
			process(t, proc, 101, 1000, "id-a")
			s, err := discovery.New(discovery.Options{PIDs: []int{101}, ProcRoot: proc})
			if err != nil {
				t.Fatal(err)
			}
			if r, err := s.Resolve(context.Background()); err != nil || !r.Complete || len(r.Targets) != 1 {
				t.Fatal(r, err)
			}
			dir := filepath.Join(proc, "101")
			switch kind {
			case "malformed stat":
				err = os.WriteFile(filepath.Join(dir, "stat"), []byte("incomplete"), 0600)
			case "missing stat":
				err = os.Remove(filepath.Join(dir, "stat"))
			case "unreadable comm":
				// A directory produces a deterministic read failure without relying
				// on the test user's permission or privilege configuration.
				if err = os.Remove(filepath.Join(dir, "comm")); err == nil {
					err = os.Mkdir(filepath.Join(dir, "comm"), 0700)
				}
			case "oversized UUID":
				process(t, proc, 101, 1000, strings.Repeat("u", discovery.MaxUUIDBytes+1))
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Resolve(context.Background())
			if err != nil || r.Complete || len(r.Targets) != 0 || len(r.Problems) != 1 {
				t.Fatalf("uncertain identity became removal: %+v %v", r, err)
			}
		})
	}
}

func TestDefinitiveExclusionRemainsComplete(t *testing.T) {
	for _, kind := range []string{"absent", "not QEMU", "generation replaced"} {
		t.Run(kind, func(t *testing.T) {
			proc := t.TempDir()
			process(t, proc, 101, 1000, "id-a")
			s, err := discovery.New(discovery.Options{PIDs: []int{101}, ProcRoot: proc})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Resolve(context.Background()); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "absent":
				err = os.RemoveAll(filepath.Join(proc, "101"))
			case "not QEMU":
				err = os.WriteFile(filepath.Join(proc, "101", "comm"), []byte("unrelated\n"), 0600)
			case "generation replaced":
				process(t, proc, 101, 2000, "id-a")
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Resolve(context.Background())
			if err != nil || !r.Complete || len(r.Targets) != 0 || len(r.Problems) != 1 {
				t.Fatalf("definitive exclusion became uncertain: %+v %v", r, err)
			}
		})
	}
}

func TestRuntimeIdentityAndErrorTextAreBounded(t *testing.T) {
	for _, data := range []string{
		`<domstatus state="running" pid="101"><domain><name>` + strings.Repeat("n", discovery.MaxDomainNameBytes+1) + `</name><uuid>id-a</uuid></domain></domstatus>`,
		`<domstatus state="running" pid="101"><domain><name>worker</name><uuid>` + strings.Repeat("u", discovery.MaxUUIDBytes+1) + `</uuid></domain></domstatus>`,
		"<" + strings.Repeat("x", discovery.MaxProblemBytes*4) + "></different>",
	} {
		proc, dir := t.TempDir(), t.TempDir()
		process(t, proc, 101, 1000, "id-a")
		if err := os.WriteFile(filepath.Join(dir, "a.xml"), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		s, err := discovery.New(discovery.Options{DomainPattern: ".*", ProcRoot: proc, RuntimeDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Resolve(context.Background())
		if err != nil || r.Complete || len(r.Targets) != 0 || len(r.Problems) != 1 {
			t.Fatalf("unbounded identity accepted: %+v %v", r, err)
		}
		if len(r.Problems[0].Err.Error()) > discovery.MaxProblemBytes || len(r.Problems[0].Domain) > discovery.MaxDomainNameBytes {
			t.Fatal("unbounded public problem")
		}
	}
}

func TestProblemHistoryIsBoundedAndOverflowIsVisible(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < discovery.MaxProblems+5; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d.xml", i)), []byte("invalid"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := discovery.New(discovery.Options{DomainPattern: ".*", RuntimeDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Resolve(context.Background())
	if err != nil || r.Complete || len(r.Problems) != discovery.MaxProblems || r.ProblemsOmitted != 5 {
		t.Fatalf("wrong bounded result: %+v %v", r, err)
	}
}

func TestRuntimeScanBudgetIncludesUnselectedEntries(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i <= discovery.MaxRuntimeEntries; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%05d.skip", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := discovery.New(discovery.Options{DomainPattern: ".*", RuntimeDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Resolve(context.Background())
	if err != nil || r.Complete || len(r.Problems) != 1 || len(r.Targets) != 0 {
		t.Fatalf("scan limit silently produced complete inventory: %+v %v", r, err)
	}
}
