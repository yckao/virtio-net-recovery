// Package architecture checks component boundaries without a separate build tool.
package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/yckao/virtio-net-recovery/"

// Application interfaces point inward. Only the CLI composes all adapters.
var applicationImports = map[string][]string{
	"internal/agent/control":       {"recovery"},
	"internal/agent/supervision":   {"internal/agent/control"},
	"internal/agent/backend":       {"internal/agent/control", "vhost"},
	"internal/agent/selection":     {"internal/agent/control", "discovery"},
	"internal/agent/telemetry":     {"internal/agent/control", "evidence"},
	"internal/agent/metrics":       {},
	"internal/agent/lease":         {},
	"internal/agent/cli":           {"internal/agent/control", "internal/agent/backend", "internal/agent/selection", "internal/agent/supervision", "internal/agent/telemetry", "internal/agent/metrics", "internal/agent/lease", "discovery", "vhost"},
	"cmd/vhost-agent":              {"internal/agent/cli"},
	"internal/faultlab/experiment": {},
	"internal/faultlab/module":     {},
	"internal/faultlab/backend":    {"internal/faultlab/experiment", "internal/faultlab/module", "vhost", "vhost/experimental/lostwakeup"},
	"internal/faultlab/cli":        {"internal/faultlab/backend", "internal/faultlab/experiment", "internal/faultlab/module", "vhost"},
	"cmd/vhost-faultlab":           {"internal/faultlab/cli"},
}

// Parse every source file, including tests and files excluded on the current OS.
// This protects dependency direction, not runtime purity or API compatibility.
func TestComponentDependencies(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	for _, component := range []string{"recovery", "evidence", "discovery", "vhost", "internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, component), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			for _, spec := range file.Imports {
				dependency, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				if !allowedImport(filepath.ToSlash(relative), dependency) {
					t.Errorf("%s must not import %s", relative, dependency)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func allowedImport(file, dependency string) bool {
	dir := filepath.ToSlash(filepath.Dir(file))
	test := strings.HasSuffix(file, "_test.go")
	// Pure production packages have an intentionally short stdlib allowance.
	// Tests and examples may format output, use clocks, and read fixtures.
	if !strings.HasPrefix(dependency, module) {
		if (dir == "recovery" || dir == "evidence") && !test {
			return dependency == "errors" || dependency == "time"
		}
		if !strings.Contains(strings.Split(dependency, "/")[0], ".") {
			info, err := os.Stat(filepath.Join(runtime.GOROOT(), "src", dependency))
			return err == nil && info.IsDir() && dependency != "C"
		}
		return dir == "vhost/internal/kernel" && (dependency == "github.com/cilium/ebpf" ||
			dependency == "github.com/cilium/ebpf/link" || dependency == "github.com/cilium/ebpf/rlimit" ||
			dependency == "golang.org/x/sys/unix")
	}
	dependency = strings.TrimPrefix(dependency, module)
	if allowed, application := applicationImports[dir]; application {
		return slices.Contains(allowed, dependency) || (test && dependency == dir)
	}
	for _, component := range []string{"recovery", "evidence", "discovery"} {
		if within(dir, component) {
			// Public packages are independently usable; their examples and
			// external tests consume only their own public package.
			return dependency == component && (test || dir != component)
		}
	}
	switch {
	case dir == "vhost":
		return dependency == "vhost/internal/kernel" || dependency == "vhost/internal/binding" ||
			(test && (dependency == "vhost" || dependency == "vhost/experimental/lostwakeup"))
	case dir == "vhost/internal/kernel", dir == "vhost/internal/binding":
		return test && dependency == dir
	case dir == "vhost/experimental/lostwakeup":
		return dependency == "vhost" || dependency == "vhost/internal/binding" || (test && dependency == dir)
	case within(dir, "vhost/examples"):
		return dependency == "vhost"
	default:
		return false
	}
}

func within(path, parent string) bool {
	return path == parent || strings.HasPrefix(path, parent+"/")
}

func TestRejectedDependencies(t *testing.T) {
	for _, tc := range []struct{ file, dependency string }{
		{"recovery/policy.go", "os"},
		{"evidence/recorder.go", "net/http"},
		{"recovery/policy_test.go", module + "evidence"},
		{"discovery/process_linux.go", module + "vhost"},
		{"vhost/session_linux.go", module + "recovery"},
		{"vhost/session_linux.go", module + "vhost/experimental/lostwakeup"},
		{"vhost/internal/kernel/probe_linux.go", module + "vhost"},
		{"internal/agent/backend/session_linux.go", module + "vhost/experimental/lostwakeup"},
		{"internal/agent/backend/session_test.go", module + "internal/faultlab/backend"},
		{"internal/agent/control/worker.go", module + "internal/agent/backend"},
		{"internal/agent/control/worker_test.go", module + "internal/agent/telemetry"},
		{"internal/agent/backend/session.go", module + "evidence"},
		{"internal/faultlab/experiment/controller.go", module + "internal/faultlab/backend"},
		{"cmd/vhost-agent/main.go", module + "vhost"},
		{"internal/faultlab/backend/injector.go", module + "evidence"},
		{"internal/agent/backend/session.go", "github.com/cilium/ebpf"},
		{"vhost/internal/kernel/probe_linux.go", "github.com/cilium/ebpf/future"},
	} {
		if allowedImport(tc.file, tc.dependency) {
			t.Errorf("allowed forbidden dependency %s -> %s", tc.file, tc.dependency)
		}
	}
}
