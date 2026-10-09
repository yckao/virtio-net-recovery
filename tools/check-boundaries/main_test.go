package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func fixture(t *testing.T, files map[string]string, rules map[string]packageRule) checker {
	t.Helper()
	t.Setenv("GOWORK", "off")
	root := t.TempDir()
	files["go.mod"] = "module fixture.invalid/boundary\n\ngo 1.25.0\n"
	for name, text := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return checker{root: root, rules: manifest{Version: 1, Modules: []moduleRule{{Dir: ".", Path: "fixture.invalid/boundary"}}, Packages: rules}}
}
func TestRejectsUndeclaredProductionImport(t *testing.T) {
	c := fixture(t, map[string]string{"pure/pure.go": "package pure; import _ \"fixture.invalid/boundary/secret\"", "secret/secret.go": "package secret"}, map[string]packageRule{"pure": {}, "secret": {}})
	err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH)
	if err == nil || !strings.Contains(err.Error(), "imports undeclared dependency fixture.invalid/boundary/secret") {
		t.Fatalf("expected import rejection, got %v", err)
	}
}
func TestRejectsUndeclaredTestImport(t *testing.T) {
	c := fixture(t, map[string]string{"pure/pure.go": "package pure", "pure/pure_test.go": "package pure_test; import _ \"fixture.invalid/boundary/secret\"", "secret/secret.go": "package secret"}, map[string]packageRule{"pure": {}, "secret": {}})
	err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH)
	if err == nil || !strings.Contains(err.Error(), "tests import undeclared dependency fixture.invalid/boundary/secret") {
		t.Fatalf("expected test import rejection, got %v", err)
	}
}
func TestRejectsPublicAliasTransitiveLeak(t *testing.T) {
	c := fixture(t, map[string]string{
		"secret/secret.go":   "package secret; type Secret struct{ Value int }",
		"allowed/allowed.go": "package allowed; import \"fixture.invalid/boundary/secret\"; type Public = secret.Secret",
		"pure/pure.go":       "package pure; import \"fixture.invalid/boundary/allowed\"; type Leak = allowed.Public",
	}, map[string]packageRule{"secret": {}, "allowed": {Imports: []string{"fixture.invalid/boundary/secret"}}, "pure": {Imports: []string{"fixture.invalid/boundary/allowed"}}})
	err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH)
	if err == nil || !strings.Contains(err.Error(), "exposes nondependency type fixture.invalid/boundary/secret.Secret") {
		t.Fatalf("expected alias leak rejection, got %v", err)
	}
}
func TestAllowsOpaquePrivateImplementationState(t *testing.T) {
	c := fixture(t, map[string]string{
		"secret/secret.go": "package secret; type Secret struct{ Value int }",
		"pure/pure.go":     "package pure; import \"fixture.invalid/boundary/secret\"; type Handle struct{ private secret.Secret }; func (Handle) Value() int {return 1}",
	}, map[string]packageRule{"secret": {}, "pure": {Imports: []string{"fixture.invalid/boundary/secret"}}})
	if err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH); err != nil {
		t.Fatal(err)
	}
}
func TestRejectsGenericConstraintAndContainerLeaks(t *testing.T) {
	c := fixture(t, map[string]string{
		"secret/secret.go":   "package secret; type Secret struct{ Value int }; type Constraint interface { ~int }",
		"allowed/allowed.go": "package allowed; import \"fixture.invalid/boundary/secret\"; type Value struct { Send chan map[string]*secret.Secret }; type Constraint = secret.Constraint",
		"pure/pure.go":       "package pure; import \"fixture.invalid/boundary/allowed\"; type Public[T allowed.Constraint] struct { Value allowed.Value }",
	}, map[string]packageRule{"secret": {}, "allowed": {Imports: []string{"fixture.invalid/boundary/secret"}}, "pure": {Imports: []string{"fixture.invalid/boundary/allowed"}}})
	err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH)
	if err == nil || !strings.Contains(err.Error(), "exposes nondependency type fixture.invalid/boundary/secret.") {
		t.Fatalf("expected reachable type rejection, got %v", err)
	}
}

func TestRejectsUnapprovedProductionStandardLibraryImports(t *testing.T) {
	c := fixture(t, map[string]string{
		"pure/pure.go": "package pure; import (_ \"time\"; _ \"os\")",
	}, map[string]packageRule{"pure": {StandardImports: []string{"errors"}}})
	err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH)
	if err == nil || !strings.Contains(err.Error(), "imports unapproved standard-library dependency time") || !strings.Contains(err.Error(), "imports unapproved standard-library dependency os") {
		t.Fatalf("expected standard-library purity rejection, got %v", err)
	}
}

func TestStandardLibraryConstraintDoesNotRestrictTestImports(t *testing.T) {
	c := fixture(t, map[string]string{
		"pure/pure.go":      "package pure; import \"errors\"; var Err = errors.New(\"example\")",
		"pure/pure_test.go": "package pure_test; import (_ \"os\"; _ \"time\"; \"testing\"); func TestExample(t *testing.T) {}",
	}, map[string]packageRule{"pure": {StandardImports: []string{"errors"}}})
	if err := c.checkPlatform(runtime.GOOS + "/" + runtime.GOARCH); err != nil {
		t.Fatal(err)
	}
}
