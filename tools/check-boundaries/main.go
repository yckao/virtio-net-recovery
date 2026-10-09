// Command check-boundaries enforces reviewed package imports and exported API
// dependencies for every module, including tests and platform-specific sources.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

type moduleRule struct {
	Dir  string `json:"dir"`
	Path string `json:"path"`
}
type packageRule struct {
	Imports         []string `json:"imports"`
	StandardImports []string `json:"stdlib_imports,omitempty"`
	TestImports     []string `json:"test_imports"`
	Reason          string   `json:"reason"`
}
type manifest struct {
	Version  int                    `json:"version"`
	Modules  []moduleRule           `json:"modules"`
	Packages map[string]packageRule `json:"packages"`
}
type listedPackage struct {
	ImportPath, Name, Dir, Export, ForTest string
	Standard                               bool
	Imports, TestImports, XTestImports     []string
	Error                                  *struct{ Err string }
	DepsErrors                             []struct{ Err string }
}

type checker struct {
	root  string
	rules manifest
}

func main() {
	root := flag.String("root", ".", "Workspace root")
	policy := flag.String("manifest", "docs/architecture/dependencies.json", "Dependency policy relative to root")
	platforms := flag.String("platforms", runtime.GOOS+"/"+runtime.GOARCH+",linux/amd64", "Comma-separated GOOS/GOARCH targets")
	flag.Parse()
	absolute, err := filepath.Abs(*root)
	if err != nil {
		fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(absolute, *policy))
	if err != nil {
		fatal(err)
	}
	var rules manifest
	if err = json.Unmarshal(data, &rules); err != nil {
		fatal(err)
	}
	if rules.Version != 1 {
		fatal(errors.New("unsupported dependency manifest version"))
	}
	c := checker{root: absolute, rules: rules}
	seen := map[string]bool{}
	for _, platform := range strings.Split(*platforms, ",") {
		if seen[platform] {
			continue
		}
		seen[platform] = true
		if err = c.checkPlatform(platform); err != nil {
			fatal(err)
		}
		fmt.Printf("boundaries verified: %s\n", platform)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func canonical(path string) string {
	if i := strings.Index(path, " ["); i >= 0 {
		return path[:i]
	}
	return path
}
func moduleFor(path string, modules []moduleRule) string {
	longest := ""
	for _, m := range modules {
		if (path == m.Path || strings.HasPrefix(path, m.Path+"/")) && len(m.Path) > len(longest) {
			longest = m.Path
		}
	}
	return longest
}
func (c checker) ruleFor(p listedPackage) (packageRule, bool) {
	root, err := filepath.EvalSymlinks(c.root)
	if err != nil {
		return packageRule{}, false
	}
	dir, err := filepath.EvalSymlinks(p.Dir)
	if err != nil {
		return packageRule{}, false
	}
	relative, err := filepath.Rel(root, dir)
	if err != nil {
		return packageRule{}, false
	}
	rule, ok := c.rules.Packages[filepath.ToSlash(relative)]
	return rule, ok
}
func (c checker) list(platform string) ([]listedPackage, error) {
	parts := strings.Split(platform, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid platform: %s", platform)
	}
	var result []listedPackage
	for _, m := range c.rules.Modules {
		cmd := exec.Command("go", "list", "-deps", "-test", "-export", "-json", "./...")
		cmd.Dir = filepath.Join(c.root, m.Dir)
		cmd.Env = append(os.Environ(), "GOOS="+parts[0], "GOARCH="+parts[1], "CGO_ENABLED=0")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go list %s (%s): %w\n%s", m.Dir, platform, err, stderr.String())
		}
		decoder := json.NewDecoder(bytes.NewReader(stdout))
		for {
			var p listedPackage
			err := decoder.Decode(&p)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			result = append(result, p)
		}
	}
	return result, nil
}
func (c checker) checkPlatform(platform string) error {
	packages, err := c.list(platform)
	if err != nil {
		return err
	}
	standard := map[string]bool{"C": true, "unsafe": true}
	exports := map[string]string{}
	for _, p := range packages {
		path := canonical(p.ImportPath)
		if p.Standard {
			standard[path] = true
		}
		if p.Export != "" && p.ForTest == "" && !strings.Contains(p.ImportPath, " [") {
			exports[path] = p.Export
		}
	}
	lookup := func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no compiler export archive for %s", path)
		}
		return os.Open(file)
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", lookup)
	checked := map[string]bool{}
	var violations []string
	for _, p := range packages {
		path := canonical(p.ImportPath)
		if moduleFor(path, c.rules.Modules) == "" || p.Standard {
			continue
		}
		// Compiler-generated testmain has no authored dependency decisions.
		if p.Name == "main" && strings.HasSuffix(path, ".test") {
			continue
		}
		key := p.ImportPath + "|" + p.Dir
		if checked[key] {
			continue
		}
		checked[key] = true
		rule, ok := c.ruleFor(p)
		if !ok {
			violations = append(violations, "undeclared package: "+path)
			continue
		}
		testVariant := p.ForTest != "" || strings.Contains(p.ImportPath, " [")
		allow := allowedSet(rule.Imports)
		standardAllow := allowedSet(rule.StandardImports)
		if testVariant {
			for _, i := range rule.TestImports {
				allow[i] = true
			}
			allow[strings.TrimSuffix(path, "_test")] = true
		}
		for _, dep := range p.Imports {
			dep = canonical(dep)
			if standard[dep] && !testVariant && rule.StandardImports != nil && !standardAllow[dep] {
				violations = append(violations, fmt.Sprintf("%s imports unapproved standard-library dependency %s", p.ImportPath, dep))
			}
			if !standard[dep] && !allow[dep] {
				violations = append(violations, fmt.Sprintf("%s imports undeclared dependency %s", p.ImportPath, dep))
			}
		}
		testAllow := allowedSet(rule.Imports)
		for _, i := range rule.TestImports {
			testAllow[i] = true
		}
		testAllow[path] = true
		for _, dep := range append(append([]string{}, p.TestImports...), p.XTestImports...) {
			dep = canonical(dep)
			if !standard[dep] && !testAllow[dep] {
				violations = append(violations, fmt.Sprintf("%s tests import undeclared dependency %s", path, dep))
			}
		}
		if testVariant {
			continue
		}
		pkg, err := imp.Import(path)
		if err != nil {
			return fmt.Errorf("read API %s: %w", path, err)
		}
		surface := surfaceChecker{owner: path, module: moduleFor(path, c.rules.Modules), allowed: allow, standard: standard, seen: map[types.Type]bool{}}
		for _, name := range pkg.Scope().Names() {
			obj := pkg.Scope().Lookup(name)
			if obj.Exported() {
				surface.walk(obj.Type(), path+"."+name)
			}
		}
		violations = append(violations, surface.violations...)
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		return fmt.Errorf("dependency violations (%s):\n%s", platform, strings.Join(unique(violations), "\n"))
	}
	return nil
}
func allowedSet(items []string) map[string]bool {
	out := map[string]bool{}
	for _, item := range items {
		out[item] = true
	}
	return out
}
func unique(items []string) []string {
	out := items[:0]
	for _, item := range items {
		if len(out) == 0 || out[len(out)-1] != item {
			out = append(out, item)
		}
	}
	return out
}

type surfaceChecker struct {
	owner, module     string
	allowed, standard map[string]bool
	seen              map[types.Type]bool
	violations        []string
}

func (c *surfaceChecker) object(obj *types.TypeName, where string) bool {
	if obj == nil || obj.Pkg() == nil {
		return true
	}
	path := obj.Pkg().Path()
	if c.standard[path] {
		return false
	}
	if path != c.owner {
		if !c.allowed[path] {
			c.violations = append(c.violations, fmt.Sprintf("%s exposes nondependency type %s.%s", where, path, obj.Name()))
		}
		if !obj.Exported() {
			c.violations = append(c.violations, fmt.Sprintf("%s exposes foreign private type %s.%s", where, path, obj.Name()))
		}
		// Public release packages must not make their own internals a public contract;
		// internal application adapters may use declared internal neighbor contracts.
		if strings.Contains(path, "/internal/") && (!strings.HasPrefix(path, c.module+"/") || !strings.Contains(c.owner, "/internal/")) {
			c.violations = append(c.violations, fmt.Sprintf("%s exposes private package type %s.%s", where, path, obj.Name()))
		}
	}
	return true
}
func (c *surfaceChecker) parameters(params *types.TypeParamList, where string) {
	if params == nil {
		return
	}
	for i := 0; i < params.Len(); i++ {
		c.walk(params.At(i).Constraint(), where+" constraint")
	}
}
func (c *surfaceChecker) walk(t types.Type, where string) {
	if t == nil || c.seen[t] {
		return
	}
	c.seen[t] = true
	switch t := t.(type) {
	case *types.Basic:
	case *types.Alias:
		if !c.object(t.Obj(), where) {
			return
		}
		c.parameters(t.TypeParams(), where)
		for i := 0; i < t.TypeArgs().Len(); i++ {
			c.walk(t.TypeArgs().At(i), where+" alias argument")
		}
		c.walk(t.Rhs(), where+" alias")
	case *types.Named:
		if !c.object(t.Obj(), where) {
			return
		}
		c.parameters(t.TypeParams(), where)
		for i := 0; i < t.TypeArgs().Len(); i++ {
			c.walk(t.TypeArgs().At(i), where+" argument")
		}
		c.walk(t.Underlying(), where)
		for _, candidate := range []types.Type{t, types.NewPointer(t)} {
			methods := types.NewMethodSet(candidate)
			for i := 0; i < methods.Len(); i++ {
				method := methods.At(i).Obj()
				if method.Exported() {
					c.walk(method.Type(), where+"."+method.Name())
				}
			}
		}
	case *types.Pointer:
		c.walk(t.Elem(), where)
	case *types.Slice:
		c.walk(t.Elem(), where)
	case *types.Array:
		c.walk(t.Elem(), where)
	case *types.Map:
		c.walk(t.Key(), where+" key")
		c.walk(t.Elem(), where+" value")
	case *types.Chan:
		c.walk(t.Elem(), where+" channel")
	case *types.Struct:
		for i := 0; i < t.NumFields(); i++ {
			field := t.Field(i)
			if field.Exported() || field.Embedded() {
				c.walk(field.Type(), where+"."+field.Name())
			}
		}
	case *types.Signature:
		c.parameters(t.TypeParams(), where)
		c.parameters(t.RecvTypeParams(), where)
		c.walk(t.Params(), where+" parameter")
		c.walk(t.Results(), where+" result")
	case *types.Tuple:
		for i := 0; i < t.Len(); i++ {
			c.walk(t.At(i).Type(), where)
		}
	case *types.Interface:
		t.Complete()
		for i := 0; i < t.NumMethods(); i++ {
			c.walk(t.Method(i).Type(), where+"."+t.Method(i).Name())
		}
		for i := 0; i < t.NumEmbeddeds(); i++ {
			c.walk(t.EmbeddedType(i), where+" embedded")
		}
	case *types.TypeParam:
		c.walk(t.Constraint(), where+" constraint")
	case *types.Union:
		for i := 0; i < t.Len(); i++ {
			c.walk(t.Term(i).Type(), where+" union")
		}
	default:
		c.violations = append(c.violations, fmt.Sprintf("%s has unchecked type %T", where, t))
	}
}
