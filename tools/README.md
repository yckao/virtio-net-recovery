# Architecture and independent release checks

Run from the repository root:

```sh
go test ./tools/check-boundaries
go run ./tools/check-boundaries
python3 tools/test-independent.py --boundaries
```

`check-boundaries` reads `docs/architecture/dependencies.json`. Every authored
package has an exact direct-import allowlist and separate additional test imports.
Standard-library imports are discovered from `go list`, not guessed from spelling.
An optional `stdlib_imports` list restricts production imports: omitted/null means
unrestricted, while an empty list permits none. The policy and evidence packages
allow only `errors` and `time`; adding `io`, `os` or `net/http` fails the gate. Test
standard-library imports remain unrestricted, and production sources are checked
separately from test variants. This is a limited purity guard: allowing `time` for
`time.Duration` does not prove an implementation avoids clocks, timers or goroutines;
those semantic restrictions still require review.
The checker loads production and test package graphs for the host and Linux/amd64,
including platform-specific and generated Go sources. A new package or dependency
requires a reviewed manifest change with a reason. Dependency cycles also fail Go's
own package loading. Test-only integration edges never authorize production edges.

The public API check uses Go compiler export archives with `go/importer` and
`go/types`. It walks exported functions, values, aliases, named types, exported or
embedded fields, promoted methods, interface terms, generic constraints and type
arguments, pointers, arrays, slices, maps and channels. An allowed dependency cannot
silently expose a third component through an alias or nested field. Foreign private
types and private-package leaks from public release packages are rejected. Ordinary
unexported implementation fields remain opaque and are not treated as API leaks.

The checker's temporary fixtures prove rejection of production imports, test-only
imports, transitive aliases and nested generic/container leaks, and acceptance of
opaque private state. The fixture packages are compiled and inspected through the
same checker as the real source. The check is a dependency/API gate, not a security
sandbox or a proof against semantic coupling, reflection, filesystem side channels,
serialization conventions, unsafe casts or runtime callbacks. Code review must
still enforce the documented component-knowledge rule.

The workspace contains development-only, exact-version replacements for the four
unpublished `v0.1.0` candidates. With the current toolchain, `use` identifies all
workspace modules but external-package graph loading still requests candidate
version metadata. The explicit version replacements resolve that metadata locally.
No release module's `go.mod` contains a replacement. Remove or update the development
entries deliberately when the dependency versions change; they are never release
independence evidence.

`test-independent.py` creates an isolated file module proxy and cache in a temporary
directory. It archives each library at candidate version `v0.1.0`, copies all six
release units and the tooling outside the workspace, and uses `GOWORK=off`. It
checks declared module requirements without filesystem replacements, runs native
race tests, vet and builds, and compiles/vets Linux/amd64 tests. Four external
consumers exercise only public APIs and resolve source from the candidate archives.
Linux cross-compilation uses `-exec=true` and is explicitly not test execution.

The default run is offline: third-party archives are read from the existing Go
download cache and copied into the isolated cache. It does not overwrite the user's
module cache or fetch unpublished GitHub versions. `--online` allows the official Go
proxy for uncached third-party dependencies. Checksum entries remain verified;
offline mode disables remote checksum-service access. Upstream dependency tests
are not downloaded merely to tidy a consuming module.

Use `--keep` to retain sources, archive hashes, environment values and `report.json`.
Failures retain their workspace automatically. `--prepare-only` creates an isolated
candidate proxy and `environment.json` for additional manual validation. The runner
may fill checksums in temporary copies but uses readonly module graphs for tests,
vet and builds. It does not edit maintained module manifests or sum files.

These checks do not compile BPF C assets, load probes, interact with QEMU, inject a
fault or qualify a kernel. Run separate disposable-kernel qualification before
claiming runtime compatibility, recovery correctness or measured overhead.
