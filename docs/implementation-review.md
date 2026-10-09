# Modular replacement: implementation review

Status: implementation candidate for owner review, 2026-10-09. The previous five pull requests were closed without merging or deleting their branches. This replacement starts from clean trunk `2bb7f4c`; it does not incorporate the rejected PR stack's architecture. Existing unrelated checkout changes remain preserved.

## Review the actual code

| Concern | Entry point | What to inspect |
| --- | --- | --- |
| Temporal policy | `modules/recovery-core/policy.go`, `completion.go`, `verification.go` | Validated operation receipts; completion pacing; generation replacement; first accepted baseline; no side effects |
| Evidence internals | `modules/recovery-evidence/recorder.go`, `history.go`, `episode.go` | Own closed input contract, owned history copies, bounded output, gap reset, late verification attribution; imports only `errors` and `time` |
| Linux effect boundary | `modules/vhost-linux/notify_operation.go`, `notify_linux.go` | Final live physical classification, pinned descriptors, one write, actual acceptance timestamp and cleanup ownership |
| Application control | `apps/vhost-agent/internal/control/worker.go`, `manual.go`, `trace.go` | Consumer-owned ports; one policy per slot; invalid inventory breaks continuity; accepted counters before telemetry |
| Composition/lifetime | `apps/vhost-agent/internal/supervision/supervisor.go`, `internal/cli` | Replacement joins cleanup; unknown inventory retains workers; cleanup failure prevents replacement; manual output never repeats writes |
| Diagnostics/delivery | `apps/vhost-agent/internal/diagnostics`, `internal/jsonlog` | Separate bounded workers, sequence loss invalidates evidence, output backpressure never runs on a recovery worker |
| Experiment | `apps/vhost-faultlab/internal/experiment`, `internal/backend` | Independent controller; unload before restoration; narrow synchronous borrowed binding; no agent/core/evidence dependency |
| Enforced dependencies | `docs/architecture/dependencies.json`, `tools/check-boundaries` | Exact package/test edges and reachable public types; no accidental producer types in evidence |

No global interface registry, shared domain-model warehouse, generic event bus or compatibility layer was added. The application owns explicit translations because the meanings differ: a backend physical decision, a policy result, an evidence input and a delivered record are separate contracts. The overhead is a small amount of intentional mapping code and six release manifests.

The only temporary filesystem replacements are exact candidate versions in the development `go.work`, required while those module versions are unpublished. Every release module's `go.mod` is replace-free; independent tests disable the workspace entirely.

## Executable review

```sh
make test check
make independent
# Pure examples: no privileges, backend, observer or evidence exporter required.
go run ./modules/recovery-core/examples/replay
go run ./modules/recovery-evidence/examples/replay
make linux
```

Public contract tests cover completion-based retry spacing, refusal/failure accounting, unavailable samples, process/attachment replacement, partial inventory, ring wrap, history ownership, gaps, late verdicts and bounded churn. A deterministic application integration runs the actual control worker with diagnostics disabled and then with a permanently blocked writer. Attempt timestamps and authoritative counts match; a 50 ms operation plus a 100 ms cadence yields 150 ms spacing. A blocked writer occupies one bounded in-flight slot and shutdown returns on its deadline.

Boundary tests deliberately introduce a forbidden production import, forbidden test import, exported alias leak and nested generic/container type leak. They are rejected. Core and evidence also have explicit production standard-library allowlists (`errors`, `time`); extra I/O imports fail. An opaque type with private state is accepted. Static checks enforce declared dependencies, not conceptual independence by themselves; the package ownership table and implementation remain part of review.

## Measured Linux checks

Executed on an isolated Linux amd64 probe VM (`6.12.51-0-virt`), separate from the existing Lab workloads:

- Backend guard/public-contract tests executed successfully.
- Real eventfd write/saturation, process memory batch read/partial-read invalidation and cooperative mutation lock tests executed successfully.
- Observation and trace BPF objects compiled with Clang 20.1.8 against Linux/libbpf headers.
- Both BPF program sets loaded and attached. Five open/close cycles per mode completed with a stable six-descriptor process count after each close. This tests resource lifecycle, not traffic-path correctness under load.
- Both Linux CLI binaries ran their help entrypoints.
- `vhost-faultlab prepare` compiled its embedded kernel asset against available `6.12.112-0-virt` headers. The artifact was not loaded; its build kernel differs from the probe's running kernel.

Temporary compiler/header package groups and test files were removed, the test driver was unloaded, and the probe VM was returned to its prior stopped state. APK dependency resolution updated the probe's existing musl, xz-libs, zlib and libexpat packages; those base-library updates remain. No production/Lab workloads or experiment module were touched.

The independent candidate-proxy gate also copies all six modules plus repository tooling outside the workspace, runs native race tests/vet/build, compiles and vets Linux variants, and runs four standalone public consumers. Linux cross-compilation is explicitly distinct from executing the syscall and BPF checks above.

## Deliberate limits and integration changes

This delivery does not claim live QEMU queue recovery, sustained multi-target overhead, traffic-path trace correctness, BFD/BGP continuity or fault-injection cleanup qualification for the replacement. Those require a separately owned disposable QEMU/guest workload on the target kernel. Historical results from the closed PRs are not evidence for this source.

The backend detects observed attachment generations; an unobserved complete ABA reconfiguration cannot be ruled out by a raw-address identity tuple. Generation checks, descriptor pinning and locks reduce race exposure but do not form an atomic kernel transaction. Locks coordinate cooperating instances only.

The recovery-once caller is external to this repository. Its replacement command is `vhost-agent kick`. Schema 1 emits one bounded target-result line after that target's effects finish, then a command-summary line. A delivery failure stops subsequent targets and exits 3; receipts may already represent accepted writes. The caller must not blindly retry exit 3. No legacy aliases or result serializers were retained.

The project-wide distribution license is still unspecified. Backend/fault kernel notices are retained; independent public release additionally requires the licensing and version steps in `releasing.md`. Container build recipes and CI are updated; local container images have not been built because no Docker daemon was running.
