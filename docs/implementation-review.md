# Implementation review

Status: revised candidate for owner review, 2026-10-09. PR #8 remains a draft. The previous implementation was rejected for excessive packaging machinery and insufficient internal separation. This revision changes the code and ownership model, not just the directory layout.

## Review entry points

| Concern | Source | Review question |
| --- | --- | --- |
| Build weight | Root `go.mod`, `Makefile` | Can an ordinary root Go command build and test everything? |
| Component independence | `internal/architecture/architecture_test.go` | Are dependencies explicit for public components and application packages, including tests/platform files? |
| Command flow | `internal/agent/cli/run.go`, `kick.go`, `queues.go` | Is dispatch performed once, with relevant flags and direct command flow? |
| Manual ownership | `internal/agent/control/commands.go`, `manual.go` | Do typed results preserve receipts and errors through cancellation and cleanup? |
| Recovery worker | `internal/agent/control/worker.go` | Are inventory, per-queue policy, effects and coverage distinct responsibilities? |
| Process lifetime | `internal/agent/supervision/supervisor.go` | Does one session own reader/notifier/lease cleanup, and are failed admissions visible? |
| Evidence internals | `evidence/recorder.go`, `history.go`, `episode.go` | Does the recorder own atomic observation ordering without producer knowledge? |
| Optional telemetry | `internal/agent/telemetry` | Are input/output loss independent, retained data bounded and final admitted frames drained? |
| Linux requests | `vhost/internal/kernel/probe_linux.go`, `vhost/session_linux.go` | Are snapshots attributed to the requesting thread and known queues preserved under capacity? |
| Experiment | `internal/faultlab/cli`, `experiment/controller.go` | Are commands direct and unload-before-restoration errors preserved? |

## Corrections

- Removed six module manifests, `go.work`, the candidate proxy runner and the separate boundary-check executable. Public package boundaries remain explicit in one root module.
- Removed the mode-driven `oneShot/executeTarget` path. Listing and kicking acquire distinct capabilities and return distinct result types. Errors remain errors until presentation. Malformed inventories cannot repeat a manual write.
- Each command accepts only its own flags. Command output has one writer for the invocation. An accepted kick receipt survives cancellation through a separate delivery deadline; cancellation and output failure prevent later target effects.
- Removed control-owned telemetry stream IDs, registry and delivery counters. A scoped observer is the only optional telemetry port. Invalid cached observations and absent live samples cannot become fresh evidence.
- Reduced diagnostics/report/JSON queue layers to one telemetry package with two bounded FIFOs, drop-new behavior and distinct reducer/writer ownership. Closing a queue drains its admitted final observations before retirement. Each optional service gets its own shutdown deadline.
- Evidence receives sample and candidate state atomically, avoiding false quiet closure/reopening at the same boundary. Sample acquisition time remains distinct from record accounting time.
- Failed process admissions and incomplete inventory have authoritative counters. Explicit zero-admission daemon requests fail with diagnostics disabled. Attempt outcomes include read failures and cancellation.
- Trace requires one complete sweep across all frozen targets; timeout before that is incomplete. Cleanup errors survive ordinary cancellation.
- BPF snapshot attribution now matches the locked requesting OS thread. Wire schema 3 rejects old mailbox objects. Queue capacity reconciles known slots and confirmed removals before new admissions; truncation is explicit instead of a fake FD.
- Faultlab also uses direct handlers and typed presentation. An accepted restoration preserves any subsequent error instead of clearing it.

## Validation

Root race tests, vet and native builds pass. Linux amd64 packages and tests cross-compile and pass vet. Boundary tests parse all Go files regardless of the current OS. Generated wire files reproduce byte-for-byte and the C layout's static assertions compile with the local C compiler.

Focused regressions cover cancellation after accepted writes, cleanup failures, partial inventory, duplicate slots, first-sweep trace coverage, process admission, malformed observation batches, evidence quiet-boundary ordering, admitted-frame retirement, trailing input gaps and output failure. A deterministic test runs the actual recovery worker with diagnostics disabled and with a blocked writer; attempt timestamps and accepted counts match.

Cross-compilation is not Linux test execution. The modified BPF program has not been loaded or exercised against a live QEMU process in this revision. Previous implementation tests and closed-PR Lab measurements are historical and do not qualify this source. No Lab/VM operation was performed during this rewrite.

The host currently has no running Docker daemon or BPF-capable compiler. Container/BPF compilation is configured in the repository workflow; any remote run result is reported separately in the PR. No image is published by that workflow.

## Remaining limits

This candidate has no live QEMU traffic, BFD/BGP, sustained multi-target overhead or fault-injection qualification. Backend generation checks and cooperating locks do not create an atomic transaction with kernel state; unobserved ABA remains a limitation. A blocked arbitrary `io.Writer` cannot be force-cancelled, so one bounded writer goroutine can remain until the destination returns or the CLI process exits.

The recovery-once wrapper is external and must adopt `vhost-agent kick` and schema 1. Exit 3 must not trigger automatic retries. Project-wide distribution licensing remains undecided; retained kernel notices do not license all Go source.
