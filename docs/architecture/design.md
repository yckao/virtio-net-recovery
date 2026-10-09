# Design and ownership

Status: revised implementation candidate after the owner rejected the first implementation. The previous multi-module workspace design is superseded. Review the source alongside [validation and limits](../implementation-review.md).

## Structure

One repository and one Go module are sufficient. The boundaries that matter are ownership of state, effects and public contracts:

```text
recovery/       temporal policy; errors + time only
evidence/      diagnostic reduction; errors + time only
discovery/     bounded procfs/libvirt selection
vhost/         Linux capabilities and private BPF/syscall implementation
internal/
   agent/
     control/      application workflows and consumer-owned ports
     supervision/  selected process admission and owned session lifetimes
     backend/      vhost-to-control translation
     selection/    discovery-to-control translation
     telemetry/    optional evidence reduction and JSON delivery
     metrics/      HTTP presentation of fixed counters
     lease/        cooperating automatic-recovery ownership
     cli/          command parsing, adapter composition and command presentation
   faultlab/     separate experiment application
   architecture/ import-boundary tests
cmd/
   vhost-agent/
   vhost-faultlab/
```

The four public packages have no dependencies on one another. Their exported values belong to their own contract. A policy knows elapsed observations and operation receipts; it does not know a Linux session. Evidence knows observations and diagnostic facts; it does not import policy events or recompute virtqueue geometry. Discovery returns process identity hints, not authority to mutate a process. The backend owns opaque process/queue capabilities and physical checks, not temporal retry policy.

A library can be extracted with its own source, tests, examples and assets. Extraction would introduce a new module path and actual release policy at that time. Six manifests, unpublished version placeholders and a local proxy do not improve these ownership boundaries.

## Application dependencies

| Consumer | Direct project dependencies | Owns |
| --- | --- | --- |
| control | recovery | Per-queue scheduling; manual and trace workflows; execution accounting; effect/observer ports |
| supervision | control | Process admission, replacement, worker join and session cleanup |
| backend | control, vhost | Opaque queue mapping and capability translation |
| selection | control, discovery | Process selection translation |
| telemetry | control, evidence | Queue observer lifetime, evidence mapping, bounded delivery and loss accounting |
| metrics | None | HTTP exposure of an owned fixed snapshot |
| lease | None | Automatic-owner lock lifetime |
| cli | Concrete adapters and their public constructor contracts | Flags, construction, shutdown and presentation |
| faultlab | Its own experiment/module/backend packages, vhost experimental API | Disposable experiment lifecycle |

The production agent cannot import the experiment application or experimental backend API. Components cannot reach through another component to its implementation. Go's `internal` rule protects the Linux implementation; ordinary AST tests enforce the project import graph, including platform-specific files and tests. These checks establish structural constraints; human review still checks responsibilities, interpretations and lifecycle coupling.

## Commands and ownership

Command selection happens once. Each command parses only relevant flags and invokes a typed workflow. There is no mode-driven `oneShot` executor, result union or business logic conditional on a command string.

- `list` resolves process identities without creating a backend.
- `list-queues` calls `control.ListQueues` with a read-only source. The use case owns its acquired session until cleanup and returns a `QueueListing`.
- `kick` calls `control.KickTarget` with a manual-capability source. It attempts each supported queue once, retains each receipt, closes the session and returns a `KickReport`. A refusal does not repeat a write or prevent another queue's attempt. Cancellation prevents subsequent attempts and preserves previous receipts.
- `trace` freezes a bounded target/queue set and captures through read-only capabilities. An empty capture is incomplete, even if its duration elapsed normally. Delivery failure ends that capture.
- `observe` and `recover` compose the same worker with different capabilities. Only recovery gets a notifier and an automatic-owner lease.

A backend adapter owns native queue handles. A command or supervisor owns a session, and that session's `Close` releases its resources. Supervisor replacement joins and closes the previous generation before opening a successor. Cleanup failures remain failures. Complete discovery may retire absent targets; incomplete discovery retains uncertain pinned targets.

Errors remain error values through use cases. Command JSON conversion is presentation, after resource cleanup. One output writer serves the invocation and has no retry queue. A delivery deadline can stop waiting for an arbitrary `io.Writer`; it cannot cancel the writer itself. At most that one goroutine remains blocked until the process exits or the writer returns. Exit 3 stops later targets and does not authorize replaying earlier writes.

## Temporal and physical recovery

The recovery policy has three private responsibilities: candidate progress, operation correlation/completion pacing, and first-write verification. Every transition validates its inputs before changing state. There is one in-flight operation and no retained event history. Missing or invalid observations break candidate continuity. Healthy observations do not construct diagnostic machinery.

The application worker refreshes queue inventory, applies samples to per-slot policy, performs requested effects and accounts actual results. Policy state survives observed attachment replacement only where appropriate: pacing/counters remain, candidate/verification baselines do not. Sample validity comes from the policy/physical contract already evaluated by the producer. Evidence does not infer it from a subset of ring fields.

The backend pins process/descriptor identity, performs a fresh physical classification and writes at most once. Cooperating mutation locks coordinate instances but do not form an atomic transaction with kernel queue state. Request-bound probe attribution prevents simultaneous Host instances in one process from consuming each other's snapshot. Known admitted queues are reconciled before new admissions; bounded inventory truncation is explicit.

Accepted writes are counted before optional output. Later valid same-generation consumed and used progress is reported separately. Neither fact alone proves guest packet delivery, BFD/BGP continuity or causality. An unobserved complete attachment ABA or full index wrap cannot be excluded by userspace samples.

## Evidence and telemetry internals

The pure evidence recorder owns bounded history, episode transitions and late verdict attribution. Its atomic observation includes sample validity and candidate state; the recorder applies both before quiet expiry. The caller does not need to order separate inputs to avoid closing and reopening an episode at the same timestamp. Input gaps invalidate continuity and clear baselines. Returned records own their data.

A worker opens one scoped queue observer, publishes bounded frames and closes that observer when its generation retires. It does not allocate telemetry stream IDs, sequences, registry entries or loss counters. Optional telemetry owns those concerns.

Telemetry has two workers: an evidence actor consumes admitted input; a JSON writer consumes reduced output. Both queues are bounded FIFOs using drop-new. No priority eviction, coalescing, shared report package or generic event bus is needed. Limits on streams, history, queue capacities and owned string fields bound retained memory. A full or blocked destination cannot wait on the control path. Input loss and output loss each have one owner and different meanings.

Retirement waits until admitted frames for that observer have been reduced. Application shutdown first joins control producers, then drains telemetry input, then output. A blocked writer produces an explicit shutdown error on the deadline. The design promises bounded retained work, not hard realtime isolation from the shared scheduler or garbage collector.

## Coverage and failure

Fixed counters retain attempts, accepted/refused/read-failed/write-failed/cancelled outcomes, later progress, failed polls, discovery and admission errors. Gauges expose active/selected/unavailable targets, sampled/unavailable queues and truncated inventories. Diagnostics cannot erase execution counters. Metrics performs no kernel reads and does not acquire worker locks.

An explicit daemon request with zero initial admissions fails even with diagnostics disabled. Dynamic domain discovery may legitimately wait for a process that does not yet exist. A failure to open one target does not pretend that target is covered. Ordinary output failure cannot change recovery permission, timing or verification.

## Experiments and tradeoffs

The fault executable has its own controller and ports. It owns a module only after load acceptance, unloads before restoration and preserves cleanup failures. Kernel time/drop limits remain independent of userspace cancellation. The production executable has no path to construct its experimental binding.

The design accepts a small amount of explicit translation and similar command scaffolding where meanings differ. It avoids premature repository splitting, generic command engines, shared cross-component DTOs and complicated loss-priority policies. These choices reduce the number of concepts required to understand a single operation. Future abstractions require a demonstrated repeated responsibility, not merely similar lines of code.
