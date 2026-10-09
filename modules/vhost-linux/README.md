# vhost-linux

A standalone Linux/amd64 vhost-net TX observer and verified eventfd notifier. The
module does not import recovery policy, evidence, discovery or an application.

## Public contract

Open one `Host` with a matching BPF object, mutation-lock directory and host-wide
queue limit. Open a `ReadSession` with a mandatory PID and `/proc/<pid>/stat`
start-time generation. `Inventory` creates opaque queue handles. `Sample` batches
cached guest-ring indices; `Inspect` obtains current attachment and ring evidence.
`Classify` is the same immediate physical eligibility rule used by conditional
notification. It does not decide whether a retry is due.

`Host.Conditional` and `Host.Manual` explicitly construct different writer views.
A reader cannot call notification through `ReadSession`. Conditional notification
checks the expected used index and fresh pending/work state; manual notification
has no ring-progress prerequisite. Both pin the vhost/eventfd descriptors, validate
identity, require an already nonblocking eventfd, take a short cooperative mutation
lock and write at most once. Neither changes QEMU file flags.

`Result` separates an unattempted write, an attempted failure and an accepted write.
Every result includes completion time. A conditional receipt includes the final
pre-write used/consumed baseline; manual receipts explicitly lack that baseline.
Cancellation after the write cannot retract a receipt. Application reporting has
no callback into this operation. An accepted eventfd write is not proof of recovery.

`Inventory.Complete` distinguishes complete directory knowledge from partial
knowledge. `Problems` names observed slots whose attachments could not be read.
Such a problem is not evidence that the slot was removed. Total failure returns an
error. Public inventory size is bounded to the configured queue count plus one
aggregate capacity problem. The private procfs scan streams 128 entries at a time,
caps descriptor work at 65,536 entries, and marks exhaustion incomplete. Only a
complete inventory retires absent slots. Handles never follow a
replacement generation, and handles from another session or host are rejected.

Sample accepts up to the configured queue limit, internally partitions reads into
private 512-queue syscall batches, and invalidates all results if any read fails. Cached `Consumed` and
`WorkQueued` fields retain their older attachment sample; `Live=false` prevents
`Classify` from treating them as fresh. The host admits at most `MaxQueues` queues
and process sessions. Returned slices/observations never share mutable ring storage.

## Ownership and concurrency

A host owns its BPF collection, optional links, bounded cancellable snapshot
mailbox, and trace-registration leases. A process session owns its pidfd, generation
ledger and cached read batch. Operations serialize per session; separate sessions
share only the host snapshot mechanism. Closing the host drains sessions before
closing BPF resources. Closing a session invalidates its handles. Close is
idempotent. Blocking kernel syscalls are not claimed to have hard deadlines.

Trace must be selected when opening the host and uses the matching trace object.
Normal mode does not load traffic-path programs. `TraceSession` is read-only;
closing its view leaves the underlying read session alive. The host owns shared
probe links until host close; process queue leases own address registrations.
Registration reference counts protect shared attachments. Inflight trace mappings
carry an epoch, preventing an old handler return from decrementing a replacement
queue's counters. Kprobe/kretprobe coverage and sampling are not causal proof.

The optional `experimental/lostwakeup` package exposes a synchronous scoped binding
for a trusted fault injector. Its sensitive borrowed FD and waiter are valid only
inside the callback. The callback performs one synchronous kernel load; the kernel
must acquire its own reference before it returns. Never log or retain the waiter,
load asynchronously, or perform unrelated fallible work after loading. The bridge
never loads a module. This opt-in Go API is not a security boundary.

## Build and use

Requirements: Go 1.25+, Linux amd64, clang with the BPF target, libbpf headers,
Linux headers, usable host and module BTF, required vhost symbols, pidfd access,
and the permissions needed for BPF, process memory reads and descriptor duplication.
The procfs mount must describe the same PID namespace as syscalls. Unsupported
kernels/ring formats fail closed; no mechanism fallback is attempted.

```sh
make generate
make
GOWORK=off go test ./...
go run ./examples/inspect --pid PID --start-time START_TIME
```

`build/vhost-observe.bpf.o` and `build/vhost-trace.bpf.o` are separate release assets.
Private generated wire schema 2 includes C/Go checks for every field offset and
struct size. Regeneration must produce no diff. Do not combine objects from another
release: a runtime schema mismatch refuses observation.

Only little-endian split rings without IOMMU translation are supported. The BPF
snapshot explicitly selects vhost-net TX slot 1; callers cannot select an RX slot.
The pointer bindings are private implementation details, never exported observations.

Pinning and revalidation do not make attachment checks and the write atomic against
kernel reconfiguration. A 16-bit index match does not prove that no full wrap
occurred. User-memory sampling is not an atomic ring snapshot. The lock coordinates
cooperating tools using the same state directory, not all privileged writers.

## Validation and release status

Portable contract tests cover physical classification, the complete guarded write
sequence through a private OS port, final progress/work revalidation, identity
replacement, cancellation, receipt preservation and attempted-write distinctions.
Linux tests additionally exercise real eventfds, nonblocking saturation, batched
self-memory reads, pidfd polling and short mutation locks without a VM.

Privileged QEMU/BPF tests are a separate release qualification requirement. Source
builds and fake/syscall tests do not qualify kernel symbol/BTF compatibility, live
VM recovery, tracing overhead or fault-injection coexistence. The initial revision
is pre-production and carries no such runtime qualification claim.

## Internal component dependencies

- Public `vhost`: owns sessions and operation semantics; depends only on standard
  library and private `internal/kernel` and `internal/binding` contracts.
- `internal/kernel`: owns procfs/pidfd/memory/BPF transport and wire schema; depends
  only on standard library, cilium/ebpf and x/sys. It knows no public/application type.
- `internal/binding`: two private value contracts; no runtime dependencies.
- `experimental/lostwakeup`: translates the public session into the opt-in private
  binding protocol; no syscall, loader, policy or application dependency.
- `examples/inspect`: uses only the public module contract and standard library.

A release must build with `GOWORK=off`, ship its matching BPF assets and notices,
and pass independent public-consumer and privileged kernel qualification gates.
