# Vhost recovery

Observe and recover stalled Linux vhost-net TX queues. One Go module contains four reusable packages and two executables. Each library owns its data and has no dependency on the other three libraries.

**Review candidate; not production-qualified.** An accepted eventfd write, later queue progress and restored guest traffic are separate claims.

| Package | Owns | Project dependencies |
| --- | --- | --- |
| [recovery](recovery) | Candidate timing, completion-paced retries and verification | None |
| [evidence](evidence) | Bounded sample history and diagnostic episodes | None |
| [discovery](discovery) | QEMU generation selection from procfs/libvirt | None |
| [vhost](vhost) | Linux queue capabilities, observation and guarded writes | Its private implementation |
| [vhost-agent](cmd/vhost-agent) | Recovery workflows, adapter composition and presentation | The four libraries |
| [vhost-faultlab](cmd/vhost-faultlab) | Bounded disposable fault experiments | vhost |

Independent ownership is enforced at package boundaries. There is one `go.mod`, no workspace, local replacements, candidate proxy or separate release matrix. A future library extraction can move its source and tests and change its import path; today's build does not carry that release machinery.

## Build

Go 1.25 or newer:

```sh
go test -race ./...
go vet ./...
go build ./cmd/...
# Equivalent convenience targets:
make test check build
make linux                 # cross-build Linux amd64 executables
make bpf                   # Linux: matching observation and trace objects
```

The backend supports Linux amd64 and split virtqueues. BPF compilation needs Clang, Linux and libbpf headers. Package the Go backend with BPF objects built from the same source. See [backend requirements](vhost/README.md).

## Commands

```sh
vhost-agent list --pid 1234
vhost-agent list-queues --pid 1234 --bpf ./vhost-observe.bpf.o
vhost-agent observe --domain '^example-' --bpf ./vhost-observe.bpf.o
vhost-agent recover --pid 1234 --bpf ./vhost-observe.bpf.o --metrics 127.0.0.1:9090
vhost-agent kick --pid 1234 --bpf ./vhost-observe.bpf.o
vhost-agent trace --pid 1234 --bpf ./vhost-trace.bpf.o --duration 10s
```

`observe` is the default. Use `COMMAND --help` for that command's flags. Explicit PIDs stay anchored to their first observed process generation. Domain selection may discover replacement processes. Explicit daemon selection fails if no requested target can initially be admitted; dynamic domain selection may wait for future targets.

Recovery requires outstanding work with no used progress across the cadence and a fresh backend decision. Each attempt is paced from completion, including refusals and errors. Verification retains the first accepted write's baseline; retries do not reset it. Recovery continues when optional diagnostics are disabled or their destination stalls.

`kick` performs one guarded write per supported selected slot. Its typed use case owns acquisition, effects and cleanup; presentation runs afterward. Schema 1 emits a result per target and a final summary. Exit `0` means execution and delivery completed, `2` means incomplete execution, and `3` means delivery failed. **Do not automatically retry exit 3: writes may already have succeeded.** The external recovery-once wrapper should call `kick`; that wrapper is not in this repository.

Telemetry uses a bounded input FIFO, a separate evidence worker, a bounded output FIFO and one JSON writer. Full queues drop new records. Input loss and output loss are counted separately; admitted observations drain before retirement. Evidence owns no producer types, I/O, clock or goroutine. `--diagnostics=false` constructs none of this path. Metrics read execution, coverage and delivery counters without touching kernel state.

## Review

[Design and ownership](docs/architecture/design.md) explains dependencies, internal responsibilities and tradeoffs. [Implementation review](docs/implementation-review.md) records corrections and verification limits. This candidate does not claim live QEMU traffic, BFD/BGP continuity or fault-injection qualification.

Kernel asset notices are retained. The repository has not selected a project-wide distribution license; [release notes](docs/releasing.md) distinguish reusable boundaries from publication readiness.
