# Modular vhost recovery

A replacement implementation for observing and recovering stalled Linux vhost-net TX queues. It separates temporal recovery policy, Linux access, process discovery, evidence recording and application composition into independently buildable Go modules.

**Implementation review candidate; not production-qualified.** A successful eventfd write is not proof of guest traffic, BFD or BGP recovery. The prior PR stack is closed and has not been merged into this branch.

| Module | Responsibility | Direct project dependencies |
| --- | --- | --- |
| [recovery-core](modules/recovery-core) | Deterministic candidate, retry and verification state | None |
| [vhost-linux](modules/vhost-linux) | Linux process/queue identity, observation and guarded writes | None |
| [qemu-discovery](modules/qemu-discovery) | QEMU generation selection from procfs/libvirt | None |
| [recovery-evidence](modules/recovery-evidence) | Bounded history and episode reduction | None |
| [vhost-agent](apps/vhost-agent) | Application workflows, translation and delivery | The four libraries |
| [vhost-faultlab](apps/vhost-faultlab) | Explicitly scoped disposable fault experiments | vhost-linux |

Libraries do not share a domain-object package. Applications translate public contracts through consumer-owned ports. Internal package edges, test imports and reachable public API types are checked in [the dependency gate](docs/architecture/dependencies.json). Evidence knows neither recovery policy nor Linux; it owns no goroutine, I/O, encoding, clock or callback.

## Build and inspect

Go 1.25 or newer is required. Workspace builds use local modules. Individual release checks use `GOWORK=off`, candidate dependency versions from a temporary file proxy and external public consumers. No module contains a filesystem `replace` directive.

```sh
make test                 # all module race tests and vet
make check                # dependency and public API boundaries
make independent          # isolated module builds and public consumers
make build                # native CLI binaries (non-Linux backend refuses access)
make linux                # Linux amd64 CLI binaries
make bpf                  # Linux: matching observation and trace objects
```

The backend supports Linux amd64, split virtqueues and the kernel facilities documented in its README. Build BPF with Clang, Linux headers and libbpf headers. Generated wire layout includes schema and field-offset checks. Backend BPF objects must come from the same release as its Go code.

## Commands

```sh
vhost-agent list --pid 1234
vhost-agent list-queues --pid 1234 --bpf ./vhost-observe.bpf.o
vhost-agent observe --domain '^example-' --bpf ./vhost-observe.bpf.o
vhost-agent recover --pid 1234 --bpf ./vhost-observe.bpf.o --metrics 127.0.0.1:9090
vhost-agent kick --pid 1234 --bpf ./vhost-observe.bpf.o
vhost-agent trace --pid 1234 --bpf ./vhost-trace.bpf.o --duration 10s
```

`observe` is the default command. Explicit PID selection stays anchored to the first observed process generation. Domain selection can discover a replacement process. Automatic recovery needs outstanding work with no used progress across the configured cadence and a fresh backend decision. Every attempt, including refusals and failed writes, is paced from completion. Accepted writes are counted immediately; later same-generation consumed and used progress is reported separately.

`kick` is the new manual contract: one guarded write per selected supported slot, without candidate timing. It emits one bounded JSON result after each target's effects finish, followed by a command summary. Output failure stops subsequent targets. Exit `0` means complete execution/delivery, `2` means incomplete execution, and `3` means delivery failed. **Never blindly retry exit 3: some or all writes may already have succeeded.** The external recovery-once wrapper should invoke `kick` and consume schema 1; that wrapper is not present in this repository. Removed mode aliases and old schemas are not retained.

Automatic diagnostics are best effort. Fixed-cardinality counters are updated before a nonblocking frame offer. A bounded diagnostic worker calls the evidence recorder; a separate bounded byte queue writes JSON. Missing input sequences invalidate evidence continuity. A stuck destination cannot hold a recovery worker. `--diagnostics=false` removes the evidence path. Metrics distinguish accepted writes, later progress, coverage, input loss and output loss.

Recovery ownership uses a process lease; each backend mutation also takes a short lock shared with manual writes. These locks coordinate cooperating tools only. They cannot make userspace observation and eventfd writes atomic with kernel state changes.

## Review and qualification

Start with [the approved architecture](docs/architecture/design.md), then [implementation review and measured validation](docs/implementation-review.md). Tests exercise public contracts, bounded resources, identity replacement, refusal/failure timing and optional-output isolation. The current branch does not claim replacement-version QEMU traffic/BFD/BGP or fault-injection qualification.

Each module includes its own README and notices. Independent publication still requires selecting the project license, reviewing GPL kernel-asset obligations, publishing dependency versions and qualifying the target kernels; see [release policy](docs/releasing.md). Local candidate versions are not published releases.
