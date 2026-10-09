# vhost-faultlab

A separate executable for a bounded, single-queue lost-wakeup experiment on an
owned disposable Linux amd64 backend kernel. It builds and controls its own GPL
kernel asset and restores through the public `vhost-linux` manual-notification
contract. It does not import the recovery agent, policy or diagnostic libraries.

This implementation has portable workflow tests and Linux build checks. It has
not been loaded into a kernel or qualified on hardware as part of this change.
An experiment does not establish a natural failure's cause.

## Commands

```sh
go build -o ./bin/vhost-faultlab ./cmd/vhost-faultlab

# Build the embedded source against matching headers. This never loads it.
./bin/vhost-faultlab prepare \
  --kernel-build-dir /absolute/path/to/kernel/build \
  --cc cc --output /absolute/new/path/vhost_fault.ko

# Read the host-global module's current state; no recovery write.
./bin/vhost-faultlab show

# Run one explicitly selected process generation and queue slot.
./bin/vhost-faultlab run \
  --pid PID --start-time PROC_START_TIME --slot VHOST_FD \
  --module /absolute/path/vhost_fault.ko \
  --bpf-object /absolute/path/vhost-observation.bpf.o \
  --state-dir /run/vhost-agent \
  --delay 1s --window 500ms --drops 1 --cleanup-timeout 5s

# Explicitly unload a leftover injector. This does not attempt recovery.
./bin/vhost-faultlab clear --state-dir /run/vhost-agent --timeout 5s
```

Supply the expected `/proc/PID/stat` process start generation and current vhost
slot obtained from the host's process/queue listing. These numbers are selection
criteria, not capabilities: the backend opens and verifies a process session,
and returns the generation-bound queue handle used by the experiment. Run with
the kernel/BPF/module permissions required by the host; no command modifies host
security settings or disables kernel protections to make an operation succeed.

The artifact must be a regular x86-64 relocatable ELF with module name
`vhost_fault`. The kernel additionally enforces its normal version, signature and
probe-support checks. `prepare` uses literal build arguments, bounded build logs,
a temporary build directory, and never overwrites an existing output.

## Ownership and cleanup

`run` acquires a cooperating host controller lock, refuses an already loaded
module, opens the selected process/attachment, and calls the opt-in backend
binding API. Its synchronous loader receives only a borrowed pinned FD and
waiter; it does not retain or report them. The kernel acquires its own file
reference during the load. A successful load remains owned even if cancellation
arrives immediately afterward.

The module limits dropping by both monotonic time and count, independently of
the controller. Userspace waits for the selected delay/window, then unloads the
module before issuing one fresh, verified restoration notification. Cancellation
uses a separate bounded cleanup context. Failed unload prevents restoration and
produces an incomplete cleanup result. The same original generation-bound queue
handle is used for restoration: replacement attachments are never followed.

No loop waits indefinitely for spontaneous recovery. An accepted restoration
write is reported as accepted; packet delivery and application recovery remain
unmeasured. Zero observed drops are reported as an experiment failure, even when
cleanup and restoration succeed. Statistics failure cannot skip unloading.

The controller lock coordinates cooperating processes only. It does not prevent
an unrelated privileged tool from unloading/replacing a module. Killing the
controller cannot prolong the kernel's drop window, but can leave the module
loaded and a queue needing manual restoration. `clear` unloads the named module
only; use the agent's manual kick command separately if restoration is needed.
Kernel syscalls already executing are not guaranteed interruptible by context
cancellation; timeout cannot be reported as confirmed cleanup.

All kernel/session/controller cleanup finishes before JSON result delivery.
The result uses schema `faultlab.v1`, separates run, cleanup and restoration
outcomes, and never includes kernel addresses. Exit categories are:

| Code | Meaning |
| --- | --- |
| 0 | Operation and result delivery completed successfully. |
| 1 | Operation, experiment evidence, cleanup or restoration failed. |
| 2 | Invalid invocation. |
| 3 | Result delivery incomplete; an operation may already have taken effect. |

Do not interpret a delivery failure as permission to repeat an injection or a
restoration write. The controller itself has no output dependency. A caller's
arbitrary blocking output writer may delay CLI return after cleanup, but cannot
keep the injector active through an output dependency.

## Component contracts

| Package | Owns | Direct nonstandard dependencies |
| --- | --- | --- |
| `cmd/vhost-faultlab` | OS signal context and process exit | `internal/cli` |
| `internal/cli` | Parsing, resource composition, JSON result projection | `internal/backend`, `internal/experiment`, `internal/module`, public `vhost-linux` |
| `internal/experiment` | Load ownership, bounded wait, unload-before-restore workflow | None; own `Injector`, `Restorer`, `Waiter` ports |
| `internal/backend` | Narrow experimental/load and restoration translations | `internal/experiment`, `internal/module`, public `vhost-linux`, public `experimental/lostwakeup` |
| `internal/module` | Embedded source, artifact validation, build, module syscalls, host lock and state | None; standard library only |

Only `internal/module/syscall_linux.go` uses `unsafe`, solely to marshal the two
Linux module syscall strings. No reflection, linker tricks, backend internals or
raw snapshot layouts are used. The module's only direct Go module dependency is
`vhost-linux`; Go also records that dependency's declared transitive requirements
and checksums for standalone resolution.

## Validation and release

```sh
go test -race ./...
GOOS=linux GOARCH=amd64 go build ./...
go vet ./...
```

Workflow tests use the experiment's own public ports with no real module load or
live queue. They cover preexisting modules, accepted load followed by cancellation
or error, stats failure, failed unload, and restoration outcome preservation.
CLI tests exercise invalid inputs without kernel effects; artifact tests reject
unrelated module names before binding acquisition.

For an independent release, run `GOWORK=off go test ./...` and the Linux build
against the declared backend version. Unpublished development versions can be
served by the repository's local candidate proxy; no sibling-path `replace`
belongs in the released module. Qualify each backend kernel/BTF/toolchain and
probe combination in a disposable environment before using the experiment.

See [NOTICE.md](NOTICE.md) for kernel-asset provenance and retained license.
