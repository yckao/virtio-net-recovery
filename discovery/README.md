# QEMU discovery

A standalone Go library selecting QEMU process generations using explicit PIDs and libvirt runtime XML. It has no recovery, backend, CLI, or telemetry dependencies.

Import `github.com/yckao/virtio-net-recovery/discovery` as `discovery`. The repository requires Go 1.25 and only the standard library. Its public contract is provisional until its first versioned release; the former application's selection structs and CLI options are not compatibility interfaces.

`New(Options)` validates and copies selection options. `Resolve(context)` returns owned targets, bounded per-target problems and a completeness flag. The selector is owned by one serialized caller. Returned identities are discovery hints, never permission to write or a substitute for a pidfd.

## Selection and coverage

Explicit PID selections anchor the first observed generation and never follow its reuse. Matching libvirt domains may follow a new process after the runtime XML UUID matches that process. Results deduplicate by PID and sort admitted targets by PID. This is an observation of changing files, not an atomic system inventory.

An incomplete inventory must not retire previously pinned targets. Malformed or unreadable process files, an identity race between the bracketing stat reads, runtime file errors, target limits and scan limits make coverage incomplete. A confirmed absent process directory, a stable non-QEMU process, an anchored PID's replacement and a runtime/process UUID mismatch are definitive exclusions and do not alone make the inventory incomplete. A later absence of one proc file while its process directory remains present is uncertain.

Directory failures and cancellation return the targets already collected together with an error and `Complete=false`. Callers may use those partial observations, but must retain unknown prior targets. Context cancellation is checked between entries; it cannot interrupt a filesystem syscall already executing. Problem errors are bounded diagnostic text, not a stable error classification API.

## Resource bounds

`MaxTargets` defaults to 128 and accepts 1..4096. One resolution examines at most 16,384 runtime directory entries in batches of 128, including entries that do not match XML selection. It returns at most 256 problems of 1,024 error bytes each. `ProblemsOmitted` counts further failures; any omission makes the inventory incomplete. The scan stops at its budget, so a truncated inventory must not be interpreted as a preference among all matching domains.

Domain names are at most 256 bytes and UUIDs 128 bytes. Larger identities are rejected, not truncated into a different target. Individual runtime XML files are capped at 4 MiB, proc stat at 64 KiB, comm at 256 bytes and cmdline at 1 MiB. Selection retains only the explicit PID anchors; it does not accumulate runtime discovery history.

## Validation and ownership

Run from the repository root:

```sh
go test -race ./discovery/...
go vet ./discovery/...
```

External tests use synthetic procfs/runtime directories through the public API. They cover explicit reuse, domain replacement, transient missing coverage, definitive removal, returned option ownership and all output/scan bounds. These tests do not establish live process pinning; that belongs to the consuming backend.

The selector owns admission and completeness. Its process reader owns bounded, bracketed process identity observations; its runtime reader owns streamed directory scanning and XML validation. Neither reader calls a recovery engine, backend or application. No public type leaks another component's implementation type.

## Provenance

The process and libvirt matching mechanism derives from this repository's reviewed `internal/selection` implementation at commit `3146e10245e2b92bc022c134648c6fc43d858fa5`. The standalone contract adds explicit completeness, resource limits, bracketed reads and private options/anchors. No third-party implementation or kernel asset is included. Independent publication requires the repository's license/provenance release check; this candidate does not claim that a publication license has been selected.
