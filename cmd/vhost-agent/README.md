# vhost-agent

The composition entry point for the four reusable packages. Commands are `observe`, `recover`, `kick`, `trace`, `list` and `list-queues`; `COMMAND --help` shows only its relevant flags.

The [design](../../docs/architecture/design.md) describes package ownership. Start implementation review with:

- `internal/agent/cli/run.go`: one command dispatch.
- `internal/agent/control/commands.go`: typed listing/manual results, effects and cleanup.
- `internal/agent/control/worker.go`: queue workflow and optional observer port.
- `internal/agent/supervision/supervisor.go`: process admission and owned session lifetime.
- `internal/agent/telemetry`: bounded evidence reduction and JSON delivery.

A command result is converted to JSON after that target's resources close. Exit 3 means delivery failed and may follow accepted writes; the external recovery-once wrapper must not retry it automatically. Trace receives read-only capabilities and treats an empty capture as incomplete.

Automatic recovery accounts execution before optional reporting. A full diagnostics queue drops new input or output and increments the corresponding counter. `--diagnostics=false` creates no evidence or JSON worker. Metrics reads fixed counters without kernel access; its listener admits at most 32 connections with header/write/idle deadlines. Explicit PID requests fail if no target can initially be admitted, including with diagnostics disabled.

The default cadence is 100 ms and inventory refresh is 5 s. These are configurable scheduling values, not a throughput guarantee. Accepted eventfd writes and later queue progress do not prove packet delivery.

Build and test from the repository root with `go build ./cmd/vhost-agent` and `go test -race ./...`. There is one Go module. Portable tests and cross-builds do not qualify a kernel/QEMU combination; see [current validation](../../docs/implementation-review.md).
