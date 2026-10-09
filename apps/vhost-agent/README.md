# vhost-agent

Application composition for the four public recovery libraries. Canonical commands are `observe`, `recover`, `kick`, `trace`, `list` and `list-queues`. Run `vhost-agent help` for flags. This is an implementation review candidate with a new schema 1; it does not preserve the retired CLI/JSON contract.

## Internal ownership

| Package | Responsibility | Allowed project dependencies |
| --- | --- | --- |
| control | Per-slot recovery workflow, manual effects, accounting and consumer ports | recovery-core |
| supervision | Target/worker lifetimes and partial-discovery reconciliation | control |
| backend | Opaque Linux handle translation and capability views | control, vhost-linux |
| selection | Public selector translation | control, qemu-discovery |
| diagnostics | Bounded frame admission, evidence mapping and stale-stream cleanup | control, report, recovery-evidence |
| report | Bounded presentation values | None |
| jsonlog | Versioned encoding and bounded destination delivery | report |
| metrics | Fixed unlabelled snapshot HTTP presentation | None |
| lease | Automatic-owner cross-process exclusion | None |
| cli | Flags and composition of actual adapters | Declared adapters and public dependency constructors |
| cmd/vhost-agent | Signal handling and process exit | cli |

Ports are owned by the consuming workflow. No library imports the application. Translations are intentionally explicit: changing a library contract requires updating the adapter that uses it, rather than spreading shared structs through the program. Backend generation handles never leave the backend adapter.

## Effects, output and limits

Recovery accounts authoritative attempts/writes before offering fixed-size frames. The diagnostic path cannot acquire a recovery lock or invoke a notifier. Input gaps terminate complete evidence continuity; output loss does not rewrite execution counters. Queued frame and JSON-byte limits include stalled work. An arbitrary `io.Writer` cannot be forcibly cancelled by Go, so bounded shutdown abandons its one blocked worker; the CLI process then exits. No worker-per-record retry is created.

Manual effects are collected once per target before rendering that target's bounded result. Delivery failure stops subsequent targets. Exit 3 reports delivery failure and must never trigger an automatic retry. Trace has a frozen target/queue set, bounded duration, and terminates on missing evidence or output failure. Trace constructs no writer capability.

The default retry/sample cadence is 100 ms and inventory refresh is 5 s. These are provisional values, not a throughput guarantee. Each target owns its policy state; a shared backend serializes snapshot mailbox access and applies a host-wide queue bound. Requested metrics binding errors surface before target discovery starts. The metrics endpoint performs no kernel reads.

## Development

Use the repository workspace or the independent-candidate proxy gate. `GOWORK=off go test -race ./...` requires published or candidate copies of the four declared module versions. No private sibling source is copied into this module. Tests drive public workflow contracts and simulated adapters; successful portable tests do not qualify a kernel/QEMU combination.

Maintain application schema and module version independently of the backend wire ABI. There are no compatibility aliases or hidden producer type assertions in evidence. Licensing and backend qualifications are recorded in the repository release notes and module notices.

Diagnostic JSON wraps each evidence/presentation record in a schema 1 delivery envelope: `delivery_incomplete` and cumulative `output_lost` describe output delivery; `record.incomplete` describes evidence continuity. HTTP metrics admit at most 32 concurrent connections with header/write/idle deadlines.
