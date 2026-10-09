# Optional telemetry

This application package owns the complete diagnostic pipeline. It implements
the control package's scoped observation port and the supervisor's notice port.
It consumes the evidence package's public API. It knows no kernel, discovery,
policy or metrics implementation.

```text
control scopes / notices -> bounded FIFO -> evidence actor
                                             |
                                        bounded FIFO
                                             |
                                        JSON writer
```

`Start(out, maxStreams)` starts exactly these two goroutines. `Open(target, queue)`
returns a scope with `Observe(frame)` and `Close(at)`. One control worker
serializes calls on each scope. Telemetry owns the stream ID, input sequence and
pending count; control owns sample validity, candidate and verification facts.
The actor uses `Frame.SampleAt`, supplied in the run's monotonic epoch, for
acquisition time. It never substitutes frame presentation time or rechecks ring
geometry. The sample and candidate state enter evidence as one observation.

Scope close is nonblocking and cannot drop admitted final frames. The actor
retires a closed scope only after its pending frames have drained. Close wakes
a lifecycle scan; ordinary frames look up only their own recorder. A final
sequence check also exposes rejected trailing frames when there is no later
observation. There is no active-stream registry shared with control.

The application joins all producers before `Telemetry.Close(ctx)`. Close drains
the input FIFO, retires remaining recorders, then drains the output FIFO. Calls
do not require a separate constructor/start/restart protocol. If a borrowed
`io.Writer` blocks, Close returns the deadline error and marks the drain
incomplete. The application has already completed backend cleanup. An in-process
caller must independently unblock its writer to join the one remaining goroutine;
generic `Write` cannot be cancelled by this package.

## Overload and ownership

The input FIFO holds 256 bounded frames/notices; the output FIFO holds 64 private,
owned presentation records. Full queues drop the new item immediately. There is
no priority scheduler, eviction, coalescing, producer-side encoding or transport
callback. Product counters are maintained before telemetry in control, so a
missing action record does not erase an accepted write.

Input loss is counted exactly once at rejection. A detected sequence gap marks
the episode incomplete but does not increment that counter a second time.
Output loss is counted exactly once for a rejected or failed output record.
After destination failure, the writer drains and counts records without retrying
the destination. Recorder errors disable only that scope's evidence; direct
action/decision/verification facts remain eligible for reporting.

Text is copied with 128-byte target-name and 256-byte message limits. Each
recorder retains 16 samples; each output record has at most 16 copied samples.
Scope admission allows `maxStreams + InputCapacity` handles, including retired
scopes still draining. Excess scopes discard observations with counted input
loss instead of allocating more state. Output memory is bounded by 64 such DTOs,
one in-flight DTO and a JSON encoding buffer capped at 64 KiB per line. Bounds
come from fixed item sizes and channel capacities, not interacting byte/count
quota algorithms. A blocked writer cannot stop evidence reduction or control.

The JSON writer alone owns the schema and encoding. Each line contains
`schema_version`, cumulative `input_lost` and `output_lost`,
`delivery_incomplete`, and a `record` object. `record.evidence_incomplete` is
separate from delivery loss. Previously queued records receive current counters
when encoded. A loss during the final blocked write is visible only through
local `Snapshot()` or a subsequent delivered line. Verification records are
projected directly from supplied control facts; the actor does not send them
through the recorder merely to format the same fact twice.

Run `go test -race ./internal/agent/telemetry` from the repository root. External
tests cover accepted-frame draining, quiet-boundary ordering, input/output loss,
source validity, blocked/failed writers, bounded churn and actual control pacing
with telemetry disabled versus a permanently blocked destination.
