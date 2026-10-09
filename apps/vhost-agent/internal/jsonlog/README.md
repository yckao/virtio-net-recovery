# Bounded JSON delivery

`New(options, io.Writer)` creates one bounded record queue. `Start(ctx)` launches
one destination worker. `TryWrite(report.Record)` validates fixed input limits,
encodes an owned payload and attempts nonblocking admission; it never calls
the supplied writer. Public report values are independent of evidence/core
types; only this package owns JSON tags and the versioned wire projection.

Count and byte limits include the single in-flight record and reserve the
largest delivery envelope metadata. The worker uses at most one additional
`MaxRecordBytes` encoding buffer while wrapping that in-flight payload. Routine snapshots
are coalesced by stream/kind, with a quarter of queue capacity reserved for
action/terminal traffic. Important records first evict old routine records, then
old queued outcomes. A blocked in-flight write cannot be evicted. All rejection,
coalescing, eviction, failed-write and shutdown-discard losses are counted in
`Stats.Dropped`; `Stats.Errors` counts destination failures and is a different,
overlapping unit. Do not add these counters. No durable delivery is promised.

Each line is a versioned delivery envelope:

```json
{"schema_version":1,"delivery_incomplete":true,"output_lost":2,"record":{"schema_version":1,"event":"action"}}
```

The `record` object contains the presentation payload (the example omits its
other fields). The destination worker stamps `output_lost` with cumulative record
loss immediately before writing, so an already queued record reports preceding
losses without relying on metrics. `delivery_incomplete` remains true after any
loss. These delivery fields are separate from `record.incomplete`, which describes
the evidence itself. A loss while the last write is already blocked can only be
reported by a later successfully delivered line or by local statistics.

`Shutdown(ctx)` closes admission and drains until the caller's deadline. If the
destination is blocked, it discards waiting records and returns the deadline
error without waiting for that writer. At most one in-flight buffer/goroutine
remains until the write returns. Reusable callers must supply a sink they can
cancel independently and subsequently join. A CLI may exit after backend cleanup.
No goroutine is created per record and shutdown never closes a borrowed writer.

Production dependencies are `report` public values and the standard library.
There is no knowledge of a recorder, recovery policy, Linux backend, metrics
implementation or control worker.
