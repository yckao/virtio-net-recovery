# Diagnostic adapter

`New(options, registry, sink)` creates a bounded frame queue without starting
resources. `Start(ctx)` starts one telemetry actor. `TryOffer(control.Frame)`
only copies/admit bounded values; it never calls the registry, reducer or sink.
The control producer increments its authoritative input-loss count when false is
returned. `Shutdown(ctx)` stops admission, drains the bounded queue and retires
recorders, with a caller-supplied deadline.

Direct production dependencies are the public contracts of `control`, `report`
and the independent `recovery-evidence` module, plus the standard library.
Mapping belongs here; neither side imports the other's types. The sink's
`TryWrite` and registry's `Active` methods must be bounded and nonblocking.
Production composition uses the bounded JSON adapter, never an arbitrary user
callback on this path. Tests composing a control worker and JSON adapter are
explicit integration tests, not private-state fixtures.

The actor checks registry retirement before allocating state and periodically
while idle. Both the admitted stream registry and `MaxStreams` bound the map.
A lost retirement frame cannot retain old generations; stale queued frames
cannot recreate them. Explicit retirement keeps a bounded tombstone until the
registry retires that stream. Recorder errors similarly disable one stream's
diagnostics without affecting control state.

Each stream's sequence identifies missing frames. A gap closes its episode as
incomplete, clears history and reapplies the frame's current sample/candidacy.
Repeated old sample timestamps are ignored; newly admitted samples use the
frame's monotonic time for reduction. Live values alone supply fresh consumed
and work state. Other frames never confer freshness on cached values.

`Snapshot` is a nonblocking atomic diagnostic view. `LostFrames` observes gaps
and discarded queued frames; it overlaps the producer's authoritative input-loss
counter and must not be added to it. `OutputLost` counts immediate sink refusals
only. The JSON writer's `Dropped` additionally includes eviction and destination
failure, and is the authoritative output-loss total. `Recorders` counts bounded
tracked entries, including disabled/retired tombstones awaiting reconciliation.
