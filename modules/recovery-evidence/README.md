# Recovery evidence

This Go module turns a caller's queue observations into bounded diagnostic
episodes. It can replay synthetic or recorded evidence inputs without importing
the recovery policy, a Linux backend, or an application.

It does not detect candidates, authorize writes, verify recovery, serialize
records, collect product metrics, or deliver output. The caller supplies those
facts and owns scheduling, delivery, and process/attachment identity.

## Public contract

Create one `Recorder` per attachment generation with `New(options, streamID)`.
Both stream and attempt references are nonzero numeric values in the caller's
namespace. An episode ID is a local ordinal, so identify an episode by stream
and episode together. A new attachment needs a new recorder and stream ID.

Call `Apply` serially with one of the following **value** inputs:

| Input | Meaning |
| --- | --- |
| `SampleObserved` | Copy an observation into bounded history. `Sample.At` supplies the input time. |
| `CandidateStateObserved` | `Active: true` may open an episode or cancel quiet; false starts quiet. |
| `ActionObserved` | Record a supplied action result; never infer a candidate or recovery. |
| `VerificationObserved` | Record a supplied verdict, correlated by its originating attempt. |
| `GapObserved` | Close an active episode as incomplete and discard its pre-gap baselines/history. |
| `TimeAdvanced` | Check deadlines; never infer quiet or open a candidate. |
| `Retired` | Close the episode and clear history permanently. Repeated retirement is idempotent. |

`Input` is a closed interface, not a plugin extension point. Pointers, embedded
interface wrappers, unknown variants, invalid enums and negative timestamps are
rejected. All timestamps are durations from a caller-defined monotonic epoch.
Backward time is rejected, and all rejected inputs leave state unchanged.

Send a valid sample before a positive candidate state. Without a current valid
sample, positive candidate state is accepted but does not open an episode.
Samples and actions never open episodes by themselves. After a gap, send the
new current sample and candidate state; do not rely on a previously delivered
candidate edge. A positive candidate cancels an established quiet period
immediately, but cannot extend an episode's lifetime. Expiry is evaluated when
inputs arrive, using their time; no timer runs inside the recorder.

`Valid` and field freshness are facts supplied by the producer. Invalid samples
can be retained as diagnostic observations but never count as progress. A
consumed/work value with `Fresh: false` must be zero. Source describes provenance
only; it does not silently confer freshness. Progress compares valid observations
to an episode baseline; the first fresh consumed observation establishes that
baseline. It does not assert that an action caused progress.

The first action in an episode produces an immediate record. Later actions
share the ordinary record interval with sample snapshots, while all action
totals remain in the closing record. Actions outside an episode yield standalone
records with `EpisodeID == 0`. Exactly-once input delivery belongs to the caller;
the recorder does not keep an unbounded attempt-deduplication ledger.

Verification records always have `EpisodeID == 0` and the supplied `AttemptID`.
This avoids attributing a late verdict to an unrelated currently open episode.
The caller can correlate against prior action records by stream and attempt.
The recorder does not retain old episodes, start a verification deadline, or
recompute a verification predicate.

Every returned `Record` owns its `Samples` slice. Callers may retain or modify
returned values without changing later records or recorder state. Returned
records contain no borrowed handles, callbacks, arbitrary payloads or JSON tags.

## Bounds and failure behavior

`DefaultOptions` selects 16 history samples, a 30-second episode lifetime and
one-second record/quiet/reopen periods. Options are explicit, validated values.
History is limited to 1–256 samples, and `Apply` returns at most four records,
each with at most the configured history capacity. There is one active episode;
retired/closed episode history is not accumulated. Reopen timing uses elapsed
durations rather than overflowing absolute-deadline additions.

Input gaps produce a `GapRecorded` record, and any interrupted episode closes
with `Incomplete: true`. New episodes start with new post-gap baselines.
Missing delivery after reduction is a different concern: the application must
track its output loss separately. Complete replay requires a complete ordered
input fixture; lossy output records are not a substitute for it.

The library owns no goroutines, locks, clocks, I/O or external dependencies.
The caller should put a bounded asynchronous adapter between its control loop
and optional diagnostics. A recorder error must not be reinterpreted as a
failed or refused recovery write.

## Internal dependency contract

Production imports are limited to `errors` and `time` from the standard library.
There are no sibling module dependencies. The one public package is the
component boundary:

- `recorder.go` owns validated public inputs and coordinates private state.
- `episode.go` owns opening, quiet/lifetime closure, progress and action totals.
- `history.go` owns fixed-capacity sample storage and detached copies.

These are private implementation files, not independent components with hidden
cross-package dependencies. Tests in `evidence_test` use only public contracts;
the replay example is a separate package and imports this module alone.

## Run independently

From this module directory:

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
GOWORK=off go run ./examples/replay
GOWORK=off go test -run '^$' -bench . -benchmem
```

No host privileges or Linux kernel are required. API and standalone builds are
under development. Publication and license selection remain outside this module
implementation; provenance must be checked before an independent release.
