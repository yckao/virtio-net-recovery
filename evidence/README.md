# Evidence recorder

`evidence` is a self-contained public Go package within this repository's single
module. It reduces caller-supplied observations into bounded diagnostic episodes.
It imports only the standard library; recovery policy, Linux access, JSON and
delivery are outside its responsibility. A future extraction can retain this
boundary, but it is not currently a separately versioned module.

Create one `Recorder` per attachment generation with `New(options, streamID)`.
The caller serializes `Apply`, supplies monotonic durations from one epoch, and
owns stream and attempt identities. Returned records and sample slices are
owned copies; callers can retain or change them without changing recorder state.

## Atomic observations

Supply a fresh sample and the current candidate verdict together:

```go
records, err := recorder.Apply(evidence.Observation{
    At: now,
    HasSample: true,
    Sample: evidence.Sample{
        At: acquiredAt, Avail: 10, Used: 4,
        Source: evidence.SourceCached, Valid: true,
    },
    CandidateKnown: true,
    Candidate: true,
})
```

`Sample.At` records acquisition time; `Observation.At` records when that state is
presented to the reducer. Acquisition must not be later than presentation or
earlier than the previous retained sample. Presentation time never moves
backward. Rejected inputs leave state unchanged.

`HasSample: false` carries no fresh sample. `CandidateKnown: false` supplies no
candidate verdict, so it does not start quiet timing. A known positive verdict
cancels quiet before the same observation evaluates expiry. This ordering is
owned by the recorder; callers do not compose separate sample/candidate events.
A positive verdict needs a valid retained sample to open an episode. A sample
or an action by itself never opens one. A positive verdict cannot extend the
configured maximum episode lifetime.

Validity and field freshness come from the producer. Invalid samples are useful
diagnostic history but never prove progress. Unknown consumed/work values must
be zero with their freshness flags cleared. The recorder does not repeat ring
geometry checks, candidate detection or recovery verification.

## Other inputs

| Value input | Meaning |
| --- | --- |
| `ActionObserved` | Count an actual action result within an episode. |
| `VerificationObserved` | Record an externally established verdict and originating attempt. |
| `GapObserved` | Close incomplete, clear history and baselines, permit fresh resynchronization. |
| `TimeAdvanced` | Check established deadlines without inventing quiet or a candidate. |
| `Retired` | Close and clear the recorder permanently; repeated retirement is idempotent. |

`Input` is a closed set of values, not an extension interface. Pointers, unknown
variants, invalid enums and negative times are rejected. Verification records
use `EpisodeID == 0` and correlate by stream/attempt, so late verdicts cannot be
attributed to whichever episode happens to be open. The recorder owns no retry
permission, verification deadline or attempt-deduplication ledger.

The first action in an episode is reported immediately. Later actions share
the sample record interval; all received action totals remain in the closing
record. These totals describe received evidence, not authoritative product
accounting. A gap closes the old episode with `Incomplete: true`. A fresh
post-gap candidate starts from newly supplied history. Delivery loss after
reduction must be tracked separately by the application.

## Bounds and validation

Defaults retain 16 samples, limit an episode to 30 seconds, and use one-second
record, quiet and reopen intervals. History is configurable from 1 to 256 samples.
There is one active episode and at most four records per input. The package
owns no goroutine, callback, lock, clock, I/O or sibling-package dependency.

`recorder.go` owns input validation and transition ordering; `episode.go` owns
episode state; `history.go` owns bounded storage. These are implementation files
of one component, not separate architectural layers.

Run from the repository root:

```sh
go test -race ./evidence/...
go vet ./evidence/...
go run ./evidence/examples/replay
```

The external tests cover observation ordering, input rollback, gaps, late
verification, ownership, history bounds and deterministic replay. No host
privileges or Linux kernel are needed.
