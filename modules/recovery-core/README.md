# recovery-core

A deterministic, standard-library-only recovery policy for one split-ring queue
slot. It accepts monotonic observations and operation receipts, and returns typed
facts plus at most one advisory operation. It performs no I/O and owns no clock,
goroutine, process, queue handle or output transport.

Import `github.com/yckao/virtio-net-recovery/modules/recovery-core` as `recovery`.
The module uses Go 1.25 or newer. The public contract is provisional until its
first versioned release.

## Use

1. Create `Policy` with validated `Config` and a nonzero attachment generation.
2. Feed `CachedObservation` values to `Observe` in monotonic order.
3. If an `Update` contains an `Operation`, execute its requested live inspection
   or conditional notification through your own backend. The request is not a
   capability: physical eligibility and identity must be revalidated there.
4. Complete that operation with its actual `OperationResult`. Always account
   the result even if cancellation or presentation failure happened afterward.
5. Apply `Unavailable` for missing observations, `Advance` for diagnostic age,
   and `ReplaceGeneration` when the same slot acquires a new attachment.
6. Complete any in-flight operation before `Stop`. Discard the policy when the
   slot disappears; a newly discovered slot receives a new policy.

Calls are serialized by the caller. Every time is a `time.Duration` elapsed from
one run epoch. Invalid API inputs return errors without mutating state. Returned
facts, operations and `Status` snapshots do not expose mutable policy state.

Run the public consumer example:

```sh
GOWORK=off go run ./examples/replay
```

It prints `attempts=1 accepted=1 later_progress=1 pending=false` without accessing
a host or performing a notification.

## Guarantees and limits

- Candidates require valid outstanding work and two observations separated by
  the cadence with no used-index change. Idle time, invalid samples and missing
  coverage cannot manufacture an aged candidate. Sizes must be powers of two
  within 1..32768; indices use 16-bit modular arithmetic.
- Observe mode requests inspections only. Recover mode charges an attempt when
  it issues a notification request, before backend validation. One operation
  can be in flight. Inspections and attempts are paced from completion.
- Refusals, unavailable operations, cancellations and write failures all retain
  normal attempt pacing. There is no lifetime quota or incident expiry.
- Slot pacing and counters survive attachment replacement; candidate and
  verification state do not. Generation IDs are caller-assigned and must not
  be reused for another attachment.
- The first accepted write retains its origin and pre-write consumed/used
  baseline. Retries do not restart its timeout. Only a later, valid,
  same-generation live observation with both indices changed confirms progress.
  A refused notification may carry such a live observation.
- Verification timeout emits one fact and leaves the baseline and retries
  active. Stop retains unresolved verification in its final status.
- Accepted writes, later ring progress and restored service are different
  claims. The policy establishes neither notification causality nor packet
  delivery. Unchanged indices cannot exclude a complete wrap between samples.
- Invalid or unsupported live data must have `LiveObservation.Valid=false`.
  A caller must not label guessed or cached consumption as valid live progress.
- At most six facts and one operation are returned per call. State size is
  constant; there is no retained event history. Healthy noncandidate observations
  allocate no result storage.

`Fact.At` is the time a transition is accounted. `ObservedAt` and `AcceptedAt`
remain separate physical timestamps. `OperationResult.CompletedAt` sets pacing;
the accepted-write timestamp sets the verification origin. Result validation
rejects wrong operation/generation IDs, contradictory outcomes and impossible
time ordering before changing any state.

## Validation

```sh
GOWORK=off go test -race ./...
GOWORK=off go test -run '^$' -fuzz FuzzRingCandidate -fuzztime 5s
GOWORK=off go test -run '^$' -fuzz FuzzSerialOutcomeAccountingAndPacing -fuzztime 5s
GOWORK=off go test -run '^$' -bench BenchmarkHealthyObservation -benchmem
```

Tests use the public API from an external package. They exercise slow outcomes,
duplicate receipts, ring wrap, observation-only behavior, generation changes,
partial progress, late verification and bounded persistent incidents.

## Dependency and ownership contract

Production imports are exactly `errors` and `time`. Public API types are owned
by this package or `time.Duration`. No sibling module is a dependency.

`Policy` owns serialization protocol, operation correlation and slot lifecycle.
The private `progress` value owns cached-index aging. The private `verification`
value owns first-write progress and timeout tracking. Completion validates the
entire receipt before any state mutation. Helpers do not acquire resources or
call back into a consumer.

Tests directly import only this module, `errors`, `testing` and `time`. The
example imports this module, `fmt` and `time`. Internal helpers are not public
APIs and are not needed by independent consumers.

## Provenance

This module implements the reviewed temporal invariants in the repository's
modular architecture design. It does not include kernel assets or copied
third-party implementation code. Independent publication requires the
repository's license/provenance release check; this candidate does not claim
that a publication license has been selected.
