# Validation methods and recovery limits

This is a public summary of the bounded validation of source `97758840064063ee53e195df2daf51399552215c` on 2026-09-30. It describes the operating contract and the evidence needed to assess it. Raw lab logs, infrastructure identities and access details are retained privately. Passing these cases permits assessment of the experimental implementation; it does not make the agent production qualified.

The owner selected the **100 ms observation/retry cadence**, matching the CLI default. The default inventory refresh is 5 seconds and the verification warning timeout is 5 seconds. Further 25 ms tests were withdrawn from the requested scope; historical 25 ms findings remain recorded below. The 100 ms choice does not imply a universal application-outage bound or production qualification.

## What the agent decides

1. Discover current QEMU processes and their vhost TX slots. Observe/recover own one per-process lock and share a low-frequency snapshot probe. They batch known user-ring indices each polling cycle; normal modes have no traffic-stage probes.
2. A consistent outstanding descriptor count with no used-ring change across at least one cadence becomes a candidate. An idle period before a new backlog does not age that backlog. Invalid or unavailable samples reset candidate timing.
3. A cadence-eligible candidate triggers a fresh pinned attachment snapshot and ring read; recover charges its attempt before this live validation. Reject inconsistent indices, new completion progress, a drained queue, queued work, or no unconsumed descriptors, in that order. Observe records the decision and never writes.
4. Recover paces attempts, including refusals, from the end of the previous attempt. For an eligible live candidate it refreshes FD inventory, checks the selected process generation, pins the matching eventfd, requires `O_NONBLOCK`, and rechecks the attachment identity, process liveness and cancellation before writing the eight-byte eventfd value `1`. Missing or changed identities are refused. The final identity snapshot is not a second delayed ring-progress confirmation.
5. The first accepted write starts a verification baseline and an originating diagnostic event ID. Repeated writes retain that baseline. Only a later live snapshot with changes in **both consumed and used indices** emits `progress_after_kick`. A verification warning preserves the baseline and retry policy. Attachment replacement clears the old verification so another queue generation cannot confirm it.

The queued-work bit is a safety check, not a worker-idle verdict: Linux clears it before entering the handler. Sixteen-bit ring indices can wrap between samples. Neither unchanged indices nor progress after a write proves the cause of a stall, packet delivery or application recovery. Diagnostic episode progress and recovery verification have separate baselines; an episode's progress may precede its first write.

`kick`, `--once` and `--rescue` perform manual verified writes without candidate detection or a progress wait. Each visited TX slot gets at most one write; a refusal/error ends that VM's slot traversal, while the manager continues with other selected VMs. Already accepted writes remain accepted. `--duration` does not extend this one-shot operation. Its JSON result records `progress: "unmeasured"`.

`trace` selects targets at startup and never kicks. It requires an explicit 1–300 second duration and bounds shared JSON output to 8 MiB. Only this mode attaches eventfd/vhost stage probes. Their aggregate counts and active-handler counter are diagnostic observations, not a complete guest/KVM timeline or proof of worker idleness. Kernel/BTF/probe compatibility and traffic overhead require controlled runtime validation.

## Merge acceptance and runtime qualification

| Question | Evidence needed for implementation acceptance | Separate runtime qualification |
| --- | --- | --- |
| Selection and writes | Current-process/attachment/eventfd validation; cancellation, refusal and syscall-error regressions; manual alias contract | Device churn and large simultaneous incidents under representative workloads |
| Observe/recover policy | Candidate aging, invalidation, pacing, retained verification baseline and no automatic observe writes | Healthy traffic at the chosen cadence; receiver outage and protocol transitions during actual faults |
| Journal and metrics | Bounded fixed labels, write/decision/progress separation, event correlation and one observation-error count per cycle | Coverage and diagnostic overhead at deployment scale |
| Build/publication | Tests, vet, Linux build, BPF/module compilation, agent/fault image revision and restricted help; exact final PR head checks | Supported deployed kernel, matching image digest and permissions; publication outcome after an authorized merge |
| Performance | Explicit bounded measurements with their confidence intervals and validity gates | Physical path saturation, ECMP, many VMs/queues and same-round original/new comparison |

Review merge readiness for each exact PR head and current base, in dependency order. Check Draft state, conflicts, required checks, current-head review coverage and whether unresolved comments still describe a real blocker. An older review is not approval of a newer head. PR publication is intentionally skipped; successful validation does not mean an image was published. Main publication separately checks the repository/package visibility policy and must not change sharing to bypass a failure.

## Evidence chain

Retain the full source commit, a clean tracked-source comparison, build command/toolchain and build manifest, binary and BPF hashes, OCI manifest/config/layer digests, image revision label, kernel/BTF and relevant runtime configuration. Verify the running binary/image against those artifacts. A revision label alone is a source claim; it is not an independent reproducible-build proof. Hosted CI proves its tested source/image, not that a separate lab run used that binary.

For each experiment record selected process/attachment identities privately, wall and monotonic time, requested and actual injection counts, candidate/live decisions, accepted writes, consumption and used-ring changes, receiver traffic gaps and BFD/BGP transitions. Preserve invalid runs and corrections. Separate agent writes from the controller's eventual cleanup kick. Hash the retained archives and summaries before analysis. After a source change, explicitly classify whether the existing runtime evidence applies to unchanged code or needs another build/run.

## Bounded test method

### Healthy traffic

- State a measurable question, duration, workload, cadence and stop condition before starting. Include no-agent and observe controls; zero observe writes is guaranteed by its mode and is not evidence of healthy traffic.
- Warm up traffic, then alternate comparable baseline/candidate windows. Use paired repeats and report effect sizes and confidence intervals, not just mean throughput. Retain concurrent-load and serial-dependence limitations. A wide interval crossing the regression budget is inconclusive.
- Collect accepted writes, poll/coverage errors and receiver behavior throughout. Zero recover writes under a healthy workload supports only that workload/window; it does not prove the detector cannot miss stalls. A healthy-run write of unknown necessity invalidates a zero-write qualification claim without establishing its cause.

### Fault, traffic and protocol continuity

- Use an owned disposable backend kernel with no foreign domains/dependencies, matching headers/BTF, the tested binary/image, and an independent management/cleanup path. A new guest on a shared backend kernel does not provide kernel isolation. Verify isolation and current resource state immediately before each campaign.
- Start traffic and, where applicable, established BFD/BGP sessions before injection. Record the BFD interval, multiplier and echo setting. Capture both peers' session transitions and receiver gaps separately from queue progress.
- Run a matched **observe control** first. Require a positive actual drop count plus fresh evidence that unconsumed descriptors and used-ring progress stay stalled after the injector disarms. A matched/dropped count alone is insufficient; active traffic may supply another wakeup. Zero actual drops is invalid.
- Run recover with the same fault/workload definition. Require positive injection, accepted writes and later combined queue progress. Count actual faults and confirmed recoveries, not requested pulses or writes. A ten-second repeated campaign is not a 30/60 second fault train.
- For a repeated train, record each bounded window and actual positive-drop episode, the complete application/session interval, retry gaps and cleanup attribution. Stop on isolation loss, identity change, unexpected error, loss of the independent control path, or the predeclared runtime/resource deadline. Report partial coverage explicitly.
- The injector matches the selected waiter's wakeups, not their source; matching recovery kicks can also be suppressed during its window. Separately test the failure boundary. If it continuously suppresses recovery kicks, recovery may wait until suppression ends. A five-second continuous block is a different mechanism from isolated dropped notifications; repeated writes during that block are not separate recoveries.
- Unload the owned fault module, confirm queue/application health, detach only owned probes and stop owned workloads. Record final identities/state, remaining resources and foreign-resource comparison. A forced controller exit still requires module cleanup and a freshly validated manual recovery target.

The controller's `progress_observed` checks a changed used index after its disarm baseline. It is less strict than the recovery agent's combined consumption/used verification and does not establish packet delivery. Normal stop and the controller recovery deadline may send a verified cleanup kick; exclude this external intervention from an automatic-recovery claim.

## Bounded results

### Initial current-version campaign

The functional timings and results below were measured on the recorded new-version source `9775884`. Original README commit `2bb7f4c` is the reference for the manual `--once` contract, not the checkout on which the 0.721-second functional run occurred.

| Case on the recorded source | Result | Practical limit |
| --- | --- | --- |
| Manual/selection/mode functional checks | 16 cases passed. `--once --duration 30` completed in 0.721 seconds with four accepted writes. | One-shot write contract only; no progress wait or recovery-latency claim. |
| Healthy 25 Gbps, 100 ms observe/recover | Five paired repeats per mode; zero writes and poll errors. Reported 95% regression intervals: observe −0.066% to 0.111%; recover −0.308% to 0.144%. | Bounded virtual-path measurement. Conditional paired log-ratio assumptions; no physical dual-link or same-round original/new qualification. |
| Historical healthy 25 ms recover | One accepted write with unknown necessity. | Does not pass the healthy zero-write gate. The owner selected 100 ms; 25 ms remains unqualified. |
| Uncapped healthy throughput | Confidence intervals were too wide to establish the 1% budget. | Inconclusive, not a performance pass. |
| Effective observe fault control | One actual drop, zero automatic writes; fresh pending/stalled queue and continued silence until separate manual cleanup. | Confirms effectiveness of this injected control, not the cause of every real stall. |
| UDP single/repeated recover, 100 ms | Receiver gaps about 175.8 ms and 196.0 ms; four actual recoveries in the ten-second repeated campaign. | These bounded cases passed a 500 ms gap gate; requested pulses did not all create faults. |
| BFD/BGP single/repeated and coupled traffic | Five continuity cases passed with BFD 500 ms × 6, no echo; both-peer observations retained. | Single peer only. One accepted-write-to-progress interval was 798 ms; there is no universal 500 ms guarantee. Coupled 25 Gbps was one observation, not a paired performance qualification. |
| Five-second continuous suppression | UDP gap about 5.184 seconds and BFD/BGP transitions; continuity gate failed. | Expected mechanism boundary. Recovery cannot bypass suppressed recovery wakeups. |
| Bounded trace | Three seconds, four queues, no writes; owned probe IDs matched before/after. | One tested kernel/workload, not general trace compatibility or overhead qualification. |

### Merge-preparation follow-up

The following completed fault runs extend the recorded current-version coverage. Counts below preserve the distinction between requested pulses, pulses with a positive drop count, actual dropped wakeups, accepted writes and confirmations. Only the UDP repeated runs have verified one-to-one drop/write/combined-progress joins. Protocol pulse totals are not independent recovery counts: one pulse may drop several wakeups, and recovery may retry.

The UDP runs used 100 ms agent polling/retry cadence, 1-second summaries, 1,500 packets/second, and a selected-waiter callback gate of 50 ms every 500 ms with at most one dropped callback per pulse. The FRR runs used 100 ms recovery cadence and a freshly probed selected protocol queue; their callback gate was 200 ms every 530 ms, capped at 1,000,000 matching callbacks per pulse. The kernel window bounds that suppression; the cap is not a fault count. Both peers retained BFD transmit/receive 500 ms, multiplier 6, echo disabled, with a 1,500 ms continuity-margin gate. Requested pulses and actual drops are reported separately.

| Case, 100 ms recover cadence | Actual evidence | Outcome and scope |
| --- | --- | --- |
| Matched observe control | One actual drop; attempts/writes/confirmations 0/0/0; 3,551.62 ms silence until separate cleanup. | Effective no-automatic-recovery control. Cleanup intervention is excluded from agent recovery. |
| UDP single fault | One drop, one accepted write, one combined-progress confirmation; maximum receiver gap 190.74 ms. | Passed the bounded 500 ms UDP gap gate. |
| UDP 30-second repeated fault train | 60 actual drops, attempts/writes/confirmations 60/60/60; each joined to consumption and used progress; maximum receiver gap 284.61 ms. | Passed this train's gap/progress gates; no write errors, refusals or poll errors. |
| UDP 60-second repeated fault train | 120 actual drops, attempts/writes/confirmations 120/120/120; each joined to consumption and used progress; maximum receiver gap 302.55 ms. | Passed this train's gap/progress gates; no write errors, refusals or poll errors. |
| Single-peer BFD/BGP, 30-second repeated train | 57 requested pulses, 29 positive-drop pulses, 32 dropped wakeups, 32 accepted writes and 29 total confirmations (28 during the fault epoch). Guest/peer maximum receive gaps 500.11/736.88 ms. | BFD/BGP transition deltas were zero. Single peer with the recorded 500 ms × 6, no-echo configuration; no one-to-one pulse/recovery claim. |
| Single-peer BFD/BGP plus 25 Gbps traffic, 60-second repeated train | 113 requested pulses, 57 positive-drop pulses, 69 drops, 69 accepted writes and 57 total confirmations (56 during the fault epoch). Guest/peer maximum receive gaps 500.49/778.53 ms. Fresh receiver window: 54.000 seconds at 24.99991 Gbps; complete 70.039-second receiver run: 24.986 Gbps; one TCP retransmission. | BFD/BGP transition deltas were zero. This is a coupled continuity observation, not paired throughput qualification, ECMP or a 500 ms protocol-gap guarantee. |
| Withdrawn healthy 25 ms observe/recover follow-up | Initial control-path startup failed after 2.9 seconds with **zero valid measurement phases**; its failure and cleanup evidence were retained. The single authorized startup repair was refused by the remaining-budget gate before it created any fixture, timer or agent. | **Not completed; further testing withdrawn by the owner.** Neither the original six 60-second phases nor the reduced six 30-second phases ran to completion. No new healthy 25 ms data, zero-write pass, non-reproduction finding or throughput qualification was obtained. This withdrawn scope is not a merge blocker for the selected 100 ms path. |

The longer UDP trains close the previously unrun 30/60 second duration gap for this selected queue/workload. The protocol runs add longer single-peer continuity evidence. Neither removes the five-second suppression failure boundary or establishes a universal 500 ms recovery deadline. The withdrawn healthy startup failure does not invalidate the completed fault runs. Owned-fixture cleanup and the final environment state were verified; detailed source/build, cleanup and unrelated-resource comparisons are retained privately.

The earlier healthy 25 ms write met the current policy's cached and live `unconsumed` conditions, but its necessity remains unknown. A later progress observation does not establish that the write was needed. The follow-up produced no new data that resolves this question. The owner selected 100 ms and cancelled further 25 ms testing; unresolved historical 25 ms necessity does not become a pending acceptance criterion for that selected path.

**Still unverified:** ECMP, multiple VMs/60 queues, large simultaneous faults, and a same-round frozen-original/new paired benchmark. These remain outstanding deployment qualification goals. Longer selected-queue fault trains do not cover them, and older-source tests cannot establish current-version coverage. The withdrawn 25 ms follow-up is recorded as historical incomplete work, not a pending test or merge gate.

Use the bounded results to review the implementation and select further tests with a specific purpose. Choose deployment acceptance criteria independently; do not convert these observations into a production guarantee.
