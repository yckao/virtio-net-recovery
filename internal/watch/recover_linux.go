//go:build linux && amd64

package watch

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

type recoveryQueue struct {
	fd                               int
	snapshot                         Snapshot
	policy                           *RecoveryPolicy
	avail, used                      uint16
	lastLive, sampledAt              float64
	lastError                        string
	journal                          *EpisodeJournal
	lastDecision                     EpisodeReason
	lastEventID, verificationEventID string
	metrics                          *Metrics
}

func (q *recoveryQueue) ensureJournal(l *logger) {
	if q.journal == nil {
		q.journal = NewEpisodeJournal(func(event string, fields map[string]any) {
			q.metrics.episodeEvent(event, fields)
			fields["vhost_fd"], fields["eventfd_id"], fields["num"] = q.fd, q.snapshot.EventID, q.snapshot.Num
			if event == "candidate" {
				q.lastEventID = fields["event_id"].(string)
			}
			l.emit(event, fields)
		}, monotonic)
	}
}

func (q *recoveryQueue) recordWrite(result EpisodeWrite, reason EpisodeReason) {
	if q.metrics != nil {
		q.metrics.Write(WriteResult(result))
	}
	q.journal.RecordWrite(result, reason)
}

func (q *recoveryQueue) decision(reason EpisodeReason) {
	q.lastDecision = reason
	q.journal.RecordDecision(reason)
}

func (q *recoveryQueue) closeEpisode(outcome EpisodeOutcome) {
	if q.journal != nil {
		q.journal.Close(outcome)
	}
}

func (q *recoveryQueue) clock() float64 {
	if q.journal != nil {
		return q.journal.now()
	}
	return monotonic()
}

func (q *recoveryQueue) forgetVerification() {
	q.policy.verification = nil
	q.verificationEventID = ""
}

func (q *recoveryQueue) observeCached(l *logger, at float64, ready bool) {
	q.ensureJournal(l)
	invalid := uint32(q.avail-q.used) > q.snapshot.Num
	q.journal.Observe(EpisodeSample{At: at, Avail: q.avail, Used: q.used,
		Source: EpisodeSourceUser, Consumed: q.snapshot.LastAvail,
		WorkQueued: q.snapshot.WorkFlags&(1<<1) != 0, Invalid: invalid}, ready)
	if invalid {
		q.decision(EpisodeReasonInvalidRing)
		q.closeEpisode(EpisodeOutcomeInvalidRing)
	} else if ready {
		q.journal.Open(EpisodeReasonCandidate)
	}
}

func (q *recoveryQueue) tickDiagnostics(l *logger, now, verifyTimeout float64) {
	q.journal.Tick()
	if q.policy.Unconfirmed(now, verifyTimeout) {
		f := q.summary(now)
		f["event_id"], f["outcome"] = q.verificationEventID, EpisodeOutcomeTimeout
		l.emit("recovery_unconfirmed", f)
	}
}

func (q *recoveryQueue) summary(now float64) map[string]any {
	return map[string]any{"vhost_fd": q.fd, "eventfd_id": q.snapshot.EventID,
		"num": q.snapshot.Num, "avail": q.avail, "used": q.used,
		"outstanding": q.avail - q.used, "episode_open": q.journal != nil && q.journal.Active(),
		"event_id": q.lastEventID, "last_decision": q.lastDecision,
		"attempts": q.policy.Attempts, "writes": q.policy.Writes,
		"max_attempt_gap": q.policy.MaxAttemptGap,
		"refusals":        q.policy.Refusals, "write_errors": q.policy.WriteErrors, "confirmed": q.policy.Confirmed,
		"verification_event_id": q.verificationEventID, "verification_pending": q.policy.verification != nil,
		"verification_timed_out": q.policy.verification != nil && q.policy.verification.unconfirmed,
		"unconfirmed_age":        q.policy.PendingAge(now), "indices_sample_at": q.sampledAt}
}

// The stopped summary must include queues not yet visited by an interrupted
// inventory refresh, including verification whose diagnostic episode closed.
func restoreUnvisitedRecoveryQueues(known, pending map[int]*recoveryQueue) {
	for fd, q := range pending {
		if known[fd] == nil {
			known[fd] = q
		}
	}
}

// runRecover has one common cadence, one batched user-ring read per healthy
// tick in observe and recover, with no traffic-path probes. Cached user
// addresses only identify a candidate; every write requires fresh process, FD and attachment validation.
func runRecover(ctx context.Context, c Config, target *Target, bpf *BPF, l *logger) (result error) {
	started, lastInventory, lastSummary := monotonic(), math.Inf(-1), math.Inf(-1)
	lastPoll, maxPollGap := started, float64(0)
	lastLoop, maxLoopGap := started, float64(0)
	queues := []*recoveryQueue{}
	known := map[int]*recoveryQueue{}
	var pendingCleanup map[int]*recoveryQueue
	policies := map[int]*RecoveryPolicy{}
	var batch *RingBatch
	var attempts, writeErrors, refusals, pollErrors uint64
	var progress recoveryProgressTotals
	discoveryFailures := map[int]string{}
	var metricsPending bool
	var metricsErrors uint64
	var metricsGap float64
	var metricsSampled int
	finishMetrics := func() {
		if metricsPending {
			recordRecoveryMetrics(c.metricWorker, known, discoveryFailures, metricsSampled, metricsGap, pollErrors > metricsErrors)
			metricsPending = false
		}
	}
	defer finishMetrics()
	forceInventory := true
	lastInventoryError := ""
	summary := func(now float64) map[string]any {
		rows := make([]map[string]any, 0, len(known))
		knownFDs, sampledFDs := make([]int, 0, len(known)), []int{}
		for fd := range known {
			knownFDs = append(knownFDs, fd)
		}
		if batch != nil {
			for _, q := range queues {
				sampledFDs = append(sampledFDs, q.fd)
			}
		}
		unavailable := unavailableRecoveryFDs(knownFDs, sampledFDs, discoveryFailures)
		for _, q := range known {
			row := q.summary(now)
			row["coverage"] = "sampled"
			if slices.Contains(unavailable, q.fd) {
				row["coverage"] = "unavailable"
			}
			rows = append(rows, row)
		}
		for _, fd := range unavailable {
			if known[fd] == nil {
				rows = append(rows, map[string]any{"vhost_fd": fd, "coverage": "unavailable",
					"error": discoveryFailures[fd], "previously_supported": false})
			}
		}
		return map[string]any{"attempts": attempts, "writes": progress.writes, "write_errors": writeErrors, "refusals": refusals, "confirmed": progress.confirmed,
			"poll_errors": pollErrors, "max_poll_gap": max(maxPollGap, now-lastPoll), "max_loop_gap": maxLoopGap,
			"queues": rows, "unavailable_queues": len(unavailable), "source": "cached_user_indices"}
	}
	defer func() {
		restoreUnvisitedRecoveryQueues(known, pendingCleanup)
		current := make(map[int]Snapshot, len(known))
		pending := make(map[int]Snapshot, len(pendingCleanup))
		for fd, q := range pendingCleanup {
			q.closeEpisode(EpisodeOutcomeStopped)
			pending[fd] = q.snapshot
		}
		for fd, q := range known {
			q.closeEpisode(EpisodeOutcomeStopped)
			current[fd] = q.snapshot
		}
		forgetSnapshotSets(current, pending, bpf.Forget)
		l.emit("stopped", summary(monotonic()))
		if result == nil {
			result = l.err
		}
	}()
	ticker := time.NewTicker(time.Duration(c.Interval * float64(time.Second)))
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return l.err
		}
		metricsPending, metricsErrors, metricsSampled = true, pollErrors, 0
		alive, err := target.CheckAlive()
		if err != nil {
			pollErrors++
			return err
		}
		if !alive {
			l.emit("target_exited", map[string]any{})
			return l.err
		}
		now := monotonic()
		metricsGap = now - lastLoop
		maxLoopGap = max(maxLoopGap, now-lastLoop)
		lastLoop = now
		if c.Duration > 0 && now-started >= c.Duration {
			return l.err
		}
		if forceInventory || now-lastInventory >= c.InventoryInterval {
			lastInventory, forceInventory = now, false
			vhosts, _, err := target.Inventory()
			if err != nil {
				pollErrors++
				batch = nil
				forceInventory = true
				for _, q := range queues {
					q.policy.Invalidate(now)
					q.decision(EpisodeReasonUnavailable)
					q.closeEpisode(EpisodeOutcomeUnavailable)
				}
				if err.Error() != lastInventoryError {
					l.emit("inventory_unavailable", map[string]any{"error": err.Error()})
					lastInventoryError = err.Error()
				}
			} else {
				lastInventoryError = ""
				old := map[int]*recoveryQueue{}
				pendingCleanup = make(map[int]*recoveryQueue, len(known))
				for fd, q := range known {
					old[fd] = q
					pendingCleanup[fd] = q
				}
				queues = nil
				known = map[int]*recoveryQueue{}
				snapshots := []Snapshot{}
				for _, remoteFD := range vhosts {
					if ctx.Err() != nil {
						return l.err
					}
					s, row, err := liveRow(target, bpf, remoteFD)
					if err != nil {
						pollErrors++
						report, refresh := noteDiscoveryFailure(discoveryFailures, remoteFD, err.Error(), old[remoteFD] != nil)
						forceInventory = forceInventory || refresh
						if report {
							l.emit("queue_unavailable", map[string]any{"vhost_fd": remoteFD, "error": err.Error(), "scope": "discovery", "coverage": "unavailable", "previously_supported": old[remoteFD] != nil})
						}
						continue
					}
					delete(discoveryFailures, remoteFD)
					q := old[remoteFD]
					delete(old, remoteFD)
					delete(pendingCleanup, remoteFD)
					p := policies[remoteFD]
					if p == nil {
						p = NewRecoveryPolicy(c.Interval)
						policies[remoteFD] = p
					}
					if q != nil && q.snapshot.Identity() != s.Identity() {
						q.decision(EpisodeReasonIdentityChange)
						q.closeEpisode(EpisodeOutcomeIdentityChange)
						bpf.Forget(q.snapshot)
						l.emit("queue_reconfigured", map[string]any{"vhost_fd": remoteFD, "unconfirmed_age": p.PendingAge(now)})
						// Preserve slot pacing and counters; an old attachment's
						// verification can never confirm a replacement attachment.
						q.forgetVerification()
						p.Invalidate(now)
						q = nil
					}
					if q == nil {
						q = &recoveryQueue{fd: remoteFD, policy: p, lastLive: math.Inf(-1), metrics: c.Metrics}
					}
					q.snapshot, q.avail, q.used, q.sampledAt = s, row.Avail, row.Used, monotonic()
					q.ensureJournal(l)
					q.journal.Observe(liveEpisodeSample(row, q.sampledAt), false)
					known[remoteFD] = q
					queues = append(queues, q)
					snapshots = append(snapshots, s)
				}
				for _, q := range old {
					if slices.Contains(vhosts, q.fd) {
						// An unavailable snapshot has not established a new
						// identity; preserve its first verification baseline.
						known[q.fd] = q
						q.policy.Invalidate(now)
						q.decision(EpisodeReasonUnavailable)
						q.closeEpisode(EpisodeOutcomeUnavailable)
						continue
					}
					bpf.Forget(q.snapshot)
					q.decision(EpisodeReasonIdentityChange)
					q.closeEpisode(EpisodeOutcomeIdentityChange)
					q.forgetVerification()
					q.policy.Invalidate(now)
				}
				// All old queues are either transferred into known or forgotten.
				pendingCleanup = nil
				for fd := range discoveryFailures {
					if !slices.Contains(vhosts, fd) {
						delete(discoveryFailures, fd)
					}
				}
				pruneRecoveryPolicies(policies, vhosts)
				batch = nil
				if len(snapshots) != 0 {
					batch, err = NewRingBatch(target, snapshots)
					if err != nil {
						pollErrors++
						return err
					}
				}
			}
		}
		if batch != nil {
			if err := batch.Read(); err != nil {
				pollErrors++
				l.emit("queue_unavailable", map[string]any{"error": err.Error(), "scope": "ring_batch"})
				batch = nil
				for _, q := range queues {
					q.policy.Invalidate(now)
					q.decision(EpisodeReasonUnavailable)
					q.closeEpisode(EpisodeOutcomeUnavailable)
				}
				forceInventory = true
			} else {
				metricsSampled = len(queues)
				sampledAt := monotonic()
				maxPollGap = max(maxPollGap, sampledAt-lastPoll)
				lastPoll = sampledAt
				for index, q := range queues {
					if ctx.Err() != nil {
						return l.err
					}
					q.avail, q.used = batch.Indices(index)
					q.sampledAt = sampledAt
					ready := q.policy.Observe(q.avail, q.used, q.snapshot.Num, sampledAt)
					q.observeCached(l, sampledAt, ready)
					verify := q.policy.NeedsVerification(q.used)
					if (ready || verify) && now-q.lastLive >= c.Interval {
						inspect := ready && (c.Mode == "observe" || q.policy.Attempt(now))
						attempt := inspect && c.Mode == "recover"
						if inspect || verify {
							q.lastLive = now
							if attempt {
								attempts++
							}
							beforeWrites, beforeConfirmed, beforeRefusals := q.policy.Writes, q.policy.Confirmed, q.policy.Refusals
							err := recoverLive(ctx, c, target, bpf, l, q, inspect)
							q.lastLive = monotonic()
							if attempt {
								q.policy.FinishAttempt(q.lastLive)
							}
							progress.add(q.policy, beforeWrites, beforeConfirmed)
							if l.err != nil {
								return l.err
							}
							if err != nil {
								if ctx.Err() != nil {
									return l.err
								}
								event := "recovery_refused"
								var writeErr *KickWriteError
								if errors.As(err, &writeErr) {
									q.policy.WriteErrors++
									writeErrors++
									event = "recovery_write_failed"
								} else if !q.policy.Refused(attempt) {
									pollErrors++
									event = "observation_unavailable"
								}
								q.policy.Invalidate(now)
								forceInventory = true
								if q.lastError != err.Error() {
									l.emit(event, map[string]any{"vhost_fd": q.fd, "event_id": q.lastEventID,
										"reason": q.lastDecision, "error": err.Error()})
									q.lastError = err.Error()
								}
							} else {
								q.lastError = ""
							}
							refusals += q.policy.Refusals - beforeRefusals
						}
					}
				}
			}
		}
		// Coverage loss must not postpone the original verification deadline.
		// Preserve the baseline so later live progress can still be observed.
		for _, q := range known {
			q.tickDiagnostics(l, monotonic(), c.VerifyTimeout)
		}
		if now-lastSummary >= c.SummaryInterval {
			l.emit("sample", summary(now))
			lastSummary = now
		}
		finishMetrics()
		if l.err != nil {
			return l.err
		}
		select {
		case <-ctx.Done():
			return l.err
		case <-ticker.C:
		}
	}
}

func recordRecoveryMetrics(w *WorkerMetrics, known map[int]*recoveryQueue, failures map[int]string, sampled int, gap float64, failed bool) {
	if w == nil {
		return
	}
	open, unavailable := 0, len(known)-sampled
	for _, q := range known {
		if q.journal != nil && q.journal.Active() {
			open++
		}
	}
	for fd := range failures {
		if known[fd] == nil {
			unavailable++
		}
	}
	w.RecordPoll(open, sampled, max(0, unavailable), time.Duration(gap*float64(time.Second)), failed)
}

func liveEpisodeSample(row QueueRow, at float64) EpisodeSample {
	return EpisodeSample{At: at, Avail: row.Avail, Used: row.Used,
		Source: EpisodeSourceLive, Consumed: row.Consumed, ConsumedFresh: true,
		WorkQueued: row.WorkQueued, WorkQueuedFresh: true}
}

func liveRecoveryDecision(s Snapshot, row QueueRow, cachedUsed uint16) EpisodeReason {
	switch {
	case uint32(row.Pending) > s.Num || uint32(row.Outstanding) > s.Num:
		return EpisodeReasonInvalidRing
	case row.Used != cachedUsed:
		return EpisodeReasonUsedProgress
	case row.Outstanding == 0:
		return EpisodeReasonDrained
	case row.WorkQueued:
		return EpisodeReasonBusy
	case row.Pending == 0:
		return EpisodeReasonNoPending
	default:
		return EpisodeReasonUnconsumed
	}
}

func recoverLive(ctx context.Context, c Config, target liveQueueTarget, bpf snapshotter, l *logger, q *recoveryQueue, candidate bool) (result error) {
	q.metrics = c.Metrics
	metricDecision := ReasonUnavailable
	outputFailed := false
	defer func() {
		if c.Metrics != nil {
			c.Metrics.Decision(metricDecision)
		}
	}()
	writesBefore := q.policy.Writes
	if q.journal == nil {
		q.observeCached(l, q.clock(), candidate)
	} else if candidate {
		q.journal.Open(EpisodeReasonCandidate)
	}
	q.lastDecision = EpisodeReasonUnavailable
	defer func() {
		if result == nil || q.policy.Writes > writesBefore || outputFailed {
			return
		}
		var writeErr *KickWriteError
		writeResult := EpisodeWriteRefused
		if errors.As(result, &writeErr) {
			q.lastDecision = EpisodeReasonWriteFailed
			writeResult = EpisodeWriteError
		} else if errors.Is(result, ErrKickIdentityChanged) {
			q.lastDecision = EpisodeReasonIdentityChange
			metricDecision = ReasonIdentityChange
		} else if errors.Is(result, context.Canceled) || errors.Is(result, context.DeadlineExceeded) {
			q.lastDecision = EpisodeReasonCancelled
		}
		q.decision(q.lastDecision)
		if candidate && c.Mode == "recover" {
			q.recordWrite(writeResult, q.lastDecision)
		}
		switch q.lastDecision {
		case EpisodeReasonIdentityChange:
			q.closeEpisode(EpisodeOutcomeIdentityChange)
			q.forgetVerification()
		case EpisodeReasonUnavailable:
			q.closeEpisode(EpisodeOutcomeUnavailable)
		}
	}()
	s, row, err := liveRow(target, bpf, q.fd)
	if err != nil {
		return err
	}
	if s.Identity() != q.snapshot.Identity() {
		metricDecision = ReasonIdentityChange
		q.decision(EpisodeReasonIdentityChange)
		return errors.New("queue identity changed during cached ring polling")
	}
	now := q.clock()
	decision := liveRecoveryDecision(s, row, q.used)
	metricDecision = DecisionReason(decision)
	q.decision(decision)
	sample := liveEpisodeSample(row, now)
	sample.Invalid = decision == EpisodeReasonInvalidRing
	q.journal.Observe(sample, candidate && decision == EpisodeReasonUnconsumed)
	if decision == EpisodeReasonInvalidRing {
		if candidate && c.Mode == "recover" {
			q.policy.Refused(true)
			q.recordWrite(EpisodeWriteRefused, decision)
		}
		q.closeEpisode(EpisodeOutcomeInvalidRing)
		q.policy.Invalidate(now)
		return nil
	}
	verificationTimedOut := q.policy.verification != nil && q.policy.verification.unconfirmed
	if latency, confirmed := q.policy.Verify(now, row.Used, row.Consumed); confirmed {
		f := fields(row)
		f["latency"], f["writes"], f["attempts"] = latency, q.policy.Writes, q.policy.Attempts
		f["event_id"], f["outcome"], f["verification_timed_out"] = q.verificationEventID, EpisodeOutcomeBoth, verificationTimedOut
		l.emit("progress_after_kick", f)
		q.verificationEventID = ""
	}
	if !candidate {
		outputFailed = l.err != nil
		return l.err
	}
	if decision != EpisodeReasonUnconsumed {
		if c.Mode == "recover" {
			q.policy.Refused(true)
			q.recordWrite(EpisodeWriteRefused, decision)
		}
		q.policy.Invalidate(now)
		return nil
	}
	if l.err != nil || c.Mode != "recover" {
		outputFailed = l.err != nil
		return l.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.CheckIdentity != nil {
		if err := c.CheckIdentity(); err != nil {
			metricDecision = ReasonIdentityChange
			q.decision(EpisodeReasonIdentityChange)
			return err
		}
	}
	vhosts, events, err := target.Inventory()
	if err != nil {
		metricDecision = ReasonUnavailable
		q.decision(EpisodeReasonUnavailable)
		return err
	}
	if !slices.Contains(vhosts, q.fd) {
		metricDecision = ReasonIdentityChange
		q.decision(EpisodeReasonIdentityChange)
		return errors.New("vhost FD disappeared before recovery")
	}
	fd, err := target.Duplicate(q.fd)
	if err != nil {
		metricDecision = ReasonUnavailable
		q.decision(EpisodeReasonUnavailable)
		return err
	}
	defer unix.Close(fd)
	if q.policy.verification == nil {
		q.journal.OpenForWrite(EpisodeReasonCandidate)
		q.journal.RecordDecision(q.lastDecision)
	}
	if l.err != nil {
		outputFailed = true
		return l.err
	}
	if err := RekickContext(ctx, target, bpf, fd, s, events); err != nil {
		var writeErr *KickWriteError
		if !errors.As(err, &writeErr) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			metricDecision = ReasonUnavailable
		}
		q.lastDecision = EpisodeReasonUnavailable
		return err
	}
	if q.policy.verification == nil {
		q.verificationEventID = q.journal.EventID()
	}
	q.policy.Written(q.clock(), row.Used, row.Consumed)
	q.recordWrite(EpisodeWriteSuccess, EpisodeReasonUnconsumed)
	return l.err
}
