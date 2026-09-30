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
	fd                              int
	snapshot                        Snapshot
	policy                          *RecoveryPolicy
	avail, used                     uint16
	lastLive, quietSince, sampledAt float64
	incident, recoveredReported     bool
	lastError                       string
}

func (q *recoveryQueue) summary(now float64) map[string]any {
	return map[string]any{"vhost_fd": q.fd, "eventfd_id": q.snapshot.EventID,
		"num": q.snapshot.Num, "avail": q.avail, "used": q.used,
		"outstanding": q.avail - q.used, "incident": q.incident,
		"attempts": q.policy.Attempts, "writes": q.policy.Writes,
		"max_attempt_gap": q.policy.MaxAttemptGap,
		"refusals":        q.policy.Refusals, "confirmed": q.policy.Confirmed,
		"unconfirmed_age": q.policy.PendingAge(now), "indices_sample_at": q.sampledAt}
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
	var pendingCleanup map[int]Snapshot
	policies := map[int]*RecoveryPolicy{}
	var batch *RingBatch
	var attempts, refusals, pollErrors uint64
	var progress recoveryProgressTotals
	forceInventory := true
	lastInventoryError := ""
	discoveryFailures := map[int]string{}
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
		return map[string]any{"attempts": attempts, "writes": progress.writes, "refusals": refusals, "confirmed": progress.confirmed,
			"poll_errors": pollErrors, "max_poll_gap": max(maxPollGap, now-lastPoll), "max_loop_gap": maxLoopGap,
			"queues": rows, "unavailable_queues": len(unavailable), "source": "cached_user_indices"}
	}
	defer func() {
		current := make(map[int]Snapshot, len(known))
		for fd, q := range known {
			current[fd] = q.snapshot
		}
		forgetSnapshotSets(current, pendingCleanup, bpf.Forget)
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
		alive, err := target.CheckAlive()
		if err != nil {
			return err
		}
		if !alive {
			l.emit("target_exited", map[string]any{})
			return l.err
		}
		now := monotonic()
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
				}
				if err.Error() != lastInventoryError {
					l.emit("inventory_unavailable", map[string]any{"error": err.Error()})
					lastInventoryError = err.Error()
				}
			} else {
				lastInventoryError = ""
				old := map[int]*recoveryQueue{}
				pendingCleanup = make(map[int]Snapshot, len(known))
				for fd, q := range known {
					old[fd] = q
					pendingCleanup[fd] = q.snapshot
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
						bpf.Forget(q.snapshot)
						l.emit("queue_reconfigured", map[string]any{"vhost_fd": remoteFD, "unconfirmed_age": p.PendingAge(now)})
						// Preserve slot pacing and counters; an old attachment's
						// verification can never confirm a replacement attachment.
						p.verification = nil
						p.Invalidate(now)
						q = nil
					}
					if q == nil {
						q = &recoveryQueue{fd: remoteFD, policy: p, lastLive: math.Inf(-1)}
					}
					q.snapshot, q.avail, q.used, q.sampledAt = s, row.Avail, row.Used, monotonic()
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
						continue
					}
					bpf.Forget(q.snapshot)
					q.policy.verification = nil
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
				}
				forceInventory = true
			} else {
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
					if ready {
						q.quietSince = 0
					} else if q.quietSince == 0 {
						q.quietSince = now
					}
					verify := q.policy.NeedsVerification(q.used)
					if (ready || verify) && now-q.lastLive >= c.Interval {
						inspect := ready && (c.Mode == "observe" || q.policy.Attempt(now))
						attempt := inspect && c.Mode == "recover"
						if inspect || verify {
							q.lastLive = now
							if attempt {
								attempts++
							}
							beforeWrites, beforeConfirmed := q.policy.Writes, q.policy.Confirmed
							err := recoverLive(ctx, c, target, bpf, l, q, inspect)
							q.lastLive = monotonic()
							if attempt {
								q.policy.FinishAttempt(q.lastLive)
							}
							progress.add(q.policy, beforeWrites, beforeConfirmed)
							if err != nil {
								if ctx.Err() != nil {
									return l.err
								}
								event := "recovery_refused"
								if q.policy.Refused(attempt) {
									refusals++
								} else {
									pollErrors++
									event = "observation_unavailable"
								}
								q.policy.Invalidate(now)
								forceInventory = true
								if q.lastError != err.Error() {
									l.emit(event, map[string]any{"vhost_fd": q.fd, "error": err.Error()})
									q.lastError = err.Error()
								}
							} else {
								q.lastError = ""
							}
						}
					}
					// Hysteresis suppresses reports only. A new candidate is
					// always eligible for paced recovery during this quiet period.
					if q.incident && q.policy.verification == nil && q.quietSince != 0 && now-q.quietSince >= 1 {
						l.emit("incident_closed", q.summary(now))
						q.incident, q.recoveredReported = false, false
					}
				}
			}
		}
		// Coverage loss must not postpone the original verification deadline.
		// Preserve the baseline so later live progress can still be observed.
		for _, q := range known {
			if q.policy.Unconfirmed(now, c.VerifyTimeout) {
				l.emit("recovery_unconfirmed", q.summary(now))
			}
		}
		if now-lastSummary >= c.SummaryInterval {
			l.emit("sample", summary(now))
			lastSummary = now
		}
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

func recoverLive(ctx context.Context, c Config, target liveQueueTarget, bpf snapshotter, l *logger, q *recoveryQueue, candidate bool) error {
	s, row, err := liveRow(target, bpf, q.fd)
	if err != nil {
		return err
	}
	if s.Identity() != q.snapshot.Identity() {
		return errors.New("queue identity changed during cached ring polling")
	}
	now := monotonic()
	if latency, confirmed := q.policy.Verify(now, row.Used, row.Consumed); confirmed && !q.recoveredReported {
		f := fields(row)
		f["latency"], f["writes"], f["attempts"] = latency, q.policy.Writes, q.policy.Attempts
		l.emit("progress_after_kick", f)
		q.recoveredReported = true
	}
	if !candidate {
		return l.err
	}
	if row.Used != q.used || row.Pending == 0 || uint32(row.Pending) > s.Num || uint32(row.Outstanding) > s.Num || row.WorkQueued {
		q.policy.Invalidate(now)
		return nil
	}
	row.Stalled = true
	if !q.incident {
		f := fields(row)
		f["age"], f["confirmation"] = now-q.policy.Progress.Since, "cached_completion_progress_and_live_pending"
		l.emit("candidate", f)
		q.incident = true
	}
	if l.err != nil || c.Mode != "recover" {
		return l.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.CheckIdentity != nil {
		if err := c.CheckIdentity(); err != nil {
			return err
		}
	}
	vhosts, events, err := target.Inventory()
	if err != nil {
		return err
	}
	if !slices.Contains(vhosts, q.fd) {
		return errors.New("vhost FD disappeared before recovery")
	}
	fd, err := target.Duplicate(q.fd)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := RekickContext(ctx, target, bpf, fd, s, events); err != nil {
		return err
	}
	q.policy.Written(monotonic(), row.Used, row.Consumed)
	f := fields(row)
	f["writes"], f["progress"] = q.policy.Writes, "unconfirmed"
	l.emit("kick_written", f)
	return l.err
}
