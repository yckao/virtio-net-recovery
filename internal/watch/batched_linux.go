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

type liveCandidate struct {
	snapshot Snapshot
	used     uint16
}

type batchedQueue struct {
	fd           int
	snapshot     Snapshot
	progress     RingProgress
	candidate    *liveCandidate
	verification *verification
	reported     bool
}

// liveRow always reads the current attachment through a fresh pinned vhost FD.
// Cached kernel addresses are compared as identity tokens, never dereferenced.
func liveRow(target *Target, bpf *BPF, remoteFD int) (Snapshot, QueueRow, error) {
	fd, err := target.Duplicate(remoteFD)
	if err != nil {
		return Snapshot{}, QueueRow{}, err
	}
	defer unix.Close(fd)
	s, err := bpf.Snapshot(fd)
	if err != nil {
		return Snapshot{}, QueueRow{}, err
	}
	avail, used, err := target.Ring(s)
	if err != nil {
		return Snapshot{}, QueueRow{}, err
	}
	return s, QueueRow{VhostFD: remoteFD, EventID: s.EventID, Num: s.Num,
		Avail: avail, Used: used, Consumed: s.LastAvail, Pending: avail - s.LastAvail,
		Outstanding: avail - used, Busy: s.WorkFlags&(1<<1) != 0}, nil
}

func runBatched(ctx context.Context, c Config, target *Target, bpf *BPF, l *logger) error {
	started, lastInventory, lastSummary := monotonic(), math.Inf(-1), math.Inf(-1)
	queues := []*batchedQueue{}
	budgets := map[int]*Detector{}
	var batch *RingBatch
	var rows []QueueRow
	writes := uint64(0)
	defer func() { l.emit("stopped", map[string]any{"writes": writes, "queues": rows}) }()
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
		if c.Duration > 0 && now-started >= c.Duration {
			return l.err
		}
		if now-lastInventory >= c.InventoryInterval {
			vhosts, _, err := target.Inventory()
			if err != nil {
				return err
			}
			old := map[int]*batchedQueue{}
			for _, q := range queues {
				old[q.fd] = q
			}
			queues = nil
			snapshots := []Snapshot{}
			for _, remoteFD := range vhosts {
				s, _, err := liveRow(target, bpf, remoteFD)
				if err != nil {
					l.emit("queue_unavailable", map[string]any{"vhost_fd": remoteFD, "error": err.Error()})
					continue
				}
				q := old[remoteFD]
				delete(old, remoteFD)
				if q != nil && q.snapshot.Identity() != s.Identity() {
					bpf.Forget(q.snapshot)
					q = nil
				}
				if q == nil {
					q = &batchedQueue{fd: remoteFD}
				}
				q.snapshot = s
				queues = append(queues, q)
				snapshots = append(snapshots, s)
				if budgets[remoteFD] == nil {
					budgets[remoteFD] = NewDetector(c.Threshold, c.Cooldown, c.MaxRecoveries)
				}
			}
			for _, q := range old {
				bpf.Forget(q.snapshot)
			}
			batch = nil
			if len(snapshots) != 0 {
				batch, err = NewRingBatch(target, snapshots)
				if err != nil {
					return err
				}
			}
			lastInventory = now
		}
		if batch != nil {
			if err := batch.Read(); err != nil {
				l.emit("queue_unavailable", map[string]any{"error": err.Error(), "scope": "ring_batch"})
				for _, q := range queues {
					q.progress.Reset(now)
					q.candidate, q.verification, q.reported = nil, nil, false
				}
				lastInventory = math.Inf(-1)
			} else {
				for index, q := range queues {
					avail, used := batch.Indices(index)
					ready := q.progress.Observe(avail, used, q.snapshot.Num, now, c.Threshold)
					if !ready {
						q.candidate, q.reported = nil, false
					}
					if !ready && q.verification == nil {
						continue
					}
					err := func() error {
						s, row, err := liveRow(target, bpf, q.fd)
						if err != nil {
							return err
						}
						if s.Identity() != q.snapshot.Identity() {
							return errors.New("queue identity changed during cached ring polling")
						}
						if v := q.verification; v != nil {
							if row.Used != v.used && row.Consumed != v.consumed {
								f := fields(row)
								f["latency"] = now - v.at
								l.emit("recovered", f)
								q.verification = nil
							} else if now-v.at >= c.VerifyTimeout {
								l.emit("recovery_unconfirmed", fields(row))
								q.verification = nil
							}
						}
						if !ready {
							return nil
						}
						if row.Used != used || row.Pending == 0 || uint32(row.Pending) > s.Num ||
							uint32(row.Outstanding) > s.Num || row.Busy {
							q.progress.Reset(now)
							q.candidate, q.reported = nil, false
							return nil
						}
						if q.candidate == nil {
							q.candidate = &liveCandidate{snapshot: s, used: row.Used}
							l.emit("candidate", fields(row))
							return nil
						}
						previous := q.candidate
						if previous.snapshot.Identity() != s.Identity() || previous.used != row.Used || previous.snapshot.LastAvail != s.LastAvail {
							q.progress.Reset(now)
							q.candidate, q.reported = nil, false
							return nil
						}
						row.Stalled = true
						if !q.reported {
							f := fields(row)
							f["age"] = now - q.progress.Since
							f["confirmation"] = "two_live_snapshots"
							l.emit("stall", f)
							q.reported = true
						}
						budget := budgets[q.fd]
						if !budget.Allowed(now) || ctx.Err() != nil {
							return nil
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
						err = Rekick(target, bpf, fd, s, events)
						unix.Close(fd)
						if err != nil {
							return err
						}
						budget.Kicked(now)
						writes++
						f := fields(row)
						f["reason"] = "stall"
						l.emit("rekick", f)
						q.verification = &verification{now, row.Used, row.Consumed, true}
						q.candidate = nil
						return nil
					}()
					if err != nil {
						l.emit("queue_unavailable", map[string]any{"vhost_fd": q.fd, "error": err.Error()})
						q.progress.Reset(now)
						q.candidate, q.verification, q.reported = nil, nil, false
						lastInventory = math.Inf(-1)
					}
				}
			}
		}
		if now-lastSummary >= c.SummaryInterval {
			rows = nil
			for _, q := range queues {
				s, row, err := liveRow(target, bpf, q.fd)
				if err != nil || s.Identity() != q.snapshot.Identity() {
					lastInventory = math.Inf(-1)
					continue
				}
				rows = append(rows, row)
			}
			l.emit("sample", map[string]any{"writes": writes, "queues": rows, "source": "live_snapshot"})
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
