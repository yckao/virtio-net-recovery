// Package watch observes vhost-net TX queues and verifies recovery targets.
package watch

import (
	"errors"
)

// Snapshot matches agent/wire.h. Kernel addresses are never emitted to logs.
type Snapshot struct {
	TimestampNS, VQ, KickFile, Context, Avail, Used               uint64
	Backend, Features, WorkFlags, Work, Wait, PollWQH, ContextWQH uint64
	Num, EventID                                                  uint32
	LastAvail, LastUsed                                           uint16
	LittleEndian                                                  uint8
	Reserved                                                      [3]uint8
}

func (s Snapshot) Identity() Snapshot {
	s.TimestampNS, s.WorkFlags, s.LastAvail, s.LastUsed = 0, 0, 0, 0
	return s
}

func (s Snapshot) Validate() error {
	if s.VQ == 0 || s.KickFile == 0 || s.Context == 0 || s.Avail == 0 || s.Used == 0 || s.Backend == 0 {
		return errors.New("queue is not configured with a kick and backend")
	}
	if s.PollWQH == 0 || s.PollWQH != s.ContextWQH {
		return errors.New("kick eventfd does not match the attached vhost waiter")
	}
	if s.Num < 1 || s.Num > 32768 || s.Num&(s.Num-1) != 0 {
		return errors.New("invalid split-ring size")
	}
	if s.Features&(1<<34) != 0 || s.Features&(1<<33) != 0 || s.LittleEndian != 1 {
		return errors.New("only little-endian split rings without IOMMU translation are supported")
	}
	return nil
}

type Counters struct {
	Signals       uint64 `json:"signals"`
	Writes        uint64 `json:"writes"`
	Wakeups       uint64 `json:"wakeups"`
	Handlers      uint64 `json:"handlers"`
	LastSignalNS  uint64 `json:"last_signal_ns"`
	LastWriteNS   uint64 `json:"last_write_ns"`
	LastWakeupNS  uint64 `json:"last_wakeup_ns"`
	LastHandlerNS uint64 `json:"last_handler_ns"`
	Active        uint64 `json:"active"`
}

type QueueRow struct {
	VhostFD     int    `json:"vhost_fd"`
	EventID     uint32 `json:"eventfd_id"`
	Num         uint32 `json:"num"`
	Avail       uint16 `json:"avail"`
	Used        uint16 `json:"used"`
	Consumed    uint16 `json:"consumed"`
	Pending     uint16 `json:"pending"`
	Outstanding uint16 `json:"outstanding"`
	Stalled     bool   `json:"stalled"`
	WorkQueued  bool   `json:"work_queued"`
	// Busy is a deprecated JSON alias for WorkQueued, never a worker-idle claim.
	Busy   bool     `json:"busy"`
	Stages Counters `json:"stages"`
}
