//go:build linux && amd64

package watch

import "golang.org/x/sys/unix"

type liveQueueTarget interface {
	recoveryTarget
	Inventory() ([]int, map[uint32][]int, error)
	Ring(Snapshot) (uint16, uint16, error)
}

// liveRow always reads the current attachment through a fresh pinned vhost FD.
// Cached kernel addresses are compared as identity tokens, never dereferenced.
func liveRow(target liveQueueTarget, bpf snapshotter, remoteFD int) (Snapshot, QueueRow, error) {
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
		Outstanding: avail - used, WorkQueued: s.WorkFlags&(1<<1) != 0, Busy: s.WorkFlags&(1<<1) != 0}, nil
}
