// Package kernel owns the private Linux/BPF transport, descriptors and kernel
// pointer identities. None of its values are public module contracts.
package kernel

import (
	"errors"
	"fmt"
)

var ErrUnsupported = errors.New("unsupported kernel observation")

// Attachment deliberately lists identity fields independently of wire layout.
type Attachment struct {
	VQ, KickFile, Context, Avail, Used, Backend, Features, Work, Wait, PollWQH, ContextWQH uint64
	Num, EventID                                                                           uint32
	LittleEndian                                                                           uint8
}

func (s Snapshot) Attachment() Attachment {
	return Attachment{s.VQ, s.KickFile, s.Context, s.Avail, s.Used, s.Backend, s.Features, s.Work, s.Wait, s.PollWQH, s.ContextWQH, s.Num, s.EventID, s.LittleEndian}
}
func (s Snapshot) Validate() error {
	if s.Schema != wireSchema {
		return fmt.Errorf("%w: BPF wire schema mismatch", ErrUnsupported)
	}
	if s.VQ == 0 || s.KickFile == 0 || s.Context == 0 || s.Avail == 0 || s.Used == 0 || s.Backend == 0 {
		return errors.New("queue lacks an active kick/backend/ring")
	}
	if s.PollWQH == 0 || s.PollWQH != s.ContextWQH {
		return errors.New("kick does not match attached waiter")
	}
	if s.Num < 1 || s.Num > 32768 || s.Num&(s.Num-1) != 0 {
		return fmt.Errorf("%w: split-ring size", ErrUnsupported)
	}
	if s.Features&(1<<34) != 0 || s.Features&(1<<33) != 0 || s.LittleEndian != 1 {
		return fmt.Errorf("%w: requires little-endian split ring without IOMMU translation", ErrUnsupported)
	}
	return nil
}

type Counters struct{ Signals, Writes, Wakeups, Handlers, LastSignalNS, LastWriteNS, LastWakeupNS, LastHandlerNS, Active, Epoch uint64 }
