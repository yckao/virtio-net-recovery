package watch

import (
	"encoding/binary"
	"testing"
)

func configured() Snapshot {
	return Snapshot{VQ: 1, KickFile: 2, Context: 3, Avail: 4, Used: 5, Backend: 6, PollWQH: 7, ContextWQH: 7, Num: 256, LittleEndian: 1, LastAvail: 10}
}

func TestWireLayout(t *testing.T) {
	if binary.Size(Snapshot{}) != 120 || binary.Size(Counters{}) != 72 {
		t.Fatal("wire layout differs from BPF map values")
	}
	if err := configured().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectUnsupportedSnapshot(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"backend": func(s *Snapshot) { s.Backend = 0 }, "kick": func(s *Snapshot) { s.KickFile = 0 },
		"waiter": func(s *Snapshot) { s.PollWQH = 8 }, "size": func(s *Snapshot) { s.Num = 255 },
		"empty": func(s *Snapshot) { s.Num = 0 }, "packed": func(s *Snapshot) { s.Features = 1 << 34 },
		"iommu": func(s *Snapshot) { s.Features = 1 << 33 }, "endian": func(s *Snapshot) { s.LittleEndian = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			s := configured()
			mutate(&s)
			if s.Validate() == nil {
				t.Fatal("accepted unsupported queue")
			}
		})
	}
}

func TestLostNotifyStopsWithoutSignalEvent(t *testing.T) {
	d := NewDetector(3, 30, 3)
	s := configured()
	if d.Observe(s, 20, 10, false, 0) || d.Observe(s, 40, 10, false, 2) || !d.Observe(s, 248, 10, false, 3) {
		t.Fatal("published backlog should age without a host notification event")
	}
}

func TestIdleAndConsumedBacklogDoNotStall(t *testing.T) {
	for _, avail := range []uint16{10, 20} {
		d := NewDetector(3, 30, 3)
		s := configured()
		s.LastAvail = avail
		for _, now := range []float64{0, 10, 100} {
			if d.Observe(s, avail, 10, false, now) {
				t.Fatal("no unconsumed descriptors")
			}
		}
	}
}

func TestWrapAndResetConditions(t *testing.T) {
	s := configured()
	s.LastAvail = 65530
	d := NewDetector(3, 30, 3)
	d.Observe(s, 3, 65530, false, 0)
	if !d.Observe(s, 3, 65530, false, 3) {
		t.Fatal("16-bit wrap is valid")
	}
	for _, kind := range []string{"completion", "consumption", "identity", "busy", "inconsistent"} {
		t.Run(kind, func(t *testing.T) {
			s := configured()
			d := NewDetector(3, 30, 3)
			d.Observe(s, 20, 10, false, 0)
			avail, used, busy := uint16(20), uint16(10), false
			switch kind {
			case "completion":
				used++
			case "consumption":
				s.LastAvail++
			case "identity":
				s.EventID++
			case "busy":
				busy = true
			case "inconsistent":
				avail = 500
			}
			if d.Observe(s, avail, used, busy, 10) || d.Observe(s, 20, used, false, 11) {
				t.Fatal("timer did not restart")
			}
		})
	}
}

func TestBudgetSurvivesIdentityChanges(t *testing.T) {
	d := NewDetector(3, 30, 2)
	s := configured()
	if !d.Allowed(0) {
		t.Fatal("initial kick denied")
	}
	d.Kicked(0)
	if d.Allowed(29) {
		t.Fatal("cooldown ignored")
	}
	s.EventID = 100
	d.Observe(s, 20, 10, false, 30)
	if !d.Allowed(30) {
		t.Fatal("cooldown did not expire")
	}
	d.Kicked(30)
	if d.Allowed(60) || !d.Allowed(3600) {
		t.Fatal("hourly budget incorrectly reset or retained")
	}
}
