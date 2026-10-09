package kernel

import "testing"

func TestAttachmentIdentityExcludesTransientWireFields(t *testing.T) {
	a := Snapshot{TimestampNS: 10, LastAvail: 4, LastUsed: 3, WorkFlags: 2}
	b := a
	b.TimestampNS++
	b.LastAvail++
	b.LastUsed++
	b.WorkFlags = 0
	b.Schema++
	b.Reserved[0] = 1
	if a.Attachment() != b.Attachment() {
		t.Fatal("wire/transient fields changed attachment identity")
	}
	b.Context++
	if a.Attachment() == b.Attachment() {
		t.Fatal("eventfd context change did not change identity")
	}
}
func TestWireValidationRejectsUnsupportedRings(t *testing.T) {
	good := Snapshot{Schema: 3, VQ: 1, KickFile: 2, Context: 3, Avail: 4, Used: 5, Backend: 6, PollWQH: 7, ContextWQH: 7, Num: 256, LittleEndian: 1}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Snapshot){func(s *Snapshot) { s.Features = 1 << 34 }, func(s *Snapshot) { s.Features = 1 << 33 }, func(s *Snapshot) { s.LittleEndian = 0 }, func(s *Snapshot) { s.ContextWQH++ }, func(s *Snapshot) { s.Schema = 2 }} {
		s := good
		change(&s)
		if s.Validate() == nil {
			t.Fatal("unsupported snapshot accepted")
		}
	}
}
func TestProcessStatUsesLastParenthesis(t *testing.T) {
	text := []byte("42 (name ) contains spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 12345")
	n, err := StartTime(text)
	if err != nil || n != 12345 {
		t.Fatalf("%d %v", n, err)
	}
	for _, text := range []string{"", "42 (bad) S", "42 no comm"} {
		if _, err := StartTime([]byte(text)); err == nil {
			t.Fatal("malformed stat accepted")
		}
	}
}
