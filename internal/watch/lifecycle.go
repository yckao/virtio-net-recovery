package watch

// Retirement avoids restarting a completed current process generation. A
// disappeared generation no longer needs a marker and must not accumulate.
func pruneRetiredTargets(retired, wanted map[string]bool) {
	for key := range retired {
		if !wanted[key] {
			delete(retired, key)
		}
	}
}

// An interrupted inventory refresh still owns snapshots not yet transferred
// into its current set. Release both attachment generations, once per slot.
func forgetSnapshotSets(current, pending map[int]Snapshot, forget func(Snapshot)) {
	for fd, s := range pending {
		if live, ok := current[fd]; !ok || live.Identity() != s.Identity() {
			forget(s)
		}
	}
	for _, s := range current {
		forget(s)
	}
}
