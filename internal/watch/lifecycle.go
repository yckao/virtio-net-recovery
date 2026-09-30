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
