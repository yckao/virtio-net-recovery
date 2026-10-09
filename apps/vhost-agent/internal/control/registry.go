package control

import "sync"

// Registry tracks current stream identities independently of the lossy telemetry
// queue. Stored entries are bounded by admitted backend queues, not event count.
type Registry struct{ active sync.Map }

func (r *Registry) Add(id uint64)         { r.active.Store(id, struct{}{}) }
func (r *Registry) Remove(id uint64)      { r.active.Delete(id) }
func (r *Registry) Active(id uint64) bool { _, ok := r.active.Load(id); return ok }
