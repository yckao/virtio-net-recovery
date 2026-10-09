// Package binding defines the private protocol used by the opt-in experimental
// bridge. Production public interfaces do not expose these values.
package binding

type Borrowed struct {
	FD     int
	Waiter uint64
}
type LoadResult struct {
	Loaded bool
	Err    error
}
