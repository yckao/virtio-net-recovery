//go:build linux || darwin

package lease_test

import (
	"testing"

	"github.com/yckao/virtio-net-recovery/internal/agent/lease"
)

func TestExclusiveOwnerAndRelease(t *testing.T) {
	dir := t.TempDir()
	release, err := lease.Acquire(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lease.Acquire(dir, 42); err == nil {
		second()
		t.Fatal("two automatic owners")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	next, err := lease.Acquire(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	next()
}
