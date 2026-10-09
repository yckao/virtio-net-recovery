//go:build linux && amd64

package kernel

import (
	"errors"
	"os"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSnapshotRequestsStayOnDistinctThreads(t *testing.T) {
	owners := make(chan uint64, 2)
	results := make(chan error, 2)
	proceed := make(chan struct{})
	for range 2 {
		go func() {
			var owner uint64
			cleared := false
			err := withSnapshotThread(func(value uint64) error {
				current := uint64(uint32(os.Getpid()))<<32 | uint64(uint32(unix.Gettid()))
				if value != 0 {
					owner = value
				} else {
					cleared = true
				}
				if owner != current {
					return errors.New("binding moved between OS threads")
				}
				return nil
			}, func() error {
				owners <- owner
				<-proceed
				for range 20 {
					runtime.Gosched()
					if uint32(owner) != uint32(unix.Gettid()) {
						return errors.New("request moved between OS threads")
					}
				}
				return nil
			})
			if !cleared {
				err = errors.Join(err, errors.New("request remained armed"))
			}
			results <- err
		}()
	}
	a, b := <-owners, <-owners
	close(proceed)
	if a == b || a>>32 != uint64(os.Getpid()) || b>>32 != uint64(os.Getpid()) {
		t.Errorf("concurrent probes share request identity: %x / %x", a, b)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
}

func TestSnapshotRequestDisarmsOnFailure(t *testing.T) {
	requestFailure, disarmFailure := errors.New("ioctl"), errors.New("disarm")
	var calls []uint64
	err := withSnapshotThread(func(owner uint64) error {
		calls = append(calls, owner)
		if owner == 0 {
			return disarmFailure
		}
		return nil
	}, func() error { return requestFailure })
	if len(calls) != 2 || calls[0] == 0 || calls[1] != 0 || !errors.Is(err, requestFailure) || !errors.Is(err, disarmFailure) {
		t.Fatalf("calls %v, error %v", calls, err)
	}
}

func TestSnapshotRequestDoesNotRunWhenBindingFails(t *testing.T) {
	failure := errors.New("bind")
	err := withSnapshotThread(func(uint64) error { return failure }, func() error {
		t.Fatal("request ran without attribution")
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
