// Package module owns this experiment's module asset and Linux module syscalls.
// It has no knowledge of a queue backend or recovery policy.
package module

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const Name = "vhost_fault"

var ErrUnsupported = errors.New("fault injection requires Linux amd64")

type Bounds struct {
	Delay, Window time.Duration
	MaxDrops      uint32
}

type Binding struct {
	FD     int
	Waiter uint64
}

type Statistics struct {
	Loaded           bool
	Matched, Dropped uint64
	Active           bool
}

func State(ctx context.Context) (Statistics, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return Statistics{}, ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return Statistics{}, err
	}
	const root = "/sys/module/vhost_fault/parameters/"
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return Statistics{}, nil
	} else if err != nil {
		return Statistics{}, err
	}
	matched, err := os.ReadFile(root + "matched")
	if err != nil {
		return Statistics{}, err
	}
	dropped, err := os.ReadFile(root + "dropped")
	if err != nil {
		return Statistics{}, err
	}
	active, err := os.ReadFile(root + "active")
	if err != nil {
		return Statistics{}, err
	}
	m, err := strconv.ParseUint(strings.TrimSpace(string(matched)), 10, 64)
	if err != nil {
		return Statistics{}, err
	}
	d, err := strconv.ParseUint(strings.TrimSpace(string(dropped)), 10, 64)
	if err != nil {
		return Statistics{}, err
	}
	a := strings.TrimSpace(string(active))
	if a != "Y" && a != "N" {
		return Statistics{}, errors.New("invalid module active state")
	}
	return Statistics{Loaded: true, Matched: m, Dropped: d, Active: a == "Y"}, nil
}
