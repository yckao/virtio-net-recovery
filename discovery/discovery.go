// Package discovery selects QEMU process generations from procfs and libvirt runtime XML.
// Selection is a hint: a consumer must pin and validate the returned generation.
package discovery

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
)

type Target struct {
	PID          int
	StartTime    uint64
	Domain, UUID string
}

func (t Target) Key() string { return fmt.Sprintf("%d:%d", t.PID, t.StartTime) }

type Options struct {
	PIDs                                []int
	DomainPattern, ProcRoot, RuntimeDir string
	MaxTargets                          int
}
type Problem struct {
	PID    int
	Domain string
	Err    error
}
type Result struct {
	Targets  []Target
	Problems []Problem
	Complete bool
	// ProblemsOmitted counts failures beyond MaxProblems. Truncation always
	// makes the inventory incomplete, never an authoritative empty result.
	ProblemsOmitted int
}

// Fixed limits bound one resolution, independently of caller-selected targets.
const (
	MaxProblems        = 256
	MaxProblemBytes    = 1024
	MaxRuntimeEntries  = 16384
	MaxDomainNameBytes = 256
	MaxUUIDBytes       = 128
)

func (r *Result) problem(pid int, domain string, err error) {
	if len(r.Problems) == MaxProblems {
		r.ProblemsOmitted++
		r.Complete = false
		return
	}
	message := err.Error()
	if len(message) > MaxProblemBytes {
		message = message[:MaxProblemBytes-3] + "..."
	}
	if len(domain) > MaxDomainNameBytes {
		domain = domain[:MaxDomainNameBytes]
	}
	r.Problems = append(r.Problems, Problem{PID: pid, Domain: domain, Err: errors.New(message)})
}

// Selector retains explicit-PID anchors. A single caller owns and serializes it.
type Selector struct {
	options  Options
	pattern  *regexp.Regexp
	anchored map[int]uint64
}

func New(o Options) (*Selector, error) {
	if len(o.PIDs) == 0 && o.DomainPattern == "" {
		return nil, errors.New("select a PID or domain pattern")
	}
	if o.ProcRoot == "" {
		o.ProcRoot = "/proc"
	}
	if o.RuntimeDir == "" {
		o.RuntimeDir = "/run/libvirt/qemu"
	}
	if o.MaxTargets == 0 {
		o.MaxTargets = 128
	}
	if o.MaxTargets < 1 || o.MaxTargets > 4096 {
		return nil, errors.New("max targets must be in 1..4096")
	}
	for _, pid := range o.PIDs {
		if pid <= 0 {
			return nil, errors.New("PID must be positive")
		}
	}
	o.PIDs = slices.Clone(o.PIDs)
	slices.Sort(o.PIDs)
	o.PIDs = slices.Compact(o.PIDs)
	if len(o.PIDs) > o.MaxTargets {
		return nil, errors.New("explicit selection exceeds target limit")
	}
	var pattern *regexp.Regexp
	if o.DomainPattern != "" {
		var err error
		pattern, err = regexp.Compile(o.DomainPattern)
		if err != nil {
			return nil, err
		}
	}
	return &Selector{o, pattern, make(map[int]uint64)}, nil
}
