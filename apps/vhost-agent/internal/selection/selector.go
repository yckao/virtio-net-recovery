// Package selection adapts a public selector to application target identities.
package selection

import (
	"context"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	discovery "github.com/yckao/virtio-net-recovery/modules/qemu-discovery"
)

type Selector struct{ source *discovery.Selector }
type Result struct {
	Targets         []control.Target
	Problems        []error
	Complete        bool
	ProblemsOmitted int
}

func New(o discovery.Options) (*Selector, error) {
	s, err := discovery.New(o)
	if err != nil {
		return nil, err
	}
	return &Selector{s}, nil
}
func (s *Selector) Resolve(ctx context.Context) (Result, error) {
	in, err := s.source.Resolve(ctx)
	if err != nil {
		return Result{}, err
	}
	out := Result{Complete: in.Complete, ProblemsOmitted: in.ProblemsOmitted}
	for _, t := range in.Targets {
		out.Targets = append(out.Targets, control.Target{PID: t.PID, StartTime: t.StartTime, Name: t.Domain})
	}
	for _, p := range in.Problems {
		out.Problems = append(out.Problems, p.Err)
	}
	return out, nil
}
