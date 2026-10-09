package cli

import (
	"context"
	"errors"

	"github.com/yckao/virtio-net-recovery/internal/agent/backend"
	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/internal/agent/lease"
	"github.com/yckao/virtio-net-recovery/internal/agent/selection"
	"github.com/yckao/virtio-net-recovery/internal/agent/supervision"
	"github.com/yckao/virtio-net-recovery/vhost"
)

type targetSource struct{ selector *selection.Selector }

func (s targetSource) Resolve(ctx context.Context) (supervision.Inventory, error) {
	r, err := s.selector.Resolve(ctx)
	return supervision.Inventory{Targets: r.Targets, Complete: r.Complete, Problems: len(r.Problems) + r.ProblemsOmitted}, err
}

type factory struct {
	host     *vhost.Host
	recover  bool
	stateDir string
}

func (f factory) Open(ctx context.Context, t control.Target) (supervision.Session, error) {
	owned := &workerSession{}
	if f.recover {
		release, err := lease.Acquire(f.stateDir, t.PID)
		if err != nil {
			return nil, err
		}
		owned.release = release
	}
	session, err := backend.Open(ctx, f.host, t)
	if err != nil {
		return nil, errors.Join(err, owned.Close())
	}
	owned.Reader = session.Reader()
	if f.recover {
		owned.writer, err = session.Writer()
		if err != nil {
			return nil, errors.Join(err, owned.Close())
		}
	}
	return owned, nil
}

type workerSession struct {
	control.Reader
	writer  control.Writer
	release func() error
}

func (s *workerSession) Writer() control.Writer { return s.writer }
func (s *workerSession) Close() error {
	var err error
	if s.Reader != nil {
		err = s.Reader.Close()
		s.Reader = nil
	}
	if s.release != nil {
		err = errors.Join(err, s.release())
		s.release = nil
	}
	return err
}
