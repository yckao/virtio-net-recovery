package cli

import (
	"context"
	"errors"

	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/backend"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/control"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/lease"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/report"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/selection"
	"github.com/yckao/virtio-net-recovery/apps/vhost-agent/internal/supervision"
	vhost "github.com/yckao/virtio-net-recovery/modules/vhost-linux"
)

type targetSource struct{ s *selection.Selector }

func (s targetSource) Resolve(ctx context.Context) (supervision.Inventory, error) {
	r, err := s.s.Resolve(ctx)
	return supervision.Inventory{Targets: r.Targets, Complete: r.Complete, Problems: len(r.Problems) + r.ProblemsOmitted}, err
}

type factory struct {
	host     *vhost.Host
	recover  bool
	stateDir string
}

func (f factory) Open(ctx context.Context, t control.Target) (control.Reader, control.Writer, func() error, error) {
	release := func() error { return nil }
	if f.recover {
		var err error
		release, err = lease.Acquire(f.stateDir, t.PID)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	session, err := backend.Open(ctx, f.host, t)
	if err != nil {
		return nil, nil, nil, errors.Join(err, release())
	}
	reader := session.Reader()
	cleanup := func() error { return errors.Join(reader.Close(), release()) }
	var writer control.Writer
	if f.recover {
		writer, err = session.Writer()
		if err != nil {
			return nil, nil, nil, errors.Join(err, cleanup())
		}
	}
	return reader, writer, cleanup, nil
}

type notices struct{ sink report.Sink }

func (n notices) TryNotice(text string) bool {
	if len(text) > report.MaxMessageBytes {
		text = text[:report.MaxMessageBytes]
	}
	return n.sink.TryWrite(report.Record{Kind: report.Status, Message: text, Incomplete: true})
}
