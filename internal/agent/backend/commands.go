package backend

import (
	"context"
	"errors"

	"github.com/yckao/virtio-net-recovery/internal/agent/control"
	"github.com/yckao/virtio-net-recovery/vhost"
)

// Commands grants only the capability requested by each command use case.
type Commands struct{ Host *vhost.Host }

func (s Commands) OpenQueues(ctx context.Context, target control.Target) (control.QueueSession, error) {
	session, err := Open(ctx, s.Host, target)
	if err != nil {
		return nil, err
	}
	return session.Reader(), nil
}

func (s Commands) OpenKick(ctx context.Context, target control.Target) (control.KickSession, error) {
	session, err := Open(ctx, s.Host, target)
	if err != nil {
		return nil, err
	}
	writer, err := session.Manual()
	if err != nil {
		return nil, errors.Join(err, session.Reader().Close())
	}
	return kickSession{session.Reader(), writer}, nil
}

type kickSession struct {
	control.QueueSession
	control.ManualWriter
}
