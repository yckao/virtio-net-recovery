package metrics

import (
	"net"
	"sync"
)

// boundListener admits at most 32 active HTTP connections. Header/write/idle
// deadlines bound each connection's residence; diagnostics need no unbounded
// request fan-out. One blocked Accept waits for capacity, never a control worker.
type boundListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

func boundedListener(listener net.Listener) *boundListener {
	return &boundListener{Listener: listener, slots: make(chan struct{}, 32), done: make(chan struct{})}
}
func (l *boundListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	connection, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &boundConnection{Conn: connection, release: func() { <-l.slots }}, nil
}
func (l *boundListener) Close() error { l.once.Do(func() { close(l.done) }); return l.Listener.Close() }

type boundConnection struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *boundConnection) Close() error { err := c.Conn.Close(); c.once.Do(c.release); return err }
