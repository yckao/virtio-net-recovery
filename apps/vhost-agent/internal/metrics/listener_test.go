package metrics

import (
	"net"
	"testing"
	"time"
)

type fakeListener struct {
	peers  []net.Conn
	closed bool
}

func (l *fakeListener) Accept() (net.Conn, error) {
	if l.closed {
		return nil, net.ErrClosed
	}
	a, b := net.Pipe()
	l.peers = append(l.peers, b)
	return a, nil
}
func (l *fakeListener) Close() error   { l.closed = true; return nil }
func (l *fakeListener) Addr() net.Addr { return nil }
func TestConnectionAdmissionAndReleaseAreBounded(t *testing.T) {
	base := &fakeListener{}
	listener := boundedListener(base)
	defer func() {
		for _, c := range base.peers {
			c.Close()
		}
	}()
	var active []net.Conn
	for i := 0; i < 32; i++ {
		c, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		active = append(active, c)
	}
	next := make(chan net.Conn, 1)
	go func() { c, _ := listener.Accept(); next <- c }()
	select {
	case <-next:
		t.Fatal("exceeded connection capacity")
	case <-time.After(10 * time.Millisecond):
	}
	active[0].Close()
	active[0].Close()
	select {
	case c := <-next:
		active = append(active, c)
	case <-time.After(time.Second):
		t.Fatal("capacity not released")
	}
	for _, c := range active {
		c.Close()
	}
	listener.Close()
	if _, err := listener.Accept(); err == nil {
		t.Fatal("closed listener accepted")
	}
}
