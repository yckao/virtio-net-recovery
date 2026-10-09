//go:build !linux || !amd64

package vhost

import "context"

type Host struct{}

func Open(o Options) (*Host, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	return nil, ErrUnsupported
}
func (*Host) OpenProcess(context.Context, ProcessIdentity) (ReadSession, error) {
	return nil, ErrUnsupported
}
func (*Host) Conditional(ReadSession) (ConditionalNotifier, error) { return nil, ErrUnsupported }
func (*Host) Manual(ReadSession) (ManualNotifier, error)           { return nil, ErrUnsupported }
func (*Host) Trace(ReadSession) (TraceSession, error)              { return nil, ErrUnsupported }
func (*Host) Close() error                                         { return nil }
