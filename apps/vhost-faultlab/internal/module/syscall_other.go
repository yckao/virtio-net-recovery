//go:build !linux || !amd64

package module

import (
	"context"
	"os"
)

func Load(context.Context, *os.File, Binding, Bounds) error { return ErrUnsupported }
func Unload(context.Context) error                          { return ErrUnsupported }
func Lock(string) (func() error, error)                     { return nil, ErrUnsupported }
