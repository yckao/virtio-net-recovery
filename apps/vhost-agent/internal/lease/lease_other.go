//go:build !linux && !darwin

package lease

import "errors"

func Acquire(string, int) (func() error, error) {
	return nil, errors.New("recovery lease unsupported on this platform")
}
