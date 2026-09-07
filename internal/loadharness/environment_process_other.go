//go:build !darwin && !linux

package loadharness

import (
	"context"
	"errors"
)

func readProcessIdentity(int) (nativeProcessIdentity, error) {
	return nativeProcessIdentity{}, errors.New("unsupported_process_observer")
}
func readProcessResources(context.Context, int) (nativeProcessResources, error) {
	return nativeProcessResources{}, errors.New("unsupported_process_observer")
}
