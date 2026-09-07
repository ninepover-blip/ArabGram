//go:build !darwin && !linux

package loadharness

import (
	"context"
	"errors"
)

func nativeClientResources(context.Context) (uint64, uint64, uint64, float64, error) {
	return 0, 0, 0, 0, errors.New("resource evidence unsupported on this platform")
}
func nativeDiskResources(string) (DiskResourceSample, error) {
	return DiskResourceSample{}, errors.New("disk evidence unsupported on this platform")
}
