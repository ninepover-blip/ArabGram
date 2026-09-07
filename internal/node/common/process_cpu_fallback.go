//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package common

func processCPUSeconds() (float64, bool) {
	return 0, false
}
