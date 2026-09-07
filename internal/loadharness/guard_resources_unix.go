//go:build darwin || linux

package loadharness

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func nativeClientResources(ctx context.Context) (rss, files, soft uint64, cpu float64, err error) {
	rss, err = currentClientRSS(ctx)
	if err != nil {
		return
	}
	path := "/dev/fd"
	if runtime.GOOS == "linux" {
		path = "/proc/self/fd"
	}
	dir, e := os.Open(path)
	if e != nil {
		err = e
		return
	}
	defer dir.Close()
	for {
		entries, e := dir.ReadDir(128)
		files += uint64(len(entries))
		if files > 1<<20 {
			err = errors.New("descriptor sample limit")
			return
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			err = e
			return
		}
	}
	var limit unix.Rlimit
	if err = unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return
	}
	soft = limit.Cur
	var usage unix.Rusage
	if err = unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		return
	}
	cpu = float64(unix.TimevalToNsec(usage.Utime)+unix.TimevalToNsec(usage.Stime)) / 1e9
	return
}
func nativeDiskResources(path string) (DiskResourceSample, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return DiskResourceSample{}, err
	}
	return DiskResourceSample{Free: uint64(s.Bavail) * uint64(s.Bsize), Total: uint64(s.Blocks) * uint64(s.Bsize)}, nil
}
