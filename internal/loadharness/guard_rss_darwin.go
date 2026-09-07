package loadharness

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// ps returns current RSS in KiB for this exact process. RUSAGE.Maxrss is a
// lifetime peak and must not be substituted for a current RSS observation.
func currentClientRSS(parent context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(parent, resourcePSDeadline)
	defer cancel()
	data, err := exec.CommandContext(ctx, "/bin/ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0, err
	}
	if len(data) > 128 {
		return 0, errors.New("unexpected RSS output")
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || n == 0 || n > 1<<40 {
		return 0, errors.New("invalid RSS")
	}
	return n * 1024, nil
}
