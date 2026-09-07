package loadharness

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
)

func currentClientRSS(ctx context.Context) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 7 {
		return 0, errors.New("invalid statm")
	}
	n, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || n == 0 || n > 1<<40 {
		return 0, errors.New("invalid RSS pages")
	}
	return n * uint64(os.Getpagesize()), nil
}
