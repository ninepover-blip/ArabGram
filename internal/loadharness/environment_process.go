package loadharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

type nativeProcessIdentity struct {
	PID, Parent int
	Token       string
}
type nativeProcessResources struct {
	Identity                     nativeProcessIdentity
	RSS, Files, FileLimit        uint64
	CPU                          float64
	CPUUnit, Executable, Command string
}

func processEpoch(v nativeProcessIdentity) string {
	d := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", v.PID, v.Token)))
	return hex.EncodeToString(d[:])
}
func processOwnedBy(pid, owner int) bool {
	for i := 0; i < 4 && pid > 1; i++ {
		v, e := readProcessIdentity(pid)
		if e != nil {
			return false
		}
		if v.Parent == owner {
			return true
		}
		pid = v.Parent
	}
	return false
}
func parseProcessTime(s string) (float64, error) {
	days := float64(0)
	if before, after, ok := strings.Cut(s, "-"); ok {
		n, e := strconv.ParseUint(before, 10, 32)
		if e != nil {
			return 0, e
		}
		days = float64(n)
		s = after
	}
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, errors.New("process_cpu_invalid")
	}
	value := float64(0)
	for i, p := range parts {
		n, e := strconv.ParseFloat(p, 64)
		if e != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) || (i > 0 && n >= 60) {
			return 0, errors.New("process_cpu_invalid")
		}
		value = value*60 + n
	}
	return days*86400 + value, nil
}
func readProcessPS(ctx context.Context, pid int) (rss uint64, cpu float64, exe, command string, err error) {
	data, e := environmentCommand(ctx, "/bin/ps", nil, 16384, "-ww", "-o", "rss=,time=,comm=", "-p", strconv.Itoa(pid))
	if e != nil {
		err = e
		return
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		err = errors.New("process_ps_invalid")
		return
	}
	n, e := strconv.ParseUint(fields[0], 10, 64)
	if e != nil || n == 0 || n > 1<<40 {
		err = errors.New("process_rss_invalid")
		return
	}
	rss = n * 1024
	cpu, err = parseProcessTime(fields[1])
	if err != nil {
		return
	}
	exe = strings.Join(fields[2:], " ")
	data, err = environmentCommand(ctx, "/bin/ps", nil, 16384, "-ww", "-o", "command=", "-p", strconv.Itoa(pid))
	command = strings.TrimSpace(string(data))
	return
}
func unchangedEnvironmentFile(path string, before os.FileInfo) bool {
	st, e := os.Stat(path)
	return e == nil && os.SameFile(st, before) && st.Size() == before.Size() && st.ModTime() == before.ModTime()
}
