package loadharness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func readLinuxProcessStat(pid int) ([]string, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return nil, e
	}
	p := strings.LastIndexByte(string(b), ')')
	if p < 0 || len(b) > 16384 {
		return nil, errors.New("process_stat_invalid")
	}
	f := strings.Fields(string(b[p+1:]))
	if len(f) < 22 || f[0] == "Z" {
		return nil, errors.New("process_stat_invalid")
	}
	return f, nil
}
func readProcessIdentity(pid int) (nativeProcessIdentity, error) {
	f, e := readLinuxProcessStat(pid)
	if e != nil {
		return nativeProcessIdentity{}, e
	}
	parent, e := strconv.Atoi(f[1])
	if e != nil {
		return nativeProcessIdentity{}, e
	}
	if n, e := strconv.ParseUint(f[19], 10, 64); e != nil || n == 0 {
		return nativeProcessIdentity{}, errors.New("process_start_invalid")
	}
	return nativeProcessIdentity{PID: pid, Parent: parent, Token: f[19]}, nil
}
func readProcessResources(ctx context.Context, pid int) (nativeProcessResources, error) {
	v := nativeProcessResources{CPUUnit: "clock_ticks"}
	var e error
	v.Identity, e = readProcessIdentity(pid)
	if e != nil {
		return v, e
	}
	f, e := readLinuxProcessStat(pid)
	if e != nil {
		return v, e
	}
	pages, e := strconv.ParseUint(f[21], 10, 64)
	if e != nil || pages == 0 || pages > 1<<40 {
		return v, errors.New("process_rss_invalid")
	}
	v.RSS = pages * uint64(os.Getpagesize())
	u, e := strconv.ParseUint(f[11], 10, 64)
	if e != nil {
		return v, e
	}
	s, e := strconv.ParseUint(f[12], 10, 64)
	if e != nil {
		return v, e
	}
	v.CPU = float64(u) + float64(s)
	v.Executable, e = os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if e != nil {
		return v, e
	}
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if e != nil || len(b) > 16384 {
		return v, errors.New("process_command_invalid")
	}
	v.Command = strings.TrimSuffix(strings.ReplaceAll(string(b), "\x00", " "), " ")
	dir, e := os.Open(fmt.Sprintf("/proc/%d/fd", pid))
	if e != nil {
		return v, e
	}
	defer dir.Close()
	for {
		entries, e := dir.ReadDir(128)
		v.Files += uint64(len(entries))
		if v.Files > 1<<20 {
			return v, errors.New("process_fd_limit")
		}
		if e != nil && e != io.EOF {
			return v, e
		}
		if e == io.EOF {
			break
		}
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
	}
	b, e = os.ReadFile(fmt.Sprintf("/proc/%d/limits", pid))
	if e != nil {
		return v, e
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Max open files") {
			fields := strings.Fields(line)
			if len(fields) != 6 {
				return v, errors.New("process_limit_invalid")
			}
			v.FileLimit, e = strconv.ParseUint(fields[3], 10, 64)
			if e != nil {
				return v, e
			}
		}
	}
	after, e := readProcessIdentity(pid)
	if e != nil || after != v.Identity || v.Files == 0 || v.FileLimit == 0 {
		return v, errors.New("process_sample_invalid")
	}
	return v, nil
}
