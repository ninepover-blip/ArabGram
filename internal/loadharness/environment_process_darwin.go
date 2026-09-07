package loadharness

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"strconv"
	"strings"
)

func readProcessIdentity(pid int) (nativeProcessIdentity, error) {
	k, e := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if e != nil || k == nil || int(k.Proc.P_pid) != pid || k.Proc.P_stat == 5 || k.Proc.P_starttime.Sec <= 0 {
		return nativeProcessIdentity{}, errors.New("process_identity_missing")
	}
	return nativeProcessIdentity{PID: pid, Parent: int(k.Eproc.Ppid), Token: fmt.Sprintf("%d/%d", k.Proc.P_starttime.Sec, k.Proc.P_starttime.Usec)}, nil
}
func readProcessResources(ctx context.Context, pid int) (nativeProcessResources, error) {
	v := nativeProcessResources{CPUUnit: "seconds"}
	var e error
	v.Identity, e = readProcessIdentity(pid)
	if e != nil {
		return v, e
	}
	v.RSS, v.CPU, v.Executable, v.Command, e = readProcessPS(ctx, pid)
	if e != nil {
		return v, e
	}
	data, e := environmentCommand(ctx, "/usr/sbin/lsof", nil, 2<<20, "-nP", "-a", "-p", strconv.Itoa(pid), "-F", "f")
	if e != nil {
		return v, e
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "f") {
			if _, e := strconv.ParseUint(line[1:], 10, 32); e == nil {
				v.Files++
			}
		}
	}
	after, e := readProcessIdentity(pid)
	if e != nil || after != v.Identity || v.Files == 0 {
		return v, errors.New("process_sample_invalid")
	}
	return v, nil
}
