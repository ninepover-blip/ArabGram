package loadharness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Explicit auxiliary process: allocation/exit probes never change a role or
// use fake process-resource samples. The parent bounds lifetime through stdin.
func TestEnvironmentHelperProcess(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_ENV_HELPER") != "1" {
		t.Skip("explicit subprocess only")
	}
	fmt.Println("ready")
	var memory []byte
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		switch s.Text() {
		case "grow":
			memory = make([]byte, 64<<20)
			for i := 0; i < len(memory); i += 4096 {
				memory[i] = byte(i + 1)
			}
			fmt.Println("grown")
		case "exit":
			os.Exit(0)
		}
	}
	runtime.KeepAlive(memory)
	os.Exit(0)
}

type environmentTestProcess struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output *bufio.Scanner
}

func startEnvironmentTestProcess(t *testing.T) *environmentTestProcess {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	c := exec.Command(exe, "-test.run=^TestEnvironmentHelperProcess$")
	c.Env = append(os.Environ(), "TELESRV_LOAD_ENV_HELPER=1")
	in, e := c.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	out, e := c.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(); e != nil {
		t.Fatal(e)
	}
	p := &environmentTestProcess{cmd: c, input: in, output: bufio.NewScanner(out)}
	t.Cleanup(func() { _ = in.Close(); _ = c.Process.Kill(); _ = c.Wait() })
	p.expect(t, "ready")
	return p
}
func (p *environmentTestProcess) expect(t *testing.T, want string) {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- p.output.Scan() }()
	select {
	case ok := <-done:
		if !ok || p.output.Text() != want {
			t.Fatalf("helper expected %s", want)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("helper response timeout")
	}
}
func TestEnvironmentNativeProcessGrowthAndExit(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native provider unavailable")
	}
	p := startEnvironmentTestProcess(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	before, e := readProcessResources(ctx, p.cmd.Process.Pid)
	if e != nil {
		t.Fatal(e)
	}
	if !processOwnedBy(p.cmd.Process.Pid, os.Getpid()) || processOwnedBy(p.cmd.Process.Pid, 1<<30) || before.Identity.Parent != os.Getpid() || before.Files == 0 || before.RSS == 0 {
		t.Fatal("native ownership/resource evidence invalid")
	}
	_, _ = io.WriteString(p.input, "grow\n")
	p.expect(t, "grown")
	after, e := readProcessResources(ctx, p.cmd.Process.Pid)
	if e != nil {
		t.Fatal(e)
	}
	if after.Identity != before.Identity || after.RSS < before.RSS+32<<20 || after.CPU < before.CPU || after.CPUUnit != before.CPUUnit {
		t.Fatalf("real resident growth lost: before=%+v after=%+v", before, after)
	}
	_, _ = io.WriteString(p.input, "exit\n")
	_ = p.cmd.Wait()
	if _, e := readProcessResources(ctx, p.cmd.Process.Pid); e == nil {
		t.Fatal("exited process sampled successfully")
	}
}
func TestEnvironmentCommandBudgets(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix command provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := environmentCommand(ctx, "/usr/bin/printf", nil, 8, "%s", "123456789"); e == nil {
		t.Fatal("oversized command output accepted")
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	start := time.Now()
	if _, e := environmentCommand(short, "/bin/sleep", nil, 8, "2"); e == nil || time.Since(start) > time.Second {
		t.Fatal("command deadline did not bound wait")
	}
}
func TestEnvironmentThresholdsAndCoverage(t *testing.T) {
	now := time.Now()
	process := EnvironmentTargetSample{Kind: "process", At: now, Epoch: "epoch", RSS: 20 << 20, RSSBudget: 100 << 20, Files: 20, FileBudget: 100, CPUUnit: "seconds"}
	container := EnvironmentTargetSample{Kind: "container", At: now, MemoryBytes: 100 << 20, MemoryBudget: 1 << 30, MemoryLimit: 2 << 30, Disk: DiskResourceSample{Free: 50 << 30, Total: 100 << 30}}
	for _, v := range []EnvironmentTargetSample{process, container} {
		if c := environmentTargetViolation(v, 20<<30); c != "" {
			t.Fatal(c)
		}
	}
	for _, mode := range []string{"rss", "fd", "os_fd", "memory", "disk_bytes", "disk_ratio", "disk_missing", "time_missing", "identity_missing"} {
		t.Run(mode, func(t *testing.T) {
			v := process
			switch mode {
			case "rss":
				v.RSS = v.RSSBudget * 85 / 100
			case "fd":
				v.Files = 85
			case "os_fd":
				v.FileLimit = 20
			case "memory":
				v = container
				v.MemoryBytes = v.MemoryBudget * 85 / 100
			case "disk_bytes":
				v = container
				v.Disk.Free = 19 << 30
			case "disk_ratio":
				v = container
				v.Disk.Total = 1 << 40
			case "disk_missing":
				v = container
				v.Disk = DiskResourceSample{}
			case "time_missing":
				v.At = time.Time{}
			case "identity_missing":
				v.Epoch = ""
			}
			if environmentTargetViolation(v, 20<<30) == "" {
				t.Fatal("unsafe target accepted")
			}
		})
	}
}
func testEnvironmentSpec() environmentSpec {
	s := environmentSpec{Version: 1, Provider: "local-process-docker-v1", OwnerPID: 123, RunID: "fixture", DockerBinary: "/docker", DockerSocket: "/docker.sock", MinDiskFree: 20 << 30,
		Containers: []environmentContainerSpec{{ID: "postgres/one", ContainerID: strings.Repeat("b", 64), StartedAt: "start", DataPath: "/data", MemoryBudget: 2 << 30}, {ID: "redis/one", ContainerID: strings.Repeat("c", 64), StartedAt: "start", DataPath: "/data", MemoryBudget: 512 << 20}}, Volumes: []environmentVolumeSpec{{ID: "volume/blobs", Path: "/blobs"}}, Postgres: environmentPostgresSpec{Container: "postgres/one", Database: "fixture", User: "load", SystemID: "12345", DatabaseOID: 123}}
	for i, role := range []string{"edge", "core", "egress", "file", "sfu"} {
		s.Processes = append(s.Processes, environmentProcessSpec{ID: role + "/one", PID: 200 + i, Executable: "/bin/" + role, BinarySHA: strings.Repeat("a", 64), Command: role, ConfigPath: "/config/" + role, ConfigSHA: strings.Repeat("a", 64), RSSBudget: 1 << 30, FileBudget: 8192})
	}
	return s
}
func TestEnvironmentConfigRejectsMissingIdentityAndUnsafeScope(t *testing.T) {
	valid := testEnvironmentSpec()
	if e := valid.validate(nil); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"role", "duplicate_pid", "duplicate_id", "disk_floor", "owner", "binary", "container_duplicate", "metrics_uncovered", "database_missing"} {
		t.Run(mode, func(t *testing.T) {
			s := testEnvironmentSpec()
			var m MetricsTargets
			switch mode {
			case "role":
				s.Processes = s.Processes[:4]
			case "duplicate_pid":
				s.Processes[1].PID = s.Processes[0].PID
			case "duplicate_id":
				s.Processes[1].ID = s.Processes[0].ID
			case "disk_floor":
				s.MinDiskFree = 19 << 30
			case "owner":
				s.OwnerPID = 1
			case "binary":
				s.Processes[0].BinarySHA = ""
			case "container_duplicate":
				s.Containers[1].ContainerID = s.Containers[0].ContainerID
			case "metrics_uncovered":
				m = MetricsTargets{{Role: "core", Instance: "two"}}
			case "database_missing":
				s.Postgres.SystemID = ""
			}
			if s.validate(m) == nil {
				t.Fatal("invalid environment accepted")
			}
		})
	}
	p := filepath.Join(t.TempDir(), "private.json")
	s := testEnvironmentSpec()
	b, _ := json.Marshal(s)
	if e := os.WriteFile(p, b, 0600); e != nil {
		t.Fatal(e)
	}
	if _, digest, e := loadEnvironmentSpec(p, nil); e != nil || len(digest) != 64 {
		t.Fatal("valid private config rejected", e)
	}
	_ = os.Chmod(p, 0644)
	if _, _, e := loadEnvironmentSpec(p, nil); e == nil {
		t.Fatal("public process config accepted")
	}
	_ = os.Chmod(p, 0600)
	_ = os.WriteFile(p, append(b, []byte("{}")...), 0600)
	if _, _, e := loadEnvironmentSpec(p, nil); e == nil {
		t.Fatal("trailing config accepted")
	}
}
func TestEnvironmentContainerOwnershipAndRestart(t *testing.T) {
	const raw = `{"Id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","State":{"Running":true,"StartedAt":"start"},"Config":{"Labels":{"telesrv.load.run":"fixture"}},"HostConfig":{"Memory":2147483648,"PortBindings":{"5432/tcp":[{"HostIp":"127.0.0.1"}]}},"Mounts":[{"Destination":"/data"}]}`
	for _, mode := range []string{"valid", "label", "id", "restart", "stopped", "paused", "oom", "port", "mount", "limit"} {
		t.Run(mode, func(t *testing.T) {
			s := testEnvironmentSpec()
			c := s.Containers[0]
			var v environmentContainerInspection
			_ = json.Unmarshal([]byte(raw), &v)
			switch mode {
			case "label":
				v.Config.Labels["telesrv.load.run"] = "other"
			case "id":
				v.ID = "other"
			case "restart":
				v.State.StartedAt = "new"
			case "stopped":
				v.State.Running = false
			case "paused":
				v.State.Paused = true
			case "oom":
				v.State.OOMKilled = true
			case "port":
				v.HostConfig.PortBindings["5432/tcp"][0].HostIP = "0.0.0.0"
			case "mount":
				v.Mounts = nil
			case "limit":
				v.HostConfig.Memory = 1 << 30
			}
			e := validateEnvironmentContainer(v, s, c)
			if (e == nil) != (mode == "valid") {
				t.Fatal(mode, e)
			}
		})
	}
}
func TestEnvironmentBacklogUsesCompleteDatabaseClockSnapshot(t *testing.T) {
	for _, mode := range []string{"valid", "scan_limit", "missing", "future", "wrong_db", "wrong_system", "wrong_oid", "wrong_age", "oldest_missing", "empty_age", "clock_skew"} {
		t.Run(mode, func(t *testing.T) {
			s := testEnvironmentSpec()
			now := time.Now()
			old := now.Add(-61 * time.Second)
			v := environmentDatabaseSample{Database: s.Postgres.Database, SystemID: s.Postgres.SystemID, DatabaseOID: s.Postgres.DatabaseOID, ObservedAt: now, Queues: map[string]environmentQueueSample{"account_pts": {Rows: 100000, Oldest: &old, AgeSeconds: 61}, "account_absolute": {}, "channel_pts": {}}}
			q := v.Queues["account_pts"]
			switch mode {
			case "scan_limit":
				q.Rows++
			case "missing":
				delete(v.Queues, "channel_pts")
			case "future":
				q.AgeSeconds = -1
			case "wrong_db":
				v.Database = "other"
			case "wrong_system":
				v.SystemID = "other"
			case "wrong_oid":
				v.DatabaseOID++
			case "wrong_age":
				q.AgeSeconds = 59
			case "oldest_missing":
				q.Oldest = nil
			case "empty_age":
				q = environmentQueueSample{AgeSeconds: 1}
			case "clock_skew":
				v.ObservedAt = now.Add(6 * time.Second)
			}
			v.Queues["account_pts"] = q
			e := validateEnvironmentDatabase(v, s.Postgres, now)
			if (e == nil) != (mode == "valid") {
				t.Fatal(mode, e)
			}
		})
	}
	g := &environmentGuard{rising: map[string]int{}}
	observe := func(age float64, rows uint64) bool {
		return len(g.observeQueueAges(map[string]environmentQueueSample{"account_pts": {Rows: rows, AgeSeconds: age}})) > 0
	}
	for _, age := range []float64{59, 60, 61, 62} {
		if observe(age, 1) {
			t.Fatal("age threshold fired early")
		}
	}
	if !observe(63, 1) {
		t.Fatal("rising age did not stop")
	}
	observe(0, 0)
	for _, age := range []float64{80, 79, 80} {
		if observe(age, 1) {
			t.Fatal("non-rising/empty age was retained")
		}
	}
	g.observeQueueAges(nil)
	if observe(90, 1) || observe(91, 1) || !observe(92, 1) {
		t.Fatal("sampling gap not reset")
	}
}
