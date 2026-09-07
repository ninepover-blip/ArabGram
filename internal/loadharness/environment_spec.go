package loadharness

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const environmentVersion = 1
const environmentQueueRows = 100000

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var databaseNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,62}$`)

type environmentProcessSpec struct {
	ID         string `json:"id"`
	PID        int    `json:"pid"`
	Executable string `json:"executable"`
	BinarySHA  string `json:"binary_sha256"`
	Command    string `json:"command"`
	ConfigPath string `json:"config_path"`
	ConfigSHA  string `json:"config_sha256"`
	RSSBudget  uint64 `json:"rss_budget_bytes"`
	FileBudget uint64 `json:"open_file_budget"`
}
type environmentContainerSpec struct {
	ID           string `json:"id"`
	ContainerID  string `json:"container_id"`
	StartedAt    string `json:"started_at"`
	DataPath     string `json:"data_path"`
	MemoryBudget uint64 `json:"memory_budget_bytes"`
}
type environmentVolumeSpec struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}
type environmentPostgresSpec struct {
	Container   string `json:"container"`
	User        string `json:"user"`
	Database    string `json:"database"`
	SystemID    string `json:"system_id"`
	DatabaseOID uint32 `json:"database_oid"`
}
type environmentSpec struct {
	Version      int                        `json:"version"`
	Provider     string                     `json:"provider"`
	OwnerPID     int                        `json:"owner_pid"`
	RunID        string                     `json:"run_id"`
	DockerBinary string                     `json:"docker_binary"`
	DockerSocket string                     `json:"docker_socket"`
	MinDiskFree  uint64                     `json:"min_disk_free_bytes"`
	Processes    []environmentProcessSpec   `json:"processes"`
	Containers   []environmentContainerSpec `json:"containers"`
	Volumes      []environmentVolumeSpec    `json:"volumes"`
	Postgres     environmentPostgresSpec    `json:"postgres"`
}

func environmentID(id string) (string, bool) {
	role, instance, ok := strings.Cut(id, "/")
	return role, ok && safeMetricLabel(role) && safeMetricLabel(instance)
}
func (s *environmentSpec) validate(metrics MetricsTargets) error {
	if s.Version != environmentVersion || s.Provider != "local-process-docker-v1" || s.OwnerPID <= 1 || s.OwnerPID > 1<<31-1 || !safeMetricLabel(s.RunID) || len(s.Processes) < 5 || len(s.Processes) > 32 || len(s.Containers) != 2 || len(s.Volumes) < 1 || len(s.Volumes) > 16 {
		return errors.New("environment definition invalid")
	}
	if !filepath.IsAbs(s.DockerBinary) || !filepath.IsAbs(s.DockerSocket) {
		return errors.New("environment requires local Docker paths")
	}
	if s.MinDiskFree == 0 {
		s.MinDiskFree = 20 << 30
	}
	if s.MinDiskFree < 20<<30 || s.MinDiskFree > 1<<50 {
		return errors.New("environment disk floor cannot be lowered")
	}
	ids := map[string]bool{}
	pids := map[int]bool{}
	roles := map[string]int{}
	for _, p := range s.Processes {
		role, ok := environmentID(p.ID)
		switch role {
		case "edge", "core", "egress", "file", "sfu", "probe":
		default:
			ok = false
		}
		if !ok || ids[p.ID] || pids[p.PID] || p.PID <= 1 || p.PID == s.OwnerPID || p.PID > 1<<31-1 || !filepath.IsAbs(p.Executable) || !digestPattern.MatchString(p.BinarySHA) || len(p.Command) == 0 || len(p.Command) > 8192 || p.RSSBudget < 1<<20 || p.RSSBudget > 1<<40 || p.FileBudget < 16 || p.FileBudget > 1<<20 {
			return errors.New("environment process definition invalid")
		}
		if role != "probe" && (!filepath.IsAbs(p.ConfigPath) || !digestPattern.MatchString(p.ConfigSHA)) {
			return errors.New("environment process config evidence required")
		}
		if role == "probe" && (p.ConfigPath != "" || p.ConfigSHA != "") {
			return errors.New("probe must not impersonate a role config")
		}
		ids[p.ID] = true
		pids[p.PID] = true
		roles[role]++
	}
	for _, role := range []string{"edge", "core", "egress", "file", "sfu"} {
		if roles[role] == 0 {
			return errors.New("environment must cover all five roles")
		}
	}
	if len(metrics) > 0 {
		seen := map[string]bool{}
		for _, m := range metrics {
			if !ids[m.id()] {
				return errors.New("metrics target has no process observation")
			}
			seen[m.id()] = true
		}
		for _, p := range s.Processes {
			role, _ := environmentID(p.ID)
			if role != "probe" && !seen[p.ID] {
				return errors.New("process has no matching metrics target")
			}
		}
	}
	containerIDs := map[string]bool{}
	containerRoles := map[string]bool{}
	for _, c := range s.Containers {
		role, ok := environmentID(c.ID)
		if !ok || (role != "postgres" && role != "redis") || ids[c.ID] || containerRoles[role] || !digestPattern.MatchString(c.ContainerID) || containerIDs[c.ContainerID] || c.StartedAt == "" || !filepath.IsAbs(c.DataPath) || c.MemoryBudget < 1<<20 || c.MemoryBudget > 1<<40 {
			return errors.New("environment container definition invalid")
		}
		ids[c.ID] = true
		containerIDs[c.ContainerID] = true
		containerRoles[role] = true
	}
	for _, v := range s.Volumes {
		role, ok := environmentID(v.ID)
		if !ok || role != "volume" || ids[v.ID] || !filepath.IsAbs(v.Path) {
			return errors.New("environment volume definition invalid")
		}
		ids[v.ID] = true
	}
	p := s.Postgres
	role, ok := environmentID(p.Container)
	if !ok || role != "postgres" || !ids[p.Container] || !databaseNamePattern.MatchString(p.User) || !databaseNamePattern.MatchString(p.Database) || !regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(p.SystemID) || p.DatabaseOID == 0 {
		return errors.New("environment database identity required")
	}
	return nil
}
func loadEnvironmentSpec(path string, metrics MetricsTargets) (environmentSpec, string, error) {
	var s environmentSpec
	st, e := os.Stat(path)
	if e != nil || !st.Mode().IsRegular() || st.Size() > 64<<10 || st.Mode().Perm()&0077 != 0 {
		return s, "", errors.New("environment config must be a private bounded regular file")
	}
	f, e := os.Open(path)
	if e != nil {
		return s, "", errors.New("environment config read failed")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(st, opened) {
		return s, "", errors.New("environment config replaced")
	}
	b, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if e != nil || len(b) > 64<<10 {
		return s, "", errors.New("environment config read failed")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(&s); e != nil {
		return s, "", errors.New("environment config decode failed")
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return s, "", errors.New("environment config has trailing data")
	}
	if e := s.validate(metrics); e != nil {
		return s, "", e
	}
	h := sha256.Sum256(b)
	return s, hex.EncodeToString(h[:]), nil
}
func verifiedEnvironmentFile(path, digest string) (os.FileInfo, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("environment source missing")
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > 512<<20 {
		return nil, errors.New("environment source invalid")
	}
	h := sha256.New()
	if _, e := io.Copy(h, io.LimitReader(f, 512<<20+1)); e != nil || hex.EncodeToString(h.Sum(nil)) != digest {
		return nil, errors.New("environment source digest mismatch")
	}
	if !unchangedEnvironmentFile(path, st) {
		return nil, errors.New("environment source changed")
	}
	return st, nil
}
