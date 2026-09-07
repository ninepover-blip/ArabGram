package loadharness

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

type environmentQueueSample struct {
	Rows       uint64     `json:"rows"`
	Oldest     *time.Time `json:"oldest,omitempty"`
	AgeSeconds float64    `json:"age_seconds"`
}
type environmentDatabaseSample struct {
	Database    string                            `json:"database"`
	SystemID    string                            `json:"system_id"`
	DatabaseOID uint32                            `json:"database_oid"`
	ObservedAt  time.Time                         `json:"observed_at"`
	Queues      map[string]environmentQueueSample `json:"queues"`
}
type environmentContainerInspection struct {
	ID    string `json:"Id"`
	State struct {
		Running, Paused, OOMKilled bool
		StartedAt                  string
	}
	Config     struct{ Labels map[string]string }
	HostConfig struct {
		Memory       int64
		PortBindings map[string][]struct {
			HostIP string `json:"HostIp"`
		}
	}
	Mounts []struct{ Destination string }
}

func environmentDocker(ctx context.Context, s environmentSpec, args ...string) ([]byte, error) {
	return environmentCommand(ctx, s.DockerBinary, localDockerEnvironment(s.DockerSocket), 1<<20, args...)
}
func inspectEnvironmentContainer(ctx context.Context, s environmentSpec, c environmentContainerSpec) (environmentContainerInspection, error) {
	var d []environmentContainerInspection
	b, e := environmentDocker(ctx, s, "inspect", c.ContainerID)
	if e != nil {
		return environmentContainerInspection{}, e
	}
	if json.Unmarshal(b, &d) != nil || len(d) != 1 {
		return environmentContainerInspection{}, errors.New("container_inspection_invalid")
	}
	v := d[0]
	return v, validateEnvironmentContainer(v, s, c)
}
func validateEnvironmentContainer(v environmentContainerInspection, s environmentSpec, c environmentContainerSpec) error {
	if v.ID != c.ContainerID || !v.State.Running || v.State.Paused || v.State.OOMKilled || v.State.StartedAt != c.StartedAt || v.Config.Labels["telesrv.load.run"] != s.RunID || v.HostConfig.Memory <= 0 || c.MemoryBudget > uint64(v.HostConfig.Memory) {
		return errors.New("container_identity_or_limit")
	}
	if len(v.HostConfig.PortBindings) == 0 {
		return errors.New("container_ports_missing")
	}
	for _, bindings := range v.HostConfig.PortBindings {
		if len(bindings) == 0 {
			return errors.New("container_binding_invalid")
		}
		for _, binding := range bindings {
			if binding.HostIP != "127.0.0.1" {
				return errors.New("container_not_loopback")
			}
		}
	}
	found := false
	for _, m := range v.Mounts {
		if m.Destination == c.DataPath {
			found = true
		}
	}
	if !found {
		return errors.New("container_data_mount_missing")
	}
	return nil
}
func readEnvironmentContainer(ctx context.Context, s environmentSpec, c environmentContainerSpec) (EnvironmentTargetSample, error) {
	v := EnvironmentTargetSample{Kind: "container", CPUUnit: "seconds", MemoryBudget: c.MemoryBudget}
	if _, e := inspectEnvironmentContainer(ctx, s, c); e != nil {
		return v, e
	}
	b, e := environmentDocker(ctx, s, "exec", c.ContainerID, "cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/cpu.stat")
	if e != nil {
		return v, e
	}
	f := strings.Fields(string(b))
	if len(f) < 4 {
		return v, errors.New("cgroup_sample_invalid")
	}
	v.MemoryBytes, e = strconv.ParseUint(f[0], 10, 64)
	if e != nil || v.MemoryBytes == 0 {
		return v, errors.New("cgroup_memory_invalid")
	}
	v.MemoryLimit, e = strconv.ParseUint(f[1], 10, 64)
	if e != nil || v.MemoryLimit < c.MemoryBudget {
		return v, errors.New("cgroup_limit_invalid")
	}
	found := false
	for i := 2; i+1 < len(f); i += 2 {
		if f[i] == "usage_usec" {
			n, e := strconv.ParseUint(f[i+1], 10, 64)
			if e != nil {
				return v, errors.New("cgroup_cpu_invalid")
			}
			v.CPU = float64(n) / 1e6
			found = true
		}
	}
	if !found {
		return v, errors.New("cgroup_cpu_missing")
	}
	b, e = environmentDocker(ctx, s, "exec", c.ContainerID, "stat", "-f", "-c", "%a %b %S", "--", c.DataPath)
	if e != nil {
		return v, e
	}
	f = strings.Fields(string(b))
	if len(f) != 3 {
		return v, errors.New("container_disk_invalid")
	}
	values := [3]uint64{}
	for i, p := range f {
		values[i], e = strconv.ParseUint(p, 10, 64)
		if e != nil {
			return v, errors.New("container_disk_invalid")
		}
	}
	if values[2] == 0 || values[1] > math.MaxUint64/values[2] || values[0] > values[1] {
		return v, errors.New("container_disk_invalid")
	}
	v.Disk = DiskResourceSample{Free: values[0] * values[2], Total: values[1] * values[2]}
	if _, e := inspectEnvironmentContainer(ctx, s, c); e != nil {
		return v, e
	}
	v.At = time.Now().UTC()
	return v, nil
}

// Read only, one statement snapshot, and no false global minimum after a
// bounded prefix: rows > environmentQueueRows is rejected by the consumer.
const environmentBacklogSQL = `BEGIN READ ONLY;
SET LOCAL statement_timeout='1500ms'; SET LOCAL lock_timeout='250ms';
SET LOCAL idle_in_transaction_session_timeout='2000ms'; SET LOCAL search_path=pg_catalog,public;
WITH p AS MATERIALIZED (SELECT created_at FROM public.dispatch_outbox LIMIT 100001),
 a AS MATERIALIZED (SELECT created_at FROM public.edge_delivery_outbox LIMIT 100001),
 c AS MATERIALIZED (SELECT created_at FROM public.channel_delivery_events LIMIT 100001),
 q AS (SELECT 'account_pts' AS kind,count(*) AS rows,min(created_at) AS oldest FROM p
 UNION ALL SELECT 'account_absolute',count(*),min(created_at) FROM a
 UNION ALL SELECT 'channel_pts',count(*),min(created_at) FROM c)
SELECT jsonb_build_object('database',current_database(),
 'system_id',(pg_control_system()).system_identifier::text,
 'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),
 'observed_at',statement_timestamp(),
 'queues',(SELECT jsonb_object_agg(kind,jsonb_build_object('rows',rows,'oldest',oldest,
 'age_seconds',CASE WHEN rows=0 THEN 0 ELSE extract(epoch FROM statement_timestamp()-oldest) END)) FROM q));
COMMIT;`

func readEnvironmentQueues(ctx context.Context, s environmentSpec) (environmentDatabaseSample, error) {
	var v environmentDatabaseSample
	container := ""
	for _, c := range s.Containers {
		if c.ID == s.Postgres.Container {
			container = c.ContainerID
		}
	}
	b, e := environmentDocker(ctx, s, "exec", "-e", "PGAPPNAME=telesrv-load-environment", container, "psql", "-U", s.Postgres.User, "-d", s.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", environmentBacklogSQL)
	if e != nil {
		return v, e
	}
	if json.Unmarshal(b, &v) != nil {
		return v, errors.New("database_sample_invalid")
	}
	return v, validateEnvironmentDatabase(v, s.Postgres, time.Now())
}
func validateEnvironmentDatabase(v environmentDatabaseSample, p environmentPostgresSpec, now time.Time) error {
	if v.Database != p.Database || v.SystemID != p.SystemID || v.DatabaseOID != p.DatabaseOID {
		return errors.New("database_identity_changed")
	}
	if v.ObservedAt.IsZero() || now.Sub(v.ObservedAt) > 5*time.Second || v.ObservedAt.Sub(now) > 5*time.Second || len(v.Queues) != 3 {
		return errors.New("database_clock_or_coverage")
	}
	for _, name := range []string{"account_pts", "account_absolute", "channel_pts"} {
		q, ok := v.Queues[name]
		if !ok || q.Rows > environmentQueueRows || math.IsNaN(q.AgeSeconds) || math.IsInf(q.AgeSeconds, 0) || q.AgeSeconds < 0 || ((q.Rows == 0) != (q.Oldest == nil)) {
			return errors.New("backlog_incomplete_or_invalid")
		}
		if q.Rows == 0 && q.AgeSeconds != 0 {
			return errors.New("empty_queue_age_invalid")
		}
		if q.Oldest != nil && math.Abs(v.ObservedAt.Sub(*q.Oldest).Seconds()-q.AgeSeconds) > 0.001 {
			return errors.New("queue_age_clock_mismatch")
		}
	}
	return nil
}
