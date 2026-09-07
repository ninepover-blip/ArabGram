//go:build darwin || linux

package loadharness

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type presenceWireUser struct {
	ID       int64  `json:"id"`
	LastSeen int64  `json:"last_seen_at"`
	Updated  string `json:"updated_at"`
}

func (p *privateLifecycleProbe) presenceUsers(label string) []presenceWireUser {
	p.t.Helper()
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	var usersOwned []int64
	for user := range p.initial {
		usersOwned = append(usersOwned, user)
	}
	if len(usersOwned) != 2 {
		p.t.Fatal("presence probe requires exactly two manifest-owned users")
	}
	q := fmt.Sprintf("BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms'; SELECT json_agg(row_to_json(u)) FROM (SELECT id,last_seen_at,updated_at FROM users WHERE id IN (%d,%d) ORDER BY id) u; COMMIT;", usersOwned[0], usersOwned[1])
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	body, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", p.spec.Postgres.User, "-d", p.spec.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", q)
	p.require(err)
	var users []presenceWireUser
	p.require(json.Unmarshal(body, &users))
	if len(users) != 2 {
		p.t.Fatal("presence query must return both manifest-owned users")
	}
	p.record("presence_pg", map[string]any{"label": label, "users": users})
	return users
}

func TestRealPresenceWorkBoundary(t *testing.T) {
	p := newPrivateLifecycleProbe(t, "presence grace/callback/batch completion; zero new messages, not a capacity or GUI acceptance")
	_, _, _, targets := lifecycleFixture(t)
	var url string
	for _, target := range targets {
		if target.Role == "core" {
			url = target.URL
		}
	}
	if url == "" {
		t.Fatal("missing declared Core metrics")
	}
	metrics := newServerMetricsClient(url)
	sample := func(label string) map[string]float64 {
		values, err := metrics.scrape(p.ctx)
		p.require(err)
		sum := float64(0)
		for _, stage := range []string{"grace", "callback", "direct_write", "last_seen_batch"} {
			value, ok := values[`telesrv_presence_work{stage="`+stage+`"}`]
			if !ok || value < 0 {
				t.Fatalf("missing/invalid presence stage %s", stage)
			}
			sum += value
		}
		if value, ok := values["telesrv_presence_work_pending"]; !ok || value != sum {
			t.Fatal("presence aggregate inconsistent with stages")
		}
		p.record("presence_metrics", map[string]any{"label": label, "values": values})
		return values
	}
	wait := func(label string, match func(map[string]float64) bool) map[string]float64 {
		deadline := time.Now().Add(12 * time.Second)
		for {
			values := sample(label)
			if match(values) {
				return values
			}
			if time.Now().After(deadline) {
				t.Fatal("presence phase timeout: " + label)
			}
			p.pause(250 * time.Millisecond)
		}
	}
	zero := func(v map[string]float64) bool { return v["telesrv_presence_work_pending"] == 0 }
	grace := func(n float64) func(map[string]float64) bool {
		return func(v map[string]float64) bool { return v[`telesrv_presence_work{stage="grace"}`] == n }
	}
	byIndex := map[int]*lifeDevice{}
	for _, d := range p.devices {
		byIndex[d.record.Index] = d
	}
	if len(byIndex) != 4 || byIndex[0] == nil || byIndex[1] == nil || byIndex[2] == nil || byIndex[3] == nil || byIndex[0].record.UserID != byIndex[2].record.UserID || byIndex[1].record.UserID != byIndex[3].record.UserID {
		t.Fatal("unexpected four-device mapping")
	}
	stop := func(index int, label string) {
		d := byIndex[index]
		before := time.Now().UTC()
		p.require(p.stop(d))
		after := time.Now().UTC()
		p.record("presence_departure", map[string]any{"label": label, "device": index, "user": d.record.UserID, "before": before, "after": after})
	}
	wait("online-settled", zero)
	initial := p.presenceUsers("online-settled")
	stop(2, "other-device-stays")
	for i := 0; i < 8; i++ {
		if !zero(sample("other-device-stays")) {
			t.Fatal("partial departure created offline work")
		}
		p.pause(250 * time.Millisecond)
	}
	if !reflect.DeepEqual(initial, p.presenceUsers("other-device-stays")) {
		t.Fatal("partial departure changed last_seen")
	}
	p.cases = append(p.cases, map[string]any{"label": "other-device-stays", "pass": true})
	stop(0, "reconnect-within-grace")
	wait("reconnect-grace-armed", grace(1))
	cursor := byIndex[0].cursor
	p.start(byIndex[0], &cursor)
	wait("reconnect-grace-canceled", zero)
	reconnected := p.presenceUsers("reconnected")
	for i := 0; i < 40; i++ {
		if !zero(sample("reconnect-no-late-offline")) {
			t.Fatal("canceled grace produced delayed work")
		}
		p.pause(250 * time.Millisecond)
	}
	if !reflect.DeepEqual(reconnected, p.presenceUsers("reconnect-no-late-offline")) {
		t.Fatal("canceled grace changed last_seen")
	}
	p.cases = append(p.cases, map[string]any{"label": "reconnect-within-grace", "pass": true, "original_cursor": cursor})
	stop(0, "owner-a-final")
	wait("owner-a-final-grace", grace(1))
	stop(1, "owner-b-other-device-stays")
	stop(3, "owner-b-final")
	wait("both-owners-grace", grace(2))
	wait("both-owners-completed", zero)
	final := p.presenceUsers("both-owners-completed")
	for _, u := range final {
		for _, before := range reconnected {
			if u.ID == before.ID && u.LastSeen <= before.LastSeen {
				t.Fatalf("last disconnect timestamp did not advance for user %d", u.ID)
			}
		}
	}
	for i := 0; i < 12; i++ {
		if !zero(sample("completed-stays-zero")) {
			t.Fatal("new presence work after completion")
		}
		p.pause(250 * time.Millisecond)
	}
	if !reflect.DeepEqual(final, p.presenceUsers("completed-stays-zero")) {
		t.Fatal("PG changed after observed presence completion")
	}
	p.cases = append(p.cases, map[string]any{"label": "both-owners-final-disconnect", "pass": true})
	p.snapshot("final")
	for _, d := range p.devices {
		if d.done != nil {
			t.Fatal(fmt.Sprintf("client %d did not join", d.record.Index))
		}
	}
}
