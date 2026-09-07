//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"telesrv/internal/domain"
)

var blockCatalogSQL = strings.Replace(partialCatalogSQL, "load_partial_%", "load_block_%", 1)

type blockEvent struct {
	Owner    int64           `json:"user_id"`
	Peer     int64           `json:"peer_id"`
	Type     string          `json:"event_type"`
	Pts      int             `json:"pts"`
	Count    int             `json:"pts_count"`
	Blocked  bool            `json:"event_bool"`
	Settings map[string]bool `json:"peer_settings"`
}
type blockFacts struct {
	Blocks []struct {
		Owner int64 `json:"owner_user_id"`
		Peer  int64 `json:"blocked_user_id"`
	} `json:"blocks"`
	Events []blockEvent   `json:"events"`
	State  map[string]any `json:"-"`
}
type blockProbe struct {
	*partialForwardProbe
	failed int
}

func (p *blockProbe) facts(label string) blockFacts {
	a, b := p.devices[0].record.UserID, p.devices[1].record.UserID
	query := fmt.Sprintf(`SELECT jsonb_build_object(
'blocks',(SELECT jsonb_agg(to_jsonb(x) ORDER BY owner_user_id,blocked_user_id) FROM contact_blocks x),
'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY user_id,pts) FROM user_update_events e WHERE (user_id=%d AND pts>%d) OR (user_id=%d AND pts>%d)),
'watermarks',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id) FROM (SELECT user_id,contiguous_pts FROM user_update_watermarks) x),
'counts',jsonb_build_object('messages',(SELECT count(*) FROM private_messages),'boxes',(SELECT count(*) FROM message_boxes),'events',(SELECT count(*) FROM user_update_events),'stories',(SELECT count(*) FROM stories)),
'message_digest',(SELECT md5(string_agg(to_jsonb(x)::text,chr(10) ORDER BY id)) FROM private_messages x),
'box_digest',(SELECT md5(string_agg(to_jsonb(x)::text,chr(10) ORDER BY owner_user_id,box_id)) FROM message_boxes x),
'drafts',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id,peer_id,top_message_id) FROM dialog_drafts x),
'contacts',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id,contact_user_id) FROM contacts x),
'privacy',(SELECT jsonb_agg(to_jsonb(x) ORDER BY owner_user_id,privacy_key) FROM account_privacy_rules x),
'queues',jsonb_build_object('pts',(SELECT count(*) FROM dispatch_outbox),'absolute',(SELECT count(*) FROM edge_delivery_outbox),'channel',(SELECT count(*) FROM channel_delivery_events)))`, a, p.initial[a], b, p.initial[b])
	raw := p.sql(label, query, false)
	var f blockFacts
	p.require(json.Unmarshal(raw, &f))
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	p.require(d.Decode(&f.State))
	p.record("block_pg", map[string]any{"label": label, "facts": f.State})
	if len(f.Blocks) > 2 || len(f.Events) > 32 {
		p.t.Fatal("block mutation safety budget")
	}
	for _, row := range f.Blocks {
		if row.Owner != a || (row.Peer != b && row.Peer != domain.ChatBotUserID) {
			p.t.Fatal("block outside owned target scope")
		}
	}
	for _, e := range f.Events {
		if e.Owner != a || e.Count != 1 || (e.Peer != b && e.Peer != domain.ChatBotUserID) || (e.Type != "peer_settings" && e.Type != "peer_story_blocked") {
			p.t.Fatal("event outside block scope")
		}
	}
	return f
}

func blockWire(t *testing.T, e blockEvent) lifeUpdate {
	t.Helper()
	var u tg.UpdateClass
	switch e.Type {
	case "peer_story_blocked":
		u = &tg.UpdatePeerBlocked{PeerID: &tg.PeerUser{UserID: e.Peer}, Blocked: e.Blocked, BlockedMyStoriesFrom: true}
	case "peer_settings":
		u = &tg.UpdatePeerSettings{Peer: &tg.PeerUser{UserID: e.Peer}, Settings: tg.PeerSettings{AddContact: e.Settings["add_contact"], BlockContact: e.Settings["block_contact"], ShareContact: e.Settings["share_contact"], NeedContactsException: e.Settings["need_contacts_exception"]}}
	default:
		t.Fatal("unknown block event")
	}
	var buf bin.Buffer
	if err := u.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	v, err := tg.DecodeUpdate(&buf)
	if err != nil {
		t.Fatal(err)
	}
	w, ok := lifeOne(v)
	if !ok {
		t.Fatal("unobserved block update")
	}
	return w
}

func blockIDs(f blockFacts) []int64 {
	out := make([]int64, 0, len(f.Blocks))
	for _, r := range f.Blocks {
		out = append(out, r.Peer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (p *blockProbe) reads(f blockFacts, label string) {
	for _, d := range p.devices {
		if d.done == nil {
			continue
		}
		var out tg.ContactsBlockedBox
		p.mediaCall(d, label+"/blocked", &tg.ContactsGetBlockedRequest{Limit: 100}, &out, "ok")
		var rows []tg.PeerBlocked
		var count int
		switch v := out.Blocked.(type) {
		case *tg.ContactsBlocked:
			rows = v.Blocked
			count = len(rows)
		case *tg.ContactsBlockedSlice:
			rows = v.Blocked
			count = v.Count
		default:
			p.t.Fatal("blocked page type")
		}
		ids := make([]int64, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, lifePeer(r.PeerID))
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		want := []int64{}
		if d.record.UserID == p.devices[0].record.UserID {
			want = blockIDs(f)
		}
		if count != len(want) || !reflect.DeepEqual(ids, want) {
			p.t.Fatal("getBlocked differs from PG")
		}
	}
}

// Faults are observed without accepting their contract violations. A diagnostic
// run continues only while actual PG, wire, PTS and delivery facts agree.
func (p *blockProbe) action(label string, req bin.Encoder, code string, wantPTS int, wantIDs []int64) {
	a := p.devices[0]
	before := p.checkpoint(label + "/before")
	old := p.facts(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.BoolBox
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := a.client.Invoke(ctx, req, &out)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("block_rpc", map[string]any{"label": label, "device": a.record.Index, "generation": a.generation, "method": fmt.Sprintf("%T", req), "request": req, "response": &out, "class": class, "expected_class": code, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	f := p.facts(label + "/after")
	live := map[int][]lifeUpdate{}
	delta := 0
	for _, e := range f.Events {
		if e.Pts <= before[a.record.Index].Pts {
			continue
		}
		delta += e.Count
		u := blockWire(p.t, e)
		for _, d := range p.devices {
			if d.record.UserID != e.Owner {
				continue
			}
			if d.done != nil {
				live[d.record.Index] = append(live[d.record.Index], u, lifeUpdate{Kind: "delete", Pts: e.Pts, Count: 1})
			} else {
				p.offline[d.record.Index] = append(p.offline[d.record.Index], u)
			}
		}
	}
	(&privateDraftProbe{privateLifecycleProbe: p.privateLifecycleProbe}).checkMultiDraftLive(mark, start, live, label)
	p.checkPTS(before, map[int64]int{a.record.UserID: delta}, label+"/after")
	pass := class == code && delta == wantPTS
	if wantIDs == nil {
		wantIDs = []int64{}
	}
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	pass = pass && reflect.DeepEqual(blockIDs(f), wantIDs)
	if class == "ok" {
		if _, ok := out.Bool.(*tg.BoolTrue); !ok {
			p.t.Fatal("successful block must return true")
		}
	}
	for _, key := range []string{"message_digest", "box_digest", "contacts", "drafts", "privacy"} {
		if !reflect.DeepEqual(old.State[key], f.State[key]) {
			p.t.Fatal("unrelated state changed", key)
		}
	}
	if code != "ok" || wantPTS == 0 {
		for _, key := range []string{"blocks", "events", "watermarks", "counts"} {
			if !reflect.DeepEqual(old.State[key], f.State[key]) {
				pass = false
			}
		}
	}
	p.reads(f, label)
	p.record("block_action", map[string]any{"label": label, "pass": pass, "pts": delta, "expected_pts": wantPTS, "block_ids": blockIDs(f), "expected_block_ids": wantIDs})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": pass})
	if !pass {
		p.failed++
	}
}

func blockFaultSQL(run string, index int, owner, peer int64) (name, table, install, remove string, err error) {
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(run) || index < 0 || index > 3 || owner != 1780243200 || (peer != 1780243201 && peer != domain.ChatBotUserID) || (index == 3) != (peer == domain.ChatBotUserID) {
		err = fmt.Errorf("unscoped block fault")
		return
	}
	name = fmt.Sprintf("load_block_%s_%d", run, index)
	table = "user_update_events"
	event := "peer_story_blocked"
	blocked := "true"
	if index == 2 {
		blocked = "false"
	}
	condition := fmt.Sprintf("NEW.user_id=%d AND NEW.peer_id=%d AND NEW.event_type='%s' AND NEW.event_bool=%s", owner, peer, event, blocked)
	lookup := "NEW.user_id"
	if index == 1 {
		table = "dispatch_outbox"
		event = "peer_settings"
		lookup = "NEW.target_user_id"
		condition = fmt.Sprintf("NEW.target_user_id=%d AND NEW.event_type='peer_settings'", owner)
	}
	install = fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $fault$
DECLARE facts jsonb; BEGIN
IF NOT EXISTS(SELECT 1 FROM user_update_events WHERE user_id=%s AND pts=NEW.pts AND peer_id=%d AND event_type='%s') THEN RETURN NEW; END IF;
SELECT jsonb_build_object('event',(SELECT to_jsonb(e)||jsonb_build_object('xmin',xmin::text) FROM user_update_events e WHERE user_id=%d AND pts=NEW.pts),
'outbox',(SELECT to_jsonb(o)||jsonb_build_object('xmin',xmin::text) FROM dispatch_outbox o WHERE target_user_id=%d AND pts=NEW.pts),
'blocks',(SELECT jsonb_agg(to_jsonb(b)||jsonb_build_object('xmin',xmin::text) ORDER BY blocked_user_id) FROM contact_blocks b WHERE owner_user_id=%d),
'transaction_id',txid_current()) INTO facts;
IF facts->'event'='null'::jsonb OR facts->'outbox'='null'::jsonb THEN RAISE EXCEPTION 'block fault missing event/outbox' USING DETAIL=facts::text; END IF;
RAISE EXCEPTION 'telesrv_%s' USING ERRCODE='P0001',DETAIL=facts::text;
END $fault$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON public.%s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (%s) EXECUTE FUNCTION public.%s()`, name, lookup, peer, event, owner, owner, owner, name, name, table, condition, name)
	remove = fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON public.%s; DROP FUNCTION IF EXISTS public.%s()", name, table, name)
	return
}

func TestRealBlocklistAtomicBoundary(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_BLOCKLIST") != "1" {
		t.Skip("requires owned blocklist fault fixture")
	}
	p := &blockProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "blocklist real COMMIT/duplicate/replacement diagnostic; not capacity or story fanout"), primary: map[int64]*lifeDevice{}, offline: map[int][]lifeUpdate{}}}
	if len(p.devices) != 4 || p.devices[0].record.UserID != 1780243200 || p.devices[1].record.UserID != 1780243201 {
		t.Fatal("block fixture owners")
	}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	peer := p.peer(a)
	bot := &tg.InputPeerUser{UserID: domain.ChatBotUserID, AccessHash: domain.ChatBotAccessHash}
	initial := p.facts("baseline")
	if len(initial.Blocks) != 0 || len(initial.Events) != 0 {
		t.Fatal("block baseline is not empty")
	}
	p.reads(initial, "initial")
	catalog := p.sql("catalog-before", blockCatalogSQL, false)
	if !strings.Contains(string(catalog), `"fault_objects": 0`) {
		t.Fatal("unreconciled block fault")
	}
	tables := map[string]string{}
	for i := 0; i < 4; i++ {
		table := "user_update_events"
		if i == 1 {
			table = "dispatch_outbox"
		}
		tables[fmt.Sprintf("load_block_%s_%d", p.id, i)] = table
	}
	reg, err := json.Marshal(map[string]any{"run_id": p.id, "system_id": p.spec.Postgres.SystemID, "database_oid": p.spec.Postgres.DatabaseOID, "tables": tables, "catalog_before": string(catalog)})
	p.require(err)
	p.require(writeNewEvidenceReport(filepath.Join(p.out, "fault-cleanup.json"), reg))
	block := &tg.ContactsBlockRequest{ID: peer}
	unblock := &tg.ContactsUnblockRequest{ID: peer}
	p.action("normal/block", block, "ok", 2, []int64{b.record.UserID})
	p.action("normal/block-repeat", block, "ok", 0, []int64{b.record.UserID})
	p.action("normal/unblock", unblock, "ok", 2, nil)
	p.action("normal/unblock-repeat", unblock, "ok", 0, nil)
	for group := 0; group < 2; group++ {
		d := p.devices[2]
		original := d.cursor
		p.require(p.stop(d))
		p.offline[d.record.Index] = nil
		p.record("block_offline", map[string]any{"group": group, "device": d.record.Index, "cursor": original})
		for i := group * 2; i < group*2+2; i++ {
			label := []string{"fault/block-state", "fault/block-settings", "fault/unblock-state", "fault/replace-second"}[i]
			var req bin.Encoder = block
			want := []int64{b.record.UserID}
			retryPTS := 2
			faultIDs := []int64{}
			if i == 2 {
				p.action(label+"/setup", block, "ok", 2, want)
				req = unblock
				want = nil
				faultIDs = []int64{b.record.UserID}
			}
			if i == 3 {
				req = &tg.ContactsSetBlockedRequest{ID: []tg.InputPeerClass{peer, bot}, Limit: 2}
				want = []int64{b.record.UserID, domain.ChatBotUserID}
				retryPTS = 4
			}
			target := b.record.UserID
			if i == 3 {
				target = domain.ChatBotUserID
			}
			name, table, install, remove, err := blockFaultSQL(p.id, i, a.record.UserID, target)
			p.require(err)
			installed := true
			t.Cleanup(func() {
				if installed {
					p.sql(label+"/cleanup", remove, true)
				}
			})
			p.record("block_fault", map[string]any{"label": label, "name": name, "table": table, "install": install, "remove": remove})
			p.sql(label+"/install", install, false)
			p.action(label+"/faulted", req, "INTERNAL_SERVER_ERROR", 0, faultIDs)
			p.sql(label+"/remove", remove, false)
			installed = false
			if !bytes.Equal(p.sql(label+"/catalog-restored", blockCatalogSQL, false), catalog) {
				t.Fatal("block fault catalog drift")
			}
			p.action(label+"/retry", req, "ok", retryPTS, want)
			p.action(label+"/repeat", req, "ok", 0, want)
			if i < 2 {
				p.action(label+"/unblock", unblock, "ok", 2, nil)
			}
			if i == 3 {
				empty := &tg.ContactsSetBlockedRequest{Limit: 0}
				p.action(label+"/clear", empty, "ok", 4, nil)
				p.action(label+"/clear-repeat", empty, "ok", 0, nil)
			}
		}
		(&pinServiceProbe{partialForwardProbe: p.partialForwardProbe}).recover(d, original, fmt.Sprintf("offline/%d/recover", group))
	}
	final := p.facts("final")
	if len(final.Blocks) != 0 {
		t.Fatal("test block rows not restored by RPC")
	}
	p.snapshot("final")
	p.checkpoint("final")
	p.record("block_summary", map[string]any{"cases": len(p.cases), "failed": p.failed, "events": len(final.Events), "expected_events": 24, "restored_blocks": true, "story_fanout_tested": false})
	if len(p.cases) != 23 {
		t.Fatal("block case cardinality")
	}
	if p.failed > 0 {
		t.Errorf("blocklist contract failures: %d", p.failed)
	}
}

func TestBlockFaultScopeAndObservation(t *testing.T) {
	for i := 0; i < 4; i++ {
		peer := int64(1780243201)
		if i == 3 {
			peer = domain.ChatBotUserID
		}
		_, table, sql, drop, err := blockFaultSQL("0123456789abcdef", i, 1780243200, peer)
		if err != nil || !strings.Contains(sql, "DEFERRABLE INITIALLY DEFERRED") || !strings.Contains(drop, "ON public."+table) {
			t.Fatal("fault scope", err)
		}
	}
	for _, args := range [][3]int64{{0, 1, 1780243201}, {0, 1780243200, 1}, {3, 1780243200, 1780243201}, {4, 1780243200, domain.ChatBotUserID}} {
		if _, _, _, _, err := blockFaultSQL("0123456789abcdef", int(args[0]), args[1], args[2]); err == nil {
			t.Fatal("unsafe fault accepted")
		}
	}
	for _, blocked := range []bool{false, true} {
		v := blockWire(t, blockEvent{Type: "peer_story_blocked", Peer: 1780243201, Blocked: blocked})
		if v.PeerBlocked == nil || v.PeerBlocked.Blocked != blocked || !v.PeerBlocked.BlockedMyStoriesFrom {
			t.Fatal("blocked flag or list scope lost")
		}
	}
}
