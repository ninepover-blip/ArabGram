//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

type draftRow struct {
	Owner int64  `json:"user_id"`
	Peer  int64  `json:"peer_id"`
	Date  int    `json:"date"`
	Top   int    `json:"top_message_id"`
	Type  string `json:"peer_type"`
	Draft struct {
		Message     string
		Date        int
		NoWebpage   bool
		InvertMedia bool
		Entities    []struct {
			Type           string
			Offset, Length int
		}
	} `json:"draft"`
}
type draftEvent struct {
	Owner int64  `json:"user_id"`
	Peer  int64  `json:"peer_id"`
	Pts   int    `json:"pts"`
	Count int    `json:"pts_count"`
	Date  int    `json:"date"`
	Type  string `json:"event_type"`
}
type draftFacts struct {
	Drafts        []draftRow   `json:"drafts"`
	Events        []draftEvent `json:"events"`
	MessageDigest string       `json:"message_digest"`
	BoxDigest     string       `json:"box_digest"`
}
type privateDraftProbe struct {
	*privateLifecycleProbe
	want      map[int64]map[int64]*tg.DraftMessage
	baseline  draftFacts
	mutations int
}

// mtproto.readLoop invokes consumeMessage in a goroutine per received frame.
// Callback scheduling is not physical wire order. For a multi-peer clear,
// compare all observations without imposing callback order; live_wire retains
// the complete packet so the independent audit can still verify draft/aux pairs.
func (p *privateDraftProbe) checkMultiDraftLive(mark int, start time.Time, want map[int][]lifeUpdate, label string) {
	p.pause(max(0, 2*time.Second-time.Since(start)))
	got := map[int][]lifeUpdate{}
	var maxNS int64
	for _, o := range p.liveSince(mark) {
		lag := o.RelativeNS - start.Sub(p.epoch).Nanoseconds()
		if lag < 0 || lag > int64(time.Second) {
			p.t.Fatal("multi-draft live latency outside bound")
		}
		maxNS = max(maxNS, lag)
		got[o.Device] = append(got[o.Device], o.Update)
	}
	canonical := func(values map[int][]lifeUpdate) map[int][]string {
		out := map[int][]string{}
		for d, updates := range values {
			for _, u := range updates {
				v, err := json.Marshal(u)
				p.require(err)
				out[d] = append(out[d], string(v))
			}
			sort.Strings(out[d])
		}
		return out
	}
	if !reflect.DeepEqual(canonical(got), canonical(want)) {
		p.record("draft_live_mismatch", map[string]any{"got": got, "want": want})
		p.t.Fatal("multi-draft callback contents mismatch")
	}
	p.record("draft_live_checked", map[string]any{"label": label, "expected": want, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "max_latency_ns": maxNS, "physical_order_verified": false})
}

// Only typed draft payloads are normalized through the same wire codec as the
// clients. Expected text, flags, entities and media still come from requests.
func draftWire(t *testing.T, value tg.DraftMessageClass) tg.DraftMessageClass {
	t.Helper()
	var b bin.Buffer
	if err := value.Encode(&b); err != nil {
		t.Fatal(err)
	}
	v, err := tg.DecodeDraftMessage(&b)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (p *privateDraftProbe) facts(label string) draftFacts {
	p.t.Helper()
	var predicates string
	for owner, pts := range p.initial {
		if predicates != "" {
			predicates += " OR "
		}
		predicates += fmt.Sprintf("(user_id=%d AND pts>%d)", owner, pts)
	}
	if len(p.initial) != 2 || p.spec.RunID != "telesrv-load-r0-20260831" || p.spec.Postgres.Database != "load_r0" || p.spec.Postgres.User != "load" {
		p.t.Fatal("draft fixture identity")
	}
	query := fmt.Sprintf(`BEGIN READ ONLY; SET LOCAL statement_timeout='2s';
SELECT jsonb_build_object('schema',(SELECT version FROM schema_migrations),'system_id',(pg_control_system()).system_identifier::text,'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),
'drafts',(SELECT jsonb_agg(to_jsonb(d) ORDER BY user_id,peer_id,top_message_id) FROM dialog_drafts d),
'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY user_id,pts) FROM user_update_events e WHERE %s),
'message_digest',(SELECT md5(string_agg(to_jsonb(m)::text,E'\n' ORDER BY id)) FROM private_messages m),
'box_digest',(SELECT md5(string_agg(to_jsonb(b)::text,E'\n' ORDER BY owner_user_id,box_id)) FROM message_boxes b),
'queues',jsonb_build_object('pts',(SELECT count(*) FROM dispatch_outbox),'absolute',(SELECT count(*) FROM edge_delivery_outbox),'channel',(SELECT count(*) FROM channel_delivery_events))); COMMIT;`, predicates)
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	raw, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", "load", "-d", "load_r0", "-Atq", "-v", "ON_ERROR_STOP=1", "-c", query)
	p.require(err)
	var identity struct {
		Schema   int
		SystemID string `json:"system_id"`
		OID      uint32 `json:"database_oid"`
	}
	p.require(json.Unmarshal(raw, &identity))
	if identity.Schema != 229 || identity.SystemID != p.spec.Postgres.SystemID || identity.OID != p.spec.Postgres.DatabaseOID {
		p.t.Fatal("draft database identity changed")
	}
	var full any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	p.require(decoder.Decode(&full))
	p.record("draft_pg", map[string]any{"label": label, "query": query, "facts": full})
	var value draftFacts
	p.require(json.Unmarshal(raw, &value))
	if label != "baseline" && (value.MessageDigest != p.baseline.MessageDigest || value.BoxDigest != p.baseline.BoxDigest) {
		p.t.Fatal("draft mutated message/box state")
	}
	return value
}

func (p *privateDraftProbe) expected(f draftFacts, owner, peer int64, emptyDate int) lifeUpdate {
	var value tg.DraftMessageClass
	if want := p.want[owner][peer]; want != nil {
		x := *want
		found := false
		for _, row := range f.Drafts {
			if row.Owner == owner && row.Peer == peer && row.Top == 0 {
				x.Date = row.Date
				found = true
			}
		}
		if !found {
			p.t.Fatal("expected draft not persisted")
		}
		value = &x
	} else {
		x := &tg.DraftMessageEmpty{}
		x.SetDate(emptyDate)
		value = x
	}
	return lifeUpdate{Kind: "draft", Peer: peer, Draft: draftWire(p.t, value)}
}

func (p *privateDraftProbe) read(label string, dialogs bool) {
	p.t.Helper()
	f := p.facts(label)
	count := 0
	for owner, drafts := range p.want {
		for peer, want := range drafts {
			count++
			found := false
			for _, row := range f.Drafts {
				if row.Owner == owner && row.Peer == peer && row.Top == 0 && row.Type == "user" && row.Draft.Message == want.Message && row.Date == row.Draft.Date {
					found = true
				}
			}
			if !found {
				p.t.Fatal("draft authoritative content mismatch")
			}
		}
	}
	if len(f.Drafts) != count {
		p.t.Fatal("draft added an unexpected owner/peer")
	}
	for _, d := range p.devices {
		if d.done == nil {
			continue
		}
		var out tg.UpdatesBox
		p.mediaCall(d, label+"/all", &tg.MessagesGetAllDraftsRequest{}, &out, "ok")
		got := lifeUpdates(out.Updates)
		want := make([]lifeUpdate, 0, len(p.want[d.record.UserID]))
		for peer := range p.want[d.record.UserID] {
			want = append(want, p.expected(f, d.record.UserID, peer, 0))
		}
		sort.Slice(want, func(i, j int) bool { return want[i].Peer < want[j].Peer })
		sort.Slice(got, func(i, j int) bool { return got[i].Peer < got[j].Peer })
		if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
			p.record("draft_read_mismatch", map[string]any{"got": got, "want": want})
			p.t.Fatal("getAllDrafts projection/owner mismatch")
		}
		if !dialogs {
			continue
		}
		var peers tg.MessagesPeerDialogs
		p.mediaCall(d, label+"/dialogs", &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{&tg.InputDialogPeer{Peer: p.peer(d)}, &tg.InputDialogPeer{Peer: &tg.InputPeerSelf{}}}}, &peers, "ok")
		if len(peers.Dialogs) != 2 {
			p.t.Fatal("private/Saved dialogs missing")
		}
		for _, item := range peers.Dialogs {
			dialog, ok := item.(*tg.Dialog)
			if !ok {
				p.t.Fatal("unexpected dialog constructor")
			}
			peer := lifePeer(dialog.Peer)
			expected := p.want[d.record.UserID][peer]
			if expected == nil {
				if dialog.Draft != nil {
					if _, ok := dialog.Draft.(*tg.DraftMessageEmpty); !ok {
						p.t.Fatal("cleared dialog retains draft")
					}
				}
				continue
			}
			if !reflect.DeepEqual(dialog.Draft, p.expected(f, d.record.UserID, peer, 0).Draft) {
				p.t.Fatal("dialog draft differs from getAllDrafts")
			}
		}
	}
	p.record("draft_read_checked", map[string]any{"label": label, "drafts": count, "dialogs": dialogs})
}

func (p *privateDraftProbe) action(d *lifeDevice, label string, req bin.Encoder, changes map[int64]*tg.DraftMessage) {
	p.t.Helper()
	p.mutations++
	if p.mutations > 18 {
		p.t.Fatal("draft mutation budget")
	}
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.BoolBox
	p.mediaCall(d, label, req, &out, "ok")
	if _, ok := out.Bool.(*tg.BoolTrue); !ok {
		p.t.Fatal("draft not accepted")
	}
	for peer, value := range changes {
		if value == nil {
			delete(p.want[d.record.UserID], peer)
		} else {
			p.want[d.record.UserID][peer] = value
		}
	}
	f := p.facts(label + "/after-rpc")
	want := map[int][]lifeUpdate{}
	seen := map[int64]bool{}
	next := before[d.record.Index].Pts
	for _, event := range f.Events {
		if event.Owner != d.record.UserID || event.Pts <= before[d.record.Index].Pts {
			continue
		}
		next++
		_, expected := changes[event.Peer]
		if !expected || seen[event.Peer] || event.Pts != next || event.Count != 1 || event.Type != "draft_message" || event.Date < int(start.Unix())-1 || event.Date > int(time.Now().Unix())+1 {
			p.t.Fatal("draft durable event mismatch")
		}
		seen[event.Peer] = true
		for _, target := range p.devices {
			if target.record.UserID == d.record.UserID && target.done != nil {
				want[target.record.Index] = append(want[target.record.Index], p.expected(f, event.Owner, event.Peer, event.Date), lifeUpdate{Kind: "delete", Pts: event.Pts, Count: 1})
			}
		}
	}
	if len(seen) != len(changes) {
		p.t.Fatal("draft event cardinality mismatch")
	}
	if len(changes) > 1 {
		p.checkMultiDraftLive(mark, start, want, label)
	} else {
		p.checkLive(mark, start, want, label)
	}
	p.checkPTS(before, map[int64]int{d.record.UserID: len(changes)}, label+"/after")
	if next-p.initial[d.record.UserID] > 12 {
		p.t.Fatal("draft PTS safety budget")
	}
	p.read(label, false)
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
	p.record("draft_action", map[string]any{"label": label, "owner": d.record.UserID, "events": len(changes), "mutation_index": p.mutations})
}

func (p *privateDraftProbe) recover(d *lifeDevice, original tg.UpdatesState, label string) {
	p.start(d, &original)
	f := p.facts(label)
	var want []lifeUpdate
	for _, event := range f.Events {
		if event.Owner == d.record.UserID && event.Pts > original.Pts {
			want = append(want, p.expected(f, event.Owner, event.Peer, event.Date))
		}
	}
	if !reflect.DeepEqual(d.recovered, want) {
		p.record("draft_recovery_mismatch", map[string]any{"got": d.recovered, "want": want})
		p.t.Fatal("draft original cursor recovery mismatch")
	}
	var empty tg.UpdatesDifferenceBox
	p.mediaCall(d, label+"/repeat-empty", &tg.UpdatesGetDifferenceRequest{Pts: d.cursor.Pts, Date: d.cursor.Date, Qts: d.cursor.Qts}, &empty, "ok")
	if _, ok := empty.Difference.(*tg.UpdatesDifferenceEmpty); !ok {
		p.t.Fatal("draft repeat difference not empty")
	}
	p.record("draft_recovery", map[string]any{"label": label, "device": d.record.Index, "original": original, "final": d.cursor, "expected": want})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func TestRealPrivateDraftLifecycle(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_DRAFT") == "" {
		t.Skip("requires owned frozen draft fixture")
	}
	p := &privateDraftProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "private/Saved draft CRUD, original cursors and normal-RPC content restoration; not capacity"), want: map[int64]map[int64]*tg.DraftMessage{}}
	a, b, as, bs := p.devices[0], p.devices[1], p.devices[2], p.devices[3]
	if len(p.devices) != 4 || a.record.UserID != as.record.UserID || b.record.UserID != bs.record.UserID || a.record.UserID == b.record.UserID {
		t.Fatal("draft requires 2 owners x 2 devices")
	}
	p.baseline = p.facts("baseline")
	var original struct {
		Drafts []draftRow `json:"drafts"`
	}
	raw, err := os.ReadFile(os.Getenv("TELESRV_LOAD_DRAFT_BASELINE"))
	p.require(err)
	p.require(json.Unmarshal(raw, &original))
	if len(original.Drafts) > 4 || !reflect.DeepEqual(original.Drafts, p.baseline.Drafts) {
		t.Fatal("draft baseline differs from read-only preflight")
	}
	originalText := map[int64]string{}
	for _, d := range []*lifeDevice{a, b} {
		owner, peer := d.record.UserID, p.peer(d).UserID
		p.want[owner] = map[int64]*tg.DraftMessage{}
		originalText[owner] = fmt.Sprintf("[rpc-startup-2-0000000078c3931e draft account %04d]", d.record.AccountIndex)
		for _, row := range original.Drafts {
			if row.Owner == owner && (row.Peer == peer || row.Peer == owner) && row.Type == "user" && row.Top == 0 {
				value := &tg.DraftMessage{Message: row.Draft.Message, NoWebpage: row.Draft.NoWebpage, InvertMedia: row.Draft.InvertMedia}
				for _, e := range row.Draft.Entities {
					if e.Type != "bold" {
						t.Fatal("unregistered baseline draft entity")
					}
					value.Entities = append(value.Entities, &tg.MessageEntityBold{Offset: e.Offset, Length: e.Length})
				}
				p.want[owner][row.Peer] = value
			}
		}
	}
	if len(originalText) != 2 {
		t.Fatal("original startup drafts missing")
	}
	p.read("initial", true)
	save := func(d *lifeDevice, label string, peer tg.InputPeerClass, text string, rich bool, noop bool) {
		id := d.record.UserID
		if x, ok := peer.(*tg.InputPeerUser); ok {
			id = x.UserID
		}
		q := &tg.MessagesSaveDraftRequest{Peer: peer, Message: text}
		var value *tg.DraftMessage
		if text != "" {
			value = &tg.DraftMessage{Message: text}
		}
		if rich {
			q.NoWebpage = true
			q.InvertMedia = true
			q.SetEntities([]tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 2}})
			value.NoWebpage = true
			value.InvertMedia = true
			value.Entities = q.Entities
		}
		changes := map[int64]*tg.DraftMessage{}
		if !noop {
			changes[id] = value
		}
		p.action(d, label, q, changes)
	}
	clear := func(d *lifeDevice, label string) {
		changes := map[int64]*tg.DraftMessage{}
		for peer := range p.want[d.record.UserID] {
			changes[peer] = nil
		}
		p.action(d, label, &tg.MessagesClearAllDraftsRequest{}, changes)
	}
	text := func(s string) string { return "草稿🧪/telesrv-lifecycle/" + p.id + "/" + s }
	save(a, "a-private-rich", p.peer(a), text("rich"), true, false)
	save(a, "a-private-repeat", p.peer(a), text("rich"), true, true)
	save(as, "a-secondary-overwrite", p.peer(as), text("secondary"), false, false)
	save(a, "a-saved-create", &tg.InputPeerSelf{}, text("saved"), false, false)
	p.read("two-peers", true)
	originalA := as.cursor
	p.require(p.stop(as))
	save(a, "a-offline-overwrite", p.peer(a), text("offline"), false, false)
	save(b, "b-independent-private", p.peer(b), text("b-private"), true, false)
	save(b, "b-independent-saved", &tg.InputPeerSelf{}, text("b-saved"), false, false)
	save(a, "a-clear-private", p.peer(a), "", false, false)
	save(a, "a-clear-private-repeat", p.peer(a), "", false, true)
	clear(a, "a-clear-all-one")
	clear(a, "a-clear-all-empty")
	p.recover(as, originalA, "a-original-recovery")
	save(a, "a-recreate-private", p.peer(a), text("recreate-private"), false, false)
	save(a, "a-recreate-saved", &tg.InputPeerSelf{}, text("recreate-saved"), false, false)
	originalB := bs.cursor
	p.require(p.stop(bs))
	clear(b, "b-clear-all-two")
	clear(b, "b-clear-all-empty")
	p.recover(bs, originalB, "b-original-recovery")
	clear(a, "a-clear-all-two")
	p.read("all-empty", true)
	save(a, "a-restore-content", p.peer(a), originalText[a.record.UserID], false, false)
	save(b, "b-restore-content", p.peer(b), originalText[b.record.UserID], false, false)
	p.read("restored", true)
	final := p.checkpoint("draft-final")
	if p.mutations != 18 || len(p.cases) != 20 || final[a.record.Index].Pts-p.initial[a.record.UserID] != 11 || final[b.record.Index].Pts-p.initial[b.record.UserID] != 5 {
		t.Fatal("draft frozen totals")
	}
	p.snapshot("final")
	p.facts("final")
	p.record("draft_summary", map[string]any{"mutations": 18, "pts": 16, "cases": 20, "new_messages": 0, "restored_content": true, "restored_date_and_pts": false})
}

func TestDraftEvidenceRetainsPayloadAndEmptyAux(t *testing.T) {
	draft := &tg.DraftMessage{Message: "草稿🧪", NoWebpage: true, InvertMedia: true, Date: 123, Entities: []tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 2}}}
	u := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateDraftMessage{Peer: &tg.PeerUser{UserID: 7}, Draft: draft}, &tg.UpdateDeleteMessages{Messages: []int{}, Pts: 8, PtsCount: 1}}}
	var wire bin.Buffer
	if err := u.Encode(&wire); err != nil {
		t.Fatal(err)
	}
	decoded, err := tg.DecodeUpdates(&wire)
	if err != nil {
		t.Fatal(err)
	}
	got := lifeUpdates(decoded)
	if len(got) != 2 || !reflect.DeepEqual(got[0].Draft, draftWire(t, draft)) || got[0].Peer != 7 || got[0].Pts != 0 || len(got[1].IDs) != 0 || got[1].Pts != 8 || got[1].Count != 1 {
		t.Fatal("draft/aux evidence lost or conflated")
	}
	data, err := json.Marshal(got)
	if err != nil || !bytes.Contains(data, []byte("草稿🧪")) || !bytes.Contains(data, []byte(`"NoWebpage":true`)) {
		t.Fatal("typed draft missing in evidence", err)
	}
}
