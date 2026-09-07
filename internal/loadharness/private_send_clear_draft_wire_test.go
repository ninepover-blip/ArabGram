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
)

var clearCatalogSQL = strings.Replace(partialCatalogSQL, "load_partial_%", "load_clear_%", 1)

func sendClearReplayWithoutHint(original *tg.MessagesSendMessageRequest) *tg.MessagesSendMessageRequest {
	clone := *original
	// TL optional bits can remain set after decoding or a previous Encode.
	// Rebuild them from the request fields when actually omitting this hint.
	clone.Flags = 0
	clone.ClearDraft = false
	return &clone
}

type clearEvent struct {
	draftEvent
	MessageID int `json:"message_box_id"`
}
type clearPG struct {
	Drafts   []draftRow   `json:"drafts"`
	Events   []clearEvent `json:"events"`
	Messages []struct {
		ID        int64
		Sender    int64 `json:"sender_user_id"`
		Recipient int64 `json:"recipient_user_id"`
		Rid       int64 `json:"random_id"`
		Body      string
	} `json:"messages"`
	Boxes []struct {
		Owner   int64 `json:"owner_user_id"`
		ID      int   `json:"box_id"`
		UID     int64 `json:"private_message_id"`
		Deleted bool  `json:"deleted"`
	} `json:"boxes"`
	State map[string]any `json:"-"`
}
type sendClearProbe struct {
	*partialForwardProbe
	want        map[int64]map[int64]*tg.DraftMessage
	replies     map[int64]*tg.Updates
	newMessages int
}

func clearFaultSQL(run string, index int, sender, recipient, random int64, body string) (name, install, remove string, err error) {
	if !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(run) || index < 0 || index > 1 || sender <= 0 || recipient <= 0 || random <= 0 || !regexp.MustCompile(`^telesrv-lifecycle/[a-f0-9]{16}/send-clear/(private|saved)/message$`).MatchString(body) || !strings.HasPrefix(body, "telesrv-lifecycle/"+run+"/") || (index == 1) != (sender == recipient) || strings.Contains(body, "/saved/") != (sender == recipient) {
		return "", "", "", fmt.Errorf("unscoped send-clear fault")
	}
	name = fmt.Sprintf("load_clear_%s_%d", run, index)
	n := 2
	if sender == recipient {
		n = 1
	}
	install = fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $fault$
DECLARE facts jsonb; BEGIN
SELECT jsonb_build_object('id',m.id,'sender',m.sender_user_id,'recipient',m.recipient_user_id,'random_id',m.random_id,'body',m.body,'sender_pts',m.sender_pts,'recipient_pts',m.recipient_pts,
'boxes',(SELECT count(*) FROM message_boxes b WHERE b.private_message_id=m.id),
'message_events',(SELECT count(*) FROM user_update_events e JOIN message_boxes b ON b.owner_user_id=e.user_id AND b.box_id=e.message_box_id WHERE b.private_message_id=m.id AND e.event_type='new_message'),
'message_outbox',(SELECT count(*) FROM dispatch_outbox o JOIN message_boxes b ON b.owner_user_id=o.target_user_id AND b.pts=o.pts WHERE b.private_message_id=m.id AND o.event_type='new_message'),
'receipt',m.sender_snapshot<>'{}'::jsonb AND m.sender_box_id>0 AND m.sender_pts>0,
'draft_rows',(SELECT count(*) FROM dialog_drafts d WHERE d.user_id=m.sender_user_id AND d.peer_type='user' AND d.peer_id=m.recipient_user_id AND d.top_message_id=0),
'draft_events',(SELECT count(*) FROM user_update_events e WHERE e.user_id=m.sender_user_id AND e.pts=m.sender_pts+1 AND e.event_type='draft_message' AND e.pts_count=1 AND e.peer_id=m.recipient_user_id),
'draft_outbox',(SELECT count(*) FROM dispatch_outbox o WHERE o.target_user_id=m.sender_user_id AND o.pts=m.sender_pts+1 AND o.event_type='draft_message' AND o.exclude_auth_key_id=decode('0000000000000000','hex') AND o.exclude_session_id=0),
'transaction_id',txid_current()) INTO facts FROM private_messages m WHERE m.id=NEW.id;
IF facts IS NULL OR (facts->>'boxes')::int<>%d OR (facts->>'message_events')::int<>%d OR (facts->>'message_outbox')::int<>%d OR NOT (facts->>'receipt')::boolean OR (facts->>'draft_rows')::int<>0 OR (facts->>'draft_events')::int<>1 OR (facts->>'draft_outbox')::int<>1 THEN RAISE EXCEPTION 'send-clear fault precondition failed' USING DETAIL=facts::text; END IF;
RAISE EXCEPTION 'telesrv_%s' USING ERRCODE='P0001',DETAIL=facts::text;
END $fault$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON public.private_messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.sender_user_id=%d AND NEW.recipient_user_id=%d AND NEW.random_id=%d AND NEW.body='%s') EXECUTE FUNCTION public.%s()`, name, n, n, n, name, name, sender, recipient, random, body, name)
	remove = fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON public.private_messages; DROP FUNCTION IF EXISTS public.%s()", name, name)
	return
}

func (p *sendClearProbe) facts(label string) clearPG {
	var predicates string
	for owner, pts := range p.initial {
		if predicates != "" {
			predicates += " OR "
		}
		predicates += fmt.Sprintf("(user_id=%d AND pts>%d)", owner, pts)
	}
	q := fmt.Sprintf(`SELECT jsonb_build_object(
'drafts',(SELECT jsonb_agg(to_jsonb(d) ORDER BY user_id,peer_id,top_message_id) FROM dialog_drafts d),
'messages',(SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM (SELECT *,encode(request_fingerprint,'hex') AS fingerprint FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%') m),
'boxes',(SELECT jsonb_agg(to_jsonb(b) ORDER BY owner_user_id,box_id) FROM message_boxes b WHERE body LIKE 'telesrv-lifecycle/%s/%%'),
'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY user_id,pts) FROM user_update_events e WHERE %s),
'watermarks',(SELECT jsonb_agg(to_jsonb(w) ORDER BY user_id) FROM (SELECT user_id,contiguous_pts FROM user_update_watermarks) w),
'dialogs',(SELECT jsonb_agg(to_jsonb(d) ORDER BY user_id,peer_id) FROM dialogs d),
'counts',jsonb_build_object('messages',(SELECT count(*) FROM private_messages),'boxes',(SELECT count(*) FROM message_boxes),'events',(SELECT count(*) FROM user_update_events)),
'old_message_digest',(SELECT md5(string_agg(to_jsonb(m)::text,E'\n' ORDER BY id)) FROM private_messages m WHERE body NOT LIKE 'telesrv-lifecycle/%s/%%'),
'old_box_digest',(SELECT md5(string_agg(to_jsonb(b)::text,E'\n' ORDER BY owner_user_id,box_id)) FROM message_boxes b WHERE body NOT LIKE 'telesrv-lifecycle/%s/%%'),
'queues',jsonb_build_object('pts',(SELECT count(*) FROM dispatch_outbox),'absolute',(SELECT count(*) FROM edge_delivery_outbox),'channel',(SELECT count(*) FROM channel_delivery_events)))`, p.id, p.id, predicates, p.id, p.id)
	raw := p.sql(label, q, false)
	var full map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	p.require(decoder.Decode(&full))
	p.record("clear_pg", map[string]any{"label": label, "facts": full})
	var f clearPG
	p.require(json.Unmarshal(raw, &f))
	f.State = map[string]any{}
	for key, value := range full {
		if key != "queues" {
			f.State[key] = value
		}
	}
	return f
}

func (p *sendClearProbe) expectedDraft(f clearPG, owner, peer int64, date int) lifeUpdate {
	var value tg.DraftMessageClass
	if expected := p.want[owner][peer]; expected != nil {
		copy := *expected
		found := false
		for _, row := range f.Drafts {
			if row.Owner == owner && row.Peer == peer && row.Top == 0 {
				copy.Date = row.Date
				found = true
			}
		}
		if !found {
			p.t.Fatal("expected clear-send draft absent")
		}
		value = &copy
	} else {
		v := &tg.DraftMessageEmpty{}
		v.SetDate(date)
		value = v
	}
	return lifeUpdate{Kind: "draft", Peer: peer, Draft: draftWire(p.t, value)}
}
func (p *sendClearProbe) readDrafts(f clearPG, label string) {
	count := 0
	for owner, rows := range p.want {
		for peer, want := range rows {
			count++
			found := false
			for _, row := range f.Drafts {
				if row.Owner == owner && row.Peer == peer && row.Top == 0 && row.Type == "user" && row.Draft.Message == want.Message {
					found = true
				}
			}
			if !found {
				p.t.Fatal("clear-send PG draft mismatch")
			}
		}
	}
	if len(f.Drafts) != count {
		p.t.Fatal("unexpected clear-send draft owner/peer")
	}
	for _, d := range p.devices {
		if d.done == nil {
			continue
		}
		var out tg.UpdatesBox
		p.mediaCall(d, label+"/drafts", &tg.MessagesGetAllDraftsRequest{}, &out, "ok")
		got := lifeUpdates(out.Updates)
		var want []lifeUpdate
		for peer := range p.want[d.record.UserID] {
			want = append(want, p.expectedDraft(f, d.record.UserID, peer, 0))
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Peer < got[j].Peer })
		sort.Slice(want, func(i, j int) bool { return want[i].Peer < want[j].Peer })
		if !reflect.DeepEqual(got, want) {
			p.record("clear_draft_mismatch", map[string]any{"got": got, "want": want})
			p.t.Fatal("clear-send wire draft mismatch")
		}
	}
}
func (p *sendClearProbe) live(mark int, start time.Time, want map[int][]lifeUpdate, label string) {
	(&privateDraftProbe{privateLifecycleProbe: p.privateLifecycleProbe}).checkMultiDraftLive(mark, start, want, label)
}
func (p *sendClearProbe) save(d *lifeDevice, label string, peer tg.InputPeerClass, text string) {
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.BoolBox
	p.mediaCall(d, label, &tg.MessagesSaveDraftRequest{Peer: peer, Message: text}, &out, "ok")
	if _, ok := out.Bool.(*tg.BoolTrue); !ok {
		p.t.Fatal("clear-send save false")
	}
	id := d.record.UserID
	if x, ok := peer.(*tg.InputPeerUser); ok {
		id = x.UserID
	}
	if text == "" {
		delete(p.want[d.record.UserID], id)
	} else {
		p.want[d.record.UserID][id] = &tg.DraftMessage{Message: text}
	}
	f := p.facts(label)
	var event *clearEvent
	for i := range f.Events {
		e := &f.Events[i]
		if e.Owner == d.record.UserID && e.Pts == before[d.record.Index].Pts+1 {
			event = e
		}
	}
	if event == nil || event.Type != "draft_message" || event.Count != 1 || event.Peer != id {
		p.t.Fatal("clear-send save event")
	}
	want := map[int][]lifeUpdate{}
	for _, target := range p.devices {
		if target.record.UserID == d.record.UserID && target.done != nil {
			want[target.record.Index] = []lifeUpdate{p.expectedDraft(f, event.Owner, event.Peer, event.Date), {Kind: "delete", Pts: event.Pts, Count: 1}}
		}
	}
	p.live(mark, start, want, label)
	p.checkPTS(before, map[int64]int{d.record.UserID: 1}, label+"/after")
	p.readDrafts(f, label)
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func (p *sendClearProbe) send(d *lifeDevice, label string, req *tg.MessagesSendMessageRequest, expectedClass string, fresh bool) {
	before := p.checkpoint(label + "/before")
	old := p.facts(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.UpdatesBox
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := d.client.Invoke(ctx, req, &out)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("clear_rpc", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "request": req, "response": out.Updates, "class": class, "expected_class": expectedClass, "started_at": start.UTC(), "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	// Always reconcile committed facts before interpreting an RPC error.
	f := p.facts(label + "/after")
	p.snapshot(label + "/after")
	if class != expectedClass {
		p.t.Fatalf("%s expected %s got %s", label, expectedClass, class)
	}
	owner, peer := d.record.UserID, d.record.UserID
	if q, ok := req.Peer.(*tg.InputPeerUser); ok {
		peer = q.UserID
	}
	want := map[int][]lifeUpdate{}
	delta := map[int64]int{}
	if !fresh {
		if !reflect.DeepEqual(old.State, f.State) {
			p.t.Fatal("send-clear failure/replay changed durable state")
		}
	} else {
		p.newMessages++
		if p.newMessages > 2 || len(f.Messages) != len(old.Messages)+1 {
			p.t.Fatal("send-clear message budget/cardinality")
		}
		delete(p.want[owner], peer)
		delta[owner] = 2
		if owner != peer {
			delta[peer] = 1
		}
		newEvents := 0
		for _, event := range f.Events {
			primary := p.primary[event.Owner]
			if primary == nil || event.Pts <= before[primary.record.Index].Pts {
				continue
			}
			newEvents++
			var value lifeUpdate
			switch event.Type {
			case "new_message":
				m := p.mediaGet(primary, event.MessageID)
				wantPeer := owner
				if event.Owner == owner {
					wantPeer = peer
				}
				if m.Message != req.Message || lifePeer(m.FromID) != owner || lifePeer(m.PeerID) != wantPeer || m.Out != (event.Owner == owner) || m.ReplyTo != nil || m.FwdFrom.Date != 0 {
					p.t.Fatal("send-clear message projection")
				}
				value = lifeMessage(m)
				value.Kind = "new"
				value.Pts = event.Pts
				value.Count = 1
			case "draft_message":
				if event.Owner != owner || event.Peer != peer || event.Pts != before[d.record.Index].Pts+2 {
					p.t.Fatal("send-clear draft event order")
				}
				value = p.expectedDraft(f, event.Owner, event.Peer, event.Date)
			default:
				p.t.Fatal("unexpected send-clear event type")
			}
			if event.Count != 1 {
				p.t.Fatal("send-clear event count")
			}
			for _, target := range p.devices {
				if target.record.UserID != event.Owner || target.done == nil || (event.Type == "new_message" && target == d) {
					continue
				}
				want[target.record.Index] = append(want[target.record.Index], value)
				if event.Type == "draft_message" {
					want[target.record.Index] = append(want[target.record.Index], lifeUpdate{Kind: "delete", Pts: event.Pts, Count: 1})
				}
			}
		}
		count := 2
		if owner != peer {
			count = 3
		}
		if newEvents != count {
			p.t.Fatal("send-clear aggregate event cardinality")
		}
	}
	if class == "ok" {
		u, ok := out.Updates.(*tg.Updates)
		if !ok || len(u.Updates) != 2 {
			p.t.Fatal("send-clear immutable echo shape")
		}
		id, idOK := u.Updates[0].(*tg.UpdateMessageID)
		msg, msgOK := u.Updates[1].(*tg.UpdateNewMessage)
		if !idOK || !msgOK || id.RandomID != req.RandomID || lifeMessage(msg.Message).ID != id.ID || lifeMessage(msg.Message).Text != req.Message {
			p.t.Fatal("send-clear echo payload")
		}
		if old := p.replies[req.RandomID]; old != nil {
			if !reflect.DeepEqual(old.Updates, u.Updates) || old.Date != u.Date {
				p.t.Fatal("send-clear replay lost original immutable response")
			}
		} else {
			if !fresh || msg.Pts != before[d.record.Index].Pts+1 || msg.PtsCount != 1 {
				p.t.Fatal("send-clear initial echo PTS")
			}
			p.replies[req.RandomID] = u
		}
	}
	p.live(mark, start, want, label)
	p.checkPTS(before, delta, label+"/after")
	p.readDrafts(f, label)
	p.record("clear_action", map[string]any{"label": label, "fresh": fresh, "delta": delta, "class": class})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func (p *sendClearProbe) recover(d *lifeDevice, original tg.UpdatesState, label string) {
	p.start(d, &original)
	f := p.facts(label)
	var want []lifeUpdate
	for _, e := range f.Events {
		if e.Owner != d.record.UserID || e.Pts <= original.Pts {
			continue
		}
		switch e.Type {
		case "draft_message":
			want = append(want, p.expectedDraft(f, e.Owner, e.Peer, e.Date))
		case "new_message":
			want = append(want, lifeMessage(p.mediaGet(p.primary[e.Owner], e.MessageID)))
		default:
			p.t.Fatal("unexpected clear-send difference event")
		}
	}
	canon := func(values []lifeUpdate) []string {
		out := make([]string, len(values))
		for i, v := range values {
			raw, err := json.Marshal(v)
			p.require(err)
			out[i] = string(raw)
		}
		sort.Strings(out)
		return out
	}
	if !reflect.DeepEqual(canon(want), canon(d.recovered)) {
		p.record("clear_recovery_mismatch", map[string]any{"got": d.recovered, "want": want})
		p.t.Fatal("clear-send original cursor recovery")
	}
	var empty tg.UpdatesDifferenceBox
	p.mediaCall(d, label+"/repeat-empty", &tg.UpdatesGetDifferenceRequest{Pts: d.cursor.Pts, Date: d.cursor.Date, Qts: d.cursor.Qts}, &empty, "ok")
	if _, ok := empty.Difference.(*tg.UpdatesDifferenceEmpty); !ok {
		p.t.Fatal("clear-send repeat recovery not empty")
	}
	p.record("clear_recovery", map[string]any{"label": label, "device": d.record.Index, "original": original, "final": d.cursor, "expected": want})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func TestRealPrivateSendClearDraft(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_SEND_CLEAR_DRAFT") != "1" {
		t.Skip("requires exact owned PostgreSQL commit fault fixture")
	}
	p := &sendClearProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "private/Saved send clear_draft commit rollback, immutable retry and original cursors; not capacity"), primary: map[int64]*lifeDevice{}}, want: map[int64]map[int64]*tg.DraftMessage{}, replies: map[int64]*tg.Updates{}}
	if len(p.devices) != 4 {
		t.Fatal("send-clear device count")
	}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
			p.want[d.record.UserID] = map[int64]*tg.DraftMessage{}
		}
	}
	a := p.devices[0]
	initial := p.facts("baseline")
	var original struct {
		Drafts []draftRow `json:"drafts"`
	}
	raw, err := os.ReadFile(os.Getenv("TELESRV_LOAD_CLEAR_BASELINE"))
	p.require(err)
	p.require(json.Unmarshal(raw, &original))
	if len(original.Drafts) != 2 || !reflect.DeepEqual(original.Drafts, initial.Drafts) {
		t.Fatal("send-clear draft preflight changed")
	}
	for _, d := range original.Drafts {
		if p.primary[d.Owner] == nil || d.Peer != p.peer(p.primary[d.Owner]).UserID || d.Type != "user" || d.Top != 0 {
			t.Fatal("unknown baseline draft")
		}
		p.want[d.Owner][d.Peer] = &tg.DraftMessage{Message: d.Draft.Message}
	}
	p.readDrafts(initial, "initial")
	catalog := p.sql("catalog-before", clearCatalogSQL, false)
	if !strings.Contains(string(catalog), `"fault_objects": 0`) {
		t.Fatal("unreconciled clear fault")
	}
	registration, err := json.Marshal(map[string]any{"run_id": p.id, "system_id": p.spec.Postgres.SystemID, "database_oid": p.spec.Postgres.DatabaseOID, "names": []string{"load_clear_" + p.id + "_0", "load_clear_" + p.id + "_1"}, "catalog_before": string(catalog)})
	p.require(err)
	p.require(writeNewEvidenceReport(filepath.Join(p.out, "fault-cleanup.json"), registration))
	for index, name := range []string{"private", "saved"} {
		label := "send-clear/" + name
		peer, peerID, offline := tg.InputPeerClass(p.peer(a)), p.peer(a).UserID, p.devices[3]
		if index == 1 {
			peer, peerID, offline = &tg.InputPeerSelf{}, a.record.UserID, p.devices[2]
		}
		restore := ""
		if v := p.want[a.record.UserID][peerID]; v != nil {
			restore = v.Message
		}
		p.save(a, label+"/setup", peer, "telesrv-lifecycle/"+p.id+"/"+label+"/pending")
		original := offline.cursor
		p.require(p.stop(offline))
		p.record("clear_offline", map[string]any{"label": label, "device": offline.record.Index, "cursor": original})
		q := &tg.MessagesSendMessageRequest{Peer: peer, RandomID: richRandom(), Message: "telesrv-lifecycle/" + p.id + "/" + label + "/message", ClearDraft: true}
		fault, install, remove, err := clearFaultSQL(p.id, index, a.record.UserID, peerID, q.RandomID, q.Message)
		p.require(err)
		installed := true
		p.t.Cleanup(func() {
			if installed {
				p.sql(label+"/cleanup", remove, true)
			}
		})
		p.record("clear_fault", map[string]any{"label": label, "name": fault, "request": q, "install": install, "remove": remove, "expected_boxes": 2 - index})
		p.sql(label+"/install", install, false)
		p.send(a, label+"/faulted", q, "INTERNAL_SERVER_ERROR", false)
		p.sql(label+"/remove", remove, false)
		installed = false
		if !bytes.Equal(p.sql(label+"/catalog-restored", clearCatalogSQL, false), catalog) {
			t.Fatal("clear fault catalog not restored")
		}
		p.send(a, label+"/retry", q, "ok", true)
		p.send(a, label+"/empty-replay", q, "ok", false)
		p.save(a, label+"/new-draft", peer, "telesrv-lifecycle/"+p.id+"/"+label+"/after-send")
		p.send(a, label+"/new-draft-replay", q, "ok", false)
		p.send(a, label+"/without-clear-replay", sendClearReplayWithoutHint(q), "ok", false)
		conflict := *q
		conflict.Message += "/conflict"
		p.send(a, label+"/conflicting-replay", &conflict, "RANDOM_ID_DUPLICATE", false)
		p.save(a, label+"/restore", peer, restore)
		p.recover(offline, original, label+"/recovery")
	}
	final := p.checkpoint("clear-final")
	if len(p.cases) != 20 || p.newMessages != 2 || final[0].Pts-p.initial[p.devices[0].record.UserID] != 10 || final[1].Pts-p.initial[p.devices[1].record.UserID] != 1 {
		t.Fatal("send-clear frozen totals")
	}
	p.snapshot("final")
	f := p.facts("final")
	p.readDrafts(f, "final")
	if !bytes.Equal(p.sql("catalog-final", clearCatalogSQL, false), catalog) {
		t.Fatal("clear fault final catalog drift")
	}
	p.record("clear_summary", map[string]any{"cases": 20, "messages": 2, "boxes": 3, "pts": 11})
}

func TestSendClearFaultRejectsUnscopedInputs(t *testing.T) {
	run := "0123456789abcdef"
	body := "telesrv-lifecycle/" + run + "/send-clear/private/message"
	for _, v := range []struct {
		run, body         string
		index             int
		sender, peer, rid int64
	}{{"unsafe';", body, 0, 1, 2, 3}, {run, "other", 0, 1, 2, 3}, {run, body, 2, 1, 2, 3}, {run, body, 0, 1, 1, 3}, {run, body, 0, 1, 2, 0}, {run, strings.Replace(body, run, "1123456789abcdef", 1), 0, 1, 2, 3}} {
		if _, _, _, err := clearFaultSQL(v.run, v.index, v.sender, v.peer, v.rid, v.body); err == nil {
			t.Fatal("unsafe send-clear fault accepted")
		}
	}
	_, install, remove, err := clearFaultSQL(run, 0, 1, 2, 3, body)
	if err != nil || !strings.Contains(install, "DEFERRABLE INITIALLY DEFERRED") || !strings.Contains(install, "NEW.random_id=3 AND NEW.body='") || !strings.Contains(install, "'draft_outbox'") || strings.Contains(install, "OR REPLACE") || strings.Contains(remove, "CASCADE") {
		t.Fatal("send-clear fault guard incomplete", err)
	}
}

func TestSendClearReplayActuallyOmitsWireFlag(t *testing.T) {
	q := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerSelf{}, RandomID: 19, Message: "pending", ClearDraft: true}
	var original bin.Buffer
	if err := q.Encode(&original); err != nil {
		t.Fatal(err)
	}
	if uint32(q.Flags)&128 == 0 {
		t.Fatal("test did not start from encoded clear_draft")
	}
	retry := sendClearReplayWithoutHint(q)
	var wire bin.Buffer
	if err := retry.Encode(&wire); err != nil {
		t.Fatal(err)
	}
	var decoded tg.MessagesSendMessageRequest
	if err := decoded.Decode(&wire); err != nil {
		t.Fatal(err)
	}
	if decoded.ClearDraft || uint32(decoded.Flags)&128 != 0 || decoded.RandomID != q.RandomID || decoded.Message != q.Message || !q.ClearDraft {
		t.Fatal("clear_draft was not omitted on wire or original request changed")
	}
}
