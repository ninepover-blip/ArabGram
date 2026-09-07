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

var pinCatalogSQL = strings.Replace(partialCatalogSQL, "load_partial_%", "load_pin_%", 1)

type pinMessage struct {
	ID           int64  `json:"id"`
	Sender       int64  `json:"sender_user_id"`
	Recipient    int64  `json:"recipient_user_id"`
	SenderBox    int    `json:"sender_box_id"`
	RecipientBox int    `json:"recipient_box_id"`
	Body         string `json:"body"`
}
type pinBox struct {
	Owner  int64  `json:"owner_user_id"`
	ID     int    `json:"box_id"`
	Peer   int64  `json:"peer_id"`
	UID    int64  `json:"private_message_id"`
	Body   string `json:"body"`
	Pinned bool   `json:"pinned"`
}
type pinEvent struct {
	draftEvent
	MessageID int   `json:"message_box_id"`
	IDs       []int `json:"message_ids"`
	Pinned    bool  `json:"event_bool"`
}
type pinFacts struct {
	Messages []pinMessage   `json:"messages"`
	Boxes    []pinBox       `json:"boxes"`
	Pins     []pinBox       `json:"pins"`
	Events   []pinEvent     `json:"events"`
	MaxUID   int64          `json:"max_uid"`
	State    map[string]any `json:"-"`
}
type pinBaseline struct {
	MaxUID   int64        `json:"max_uid"`
	Messages []pinMessage `json:"messages"`
	Boxes    []pinBox     `json:"boxes"`
	Pins     []pinBox     `json:"historical_pins"`
}
type pinServiceProbe struct {
	*partialForwardProbe
	base   pinBaseline
	failed int
}

func pinFaultSQL(run string, index int, target pinMessage) (name, install, remove string, err error) {
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(run) || index < 0 || index > 1 || target.ID <= 0 || target.Sender <= 0 || target.Recipient <= 0 || target.Sender == target.Recipient || target.SenderBox <= 0 || target.RecipientBox <= 0 || !regexp.MustCompile(`^telesrv-lifecycle/[0-9a-f]{16}/send-clear/private/message$`).MatchString(target.Body) {
		return "", "", "", fmt.Errorf("unscoped pin service fault")
	}
	name = fmt.Sprintf("load_pin_%s_%d", run, index)
	install = fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $fault$
DECLARE facts jsonb; BEGIN
IF NOT EXISTS(SELECT 1 FROM private_messages WHERE id=%d AND sender_user_id=%d AND recipient_user_id=%d AND body='%s') THEN RAISE EXCEPTION 'pin source changed'; END IF;
SELECT jsonb_build_object('id',m.id,'sender',m.sender_user_id,'recipient',m.recipient_user_id,'random_id',m.random_id,'reply_id',m.reply_to_msg_id,'sender_pts',m.sender_pts,'recipient_pts',m.recipient_pts,
'boxes',(SELECT count(*) FROM message_boxes b WHERE b.private_message_id=m.id),
'message_events',(SELECT count(*) FROM user_update_events e JOIN message_boxes b ON b.owner_user_id=e.user_id AND b.box_id=e.message_box_id WHERE b.private_message_id=m.id AND e.event_type='new_message'),
'message_outbox',(SELECT count(*) FROM dispatch_outbox o JOIN message_boxes b ON b.owner_user_id=o.target_user_id AND b.pts=o.pts WHERE b.private_message_id=m.id AND o.event_type='new_message'),
'receipt',m.sender_snapshot<>'{}'::jsonb AND m.sender_box_id>0 AND m.sender_pts>0,
'target_boxes',(SELECT jsonb_agg(jsonb_build_object('owner',owner_user_id,'id',box_id,'pinned',pinned,'xmin',xmin::text) ORDER BY owner_user_id) FROM message_boxes WHERE private_message_id=%d),
'transaction_id',txid_current()) INTO facts FROM private_messages m WHERE m.id=NEW.id AND m.sender_user_id=NEW.sender_user_id;
IF facts IS NULL OR (facts->>'boxes')::int<>2 OR (facts->>'message_events')::int<>2 OR (facts->>'message_outbox')::int<>2 OR NOT (facts->>'receipt')::boolean THEN RAISE EXCEPTION 'pin fault precondition failed' USING DETAIL=facts::text; END IF;
RAISE EXCEPTION 'telesrv_%s' USING ERRCODE='P0001',DETAIL=facts::text;
END $fault$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON public.private_messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.sender_user_id=%d AND NEW.recipient_user_id=%d AND NEW.reply_to_msg_id=%d AND NEW.media->>'kind'='service' AND NEW.media->'service_action'->>'kind'='pin_message') EXECUTE FUNCTION public.%s()`, name, target.ID, target.Sender, target.Recipient, target.Body, target.ID, name, name, target.Sender, target.Recipient, target.SenderBox, name)
	remove = fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON public.private_messages; DROP FUNCTION IF EXISTS public.%s()", name, name)
	return
}

func (p *pinServiceProbe) facts(label string) pinFacts {
	var clauses []string
	for owner, pts := range p.initial {
		clauses = append(clauses, fmt.Sprintf("(user_id=%d AND pts>%d)", owner, pts))
	}
	sort.Strings(clauses)
	ids := fmt.Sprintf("%d,%d", p.base.Messages[0].ID, p.base.Messages[1].ID)
	query := fmt.Sprintf(`SELECT jsonb_build_object(
'max_uid',(SELECT max(id) FROM private_messages),
'messages',(SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM (SELECT *,encode(request_fingerprint,'hex') AS fingerprint FROM private_messages WHERE id>%d) m),
'boxes',(SELECT jsonb_agg(to_jsonb(b) ORDER BY owner_user_id,box_id) FROM message_boxes b WHERE private_message_id IN (%s) OR private_message_id>%d),
'pins',(SELECT jsonb_agg(to_jsonb(b) ORDER BY owner_user_id,box_id) FROM message_boxes b WHERE pinned),
'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY user_id,pts) FROM user_update_events e WHERE %s),
'drafts',(SELECT jsonb_agg(to_jsonb(d) ORDER BY user_id,peer_id,top_message_id) FROM dialog_drafts d),
'watermarks',(SELECT jsonb_agg(to_jsonb(w) ORDER BY user_id) FROM (SELECT user_id,contiguous_pts FROM user_update_watermarks) w),
'dialogs',(SELECT jsonb_agg(to_jsonb(d) ORDER BY user_id,peer_id) FROM dialogs d),
'counts',jsonb_build_object('messages',(SELECT count(*) FROM private_messages),'boxes',(SELECT count(*) FROM message_boxes),'events',(SELECT count(*) FROM user_update_events)),
'old_message_digest',(SELECT md5(string_agg(to_jsonb(m)::text,chr(10) ORDER BY id)) FROM private_messages m WHERE id<=%d),
'old_box_digest',(SELECT md5(string_agg(to_jsonb(b)::text,chr(10) ORDER BY owner_user_id,box_id)) FROM message_boxes b WHERE private_message_id<=%d AND private_message_id NOT IN (%s)),
'queues',jsonb_build_object('pts',(SELECT count(*) FROM dispatch_outbox),'absolute',(SELECT count(*) FROM edge_delivery_outbox),'channel',(SELECT count(*) FROM channel_delivery_events)))`, p.base.MaxUID, ids, p.base.MaxUID, strings.Join(clauses, " OR "), p.base.MaxUID, p.base.MaxUID, ids)
	raw := p.sql(label, query, false)
	var full map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	p.require(decoder.Decode(&full))
	p.record("pin_pg", map[string]any{"label": label, "facts": full})
	var f pinFacts
	p.require(json.Unmarshal(raw, &f))
	f.State = full
	if len(f.Messages) > 4 || len(f.Events) > 28 || len(f.Boxes) > 11 {
		p.t.Fatal("pin mutation budget exceeded")
	}
	return f
}

func (p *pinServiceProbe) get(d *lifeDevice, id int, label string) tg.MessageClass {
	var out tg.MessagesMessagesBox
	p.mediaCall(d, label, &tg.MessagesGetMessagesRequest{ID: []tg.InputMessageClass{&tg.InputMessageID{ID: id}}}, &out, "ok")
	var values []tg.MessageClass
	switch v := out.Messages.(type) {
	case *tg.MessagesMessages:
		values = v.Messages
	case *tg.MessagesMessagesSlice:
		values = v.Messages
	}
	if len(values) != 1 {
		p.t.Fatal("pin get cardinality")
	}
	return values[0]
}
func (p *pinServiceProbe) reads(f pinFacts, label string) {
	for _, b := range f.Boxes {
		if b.UID > p.base.MaxUID {
			continue
		}
		v, ok := p.get(p.primary[b.Owner], b.ID, label+"/target").(*tg.Message)
		if !ok || v.ID != b.ID || v.Pinned != b.Pinned || v.Message != b.Body || lifePeer(v.PeerID) != b.Peer {
			p.t.Fatal("pin target wire disagrees with PG")
		}
	}
	for _, pair := range [][2]int64{{p.devices[0].record.UserID, p.devices[1].record.UserID}, {p.devices[1].record.UserID, p.devices[0].record.UserID}, {p.devices[0].record.UserID, p.devices[0].record.UserID}} {
		d := p.primary[pair[0]]
		peer := tg.InputPeerClass(p.peer(d))
		if pair[0] == pair[1] {
			peer = &tg.InputPeerSelf{}
		}
		var out tg.MessagesMessagesBox
		p.mediaCall(d, label+"/search", &tg.MessagesSearchRequest{Peer: peer, Filter: &tg.InputMessagesFilterPinned{}, Limit: 100}, &out, "ok")
		var messages []tg.MessageClass
		var count int
		switch v := out.Messages.(type) {
		case *tg.MessagesMessagesSlice:
			messages, count = v.Messages, v.Count
		case *tg.MessagesMessages:
			messages, count = v.Messages, len(v.Messages)
		default:
			p.t.Fatal("unexpected pin search constructor")
		}
		var ids []int
		for _, b := range f.Pins {
			if b.Owner == pair[0] && b.Peer == pair[1] {
				ids = append(ids, b.ID)
			}
		}
		sort.Sort(sort.Reverse(sort.IntSlice(ids)))
		if count != len(ids) || len(messages) != len(ids) {
			p.t.Fatal("pin search total mismatch")
		}
		for i, m := range messages {
			x, ok := m.(*tg.Message)
			if !ok || !x.Pinned || x.ID != ids[i] {
				p.t.Fatal("pin search item mismatch")
			}
		}
	}
}

// Contract failures remain failures, but bounded diagnostics continue through
// the exact target's unpin and DDL removal. Safety and observation failures stop.
func (p *pinServiceProbe) action(label string, req bin.Encoder, code string, wantA, wantB, newMessages int) {
	a := p.devices[0]
	before := p.checkpoint(label + "/before")
	old := p.facts(label + "/before")
	mark, start := p.mark(), time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	var response bin.Decoder = &tg.UpdatesBox{}
	if _, ok := req.(*tg.MessagesUnpinAllMessagesRequest); ok {
		response = &tg.MessagesAffectedHistory{}
	}
	err := a.client.Invoke(ctx, req, response)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("pin_rpc", map[string]any{"label": label, "device": a.record.Index, "generation": a.generation, "method": fmt.Sprintf("%T", req), "request": req, "response": response, "class": class, "expected_class": code, "started_at": start.UTC(), "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	f := p.facts(label + "/after")
	delta := map[int64]int{}
	wantLive := map[int][]lifeUpdate{}
	var expectedEcho []lifeUpdate
	for _, e := range f.Events {
		primary := p.primary[e.Owner]
		if primary == nil {
			p.t.Fatal("unexpected pin event owner")
		}
		if e.Pts <= before[primary.record.Index].Pts {
			continue
		}
		delta[e.Owner]++
		v := lifeUpdate{Kind: "pinned", Peer: e.Peer, IDs: e.IDs, Pinned: e.Pinned, Pts: e.Pts, Count: e.Count}
		switch e.Type {
		case "pinned_messages":
		case "new_message":
			wire := p.get(primary, e.MessageID, label+"/service")
			service, ok := wire.(*tg.MessageService)
			if !ok {
				p.t.Fatal("pin service constructor missing")
			}
			if _, ok := service.Action.(*tg.MessageActionPinMessage); !ok {
				p.t.Fatal("pin service action missing")
			}
			v = lifeMessage(wire)
			v.Kind, v.Pts, v.Count = "new", e.Pts, e.Count
		default:
			p.t.Fatal("unexpected pin durable event")
		}
		if e.Owner == a.record.UserID {
			expectedEcho = append(expectedEcho, v)
		}
		for _, d := range p.devices {
			if d.record.UserID != e.Owner || d == a {
				continue
			}
			if d.done != nil {
				wantLive[d.record.Index] = append(wantLive[d.record.Index], v)
			} else {
				r := v
				if e.Type == "new_message" {
					r.Kind, r.Pts, r.Count = "service", 0, 0
				}
				p.offline[d.record.Index] = append(p.offline[d.record.Index], r)
			}
		}
	}
	(&privateDraftProbe{privateLifecycleProbe: p.privateLifecycleProbe}).checkMultiDraftLive(mark, start, wantLive, label)
	p.checkPTS(before, delta, label+"/after")
	if class == "ok" {
		switch v := response.(type) {
		case *tg.UpdatesBox:
			if !reflect.DeepEqual(lifeUpdates(v.Updates), expectedEcho) {
				p.record("pin_echo_mismatch", map[string]any{"got": lifeUpdates(v.Updates), "want": expectedEcho})
				p.t.Fatal("pin echo mismatch")
			}
		case *tg.MessagesAffectedHistory:
			if v.Pts != before[0].Pts+delta[a.record.UserID] || v.PtsCount != delta[a.record.UserID] || v.Offset != 0 {
				p.t.Fatal("pin affected history mismatch")
			}
		}
	}
	pass := class == code && delta[a.record.UserID] == wantA && delta[p.devices[1].record.UserID] == wantB && len(f.Messages)-len(old.Messages) == newMessages
	if code != "ok" {
		for k, v := range old.State {
			if k != "queues" && !reflect.DeepEqual(v, f.State[k]) {
				pass = false
			}
		}
	}
	p.reads(f, label)
	p.record("pin_action", map[string]any{"label": label, "pass": pass, "delta": delta, "expected_delta": map[int64]int{a.record.UserID: wantA, p.devices[1].record.UserID: wantB}, "new_messages": len(f.Messages) - len(old.Messages), "expected_new_messages": newMessages})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": pass})
	if !pass {
		p.failed++
	}
}

func (p *pinServiceProbe) recover(d *lifeDevice, original tg.UpdatesState, label string) {
	p.start(d, &original)
	canonical := func(xs []lifeUpdate) []string {
		out := make([]string, len(xs))
		for i, x := range xs {
			data, err := json.Marshal(x)
			p.require(err)
			out[i] = string(data)
		}
		sort.Strings(out)
		return out
	}
	if !reflect.DeepEqual(canonical(d.recovered), canonical(p.offline[d.record.Index])) {
		p.record("pin_recovery_mismatch", map[string]any{"got": d.recovered, "want": p.offline[d.record.Index]})
		p.t.Fatal("pin original cursor recovery mismatch")
	}
	var out tg.UpdatesDifferenceBox
	p.mediaCall(d, label+"/repeat-empty", &tg.UpdatesGetDifferenceRequest{Pts: d.cursor.Pts, Date: d.cursor.Date, Qts: d.cursor.Qts}, &out, "ok")
	if _, ok := out.Difference.(*tg.UpdatesDifferenceEmpty); !ok {
		p.t.Fatal("pin repeat recovery not empty")
	}
	p.record("pin_recovery", map[string]any{"label": label, "device": d.record.Index, "original": original, "final": d.cursor, "expected": p.offline[d.record.Index]})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func TestRealPrivatePinService(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_PIN_SERVICE") != "1" {
		t.Skip("requires owned pin fault fixture")
	}
	var baseline pinBaseline
	raw, err := os.ReadFile(os.Getenv("TELESRV_LOAD_PIN_BASELINE"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	if len(baseline.Messages) != 2 || len(baseline.Boxes) != 3 || len(baseline.Pins) != 2 || baseline.MaxUID <= 0 {
		t.Fatal("pin target baseline invalid")
	}
	p := &pinServiceProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "private pin/service message atomicity diagnostics; contract failures retained; not capacity"), primary: map[int64]*lifeDevice{}, offline: map[int][]lifeUpdate{}}, base: baseline}
	if len(p.devices) != 4 {
		t.Fatal("pin devices")
	}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	target, saved := baseline.Messages[0], baseline.Messages[1]
	if target.Sender != a.record.UserID || target.Recipient != b.record.UserID || saved.Sender != a.record.UserID || saved.Recipient != a.record.UserID {
		t.Fatal("pin owned target peers")
	}
	initial := p.facts("baseline")
	if len(initial.Messages) != 0 || initial.MaxUID != baseline.MaxUID || !reflect.DeepEqual(initial.Boxes, baseline.Boxes) {
		t.Fatal("pin target source changed")
	}
	for i, pin := range initial.Pins {
		if i >= len(baseline.Pins) || pin.Owner != baseline.Pins[i].Owner || pin.ID != baseline.Pins[i].ID || pin.Body != baseline.Pins[i].Body {
			t.Fatal("historical pin changed")
		}
	}
	if len(initial.Pins) != 2 {
		t.Fatal("unexpected historical pins")
	}
	p.reads(initial, "initial")
	catalog := p.sql("catalog-before", pinCatalogSQL, false)
	if !strings.Contains(string(catalog), `"fault_objects": 0`) {
		t.Fatal("unreconciled pin fault")
	}
	reg, err := json.Marshal(map[string]any{"run_id": p.id, "system_id": p.spec.Postgres.SystemID, "database_oid": p.spec.Postgres.DatabaseOID, "names": []string{"load_pin_" + p.id + "_0", "load_pin_" + p.id + "_1"}, "catalog_before": string(catalog)})
	p.require(err)
	p.require(writeNewEvidenceReport(filepath.Join(p.out, "fault-cleanup.json"), reg))
	q := func(oneside, unpin, self bool) *tg.MessagesUpdatePinnedMessageRequest {
		peer := tg.InputPeerClass(p.peer(a))
		id := target.SenderBox
		if self {
			peer = &tg.InputPeerSelf{}
			id = saved.SenderBox
		}
		return &tg.MessagesUpdatePinnedMessageRequest{Peer: peer, ID: id, PmOneside: oneside, Unpin: unpin}
	}
	p.action("normal/shared", q(false, false, false), "ok", 2, 2, 1)
	p.action("normal/shared-repeat", q(false, false, false), "ok", 0, 0, 0)
	p.action("normal/shared-unpin", q(false, true, false), "ok", 1, 1, 0)
	p.action("normal/unpin-repeat", q(false, true, false), "ok", 0, 0, 0)
	p.action("normal/oneside", q(true, false, false), "ok", 1, 0, 0)
	p.action("normal/oneside-repeat", q(true, false, false), "ok", 0, 0, 0)
	p.action("normal/upgrade", q(false, false, false), "ok", 1, 2, 1)
	p.action("normal/upgrade-repeat", q(false, false, false), "ok", 0, 0, 0)
	p.action("normal/upgrade-unpin", q(false, true, false), "ok", 1, 1, 0)
	p.action("saved/pin", q(false, false, true), "ok", 1, 0, 0)
	p.action("saved/repeat", q(false, false, true), "ok", 0, 0, 0)
	p.action("saved/unpin-all", &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}, "ok", 1, 0, 0)
	p.action("saved/unpin-all-empty", &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}, "ok", 0, 0, 0)
	for i, scene := range []string{"fault/shared", "fault/upgrade"} {
		d := p.devices[3]
		wantA := 2
		if i == 1 {
			p.action(scene+"/oneside", q(true, false, false), "ok", 1, 0, 0)
			d = p.devices[2]
			wantA = 1
		}
		original := d.cursor
		p.require(p.stop(d))
		p.record("pin_offline", map[string]any{"label": scene, "device": d.record.Index, "cursor": original})
		name, install, remove, err := pinFaultSQL(p.id, i, target)
		p.require(err)
		installed := true
		t.Cleanup(func() {
			if installed {
				p.sql(scene+"/cleanup", remove, true)
			}
		})
		p.record("pin_fault", map[string]any{"label": scene, "name": name, "target": target, "install": install, "remove": remove})
		p.sql(scene+"/install", install, false)
		p.action(scene+"/faulted", q(false, false, false), "INTERNAL_SERVER_ERROR", 0, 0, 0)
		p.sql(scene+"/remove", remove, false)
		installed = false
		if !bytes.Equal(p.sql(scene+"/catalog-restored", pinCatalogSQL, false), catalog) {
			t.Fatal("pin catalog drift")
		}
		p.action(scene+"/retry", q(false, false, false), "ok", wantA, 2, 1)
		p.action(scene+"/repeat", q(false, false, false), "ok", 0, 0, 0)
		p.action(scene+"/unpin", q(false, true, false), "ok", 1, 1, 0)
		p.recover(d, original, scene+"/recovery")
	}
	final := p.facts("final")
	p.reads(final, "final")
	p.checkpoint("pin-final")
	p.snapshot("final")
	if !reflect.DeepEqual(final.Pins, initial.Pins) {
		t.Fatal("historical pins changed")
	}
	if !bytes.Equal(p.sql("catalog-final", pinCatalogSQL, false), catalog) {
		t.Fatal("pin final catalog drift")
	}
	p.record("pin_summary", map[string]any{"cases": len(p.cases), "failed": p.failed, "messages": len(final.Messages), "pts": len(final.Events)})
	if len(p.cases) != 24 {
		t.Fatal("pin frozen case count")
	}
	if p.failed != 0 {
		t.Errorf("pin atomicity contract failed in %d cases; all evidence and cleanup retained", p.failed)
	}
}

func TestPinServiceObservationAndFaultScope(t *testing.T) {
	s := &tg.MessageService{ID: 9, PeerID: &tg.PeerUser{UserID: 2}, FromID: &tg.PeerUser{UserID: 1}, Action: &tg.MessageActionPinMessage{}, ReplyTo: &tg.MessageReplyHeader{ReplyToMsgID: 7}}
	v, ok := lifeOne(&tg.UpdateNewMessage{Message: s, Pts: 22, PtsCount: 1})
	if !ok || v.ID != 9 || v.Service != s || v.Reply == nil || v.Pts != 22 {
		t.Fatal("service observation lost payload")
	}
	target := pinMessage{ID: 7, Sender: 1, Recipient: 2, SenderBox: 4, RecipientBox: 5, Body: "telesrv-lifecycle/0123456789abcdef/send-clear/private/message"}
	_, install, remove, err := pinFaultSQL("1123456789abcdef", 0, target)
	if err != nil || !strings.Contains(install, "DEFERRABLE INITIALLY DEFERRED") || strings.Contains(remove, "CASCADE") {
		t.Fatal("pin scope incomplete", err)
	}
	for _, bad := range []pinMessage{{}, {ID: 7, Sender: 1, Recipient: 1, SenderBox: 4, RecipientBox: 5, Body: target.Body}, {ID: 7, Sender: 1, Recipient: 2, SenderBox: 4, RecipientBox: 5, Body: "unowned'"}} {
		if _, _, _, err := pinFaultSQL("1123456789abcdef", 0, bad); err == nil {
			t.Fatal("unscoped pin fault accepted")
		}
	}
}
