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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type partialItem struct {
	RandomID int64                  `json:"random_id"`
	UID      int64                  `json:"uid"`
	Sender   int64                  `json:"sender"`
	Messages map[string]*tg.Message `json:"messages"`
}
type partialFacts struct {
	Messages []struct {
		ID        int64  `json:"id"`
		Sender    int64  `json:"sender_user_id"`
		Recipient int64  `json:"recipient_user_id"`
		RandomID  int64  `json:"random_id"`
		Body      string `json:"body"`
	} `json:"messages"`
	Boxes []struct {
		Owner   int64 `json:"owner_user_id"`
		ID      int   `json:"box_id"`
		UID     int64 `json:"private_message_id"`
		Deleted bool  `json:"deleted"`
	} `json:"boxes"`
}
type partialForwardProbe struct {
	*privateLifecycleProbe
	primary map[int64]*lifeDevice
	items   map[int64]partialItem
	offline map[int][]lifeUpdate
}

const partialCatalogSQL = `SELECT jsonb_build_object('schema',(SELECT version FROM schema_migrations),
'functions',md5(COALESCE((SELECT string_agg(p.oid::regprocedure::text||pg_get_functiondef(p.oid),E'\n' ORDER BY p.oid::regprocedure::text) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind='f'),'')),
'triggers',md5(COALESCE((SELECT string_agg(t.tgname||pg_get_triggerdef(t.oid),E'\n' ORDER BY t.tgrelid::regclass::text,t.tgname) FROM pg_trigger t WHERE NOT t.tgisinternal),'')),
'fault_objects',(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname LIKE 'load_partial_%'))`

func (p *partialForwardProbe) sql(label, query string, cleanup bool) []byte {
	p.t.Helper()
	if p.spec.RunID != "telesrv-load-r0-20260831" || p.spec.Postgres.Database != "load_r0" || p.spec.Postgres.User != "load" {
		p.t.Fatal("partial fault requires owned R0 database")
	}
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	parent := p.ctx
	if cleanup {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	guard := fmt.Sprintf(`BEGIN; SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='2s'; DO $guard$ BEGIN IF current_database()<>'load_r0' OR (pg_control_system()).system_identifier::text<>'%s' OR (SELECT oid FROM pg_database WHERE datname=current_database())<>%d OR (SELECT version FROM schema_migrations)<>229 THEN RAISE EXCEPTION 'partial fault database identity mismatch'; END IF; END $guard$;`, p.spec.Postgres.SystemID, p.spec.Postgres.DatabaseOID)
	raw, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", "load", "-d", "load_r0", "-Atq", "-v", "ON_ERROR_STOP=1", "-c", guard+query+"; COMMIT;")
	p.record("partial_sql", map[string]any{"label": label, "query": query, "cleanup": cleanup, "response": string(raw), "class": classifyError(err)})
	p.require(err)
	return raw
}

func partialFaultSQL(run string, index int, sender, recipient, random int64, body string) (name, install, remove string, err error) {
	if !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(run) || index < 0 || index > 1 || sender <= 0 || recipient <= 0 || random <= 0 || !regexp.MustCompile(`^telesrv-lifecycle/[a-f0-9]{16}/partial/(saved-to-private|private-to-saved)/source-[01]$`).MatchString(body) || !strings.HasPrefix(body, "telesrv-lifecycle/"+run+"/") {
		return "", "", "", fmt.Errorf("unscoped partial fault")
	}
	name = fmt.Sprintf("load_partial_%s_%d", run, index)
	n := 2
	if sender == recipient {
		n = 1
	}
	install = fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $fault$
DECLARE facts jsonb; BEGIN
SELECT jsonb_build_object('id',m.id,'sender',m.sender_user_id,'recipient',m.recipient_user_id,'random_id',m.random_id,'body',m.body,
'boxes',(SELECT count(*) FROM message_boxes b WHERE b.private_message_id=m.id),
'events',(SELECT count(*) FROM user_update_events e JOIN message_boxes b ON b.owner_user_id=e.user_id AND b.box_id=e.message_box_id WHERE b.private_message_id=m.id AND e.event_type='new_message'),
'outbox',(SELECT count(*) FROM dispatch_outbox o JOIN message_boxes b ON b.owner_user_id=o.target_user_id AND b.pts=o.pts WHERE b.private_message_id=m.id),
'receipt',m.sender_snapshot<>'{}'::jsonb AND m.sender_box_id>0 AND m.sender_pts>0,
'sender_pts',m.sender_pts,'recipient_pts',m.recipient_pts,'transaction_id',txid_current()) INTO facts FROM private_messages m WHERE m.id=NEW.id;
IF facts IS NULL OR (facts->>'boxes')::int<>%d OR (facts->>'events')::int<>%d OR (facts->>'outbox')::int<>%d OR NOT (facts->>'receipt')::boolean THEN RAISE EXCEPTION 'partial fault precondition failed' USING DETAIL=facts::text; END IF;
RAISE EXCEPTION 'telesrv_%s' USING ERRCODE='P0001',DETAIL=facts::text;
END $fault$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON public.private_messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.sender_user_id=%d AND NEW.recipient_user_id=%d AND NEW.random_id=%d AND NEW.body='%s') EXECUTE FUNCTION public.%s()`, name, n, n, n, name, name, sender, recipient, random, body, name)
	remove = fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON public.private_messages; DROP FUNCTION IF EXISTS public.%s()", name, name)
	return
}

func (p *partialForwardProbe) facts(label string) partialFacts {
	query := fmt.Sprintf(`SELECT jsonb_build_object('messages',(SELECT jsonb_agg(to_jsonb(m)) FROM (SELECT id,sender_user_id,recipient_user_id,random_id,body,sender_box_id,recipient_box_id,sender_pts,recipient_pts,encode(request_fingerprint,'hex') AS fingerprint,sender_snapshot FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%' ORDER BY id) m),'boxes',(SELECT jsonb_agg(to_jsonb(b)) FROM (SELECT owner_user_id,box_id,private_message_id,body,pts,deleted,outgoing,fwd_from_peer_id,fwd_date,fwd_saved_from_peer_id,fwd_saved_from_msg_id FROM message_boxes WHERE body LIKE 'telesrv-lifecycle/%s/%%' ORDER BY owner_user_id,box_id) b))`, p.id, p.id)
	raw := p.sql(label, query, false)
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	p.require(d.Decode(&value))
	p.record("partial_pg", map[string]any{"label": label, "facts": value})
	var f partialFacts
	p.require(json.Unmarshal(raw, &f))
	return f
}

func (p *partialForwardProbe) remember(owner int64, v lifeUpdate) {
	for _, d := range p.devices {
		if d.record.UserID == owner && d.done == nil {
			p.offline[d.record.Index] = append(p.offline[d.record.Index], v)
		}
	}
}

// Reconcile PostgreSQL before interpreting any RPC error as "not committed".
func (p *partialForwardProbe) action(d *lifeDevice, label string, req bin.Encoder, wantClass string, fresh []int64) {
	if len(p.items)+len(fresh) > 10 || len(fresh) > 2 {
		p.t.Fatal("partial new-message admission budget")
	}
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	var out tg.UpdatesBox
	err := d.client.Invoke(ctx, req, &out)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("partial_rpc", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "request": req, "method": fmt.Sprintf("%T", req), "response": out.Updates, "class": class, "expected_class": wantClass, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	p.snapshot(label + "/after-rpc")
	facts := p.facts(label)
	var actual []int64
	newItems := []partialItem{}
	wantLive := map[int][]lifeUpdate{}
	delta := map[int64]int{}
	for _, m := range facts.Messages {
		if _, old := p.items[m.RandomID]; old {
			continue
		}
		actual = append(actual, m.RandomID)
		item := partialItem{RandomID: m.RandomID, UID: m.ID, Sender: m.Sender, Messages: map[string]*tg.Message{}}
		for _, b := range facts.Boxes {
			if b.UID != m.ID {
				continue
			}
			if b.Deleted || p.primary[b.Owner] == nil {
				p.t.Fatal("unexpected new owner/deleted box")
			}
			wire := p.mediaGet(p.primary[b.Owner], b.ID)
			if wire.Message != m.Body || lifePeer(wire.FromID) != m.Sender {
				p.t.Fatal("new message wire/PG mismatch")
			}
			item.Messages[strconv.FormatInt(b.Owner, 10)] = wire
			delta[b.Owner]++
			p.remember(b.Owner, lifeMessage(wire))
			for _, target := range p.devices {
				if target.record.UserID == b.Owner && target != d && target.done != nil {
					v := lifeMessage(wire)
					v.Kind, v.Pts, v.Count = "new", before[target.record.Index].Pts+delta[b.Owner], 1
					wantLive[target.record.Index] = append(wantLive[target.record.Index], v)
				}
			}
		}
		p.items[m.RandomID] = item
		newItems = append(newItems, item)
	}
	p.record("partial_action", map[string]any{"label": label, "new_items": newItems, "expected_random_ids": fresh, "class": class, "expected_class": wantClass, "delta": delta})
	if class != wantClass || !reflect.DeepEqual(actual, fresh) || len(facts.Messages) != len(p.items) {
		p.t.Fatalf("%s actual class/commits %s/%v, wanted %s/%v", label, class, actual, wantClass, fresh)
	}
	p.checkLive(mark, start, wantLive, label)
	p.checkPTS(before, delta, label+"/after")
	if q, ok := req.(*tg.MessagesForwardMessagesRequest); ok && class == "ok" {
		u, ok := out.Updates.(*tg.Updates)
		if !ok || len(u.Updates) != 2*len(q.ID) {
			p.t.Fatal("forward echo vector length")
		}
		for i, random := range q.RandomID {
			m := p.items[random].Messages[strconv.FormatInt(d.record.UserID, 10)]
			id, idOK := u.Updates[i*2].(*tg.UpdateMessageID)
			update, updateOK := u.Updates[i*2+1].(*tg.UpdateNewMessage)
			if !idOK || !updateOK || id.RandomID != random || id.ID != m.ID || !reflect.DeepEqual(lifeMessage(update.Message), lifeMessage(m)) {
				p.t.Fatal("forward exact replay lost original item/ID")
			}
		}
	}
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
	p.snapshot(label + "/drained")
}

func (p *partialForwardProbe) deleteSource(d *lifeDevice, item partialItem, label string) {
	owner := d.record.UserID
	m := item.Messages[strconv.FormatInt(owner, 10)]
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.MessagesAffectedMessages
	p.mediaCall(d, label, &tg.MessagesDeleteMessagesRequest{ID: []int{m.ID}}, &out, "ok")
	v := lifeUpdate{Kind: "delete", IDs: []int{m.ID}, Pts: before[d.record.Index].Pts + 1, Count: 1}
	if out.Pts != v.Pts || out.PtsCount != 1 {
		p.t.Fatal("source delete PTS")
	}
	want := map[int][]lifeUpdate{}
	p.remember(owner, v)
	for _, target := range p.devices {
		if target != d && target.done != nil && target.record.UserID == owner {
			want[target.record.Index] = []lifeUpdate{v}
		}
	}
	p.checkLive(mark, start, want, label)
	p.checkPTS(before, map[int64]int{owner: 1}, label+"/after")
	var read tg.MessagesMessagesBox
	p.mediaCall(d, label+"/absent", &tg.MessagesGetMessagesRequest{ID: []tg.InputMessageClass{&tg.InputMessageID{ID: m.ID}}}, &read, "ok")
	if !reflect.DeepEqual(lifeMessages(read.Messages), []lifeUpdate{{Kind: "empty", ID: m.ID}}) {
		p.t.Fatal("source owner still has deleted source")
	}
	for other, msg := range item.Messages {
		id, _ := strconv.ParseInt(other, 10, 64)
		if id != owner && !reflect.DeepEqual(p.mediaGet(p.primary[id], msg.ID), msg) {
			p.t.Fatal("delete-local changed other owner source")
		}
	}
	p.record("partial_delete", map[string]any{"label": label, "owner": owner, "source": item, "pts": out.Pts})
	p.snapshot(label)
	p.facts(label)
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func TestRealPrivatePartialForward(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_PARTIAL_FORWARD") != "1" {
		t.Skip("requires owned bounded PostgreSQL fault fixture")
	}
	p := &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "real PG partial forward commit, immutable replay and original cursor; not capacity"), primary: map[int64]*lifeDevice{}, items: map[int64]partialItem{}, offline: map[int][]lifeUpdate{}}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	if len(p.primary) != 2 || a.record.DeviceIndex != 0 || b.record.DeviceIndex != 0 {
		t.Fatal("partial fixture requires two owned primary accounts")
	}
	catalog := p.sql("catalog-before", partialCatalogSQL, false)
	if !strings.Contains(string(catalog), `"fault_objects": 0`) {
		t.Fatal("unreconciled fault objects already present")
	}
	// The guardian can remove these exact objects even if the test is killed
	// before Go cleanup runs. Persist the intent before creating any DDL.
	cleanup, err := json.Marshal(map[string]any{"run_id": p.id, "system_id": p.spec.Postgres.SystemID, "database_oid": p.spec.Postgres.DatabaseOID, "names": []string{"load_partial_" + p.id + "_0", "load_partial_" + p.id + "_1"}, "catalog_before": string(catalog)})
	p.require(err)
	p.require(writeNewEvidenceReport(filepath.Join(p.out, "fault-cleanup.json"), cleanup))
	for scene, name := range []string{"saved-to-private", "private-to-saved"} {
		label := "partial/" + name
		from, to := tg.InputPeerClass(&tg.InputPeerSelf{}), tg.InputPeerClass(p.peer(a))
		sourceCaller, sourcePeer, offline := a, tg.InputPeerClass(&tg.InputPeerSelf{}), p.devices[3]
		if scene == 1 {
			from, to, sourceCaller, sourcePeer, offline = p.peer(a), &tg.InputPeerSelf{}, b, p.peer(b), p.devices[2]
		}
		var sources []partialItem
		for index := 0; index < 2; index++ {
			random := richRandom()
			p.action(sourceCaller, fmt.Sprintf("%s/source-%d", label, index), &tg.MessagesSendMessageRequest{Peer: sourcePeer, Message: fmt.Sprintf("telesrv-lifecycle/%s/%s/source-%d", p.id, label, index), RandomID: random}, "ok", []int64{random})
			sources = append(sources, p.items[random])
		}
		original := offline.cursor
		p.record("partial_offline", map[string]any{"label": label, "device": offline.record.Index, "generation": offline.generation, "cursor": original})
		p.require(p.stop(offline))
		owner := strconv.FormatInt(a.record.UserID, 10)
		request := &tg.MessagesForwardMessagesRequest{FromPeer: from, ToPeer: to, ID: []int{sources[0].Messages[owner].ID, sources[1].Messages[owner].ID}, RandomID: []int64{richRandom(), richRandom()}}
		recipient := b.record.UserID
		if scene == 1 {
			recipient = a.record.UserID
		}
		faultName, install, remove, err := partialFaultSQL(p.id, scene, a.record.UserID, recipient, request.RandomID[1], sources[1].Messages[owner].Message)
		p.require(err)
		installed := true
		t.Cleanup(func() {
			if installed {
				p.sql(faultName+"/cleanup", remove, true)
			}
		})
		p.sql(faultName+"/install", install, false)
		p.record("partial_fault", map[string]any{"label": label, "name": faultName, "request": request, "sources": sources, "expected_boxes": 2 - scene})
		p.action(a, label+"/faulted-batch", request, "INTERNAL_SERVER_ERROR", request.RandomID[:1])
		p.deleteSource(a, sources[0], label+"/delete-committed-source")
		p.sql(faultName+"/remove", remove, true)
		installed = false
		if got := p.sql(label+"/catalog-restored", partialCatalogSQL, false); !bytes.Equal(got, catalog) {
			t.Fatal("fault DDL catalog not restored")
		}
		p.action(a, label+"/retry", request, "ok", request.RandomID[1:])
		p.deleteSource(a, sources[1], label+"/delete-second-source")
		p.action(a, label+"/exact-replay", request, "ok", nil)
		conflict := *request
		conflict.ID = []int{request.ID[1], request.ID[0]}
		p.action(a, label+"/conflicting-replay", &conflict, "RANDOM_ID_DUPLICATE", nil)
		p.start(offline, &original)
		canonical := func(values []lifeUpdate) []string {
			out := make([]string, len(values))
			for i, v := range values {
				data, err := json.Marshal(v)
				p.require(err)
				out[i] = string(data)
			}
			sort.Strings(out)
			return out
		}
		if !reflect.DeepEqual(canonical(offline.recovered), canonical(p.offline[offline.record.Index])) {
			p.record("partial_recovery_mismatch", map[string]any{"got": offline.recovered, "want": p.offline[offline.record.Index]})
			t.Fatal("partial forward original cursor mismatch")
		}
		var empty tg.UpdatesDifferenceBox
		p.mediaCall(offline, label+"/repeat-empty", &tg.UpdatesGetDifferenceRequest{Pts: offline.cursor.Pts, Date: offline.cursor.Date, Qts: offline.cursor.Qts}, &empty, "ok")
		if _, ok := empty.Difference.(*tg.UpdatesDifferenceEmpty); !ok {
			t.Fatal("repeat recovery not empty")
		}
		p.record("partial_recovery", map[string]any{"label": label, "device": offline.record.Index, "original": original, "final": offline.cursor, "expected": p.offline[offline.record.Index]})
		p.cases = append(p.cases, map[string]any{"label": label + "/recovery", "pass": true})
	}
	final := p.checkpoint("partial-final")
	pts := 0
	for owner, d := range p.primary {
		pts += final[d.record.Index].Pts - p.initial[owner]
	}
	if len(p.items) != 8 || pts != 16 || len(p.cases) != 18 {
		t.Fatalf("partial totals messages=%d pts=%d cases=%d", len(p.items), pts, len(p.cases))
	}
	p.snapshot("final")
	p.facts("final")
	if got := p.sql("catalog-final", partialCatalogSQL, false); !bytes.Equal(got, catalog) {
		t.Fatal("final fault catalog drift")
	}
	p.record("partial_summary", map[string]any{"messages": len(p.items), "pts": pts, "cases": len(p.cases)})
}

func TestPartialForwardFaultRejectsUnscopedInput(t *testing.T) {
	run := "0123456789abcdef"
	body := "telesrv-lifecycle/" + run + "/partial/saved-to-private/source-1"
	for _, bad := range []struct {
		run, body         string
		index             int
		sender, peer, rid int64
	}{{"x';DROP", body, 0, 1, 2, 3}, {run, "other source", 0, 1, 2, 3}, {run, strings.Replace(body, run, "1123456789abcdef", 1), 0, 1, 2, 3}, {run, body, 2, 1, 2, 3}, {run, body, 0, 0, 2, 3}, {run, body, 0, 1, 2, 0}} {
		if _, _, _, err := partialFaultSQL(bad.run, bad.index, bad.sender, bad.peer, bad.rid, bad.body); err == nil {
			t.Fatal("unsafe fault scope accepted")
		}
	}
	_, install, remove, err := partialFaultSQL(run, 0, 1, 2, 3, body)
	if err != nil || !strings.Contains(install, "DEFERRABLE INITIALLY DEFERRED") || !strings.Contains(install, "NEW.sender_user_id=1 AND NEW.recipient_user_id=2 AND NEW.random_id=3 AND NEW.body='") || strings.Contains(install, "OR REPLACE") || strings.Contains(remove, "CASCADE") {
		t.Fatal("fault must be exact scoped and cleanup non-cascading", err)
	}
}
