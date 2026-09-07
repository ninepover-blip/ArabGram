//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type pinPermissionProbe struct{ *pinServiceProbe }

func (p *pinPermissionProbe) reads(f pinFacts, label string) {
	(&pinBoundaryProbe{pinServiceProbe: p.pinServiceProbe}).reads(f, label)
}
func (p *pinPermissionProbe) control(label string) map[string]any {
	raw := p.sql(label+"/control", `SELECT jsonb_build_object('blocks',(SELECT jsonb_agg(to_jsonb(x) ORDER BY owner_user_id,blocked_user_id) FROM contact_blocks x),'restrictions',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id) FROM account_restrictions x))`, false)
	var out map[string]any
	p.require(json.Unmarshal(raw, &out))
	p.record("permission_control", map[string]any{"label": label, "state": out})
	return out
}
func (p *pinPermissionProbe) admin(frozen bool, label string) {
	var cfg struct {
		URL   string `json:"url"`
		Token string `json:"token"`
		Owner int64  `json:"owner_user_id"`
	}
	path := os.Getenv("TELESRV_LOAD_PIN_ADMIN")
	st, err := os.Stat(path)
	p.require(err)
	if st.Mode().Perm()&0077 != 0 {
		p.t.Fatal("private admin config permissions")
	}
	raw, err := os.ReadFile(path)
	p.require(err)
	p.require(json.Unmarshal(raw, &cfg))
	a := p.devices[0]
	if cfg.URL != "http://127.0.0.1:22599/v1/accounts/set-frozen" || cfg.Owner != a.record.UserID || len(cfg.Token) < 32 {
		p.t.Fatal("admin scope")
	}
	if frozen {
		registration, err := json.Marshal(map[string]any{"run_id": p.id, "user_id": a.record.UserID, "url": cfg.URL})
		p.require(err)
		p.require(writeNewEvidenceReport(filepath.Join(p.out, "admin-cleanup.json"), registration))
	}
	before := p.facts(label + "/before")
	states := p.checkpoint(label + "/before")
	controlBefore := p.control(label + "/before")
	body := map[string]any{"command_id": "load-pin-permissions-" + p.id + "-" + label, "actor": "isolated-load-pin-permissions", "reason": "owned pin permissions fixture", "user_id": a.record.UserID, "frozen": frozen}
	if frozen {
		body["freeze_until"] = time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
		body["freeze_appeal_url"] = "http://127.0.0.1:22599/owned-fixture"
	}
	data, err := json.Marshal(body)
	p.require(err)
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(data))
	p.require(err)
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	p.require(err)
	reply, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	p.require(err)
	var result map[string]any
	p.require(json.Unmarshal(reply, &result))
	p.record("permission_admin", map[string]any{"label": label, "request": body, "response": result, "status": response.StatusCode, "started_relative_ns": start.Sub(p.epoch).Nanoseconds()})
	if response.StatusCode != 200 || result["status"] != "completed" || result["already_executed"] != false {
		p.t.Fatal("admin command failed", string(reply))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw = p.sql(label+"/delivery", `SELECT jsonb_build_object('notifications',(SELECT jsonb_agg(to_jsonb(n) ORDER BY id) FROM account_freeze_notifications n),'pending',(SELECT count(*) FROM account_freeze_notifications WHERE status<>'delivered'),'absolute',(SELECT count(*) FROM edge_delivery_outbox))`, false)
		var state struct {
			Pending  int `json:"pending"`
			Absolute int `json:"absolute"`
		}
		p.require(json.Unmarshal(raw, &state))
		var evidence map[string]any
		p.require(json.Unmarshal(raw, &evidence))
		p.record("permission_admin_delivery", map[string]any{"label": label, "state": evidence})
		if state.Pending == 0 && state.Absolute == 0 {
			break
		}
		if time.Now().After(deadline) {
			p.t.Fatal("freeze delivery did not drain")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	after := p.facts(label + "/after")
	p.checkPTS(states, map[int64]int{}, label+"/after")
	controlAfter := p.control(label + "/after")
	for k, v := range before.State {
		if k != "queues" && !reflect.DeepEqual(v, after.State[k]) {
			p.t.Fatal("admin changed PTS/message state", k)
		}
	}
	p.record("permission_admin_state", map[string]any{"label": label, "before": controlBefore, "after": controlAfter})
	// Re-read the authoritative projection on every connected device; updateUser
	// is a non-PTS reload hint, and must not be hidden inside difference recovery.
	for _, d := range p.devices {
		if d.done == nil {
			continue
		}
		var input tg.InputUserClass = &tg.InputUserSelf{}
		if d.record.UserID != a.record.UserID {
			peer := p.peer(d)
			input = &tg.InputUser{UserID: peer.UserID, AccessHash: peer.AccessHash}
		}
		var users tg.UserClassVector
		p.mediaCall(d, label+"/hydrate", &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{input}}, &users, "ok")
	}
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}
func (p *pinPermissionProbe) action(d *lifeDevice, label string, req bin.Encoder, code string, wantA, wantB, newMessages int) pinFacts {
	before := p.checkpoint(label + "/before")
	old := p.facts(label + "/before")
	controlBefore := p.control(label + "/before")
	mark, start := p.mark(), time.Now()
	var response bin.Decoder = &tg.UpdatesBox{}
	switch req.(type) {
	case *tg.MessagesUnpinAllMessagesRequest:
		response = &tg.MessagesAffectedHistory{}
	case *tg.ContactsBlockRequest, *tg.ContactsUnblockRequest:
		response = &tg.BoolBox{}
	}
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := d.client.Invoke(ctx, req, response)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("permission_rpc", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "method": fmt.Sprintf("%T", req), "request": req, "response": response, "class": class, "expected_class": code, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	f := p.facts(label + "/after")
	controlAfter := p.control(label + "/after")
	delta := map[int64]int{}
	live := map[int][]lifeUpdate{}
	var echo []lifeUpdate
	for _, e := range f.Events {
		primary := p.primary[e.Owner]
		if primary == nil {
			p.t.Fatal("unexpected boundary event owner")
		}
		if e.Pts <= before[primary.record.Index].Pts {
			continue
		}
		delta[e.Owner] += e.Count
		var value lifeUpdate
		switch e.Type {
		case "pinned_messages":
			value = lifeUpdate{Kind: "pinned", Peer: e.Peer, IDs: e.IDs, Pinned: e.Pinned, Pts: e.Pts, Count: e.Count}
		case "peer_story_blocked", "peer_settings":
			raw, _ := json.Marshal(f.State["events"])
			var es []blockEvent
			p.require(json.Unmarshal(raw, &es))
			for _, be := range es {
				if be.Owner == e.Owner && be.Pts == e.Pts {
					value = blockWire(p.t, be)
				}
			}
		case "new_message":
			wire := p.get(primary, e.MessageID, label+"/created")
			value = lifeMessage(wire)
			value.Kind, value.Pts, value.Count = "new", e.Pts, e.Count
			if s, ok := wire.(*tg.MessageService); ok {
				if _, ok := s.Action.(*tg.MessageActionPinMessage); !ok {
					p.t.Fatal("unexpected service action")
				}
			}
		default:
			p.t.Fatal("unexpected boundary event type", e.Type)
		}
		isBlock := e.Type == "peer_story_blocked" || e.Type == "peer_settings"
		if e.Owner == d.record.UserID && !isBlock {
			echo = append(echo, value)
		}
		for _, other := range p.devices {
			if (other == d && !isBlock) || other.record.UserID != e.Owner {
				continue
			}
			if other.done != nil {
				live[other.record.Index] = append(live[other.record.Index], value)
				if isBlock {
					live[other.record.Index] = append(live[other.record.Index], lifeUpdate{Kind: "delete", Pts: e.Pts, Count: 1})
				}
			} else {
				recovered := value
				if e.Type == "new_message" {
					recovered.Kind, recovered.Pts, recovered.Count = "service", 0, 0
					if value.Service == nil {
						recovered.Kind = "message"
					}
				}
				p.offline[other.record.Index] = append(p.offline[other.record.Index], recovered)
			}
		}
	}
	(&privateDraftProbe{privateLifecycleProbe: p.privateLifecycleProbe}).checkMultiDraftLive(mark, start, live, label)
	p.checkPTS(before, delta, label+"/after")
	if class == "ok" {
		switch v := response.(type) {
		case *tg.UpdatesBox:
			if !reflect.DeepEqual(lifeUpdates(v.Updates), echo) {
				p.record("permission_echo_mismatch", map[string]any{"got": lifeUpdates(v.Updates), "want": echo})
				p.t.Fatal("boundary echo mismatch")
			}
		case *tg.BoolBox:
			if _, ok := v.Bool.(*tg.BoolTrue); !ok {
				p.t.Fatal("Bool constructor")
			}
		case *tg.MessagesAffectedHistory:
			if v.Offset != 0 || v.Pts != before[d.record.Index].Pts+delta[d.record.UserID] || v.PtsCount != delta[d.record.UserID] {
				p.t.Fatal("unpin-all affected PTS mismatch")
			}
		}
	}
	pass := class == code && delta[p.devices[0].record.UserID] == wantA && delta[p.devices[1].record.UserID] == wantB && len(f.Messages)-len(old.Messages) == newMessages
	if code != "ok" {
		if !reflect.DeepEqual(controlBefore, controlAfter) {
			pass = false
		}
		for k, v := range old.State {
			if k != "queues" && !reflect.DeepEqual(v, f.State[k]) {
				pass = false
			}
		}
	}
	p.reads(f, label)
	p.record("permission_action", map[string]any{"label": label, "pass": pass, "delta": delta, "expected_delta": map[int64]int{p.devices[0].record.UserID: wantA, p.devices[1].record.UserID: wantB}, "new_messages": len(f.Messages) - len(old.Messages), "expected_new_messages": newMessages})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": pass})
	if !pass {
		p.failed++
	}
	return f
}

func TestRealPrivatePinPermissions(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_PIN_PERMISSIONS") != "1" {
		t.Skip("requires owned permission fixture")
	}
	raw, err := os.ReadFile(os.Getenv("TELESRV_LOAD_PIN_BASELINE"))
	if err != nil {
		t.Fatal(err)
	}
	var baseline pinBaseline
	if err = json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	if len(baseline.Messages) != 2 || len(baseline.Boxes) != 3 || len(baseline.Pins) != 2 || baseline.MaxUID <= 0 {
		t.Fatal("permission baseline")
	}
	p := &pinPermissionProbe{pinServiceProbe: &pinServiceProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "blocked/frozen pin and isolated Saved unpin-all; no capacity claim"), primary: map[int64]*lifeDevice{}, offline: map[int][]lifeUpdate{}}, base: baseline}}
	if len(p.devices) != 4 {
		t.Fatal("four devices required")
	}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	target := baseline.Messages[0]
	if target.Sender != a.record.UserID || target.Recipient != b.record.UserID {
		t.Fatal("target owner")
	}
	initial := p.facts("baseline")
	if len(initial.Messages) != 0 || initial.MaxUID != baseline.MaxUID || !reflect.DeepEqual(initial.Boxes, baseline.Boxes) {
		t.Fatal("baseline drift")
	}
	for _, pin := range initial.Pins {
		if pin.Peer == pin.Owner {
			t.Fatal("Saved unpinAll would affect historical pins")
		}
	}
	var controlBaseline struct {
		Controls map[string]any `json:"permission_control"`
	}
	p.require(json.Unmarshal(raw, &controlBaseline))
	c := p.control("baseline")
	if controlBaseline.Controls == nil || !reflect.DeepEqual(c, controlBaseline.Controls) || c["blocks"] != nil {
		t.Fatal("unreconciled controls")
	}
	p.reads(initial, "initial")
	pin := func(d *lifeDevice, oneside, unpin bool) *tg.MessagesUpdatePinnedMessageRequest {
		id := target.SenderBox
		if d.record.UserID == b.record.UserID {
			id = target.RecipientBox
		}
		return &tg.MessagesUpdatePinnedMessageRequest{Peer: p.peer(d), ID: id, PmOneside: oneside, Unpin: unpin}
	}
	off := p.devices[3]
	cursor := off.cursor
	p.require(p.stop(off))
	p.record("permission_offline", map[string]any{"label": "blocked", "device": off.record.Index, "cursor": cursor})
	p.action(a, "block/a-blocks-b", &tg.ContactsBlockRequest{ID: p.peer(a)}, "ok", 2, 0, 0)
	p.action(b, "block/b-shared", pin(b, false, false), "ok", 1, 2, 1)
	p.action(b, "block/b-repeat", pin(b, false, false), "ok", 0, 0, 0)
	p.action(b, "block/b-unpin", pin(b, false, true), "ok", 1, 1, 0)
	p.action(b, "block/b-oneside", pin(b, true, false), "ok", 0, 1, 0)
	p.action(b, "block/b-oneside-repeat", pin(b, true, false), "ok", 0, 0, 0)
	p.action(b, "block/b-oneside-unpin", pin(b, false, true), "ok", 0, 1, 0)
	p.action(a, "block/a-shared", pin(a, false, false), "ok", 2, 2, 1)
	p.action(a, "block/a-unpin", pin(a, false, true), "ok", 1, 1, 0)
	p.action(a, "block/a-unblocks-b", &tg.ContactsUnblockRequest{ID: p.peer(a)}, "ok", 2, 0, 0)
	p.recover(off, cursor, "blocked/recover")
	saved := []int{}
	for i := 0; i < 2; i++ {
		var key [8]byte
		_, err := rand.Read(key[:])
		p.require(err)
		rid := int64(binary.LittleEndian.Uint64(key[:]) & math.MaxInt64)
		if rid == 0 {
			t.Fatal("random ID zero")
		}
		body := fmt.Sprintf("telesrv-lifecycle/%s/pin-permissions/saved/%d", p.id, i)
		f := p.action(a, fmt.Sprintf("saved/send-%d", i), &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerSelf{}, Message: body, RandomID: rid}, "ok", 1, 0, 1)
		id := 0
		for _, m := range f.Messages {
			if m.Body == body {
				id = m.SenderBox
			}
		}
		if id == 0 {
			t.Fatal("saved source missing")
		}
		saved = append(saved, id)
		p.action(a, fmt.Sprintf("saved/pin-%d", i), &tg.MessagesUpdatePinnedMessageRequest{Peer: &tg.InputPeerSelf{}, ID: id}, "ok", 1, 0, 0)
	}
	p.admin(true, "freeze")
	for _, tc := range []struct {
		label string
		d     *lifeDevice
		req   bin.Encoder
	}{
		{"shared", a, pin(a, false, false)}, {"oneside", a, pin(a, true, false)}, {"unpin", a, pin(a, false, true)},
		{"saved-pin", a, &tg.MessagesUpdatePinnedMessageRequest{Peer: &tg.InputPeerSelf{}, ID: saved[0]}},
		{"saved-unpin-all", a, &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}},
		{"secondary-shared", p.devices[2], pin(p.devices[2], false, false)},
		{"secondary-unpin-all", p.devices[2], &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}},
	} {
		p.action(tc.d, "frozen/"+tc.label, tc.req, "FROZEN_METHOD_INVALID", 0, 0, 0)
	}
	p.action(b, "frozen/other-user-pin", pin(b, true, false), "ok", 0, 1, 0)
	p.action(b, "frozen/other-user-unpin", pin(b, false, true), "ok", 0, 1, 0)
	off = p.devices[2]
	cursor = off.cursor
	p.require(p.stop(off))
	p.record("permission_offline", map[string]any{"label": "unfreeze", "device": off.record.Index, "cursor": cursor})
	p.admin(false, "unfreeze")
	unpinAll := &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}
	p.action(a, "saved/clear", unpinAll, "ok", 1, 0, 0)
	p.action(a, "saved/clear-repeat", unpinAll, "ok", 0, 0, 0)
	for i, id := range saved {
		p.action(a, fmt.Sprintf("saved/repin-%d", i), &tg.MessagesUpdatePinnedMessageRequest{Peer: &tg.InputPeerSelf{}, ID: id}, "ok", 1, 0, 0)
	}
	p.action(a, "saved/reclear", unpinAll, "ok", 1, 0, 0)
	p.action(a, "saved/reclear-repeat", unpinAll, "ok", 0, 0, 0)
	p.recover(off, cursor, "unfreeze/recover")
	final := p.facts("final")
	p.reads(final, "final")
	p.control("final")
	p.checkpoint("permission-final")
	p.snapshot("final")
	if !reflect.DeepEqual(final.Pins, initial.Pins) || len(final.Messages) != 4 || len(final.Events) != 27 {
		t.Fatal("final permission invariant")
	}
	p.record("permission_summary", map[string]any{"cases": len(p.cases), "failed": p.failed, "messages": len(final.Messages), "pts": len(final.Events), "saved_ids": saved})
	if len(p.cases) != 33 {
		t.Fatal("permission case count", len(p.cases))
	}
	if p.failed != 0 {
		t.Errorf("permission contract failed: %d", p.failed)
	}
}

// Recovery is a separate, explicitly scoped wire action. It never deletes the
// interrupted run's messages/events or clears unrelated historical pins.
func TestRealPrivatePinPermissionRecovery(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_PIN_PERMISSION_RECOVERY") != "1" {
		t.Skip("requires scoped recovery manifest")
	}
	raw, err := os.ReadFile(os.Getenv("TELESRV_LOAD_PIN_RECOVERY"))
	if err != nil {
		t.Fatal(err)
	}
	var scope struct {
		SourceRun string         `json:"source_run"`
		Owner     int64          `json:"owner"`
		IDs       []int          `json:"ids"`
		Before    map[string]any `json:"before"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&scope); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(os.Getenv("TELESRV_LOAD_PIN_BASELINE"))
	if err != nil {
		t.Fatal(err)
	}
	var base pinBaseline
	if err = json.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}
	p := &pinPermissionProbe{pinServiceProbe: &pinServiceProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "scoped recovery of two Saved pins from interrupted permission probe"), primary: map[int64]*lifeDevice{}, offline: map[int][]lifeUpdate{}}, base: base}}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	if len(p.devices) != 4 || scope.Owner != p.devices[0].record.UserID || len(scope.IDs) != 2 || len(scope.SourceRun) != 16 {
		t.Fatal("recovery owner scope")
	}
	f := p.facts("baseline")
	for _, k := range []string{"messages", "boxes", "pins", "max_uid", "old_message_digest", "old_box_digest", "drafts"} {
		if !reflect.DeepEqual(f.State[k], scope.Before[k]) {
			t.Fatal("recovery precondition drift", k)
		}
	}
	found := map[int]bool{}
	for _, box := range f.Boxes {
		if box.Owner == scope.Owner && box.Peer == scope.Owner && box.Pinned {
			if box.Body != fmt.Sprintf("telesrv-lifecycle/%s/pin-permissions/saved/0", scope.SourceRun) && box.Body != fmt.Sprintf("telesrv-lifecycle/%s/pin-permissions/saved/1", scope.SourceRun) {
				t.Fatal("foreign Saved pin")
			}
			found[box.ID] = true
		}
	}
	if len(found) != 2 || !found[scope.IDs[0]] || !found[scope.IDs[1]] {
		t.Fatal("recovery ID mismatch")
	}
	final := p.action(p.devices[0], "recovery/clear-saved", &tg.MessagesUnpinAllMessagesRequest{Peer: &tg.InputPeerSelf{}}, "ok", 1, 0, 0)
	if p.failed != 0 || len(final.Pins) != 2 || len(final.Events) != 1 || len(final.Messages) != 4 {
		t.Fatal("recovery did not preserve history")
	}
	p.control("final")
	p.checkpoint("recovery-final")
	p.snapshot("final")
	p.record("permission_recovery_summary", map[string]any{"pass": true, "ids": scope.IDs, "source_run": scope.SourceRun})
}
