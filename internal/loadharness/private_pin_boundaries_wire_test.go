//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type pinBoundaryProbe struct{ *pinServiceProbe }

func (p *pinBoundaryProbe) facts(label string) pinFacts {
	f := p.pinServiceProbe.facts(label)
	if len(f.Events) > 20 {
		p.t.Fatal("pin boundary PTS budget exceeded")
	}
	return f
}

type pinBoundaryBox struct {
	pinBox
	Deleted bool `json:"deleted"`
}

func (p *pinBoundaryProbe) boxes(f pinFacts) []pinBoundaryBox {
	raw, err := json.Marshal(f.State["boxes"])
	p.require(err)
	var boxes []pinBoundaryBox
	p.require(json.Unmarshal(raw, &boxes))
	return boxes
}
func (p *pinBoundaryProbe) reads(f pinFacts, label string) {
	p.pinServiceProbe.reads(f, label)
	for _, box := range p.boxes(f) {
		if box.UID <= p.base.MaxUID {
			continue
		}
		value := p.get(p.primary[box.Owner], box.ID, label+"/new-box")
		if box.Deleted {
			if v, ok := value.(*tg.MessageEmpty); !ok || v.ID != box.ID {
				p.t.Fatal("deleted box visible on wire")
			}
			continue
		}
		switch v := value.(type) {
		case *tg.Message:
			if v.ID != box.ID || v.Message != box.Body || v.Pinned != box.Pinned {
				p.t.Fatal("new text read disagrees with PG")
			}
		case *tg.MessageService:
			if v.ID != box.ID || box.Body != "" {
				p.t.Fatal("service read disagrees with PG")
			}
		default:
			p.t.Fatal("unexpected visible box constructor")
		}
	}
}

func (p *pinBoundaryProbe) action(d *lifeDevice, label string, req bin.Encoder, code string, wantA, wantB, newMessages int) pinFacts {
	before := p.checkpoint(label + "/before")
	old := p.facts(label + "/before")
	mark, start := p.mark(), time.Now()
	var response bin.Decoder = &tg.UpdatesBox{}
	if _, ok := req.(*tg.MessagesDeleteMessagesRequest); ok {
		response = &tg.MessagesAffectedMessages{}
	}
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := d.client.Invoke(ctx, req, response)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("boundary_rpc", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "method": fmt.Sprintf("%T", req), "request": req, "response": response, "class": class, "expected_class": code, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	f := p.facts(label + "/after")
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
		case "delete_messages":
			value = lifeUpdate{Kind: "delete", IDs: e.IDs, Pts: e.Pts, Count: e.Count}
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
		if e.Owner == d.record.UserID {
			echo = append(echo, value)
		}
		for _, other := range p.devices {
			if other == d || other.record.UserID != e.Owner {
				continue
			}
			if other.done != nil {
				live[other.record.Index] = append(live[other.record.Index], value)
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
				p.record("boundary_echo_mismatch", map[string]any{"got": lifeUpdates(v.Updates), "want": echo})
				p.t.Fatal("boundary echo mismatch")
			}
		case *tg.MessagesAffectedMessages:
			if v.Pts != before[d.record.Index].Pts+delta[d.record.UserID] || v.PtsCount != delta[d.record.UserID] {
				p.t.Fatal("delete affected PTS mismatch")
			}
		}
	}
	pass := class == code && delta[p.devices[0].record.UserID] == wantA && delta[p.devices[1].record.UserID] == wantB && len(f.Messages)-len(old.Messages) == newMessages
	if code != "ok" {
		for k, v := range old.State {
			if k != "queues" && !reflect.DeepEqual(v, f.State[k]) {
				pass = false
			}
		}
	}
	p.reads(f, label)
	p.record("boundary_action", map[string]any{"label": label, "pass": pass, "delta": delta, "expected_delta": map[int64]int{p.devices[0].record.UserID: wantA, p.devices[1].record.UserID: wantB}, "new_messages": len(f.Messages) - len(old.Messages), "expected_new_messages": newMessages})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": pass})
	if !pass {
		p.failed++
	}
	return f
}

func TestRealPrivatePinBoundaries(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_PIN_BOUNDARIES") != "1" {
		t.Skip("requires owned pin boundary fixture")
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
		t.Fatal("boundary baseline invalid")
	}
	p := &pinBoundaryProbe{pinServiceProbe: &pinServiceProbe{partialForwardProbe: &partialForwardProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "pin silent, peer-only unpin, local deletion visibility and invalid target wire; not capacity or blocklist acceptance"), primary: map[int64]*lifeDevice{}, offline: map[int][]lifeUpdate{}}, base: baseline}}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	target := baseline.Messages[0]
	if len(p.devices) != 4 || target.Sender != a.record.UserID || target.Recipient != b.record.UserID {
		t.Fatal("boundary ownership")
	}
	initial := p.facts("baseline")
	if len(initial.Messages) != 0 || initial.MaxUID != baseline.MaxUID || !reflect.DeepEqual(initial.Boxes, baseline.Boxes) || len(initial.Pins) != 2 {
		t.Fatal("baseline changed")
	}
	for i, v := range initial.Pins {
		if v.Owner != baseline.Pins[i].Owner || v.ID != baseline.Pins[i].ID || v.Body != baseline.Pins[i].Body {
			t.Fatal("historical pin changed")
		}
	}
	p.reads(initial, "initial")
	catalog := p.sql("catalog-before", pinCatalogSQL, false)
	if !bytes.Contains(catalog, []byte(`"fault_objects": 0`)) {
		t.Fatal("unreconciled fault DDL")
	}
	var key [8]byte
	_, err = rand.Read(key[:])
	p.require(err)
	rid := int64(binary.LittleEndian.Uint64(key[:]) & math.MaxInt64)
	if rid == 0 {
		t.Fatal("zero random source ID")
	}
	body := "telesrv-lifecycle/" + p.id + "/pin-boundary/source"
	seeded := p.action(a, "source/send", &tg.MessagesSendMessageRequest{Peer: p.peer(a), RandomID: rid, Message: body}, "ok", 1, 1, 1)
	if len(seeded.Messages) != 1 || seeded.Messages[0].Body != body || seeded.Messages[0].Sender != a.record.UserID || seeded.Messages[0].Recipient != b.record.UserID {
		t.Fatal("source scope mismatch")
	}
	source := seeded.Messages[0]
	pin := func(d *lifeDevice, id int, silent, oneside, unpin bool) *tg.MessagesUpdatePinnedMessageRequest {
		return &tg.MessagesUpdatePinnedMessageRequest{Peer: p.peer(d), ID: id, Silent: silent, PmOneside: oneside, Unpin: unpin}
	}
	off := p.devices[3]
	cursor := off.cursor
	p.require(p.stop(off))
	p.record("pin_offline", map[string]any{"label": "silent", "device": off.record.Index, "cursor": cursor})
	shared := p.action(a, "silent/shared", pin(a, target.SenderBox, true, false, false), "ok", 2, 2, 1)
	serviceID := shared.Messages[1].SenderBox
	p.action(a, "silent/repeat", pin(a, target.SenderBox, true, false, false), "ok", 0, 0, 0)
	p.action(a, "silent/unpin", pin(a, target.SenderBox, false, false, true), "ok", 1, 1, 0)
	p.recover(off, cursor, "silent/recovery")
	p.action(a, "peer-only/oneside", pin(a, target.SenderBox, false, true, false), "ok", 1, 0, 0)
	p.action(b, "peer-only/unpin", pin(b, target.RecipientBox, false, false, true), "ok", 1, 0, 0)
	p.action(b, "peer-only/repeat", pin(b, target.RecipientBox, false, false, true), "ok", 0, 0, 0)
	p.action(b, "source/delete-peer", &tg.MessagesDeleteMessagesRequest{ID: []int{source.RecipientBox}}, "ok", 0, 1, 0)
	off = p.devices[2]
	cursor = off.cursor
	p.require(p.stop(off))
	p.record("pin_offline", map[string]any{"label": "missing-peer", "device": off.record.Index, "cursor": cursor})
	p.action(a, "missing-peer/shared", pin(a, source.SenderBox, true, false, false), "ok", 2, 1, 1)
	p.action(a, "missing-peer/repeat", pin(a, source.SenderBox, true, false, false), "ok", 0, 0, 0)
	p.action(a, "missing-peer/unpin", pin(a, source.SenderBox, false, false, true), "ok", 1, 0, 0)
	p.action(b, "missing-peer/unpin-hidden", pin(b, source.RecipientBox, false, false, true), "MESSAGE_ID_INVALID", 0, 0, 0)
	p.action(a, "source/delete-owner", &tg.MessagesDeleteMessagesRequest{ID: []int{source.SenderBox}}, "ok", 1, 0, 0)
	p.recover(off, cursor, "missing-peer/recovery")
	p.action(a, "hidden-owner/pin", pin(a, source.SenderBox, false, false, false), "MESSAGE_ID_INVALID", 0, 0, 0)
	p.action(b, "hidden-peer/pin", pin(b, source.RecipientBox, false, false, false), "MESSAGE_ID_INVALID", 0, 0, 0)
	p.action(a, "wrong-peer/self", &tg.MessagesUpdatePinnedMessageRequest{Peer: &tg.InputPeerSelf{}, ID: target.SenderBox}, "MESSAGE_ID_INVALID", 0, 0, 0)
	p.action(a, "service/pin", pin(a, serviceID, false, false, false), "MESSAGE_ID_INVALID", 0, 0, 0)
	for _, tc := range []struct {
		name string
		id   int
	}{{"zero", 0}, {"negative", -1}, {"max", math.MaxInt32}} {
		p.action(a, "invalid/"+tc.name, pin(a, tc.id, false, false, false), "MESSAGE_ID_INVALID", 0, 0, 0)
	}
	final := p.facts("final")
	p.reads(final, "final")
	p.checkpoint("boundary-final")
	p.snapshot("final")
	if !reflect.DeepEqual(final.Pins, initial.Pins) || !bytes.Equal(p.sql("catalog-final", pinCatalogSQL, false), catalog) {
		t.Fatal("pin or catalog final drift")
	}
	p.record("boundary_summary", map[string]any{"cases": len(p.cases), "failed": p.failed, "messages": len(final.Messages), "pts": len(final.Events), "source": source})
	if len(p.cases) != 22 {
		t.Fatal("frozen boundary case count")
	}
	if p.failed != 0 {
		t.Errorf("boundary contract failed in %d cases; preserve evidence", p.failed)
	}
}

func TestPinBoundaryWireFlagsAndMissingReply(t *testing.T) {
	for _, tc := range []struct {
		silent, oneside, unpin bool
		flag                   uint32
	}{{true, false, false, 1}, {false, true, false, 4}, {false, false, true, 2}} {
		req := &tg.MessagesUpdatePinnedMessageRequest{Peer: &tg.InputPeerSelf{}, ID: 9, Silent: tc.silent, PmOneside: tc.oneside, Unpin: tc.unpin}
		var buf bin.Buffer
		if err := req.Encode(&buf); err != nil {
			t.Fatal(err)
		}
		var got tg.MessagesUpdatePinnedMessageRequest
		if err := got.Decode(&buf); err != nil {
			t.Fatal(err)
		}
		if uint32(got.Flags) != tc.flag || got.Silent != tc.silent || got.PmOneside != tc.oneside || got.Unpin != tc.unpin {
			t.Fatalf("wire flags=%+v", got)
		}
	}
	service := &tg.MessageService{ID: 10, PeerID: &tg.PeerUser{UserID: 11}, FromID: &tg.PeerUser{UserID: 11}, Date: 1700000000, Silent: true, Action: &tg.MessageActionPinMessage{}}
	var buf bin.Buffer
	if err := service.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	var got tg.MessageService
	if err := got.Decode(&buf); err != nil {
		t.Fatal(err)
	}
	value := lifeMessage(&got)
	if value.Service == nil || !value.Silent || value.Reply != nil || got.ReplyTo != nil {
		t.Fatal("silent service without recipient source gained a reply or lost payload")
	}
}
