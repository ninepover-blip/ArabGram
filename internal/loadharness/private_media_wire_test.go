//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

// Keep full typed payloads: the lifecycle shorthand omits media constructors.
func (p *privateLifecycleProbe) mediaCall(d *lifeDevice, label string, req bin.Encoder, out bin.Decoder, want string) {
	p.t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := d.client.Invoke(ctx, req, out)
	cancel()
	class := classifyError(err)
	if e, ok := tgerr.As(err); ok {
		class = e.Type
	}
	p.record("media_rpc", map[string]any{"device": d.record.Index, "generation": d.generation, "label": label, "method": fmt.Sprintf("%T", req), "request": req, "response": out, "class": class, "expected_error": want, "started_at": start.UTC(), "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
	if class != want {
		p.t.Fatalf("%s: want %s got %s", label, want, class)
	}
}
func mediaMessage(out tg.MessagesMessagesClass) *tg.Message {
	var list []tg.MessageClass
	switch x := out.(type) {
	case *tg.MessagesMessages:
		list = x.Messages
	case *tg.MessagesMessagesSlice:
		list = x.Messages
	}
	if len(list) != 1 {
		return nil
	}
	m, _ := list[0].(*tg.Message)
	return m
}
func (p *privateLifecycleProbe) mediaGet(d *lifeDevice, id int) *tg.Message {
	var out tg.MessagesMessagesBox
	p.mediaCall(d, "fixture/get", &tg.MessagesGetMessagesRequest{ID: []tg.InputMessageClass{&tg.InputMessageID{ID: id}}}, &out, "ok")
	m := mediaMessage(out.Messages)
	if m == nil {
		p.t.Fatal("missing media message")
	}
	return m
}
func mediaInput(m *tg.Message) tg.InputMediaClass {
	switch x := m.Media.(type) {
	case *tg.MessageMediaDocument:
		if d, ok := x.Document.(*tg.Document); ok {
			return &tg.InputMediaDocument{ID: &tg.InputDocument{ID: d.ID, AccessHash: d.AccessHash, FileReference: d.FileReference}}
		}
	case *tg.MessageMediaPhoto:
		if p, ok := x.Photo.(*tg.Photo); ok {
			return &tg.InputMediaPhoto{ID: &tg.InputPhoto{ID: p.ID, AccessHash: p.AccessHash, FileReference: p.FileReference}}
		}
	}
	return nil
}
func (p *privateLifecycleProbe) mediaUpload(d *lifeDevice, kind string, id int64) tg.InputMediaClass {
	data := []byte("telesrv synthetic media pagination fixture\n")
	name, mime := "fixture.txt", "text/plain"
	if kind == "photo" {
		im := image.NewNRGBA(image.Rect(0, 0, 32, 32))
		for y := 0; y < 32; y++ {
			for x := 0; x < 32; x++ {
				im.SetNRGBA(x, y, color.NRGBA{uint8(x * 7), uint8(y * 7), 120, 255})
			}
		}
		var b bytes.Buffer
		p.require(png.Encode(&b, im))
		data = b.Bytes()
		name, mime = "fixture.png", "image/png"
	}
	if len(data) > 16384 {
		p.t.Fatal("upload fixture exceeds budget")
	}
	var saved tg.BoolBox
	p.mediaCall(d, "fixture/upload", &tg.UploadSaveFilePartRequest{FileID: id, FilePart: 0, Bytes: data}, &saved, "ok")
	if _, ok := saved.Bool.(*tg.BoolTrue); !ok {
		p.t.Fatal("upload not accepted")
	}
	file := &tg.InputFile{ID: id, Parts: 1, Name: name, MD5Checksum: fmt.Sprintf("%x", md5.Sum(data))}
	if kind == "photo" {
		return &tg.InputMediaUploadedPhoto{File: file}
	}
	return &tg.InputMediaUploadedDocument{File: file, MimeType: mime, Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: name}}}
}

type mediaFixtureItem struct {
	Owner int64  `json:"owner"`
	ID    int    `json:"id"`
	Peer  int64  `json:"peer"`
	Body  string `json:"body"`
	Kind  string `json:"kind"`
	Saved bool   `json:"saved"`
}

func TestRealPrivateMediaFixture(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_MEDIA_SETUP") != "1" {
		t.Skip("requires bounded isolated media setup authorization")
	}
	p := newPrivateLifecycleProbe(t, "M02 bounded real RPC media/Saved/pin fixture; not capacity")
	var primary []*lifeDevice
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			primary = append(primary, d)
		}
	}
	if len(primary) != 2 {
		t.Fatal("fixture owner shape")
	}
	for _, d := range primary {
		var users tg.UserClassVector
		p.mediaCall(d, "fixture/authorization", &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{&tg.InputUserSelf{}}}, &users, "ok")
		u, ok := users.Elems[0].(*tg.User)
		if !ok || !u.Premium {
			t.Fatal("setup contract requires verified current Premium fixture; do not mutate entitlement")
		}
	}
	cache := map[string]tg.InputMediaClass{}
	var items []mediaFixtureItem
	var savedItems []mediaFixtureItem
	send := func(d *lifeDevice, kind, body string, req bin.Encoder, saved bool) {
		before := p.checkpoint("fixture/before")
		mark, start := p.mark(), time.Now()
		p.record("fixture_intent", map[string]any{"device": d.record.Index, "request": req, "saved": saved, "body": body})
		var out tg.UpdatesBox
		p.mediaCall(d, "fixture/send", req, &out, "ok")
		id := 0
		for _, u := range lifeUpdates(out.Updates) {
			if u.Kind == "new" || u.Kind == "sent" {
				id = u.ID
			}
		}
		if id <= 0 {
			t.Fatal("missing sender box ID")
		}
		m := p.mediaGet(d, id)
		input := mediaInput(m)
		if input == nil {
			t.Fatal("media constructor lost")
		}
		cache[fmt.Sprint(d.record.UserID)+kind] = input
		ids := map[int64]int{d.record.UserID: id}
		want := map[int][]lifeUpdate{}
		delta := map[int64]int{d.record.UserID: 1}
		if !saved {
			other := primary[0]
			if other == d {
				other = primary[1]
			}
			var h tg.MessagesMessagesBox
			p.mediaCall(other, "fixture/recipient-box", &tg.MessagesGetHistoryRequest{Peer: p.peer(other), Limit: 1}, &h, "ok")
			rm := mediaMessage(h.Messages)
			if rm == nil || rm.Message != body || mediaInput(rm) == nil {
				t.Fatal("recipient media mismatch")
			}
			ids[other.record.UserID] = rm.ID
			delta[other.record.UserID] = 1
			items = append(items, mediaFixtureItem{other.record.UserID, rm.ID, d.record.UserID, body, kind, false})
		}
		peer := p.peer(d).UserID
		if saved {
			peer = d.record.UserID
		}
		item := mediaFixtureItem{d.record.UserID, id, peer, body, kind, saved}
		items = append(items, item)
		if saved {
			savedItems = append(savedItems, item)
		}
		for _, target := range p.devices {
			if target == d || saved && target.record.UserID != d.record.UserID {
				continue
			}
			v := lifeMessage(m)
			v.Kind = "new"
			v.ID = ids[target.record.UserID]
			v.Peer = p.peer(target).UserID
			if saved {
				v.Peer = d.record.UserID
			}
			v.Out = target.record.UserID == d.record.UserID
			v.Pts = before[target.record.Index].Pts + 1
			v.Count = 1
			want[target.record.Index] = []lifeUpdate{v}
		}
		p.checkLive(mark, start, want, "fixture/send")
		p.checkPTS(before, delta, "fixture/after")
		p.record("fixture_message", map[string]any{"item": item, "owner_ids": fmt.Sprint(ids), "saved": saved})
	}
	for i := 0; i < 12; i++ {
		d := primary[i%2]
		kind := "document"
		if i >= 6 {
			kind = "photo"
		}
		key := fmt.Sprint(d.record.UserID) + kind
		input := cache[key]
		if input == nil {
			input = p.mediaUpload(d, kind, time.Now().UnixNano())
		}
		body := fmt.Sprintf("telesrv-lifecycle/%s/media/%s/%02d", p.id, kind, i)
		send(d, kind, body, &tg.MessagesSendMediaRequest{Peer: p.peer(d), Media: input, Message: body, RandomID: time.Now().UnixNano()}, false)
	}
	for _, d := range primary {
		for _, kind := range []string{"document", "photo"} {
			var source mediaFixtureItem
			for _, it := range items {
				if it.Owner == d.record.UserID && it.Kind == kind && !it.Saved {
					source = it
					break
				}
			}
			send(d, kind, source.Body, &tg.MessagesForwardMessagesRequest{FromPeer: p.peer(d), ToPeer: &tg.InputPeerSelf{}, ID: []int{source.ID}, RandomID: []int64{time.Now().UnixNano()}}, true)
		}
	}
	for _, d := range primary {
		var item mediaFixtureItem
		for _, x := range items {
			if x.Owner == d.record.UserID && !x.Saved && x.Kind == "document" {
				item = x
				break
			}
		}
		before := p.checkpoint("pin/before")
		mark, start := p.mark(), time.Now()
		var out tg.UpdatesBox
		p.mediaCall(d, "fixture/pin", &tg.MessagesUpdatePinnedMessageRequest{Peer: p.peer(d), ID: item.ID, PmOneside: true}, &out, "ok")
		v := lifeUpdate{Kind: "pinned", Peer: p.peer(d).UserID, IDs: []int{item.ID}, Pinned: true, Pts: before[d.record.Index].Pts + 1, Count: 1}
		if x := lifeUpdates(out.Updates); len(x) != 1 || x[0].Kind != "pinned" {
			t.Fatal("pin echo missing")
		}
		want := map[int][]lifeUpdate{}
		for _, target := range p.devices {
			if target != d && target.record.UserID == d.record.UserID {
				want[target.record.Index] = []lifeUpdate{v}
			}
		}
		p.checkLive(mark, start, want, "fixture/pin")
		p.checkPTS(before, map[int64]int{d.record.UserID: 1}, "pin/after")
		var self mediaFixtureItem
		for _, x := range savedItems {
			if x.Owner == d.record.UserID {
				self = x
				break
			}
		}
		out = tg.UpdatesBox{}
		before = p.checkpoint("tag/before")
		mark, start = p.mark(), time.Now()
		p.mediaCall(d, "fixture/saved-tag", &tg.MessagesSendReactionRequest{Peer: &tg.InputPeerSelf{}, MsgID: self.ID, Reaction: []tg.ReactionClass{&tg.ReactionEmoji{Emoticon: "👍"}}}, &out, "ok")
		tag := lifeUpdate{Kind: "reaction", Peer: d.record.UserID, ID: self.ID, Tags: true, Reactions: []lifeReaction{{Emoji: "👍", Count: 1, Chosen: true}}}
		want = map[int][]lifeUpdate{}
		for _, target := range p.devices {
			if target != d && target.record.UserID == d.record.UserID {
				want[target.record.Index] = []lifeUpdate{tag}
			}
		}
		p.checkLive(mark, start, want, "fixture/saved-tag")
		p.checkPTS(before, map[int64]int{}, "tag/after")
	}
	p.checkPTS(map[int]tg.UpdatesState{0: {Pts: p.initial[p.devices[0].record.UserID]}, 1: {Pts: p.initial[p.devices[1].record.UserID]}, 2: {Pts: p.initial[p.devices[2].record.UserID]}, 3: {Pts: p.initial[p.devices[3].record.UserID]}}, map[int64]int{primary[0].record.UserID: 15, primary[1].record.UserID: 15}, "fixture/final")
	if len(items) != 28 || len(savedItems) != 4 {
		t.Fatal("fixture message budget mismatch")
	}
	p.snapshot("final")
	data, err := json.MarshalIndent(map[string]any{"run_id": p.id, "items": items, "messages": 16, "boxes": 28, "pts": 30, "tag_wire": "two authorized Premium tag assignments; non-PTS durable delivery"}, "", "  ")
	p.require(err)
	p.require(writeNewEvidenceReport(filepath.Join(p.out, "media-fixture.json"), data))
	p.cases = append(p.cases, map[string]any{"fixture": true, "pass": true})
}
