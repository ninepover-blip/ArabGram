//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

type externalMediaItem struct {
	label    string
	caller   *lifeDevice
	saved    bool
	request  bin.Encoder
	messages map[int64]*tg.Message
}

type externalMediaProbe struct {
	*privateLifecycleProbe
	primary                    map[int64]*lifeDevice
	offlineWant                map[int][]lifeUpdate
	newMessages, downloadBytes int
}

func (p *externalMediaProbe) remember(owner int64, v lifeUpdate) {
	for _, d := range p.devices {
		if d.record.UserID == owner && d.record.DeviceIndex == 1 && d.done == nil {
			p.offlineWant[d.record.Index] = append(p.offlineWant[d.record.Index], v)
		}
	}
}

func (p *externalMediaProbe) send(d *lifeDevice, label string, req bin.Encoder, saved, reply bool) externalMediaItem {
	if p.newMessages >= 16 {
		p.t.Fatal("new-message admission budget")
	}
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	p.record("external_intent", map[string]any{"label": label, "device": d.record.Index, "request": req, "saved": saved})
	var out tg.UpdatesBox
	p.mediaCall(d, label, req, &out, "ok")
	var id int
	for _, v := range lifeUpdates(out.Updates) {
		if v.Kind == "new" || v.Kind == "sent" {
			if id != 0 {
				p.t.Fatal("multiple send IDs")
			}
			id = v.ID
		}
	}
	if id <= 0 {
		p.t.Fatal("missing send ID")
	}
	item := externalMediaItem{label: label, caller: d, saved: saved, request: req, messages: map[int64]*tg.Message{d.record.UserID: p.mediaGet(d, id)}}
	if !saved {
		other := p.primary[p.peer(d).UserID]
		var out tg.MessagesMessagesBox
		p.mediaCall(other, label+"/recipient", &tg.MessagesGetHistoryRequest{Peer: p.peer(other), Limit: 1}, &out, "ok")
		m := mediaMessage(out.Messages)
		if m == nil || m.Message != item.messages[d.record.UserID].Message {
			p.t.Fatal("recipient source mismatch")
		}
		item.messages[other.record.UserID] = m
	}
	want := map[int][]lifeUpdate{}
	delta := map[int64]int{}
	for owner, m := range item.messages {
		delta[owner] = 1
		for _, target := range p.devices {
			if target.record.UserID == owner && target != d && target.done != nil {
				v := lifeMessage(m)
				v.Kind = "new"
				v.Pts = before[target.record.Index].Pts + 1
				v.Count = 1
				want[target.record.Index] = []lifeUpdate{v}
			}
		}
		if reply {
			p.remember(owner, lifeMessage(m))
		}
	}
	p.checkLive(mark, start, want, label)
	p.checkPTS(before, delta, label+"/after")
	p.newMessages++
	if p.newMessages > 16 {
		p.t.Fatal("new-message budget")
	}
	p.record("external_write", map[string]any{"label": label, "device": d.record.Index, "saved": saved, "reply": reply, "request": req, "messages": item.messages, "delta": delta})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
	p.snapshot(label)
	return item
}

func (p *externalMediaProbe) verify(label string, source *tg.Message, caller int64, reply externalMediaItem, manual bool) {
	for owner, m := range reply.messages {
		h, ok := m.ReplyTo.(*tg.MessageReplyHeader)
		if !ok {
			p.t.Fatal("missing external header")
		}
		f, hasFrom := h.GetReplyFrom()
		media, hasMedia := h.GetReplyMedia()
		if !hasFrom || lifePeer(f.FromID) != lifePeer(source.FromID) || f.Date != source.Date || !hasMedia || !reflect.DeepEqual(media, source.Media) {
			p.t.Fatalf("%s author/media snapshot mismatch", label)
		}
		wantID := 0
		if owner == caller {
			wantID = source.ID
		}
		_, hasID := h.GetReplyToMsgID()
		if h.ReplyToMsgID != wantID || hasID != (wantID != 0) {
			p.t.Fatal("external source owner ID leaked or lost")
		}
		text, offset, entities := source.Message, 0, source.Entities
		if manual {
			text = "quote"
			offset = len(utf16.Encode([]rune(source.Message))) - 5
			entities = []tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 5}}
		}
		_, hasOffset := h.GetQuoteOffset()
		if h.Quote != manual || h.QuoteText != text || h.QuoteOffset != offset || hasOffset != manual || !reflect.DeepEqual(h.QuoteEntities, entities) {
			p.t.Fatal("external quote preview mismatch")
		}
		p.record("external_reply_checked", map[string]any{"label": label, "owner": owner, "manual": manual, "source": source, "message": m})
	}
}

func (p *externalMediaProbe) mutate(source externalMediaItem, edit bool) {
	label := source.label + "/delete"
	if edit {
		label = source.label + "/edit"
	}
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	d := source.caller
	origin := source.messages[d.record.UserID]
	want := map[int][]lifeUpdate{}
	delta := map[int64]int{}
	all := map[string]lifeUpdate{}
	for owner, m := range source.messages {
		v := lifeMessage(m)
		v.Kind = "edit"
		v.Text += "/edited"
		v.Pts = before[p.primary[owner].record.Index].Pts + 1
		v.Count = 1
		if !edit {
			v = lifeUpdate{Kind: "delete", IDs: []int{m.ID}, Pts: v.Pts, Count: 1}
		}
		all[fmt.Sprint(owner)] = v
		delta[owner] = 1
		p.remember(owner, v)
		for _, target := range p.devices {
			if target.record.UserID == owner && target != d && target.done != nil {
				want[target.record.Index] = []lifeUpdate{v}
			}
		}
	}
	var req bin.Encoder
	if edit {
		peer := tg.InputPeerClass(p.peer(d))
		if source.saved {
			peer = &tg.InputPeerSelf{}
		}
		r := &tg.MessagesEditMessageRequest{Peer: peer, ID: origin.ID}
		r.SetMessage(origin.Message + "/edited")
		req = r
		var out tg.UpdatesBox
		p.mediaCall(d, label, r, &out, "ok")
		if !reflect.DeepEqual(lifeUpdates(out.Updates), []lifeUpdate{all[fmt.Sprint(d.record.UserID)]}) {
			p.t.Fatal("edit echo mismatch")
		}
	} else {
		r := &tg.MessagesDeleteMessagesRequest{ID: []int{origin.ID}, Revoke: !source.saved}
		req = r
		var out tg.MessagesAffectedMessages
		p.mediaCall(d, label, r, &out, "ok")
		if out.Pts != all[fmt.Sprint(d.record.UserID)].Pts || out.PtsCount != 1 {
			p.t.Fatal("delete echo mismatch")
		}
	}
	p.checkLive(mark, start, want, label)
	p.checkPTS(before, delta, label+"/after")
	p.record("external_mutation", map[string]any{"label": label, "request": req, "device": d.record.Index, "expected": all, "delta": delta})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
	p.snapshot(label)
}

func (p *externalMediaProbe) download(d *lifeDevice, label string, media tg.MessageMediaClass) string {
	if p.downloadBytes+16384 > 524288 {
		p.t.Fatal("download admission budget")
	}
	var location tg.InputFileLocationClass
	switch x := media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := x.Document.(*tg.Document)
		if !ok || doc.Size <= 0 || doc.Size > 16384 {
			p.t.Fatal("invalid bounded document")
		}
		location = &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash, FileReference: doc.FileReference}
	case *tg.MessageMediaPhoto:
		photo, ok := x.Photo.(*tg.Photo)
		if !ok {
			p.t.Fatal("photo missing")
		}
		var best *tg.PhotoSize
		for _, s := range photo.Sizes {
			if v, ok := s.(*tg.PhotoSize); ok && (best == nil || v.Size > best.Size) {
				best = v
			}
		}
		if best == nil || best.Size <= 0 || best.Size > 16384 {
			p.t.Fatal("photo thumbnail outside budget")
		}
		location = &tg.InputPhotoFileLocation{ID: photo.ID, AccessHash: photo.AccessHash, FileReference: photo.FileReference, ThumbSize: best.Type}
	default:
		p.t.Fatalf("unexpected external media %T", media)
	}
	var out tg.UploadFileBox
	p.mediaCall(d, label, &tg.UploadGetFileRequest{Location: location, Limit: 16384}, &out, "ok")
	file, ok := out.File.(*tg.UploadFile)
	if !ok || len(file.Bytes) == 0 || len(file.Bytes) > 16384 {
		p.t.Fatal("download payload mismatch")
	}
	p.downloadBytes += len(file.Bytes)
	if p.downloadBytes > 524288 {
		p.t.Fatal("download budget")
	}
	h := sha256.Sum256(file.Bytes)
	hash := hex.EncodeToString(h[:])
	p.record("external_download", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "location": location, "bytes": len(file.Bytes), "sha256": hash})
	return hash
}

func (p *externalMediaProbe) replay(item externalMediaItem) {
	label := item.label + "/replay"
	before := map[int]tg.UpdatesState{}
	for _, d := range p.devices {
		if d.done != nil {
			before[d.record.Index] = d.cursor
		}
	}
	mark, start := p.mark(), time.Now()
	var out tg.UpdatesBox
	p.mediaCall(item.caller, label, item.request, &out, "ok")
	var got *tg.Message
	if u, ok := out.Updates.(*tg.Updates); ok {
		for _, update := range u.Updates {
			if n, ok := update.(*tg.UpdateNewMessage); ok {
				got, _ = n.Message.(*tg.Message)
			}
		}
	}
	if got == nil || !reflect.DeepEqual(lifeMessage(got), lifeMessage(item.messages[item.caller.record.UserID])) {
		p.t.Fatal("replay after deleted media source changed")
	}
	p.checkLive(mark, start, map[int][]lifeUpdate{}, label)
	p.checkPTS(before, nil, label+"/after")
	p.record("external_replay", map[string]any{"label": label, "device": item.caller.record.Index, "request": item.request, "message": got})
	p.cases = append(p.cases, map[string]any{"label": label, "pass": true})
}

func (p *externalMediaProbe) facts(label string) {
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	query := fmt.Sprintf(`BEGIN READ ONLY; SELECT jsonb_build_object('messages',(SELECT jsonb_agg(to_jsonb(p)) FROM (SELECT id,sender_user_id,recipient_user_id,body,media,reply_external,sender_snapshot FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%' ORDER BY id) p),'boxes',(SELECT jsonb_agg(to_jsonb(b)) FROM (SELECT owner_user_id,box_id,private_message_id,body,media,reply_external,deleted FROM message_boxes WHERE body LIKE 'telesrv-lifecycle/%s/%%' ORDER BY owner_user_id,box_id) b)); COMMIT;`, p.id, p.id)
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	raw, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", p.spec.Postgres.User, "-d", p.spec.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", query)
	p.require(err)
	// Preserve 64-bit IDs as raw JSON; the evidence writer does not permit
	// arbitrary MarshalJSON values, so decode with UseNumber into bounded data.
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	p.require(decoder.Decode(&value))
	p.record("external_pg", map[string]any{"label": label, "facts": value})
}

func TestRealPrivateExternalMedia(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_EXTERNAL_MEDIA") != "1" {
		t.Skip("requires owned bounded external-media fixture")
	}
	p := &externalMediaProbe{privateLifecycleProbe: newPrivateLifecycleProbe(t, "external media reply and original-cursor recovery; not capacity"), primary: map[int64]*lifeDevice{}, offlineWant: map[int][]lifeUpdate{}}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	if len(p.primary) != 2 || a.record.DeviceIndex != 0 || b.record.DeviceIndex != 0 {
		t.Fatal("fixture shape")
	}
	wantHash := map[int64]map[int]string{a.record.UserID: {}, b.record.UserID: {}}
	cursors := map[int]tg.UpdatesState{}
	for _, c := range []struct {
		label, kind     string
		source, replier *lifeDevice
		saved, offline  bool
	}{
		{"online/photo-saved", "photo", a, a, true, false},
		{"online/document-private", "document", b, a, false, false},
		{"offline/document-saved", "document", b, b, true, true},
		{"offline/photo-private", "photo", a, b, false, true},
	} {
		if c.offline && len(cursors) == 0 {
			for _, d := range p.devices {
				if d.record.DeviceIndex == 1 {
					cursors[d.record.Index] = d.cursor
					p.record("external_offline", map[string]any{"device": d.record.Index, "generation": d.generation, "original_cursor": d.cursor})
					p.require(p.stop(d))
				}
			}
		}
		peer := tg.InputPeerClass(p.peer(c.source))
		if c.saved {
			peer = &tg.InputPeerSelf{}
		}
		body := "telesrv-lifecycle/" + p.id + "/" + c.label + " 🌕 quote"
		request := &tg.MessagesSendMediaRequest{Peer: peer, Media: p.mediaUpload(c.source, c.kind, richRandom()), Message: body, RandomID: richRandom()}
		request.SetEntities([]tg.MessageEntityClass{&tg.MessageEntityBold{Offset: len(utf16.Encode([]rune(body))) - 5, Length: 5}})
		source := p.send(c.source, c.label+"/source", request, c.saved, false)
		localSource := source.messages[c.replier.record.UserID]
		hash := p.download(c.source, c.label+"/source-download", source.messages[c.source.record.UserID].Media)
		var replies []externalMediaItem
		for _, manual := range []bool{false, true} {
			name := c.label + "/auto"
			if manual {
				name = c.label + "/manual"
			}
			to := tg.InputPeerClass(&tg.InputPeerSelf{})
			from := tg.InputPeerClass(p.peer(c.replier))
			if c.saved {
				to = p.peer(c.replier)
				from = &tg.InputPeerSelf{}
			}
			r := &tg.InputReplyToMessage{ReplyToMsgID: localSource.ID}
			r.SetReplyToPeerID(from)
			if manual {
				r.SetQuoteText("quote")
				r.SetQuoteOffset(len(utf16.Encode([]rune(body))) - 5)
				r.SetQuoteEntities([]tg.MessageEntityClass{&tg.MessageEntityBold{Length: 5}})
			}
			send := &tg.MessagesSendMessageRequest{Peer: to, Message: "telesrv-lifecycle/" + p.id + "/" + name, RandomID: richRandom()}
			send.SetReplyTo(r)
			item := p.send(c.replier, name, send, !c.saved, true)
			p.verify(name, localSource, c.replier.record.UserID, item, manual)
			replies = append(replies, item)
			for owner, m := range item.messages {
				wantHash[owner][m.ID] = hash
			}
		}
		for _, edit := range []bool{true, false} {
			p.mutate(source, edit)
			for i, item := range replies {
				fresh := item
				fresh.messages = map[int64]*tg.Message{}
				for owner, m := range item.messages {
					fresh.messages[owner] = p.mediaGet(p.primary[owner], m.ID)
				}
				p.verify(fmt.Sprintf("%s/after-edit-%t", item.label, edit), localSource, c.replier.record.UserID, fresh, i == 1)
				if !edit {
					for owner, m := range fresh.messages {
						if p.download(p.primary[owner], item.label+"/deleted-source-download", m.ReplyTo.(*tg.MessageReplyHeader).ReplyMedia) != hash {
							t.Fatal("reply media bytes changed after source deletion")
						}
					}
				}
			}
		}
		for _, item := range replies {
			p.replay(item)
		}
		p.facts(c.label)
	}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 1 {
			original := cursors[d.record.Index]
			p.start(d, &original)
			canonical := func(in []lifeUpdate) []string {
				out := make([]string, len(in))
				for i, v := range in {
					b, err := json.Marshal(v)
					p.require(err)
					out[i] = string(b)
				}
				sort.Strings(out)
				return out
			}
			if !reflect.DeepEqual(canonical(d.recovered), canonical(p.offlineWant[d.record.Index])) {
				p.record("external_recovery_mismatch", map[string]any{"device": d.record.Index, "got": d.recovered, "want": p.offlineWant[d.record.Index]})
				t.Fatal("original cursor recovered wrong media/state")
			}
			for _, v := range d.recovered {
				if v.Kind == "message" {
					if v.Reply == nil || p.download(d, "recovered-reply-download", v.Reply.ExternalMedia) != wantHash[d.record.UserID][v.ID] {
						t.Fatal("offline reply download differs")
					}
				}
			}
			var empty tg.UpdatesDifferenceBox
			p.mediaCall(d, "repeat-empty", &tg.UpdatesGetDifferenceRequest{Pts: d.cursor.Pts, Date: d.cursor.Date, Qts: d.cursor.Qts}, &empty, "ok")
			if _, ok := empty.Difference.(*tg.UpdatesDifferenceEmpty); !ok {
				t.Fatal("repeat difference not empty")
			}
			p.record("external_recovery_checked", map[string]any{"device": d.record.Index, "original_cursor": original, "final_cursor": d.cursor, "expected": p.offlineWant[d.record.Index]})
			p.cases = append(p.cases, map[string]any{"label": "original-cursor-recovery", "device": d.record.Index, "pass": true})
		}
	}
	final := p.checkpoint("external-final")
	total := 0
	for owner, d := range p.primary {
		total += final[d.record.Index].Pts - p.initial[owner]
	}
	if p.newMessages != 12 || total != 30 {
		t.Fatalf("message/PTS facts %d/%d", p.newMessages, total)
	}
	p.snapshot("final")
	p.facts("final")
	p.record("external_summary", map[string]any{"new_messages": p.newMessages, "pts": total, "download_bytes": p.downloadBytes, "cases": len(p.cases)})
}

func TestExternalMediaEvidenceRetainsNestedPayload(t *testing.T) {
	m := &tg.Message{ID: 9, Message: "reply"}
	header := &tg.MessageReplyHeader{}
	header.SetReplyMedia(&tg.MessageMediaDocument{Document: &tg.Document{ID: 9007199254740993, AccessHash: 17, FileReference: []byte{0, 128, 255}}})
	m.SetReplyTo(header)
	value := lifeMessage(m)
	if value.Reply == nil || value.Reply.ExternalMedia == nil {
		t.Fatal("external media omitted")
	}
	w, err := newBoundedEventWriter(filepath.Join(t.TempDir(), "events.ndjson"), EventLimits{LineBytes: 2 << 20, SegmentBytes: 16 << 20, TotalBytes: 128 << 20})
	if err != nil {
		t.Fatal(err)
	}
	w.write(map[string]any{"update": value, "difference": &tg.UpdatesDifference{NewMessages: []tg.MessageClass{m}}})
	if err := w.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(w.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"ID":9007199254740993`)) || !bytes.Contains(raw, []byte(`"FileReference":"AID/"`)) {
		t.Fatal("nested wire ID/reference lost")
	}
}
