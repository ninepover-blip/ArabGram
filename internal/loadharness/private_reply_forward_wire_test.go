//go:build darwin || linux

package loadharness

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

func TestRichRequestEvidenceSurvivesBoundedWriter(t *testing.T) {
	reply := &tg.InputReplyToMessage{ReplyToMsgID: 17}
	reply.SetQuoteText("seed")
	send := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerSelf{}, RandomID: 9223372036854775806, Message: "test", Silent: true}
	send.SetReplyTo(reply)
	for _, tc := range []struct {
		name string
		req  bin.Encoder
		key  string
		want any
	}{
		{"reply", send, "Silent", true},
		{"forward", &tg.MessagesForwardMessagesRequest{FromPeer: &tg.InputPeerSelf{}, ToPeer: &tg.InputPeerSelf{}, ID: []int{17, 4}, RandomID: []int64{12, 13}, DropAuthor: true}, "DropAuthor", true},
		{"history", &tg.MessagesGetHistoryRequest{Peer: &tg.InputPeerSelf{}, OffsetID: 17, AddOffset: -3, Limit: 6}, "AddOffset", float64(-3)},
		{"search", &tg.MessagesSearchRequest{Peer: &tg.InputPeerSelf{}, Q: "needle", Filter: &tg.InputMessagesFilterEmpty{}, OffsetID: 17, Limit: 3}, "Q", "needle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.ndjson")
			writer, err := newBoundedEventWriter(path, EventLimits{LineBytes: 2 << 20, SegmentBytes: 16 << 20, TotalBytes: 128 << 20})
			if err != nil {
				t.Fatal(err)
			}
			writer.write(map[string]any{"type": "rpc", "request": lifeRequest(tc.req)})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := writer.finalize(ctx); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var result struct {
				Request struct {
					Wire map[string]any `json:"wire"`
				} `json:"request"`
			}
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			if result.Request.Wire[tc.key] != tc.want {
				t.Fatalf("request field lost in bounded evidence: %s", tc.key)
			}
			if tc.name == "reply" {
				if result.Request.Wire["ReplyTo"].(map[string]any)["QuoteText"] != "seed" || !strings.Contains(string(data), `"RandomID":9223372036854775806`) {
					t.Fatal("reply metadata or exact int64 random ID lost")
				}
			}
		})
	}
}

type lifeEntity struct {
	Kind   string `json:"kind"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}
type lifeReply struct {
	ExternalMedia tg.MessageMediaClass `json:"external_media,omitempty"`
	ExternalFrom  *lifeForward         `json:"external_from,omitempty"`
	Kind          string               `json:"kind"`
	ID            int                  `json:"id"`
	Peer          int64                `json:"peer"`
	Top           int                  `json:"top"`
	Quoted        bool                 `json:"quoted"`
	Text          string               `json:"text"`
	Offset        int                  `json:"offset"`
	Entities      []lifeEntity         `json:"entities"`
	HasID         bool                 `json:"has_id"`
	HasText       bool                 `json:"has_text"`
	HasOffset     bool                 `json:"has_offset"`
	HasEntities   bool                 `json:"has_entities"`
}
type lifeForward struct {
	From      int64  `json:"from"`
	Name      string `json:"name"`
	Date      int    `json:"date"`
	SavedPeer int64  `json:"saved_peer"`
	SavedID   int    `json:"saved_id"`
}

func lifeEntities(in []tg.MessageEntityClass) (out []lifeEntity) {
	for _, e := range in {
		switch v := e.(type) {
		case *tg.MessageEntityBold:
			out = append(out, lifeEntity{"bold", v.Offset, v.Length})
		default:
			out = append(out, lifeEntity{Kind: fmt.Sprintf("%T", e)})
		}
	}
	return
}
func lifeReplyHeader(in tg.MessageReplyHeaderClass) *lifeReply {
	if in == nil {
		return nil
	}
	v, ok := in.(*tg.MessageReplyHeader)
	if !ok {
		return &lifeReply{Kind: fmt.Sprintf("%T", in)}
	}
	_, hi := v.GetReplyToMsgID()
	_, ht := v.GetQuoteText()
	_, ho := v.GetQuoteOffset()
	_, he := v.GetQuoteEntities()
	result := &lifeReply{Kind: "message", ID: v.ReplyToMsgID, Peer: lifePeer(v.ReplyToPeerID), Top: v.ReplyToTopID, Quoted: v.Quote, Text: v.QuoteText, Offset: v.QuoteOffset, Entities: lifeEntities(v.QuoteEntities), HasID: hi, HasText: ht, HasOffset: ho, HasEntities: he}
	result.ExternalMedia, _ = v.GetReplyMedia()
	if f, ok := v.GetReplyFrom(); ok {
		result.ExternalFrom = &lifeForward{lifePeer(f.FromID), f.FromName, f.Date, lifePeer(f.SavedFromPeer), f.SavedFromMsgID}
	}
	return result
}
func lifeForwardHeader(m *tg.Message) *lifeForward {
	v, ok := m.GetFwdFrom()
	if !ok {
		return nil
	}
	return &lifeForward{lifePeer(v.FromID), v.FromName, v.Date, lifePeer(v.SavedFromPeer), v.SavedFromMsgID}
}

type richItem struct {
	body              string
	from              int64
	silent, protected bool
	reply             *richItem
	forward           *lifeForward
	random            int64
	values            map[int64]lifeUpdate
	pts               map[int64]int
	dates             map[int64]int
}
type richProbe struct {
	*privateLifecycleProbe
	primary map[int64]*lifeDevice
	all     []*richItem
}

// Published td v1.3.2 dispatches separate received frames concurrently. Compare
// callback membership exactly; raw observation order remains in the evidence.
// This does not certify physical frame ordering on the server or client.
func richCompareLive(obs []lifeObservation, want map[int][]lifeUpdate, startNS int64) (int64, []int, error) {
	got := map[int][]lifeUpdate{}
	reordered := map[int]bool{}
	var maxNS int64
	for _, o := range obs {
		lag := o.RelativeNS - startNS
		if lag < 0 || lag > int64(time.Second) {
			return 0, nil, fmt.Errorf("live callback outside [0,1s]: %d", lag)
		}
		maxNS = max(maxNS, lag)
		previous := got[o.Device]
		if len(previous) > 0 && o.Update.Pts < previous[len(previous)-1].Pts {
			reordered[o.Device] = true
		}
		got[o.Device] = append(previous, o.Update)
	}
	canonical := func(in map[int][]lifeUpdate) map[int][]lifeUpdate {
		out := map[int][]lifeUpdate{}
		for d, values := range in {
			out[d] = append([]lifeUpdate(nil), values...)
			sort.Slice(out[d], func(i, j int) bool {
				a, b := out[d][i], out[d][j]
				if a.Pts != b.Pts {
					return a.Pts < b.Pts
				}
				return a.ID < b.ID
			})
		}
		return out
	}
	if !reflect.DeepEqual(canonical(got), canonical(want)) {
		return 0, nil, fmt.Errorf("live callback membership mismatch: got %+v want %+v", got, want)
	}
	var devices []int
	for d := range reordered {
		devices = append(devices, d)
	}
	sort.Ints(devices)
	return maxNS, devices, nil
}

func (p *richProbe) checkLive(mark int, start time.Time, want map[int][]lifeUpdate, label string) {
	p.t.Helper()
	p.pause(max(0, 2*time.Second-time.Since(start)))
	maxNS, reordered, err := richCompareLive(p.liveSince(mark), want, start.Sub(p.epoch).Nanoseconds())
	p.require(err)
	p.record("live_checked", map[string]any{"label": label, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "expected": want, "max_latency_ns": maxNS, "window_ns": time.Since(start).Nanoseconds(), "callback_reordered_devices": reordered})
}

func TestRichConcurrentCallbackMembership(t *testing.T) {
	a := lifeUpdate{Kind: "new", ID: 10, Pts: 20, Count: 1}
	b := lifeUpdate{Kind: "new", ID: 11, Pts: 21, Count: 1}
	want := map[int][]lifeUpdate{2: {a, b}}
	obs := []lifeObservation{{Device: 2, RelativeNS: 200, Update: b}, {Device: 2, RelativeNS: 300, Update: a}}
	maxNS, devices, err := richCompareLive(obs, want, 100)
	if err != nil || maxNS != 200 || !reflect.DeepEqual(devices, []int{2}) || !reflect.DeepEqual(obs[0].Update, b) {
		t.Fatalf("concurrent callback comparison changed raw order: %v %v %v", maxNS, devices, err)
	}
	for _, name := range []string{"missing", "duplicate", "wrong-device", "wrong-id", "wrong-pts", "late"} {
		t.Run(name, func(t *testing.T) {
			bad := append([]lifeObservation(nil), obs...)
			switch name {
			case "missing":
				bad = bad[:1]
			case "duplicate":
				bad = append(bad, bad[0])
			case "wrong-device":
				bad[0].Device++
			case "wrong-id":
				bad[0].Update.ID++
			case "wrong-pts":
				bad[0].Update.Pts++
			case "late":
				bad[0].RelativeNS = int64(time.Second) + 101
			}
			if _, _, err := richCompareLive(bad, want, 100); err == nil {
				t.Fatal("invalid callback evidence accepted")
			}
		})
	}
}

func richRandom() int64 {
	var b [8]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return int64(binary.LittleEndian.Uint64(b[:]) & math.MaxInt64)
}
func (p *richProbe) item(caller *lifeDevice, label string) *richItem {
	return &richItem{body: "telesrv-lifecycle/" + p.id + "/" + label, from: caller.record.UserID, random: richRandom(), values: map[int64]lifeUpdate{}, pts: map[int64]int{}, dates: map[int64]int{}}
}
func (p *richProbe) messages(d *lifeDevice, label string, req bin.Encoder) ([]*tg.Message, tg.MessagesMessagesClass) {
	var out tg.MessagesMessagesBox
	p.invoke(d, label, req, &out, "ok")
	var values []tg.MessageClass
	count := 0
	inexact := false
	offset := 0
	hasOffset := false
	switch v := out.Messages.(type) {
	case *tg.MessagesMessages:
		values = v.Messages
	case *tg.MessagesMessagesSlice:
		values = v.Messages
		count = v.Count
		inexact = v.Inexact
		offset, hasOffset = v.GetOffsetIDOffset()
	default:
		p.t.Fatalf("%s unexpected container %T", label, out.Messages)
	}
	result := make([]*tg.Message, 0, len(values))
	for _, v := range values {
		m, ok := v.(*tg.Message)
		if !ok {
			p.t.Fatalf("%s nonmessage %T", label, v)
		}
		result = append(result, m)
	}
	p.record("page", map[string]any{"label": label, "device": d.record.Index, "wire_type": fmt.Sprintf("%T", out.Messages), "count": count, "inexact": inexact, "offset": offset, "has_offset": hasOffset, "messages": lifeMessages(out.Messages)})
	return result, out.Messages
}
func (p *richProbe) expected(item *richItem, owner int64, id int) lifeUpdate {
	v := lifeUpdate{Kind: "message", ID: id, Peer: p.peer(p.primary[owner]).UserID, From: item.from, Text: item.body, Out: owner == item.from, Silent: item.silent, NoForwards: item.protected, Forward: item.forward}
	if item.reply != nil {
		v.Reply = &lifeReply{Kind: "message", ID: item.reply.values[owner].ID, Quoted: true, Text: "seed", Offset: strings.Index(item.reply.body, "seed"), Entities: []lifeEntity{{"bold", 0, 4}}, HasID: true, HasText: true, HasOffset: true, HasEntities: true}
	}
	return v
}
func (p *richProbe) write(caller *lifeDevice, label string, req bin.Encoder, items []*richItem, fresh []bool) {
	p.t.Helper()
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.UpdatesBox
	p.invoke(caller, label, req, &out, "ok")
	got := lifeUpdates(out.Updates)
	if len(got) != len(items) {
		p.t.Fatalf("%s echo length %d != %d", label, len(got), len(items))
	}
	n := 0
	for _, b := range fresh {
		if b {
			n++
		}
	}
	histories := map[int64][]*tg.Message{}
	if n > 0 {
		for owner, d := range p.primary {
			ms, _ := p.messages(d, label+"/owner_ids", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), Limit: n})
			if len(ms) != n {
				p.t.Fatal("new owner ID count")
			}
			histories[owner] = ms
		}
	}
	want := map[int][]lifeUpdate{}
	delta := map[int64]int{}
	newIndex := 0
	for i, item := range items {
		if fresh[i] {
			for owner, d := range p.primary {
				m := histories[owner][n-newIndex-1]
				v := p.expected(item, owner, m.ID)
				if !reflect.DeepEqual(lifeMessage(m), v) {
					p.t.Fatalf("%s metadata owner %d got %+v want %+v", label, owner, lifeMessage(m), v)
				}
				item.values[owner] = v
				item.dates[owner] = m.Date
				item.pts[owner] = before[d.record.Index].Pts + newIndex + 1
				delta[owner] = n
			}
			if item.values[caller.record.UserID].ID == item.values[p.peer(caller).UserID].ID {
				p.t.Fatal("fixture lacks distinct owner-local IDs")
			}
			p.all = append(p.all, item)
			newIndex++
		}
		echo := got[i]
		v := item.values[caller.record.UserID]
		if echo.Kind == "sent" {
			if echo.ID != v.ID || echo.Pts != item.pts[caller.record.UserID] || echo.Count != 1 {
				p.t.Fatal("short echo ID/PTS mismatch")
			}
		} else {
			v.Kind = "new"
			v.Pts = item.pts[caller.record.UserID]
			v.Count = 1
			if !reflect.DeepEqual(echo, v) {
				p.t.Fatalf("%s echo got %+v want %+v", label, echo, v)
			}
		}
		if fresh[i] {
			for _, d := range p.devices {
				if d == caller || d.done == nil {
					continue
				}
				v := item.values[d.record.UserID]
				v.Kind = "new"
				v.Pts = item.pts[d.record.UserID]
				v.Count = 1
				want[d.record.Index] = append(want[d.record.Index], v)
			}
		}
	}
	if r, ok := req.(*tg.MessagesForwardMessagesRequest); ok {
		updates, ok := out.Updates.(*tg.Updates)
		if !ok || len(updates.Updates) != 2*len(items) {
			p.t.Fatal("forward mapping envelope")
		}
		mappings := make([]map[string]any, 0, len(items))
		for i, item := range items {
			id, ok := updates.Updates[2*i].(*tg.UpdateMessageID)
			if !ok || id.RandomID != r.RandomID[i] || id.ID != item.values[caller.record.UserID].ID {
				p.t.Fatal("forward random-ID mapping/order")
			}
			mappings = append(mappings, map[string]any{"random_id": id.RandomID, "id": id.ID, "fresh": fresh[i]})
		}
		p.record("forward_mapping", map[string]any{"label": label, "device": caller.record.Index, "mappings": mappings})
	}
	p.checkLive(mark, start, want, label)
	p.checkPTS(before, delta, label+"/after")
	records := make([]map[string]any, 0, len(items))
	for i, item := range items {
		owners := map[string]any{}
		for owner, v := range item.values {
			owners[fmt.Sprint(owner)] = map[string]any{"message": v, "pts": item.pts[owner], "date": item.dates[owner]}
		}
		records = append(records, map[string]any{"random_id": item.random, "fresh": fresh[i], "owners": owners})
	}
	p.record("rich_write_checked", map[string]any{"label": label, "caller": caller.record.Index, "items": records})
	p.snapshot(label)
	p.cases = append(p.cases, map[string]any{"label": label, "kind": "write", "new_messages": n})
}
func (p *richProbe) send(caller *lifeDevice, label string, item *richItem) *tg.MessagesSendMessageRequest {
	req := &tg.MessagesSendMessageRequest{Peer: p.peer(caller), RandomID: item.random, Message: item.body, Silent: item.silent, Noforwards: item.protected}
	if item.reply != nil {
		r := &tg.InputReplyToMessage{ReplyToMsgID: item.reply.values[caller.record.UserID].ID}
		r.SetQuoteText("seed")
		r.SetQuoteOffset(strings.Index(item.reply.body, "seed"))
		r.SetQuoteEntities([]tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 4}})
		req.SetReplyTo(r)
	}
	p.write(caller, label, req, []*richItem{item}, []bool{true})
	return req
}
func (p *richProbe) reject(caller *lifeDevice, label string, req bin.Encoder, want string) {
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	var out tg.UpdatesBox
	p.invoke(caller, label, req, &out, want)
	p.checkLive(mark, start, map[int][]lifeUpdate{}, label)
	p.checkPTS(before, nil, label+"/after")
	p.snapshot(label)
	p.cases = append(p.cases, map[string]any{"label": label, "kind": "reject", "error": want})
}
func (p *richProbe) mode(mode string, a, b *lifeDevice) {
	var offline []*lifeDevice
	cursors := map[int]tg.UpdatesState{}
	startIndex := len(p.all)
	if mode == "offline" {
		for _, d := range p.devices {
			if d.record.DeviceIndex == 1 {
				cursors[d.record.Index] = d.cursor
				offline = append(offline, d)
				p.require(p.stop(d))
			}
		}
	}
	seedA := p.item(a, mode+"/seed-A")
	p.send(a, mode+"/seed-A", seedA)
	seedB := p.item(b, mode+"/seed-B")
	p.send(b, mode+"/seed-B", seedB)
	replyA := p.item(a, mode+"/reply-A")
	replyA.reply = seedB
	replyA.silent = true
	reqReply := p.send(a, mode+"/reply-A", replyA)
	p.write(a, mode+"/reply-replay", reqReply, []*richItem{replyA}, []bool{false})
	changed := *reqReply
	changed.Message += "/changed"
	p.reject(a, mode+"/reply-collision", &changed, "RANDOM_ID_DUPLICATE")
	replyB := p.item(b, mode+"/reply-B-protected")
	replyB.reply = seedA
	replyB.protected = true
	p.send(b, mode+"/reply-B-protected", replyB)
	f1, f2 := p.item(a, "unused-1"), p.item(a, "unused-2")
	for i, x := range []*richItem{f1, f2} {
		src := []*richItem{seedB, seedA}[i]
		x.body = src.body
		x.forward = &lifeForward{From: src.from, Date: src.dates[a.record.UserID]}
	}
	forward := &tg.MessagesForwardMessagesRequest{FromPeer: p.peer(a), ToPeer: p.peer(a), ID: []int{seedB.values[a.record.UserID].ID, seedA.values[a.record.UserID].ID}, RandomID: []int64{f1.random, f2.random}}
	p.write(a, mode+"/forward-batch", forward, []*richItem{f1, f2}, []bool{true, true})
	p.write(a, mode+"/forward-replay", forward, []*richItem{f1, f2}, []bool{false, false})
	f3 := p.item(a, "unused-3")
	f3.body = seedA.body
	f3.forward = f2.forward
	mixed := *forward
	mixed.RandomID = []int64{f1.random, f3.random}
	p.write(a, mode+"/forward-mixed", &mixed, []*richItem{f1, f3}, []bool{false, true})
	drop := p.item(b, "unused-drop")
	drop.body = seedA.body
	dreq := &tg.MessagesForwardMessagesRequest{FromPeer: p.peer(b), ToPeer: p.peer(b), ID: []int{seedA.values[b.record.UserID].ID}, RandomID: []int64{drop.random}, DropAuthor: true}
	p.write(b, mode+"/forward-drop-author", dreq, []*richItem{drop}, []bool{true})
	protected := &tg.MessagesForwardMessagesRequest{FromPeer: p.peer(a), ToPeer: p.peer(a), ID: []int{replyB.values[a.record.UserID].ID}, RandomID: []int64{richRandom()}}
	p.reject(a, mode+"/forward-protected", protected, "CHAT_FORWARDS_RESTRICTED")
	invalid := &tg.MessagesSendMessageRequest{Peer: p.peer(a), Message: p.item(a, "invalid").body, RandomID: richRandom()}
	invalid.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: math.MaxInt32})
	p.reject(a, mode+"/reply-missing", invalid, "REPLY_MESSAGE_ID_INVALID")
	wrong := *forward
	wrong.FromPeer = &tg.InputPeerSelf{}
	wrong.RandomID = []int64{richRandom(), richRandom()}
	p.reject(a, mode+"/forward-wrong-dialog", &wrong, "MESSAGE_ID_INVALID")
	dup := *forward
	rid := richRandom()
	dup.RandomID = []int64{rid, rid}
	p.reject(a, mode+"/forward-duplicate-random", &dup, "RANDOM_ID_DUPLICATE")
	mismatch := *forward
	mismatch.RandomID = []int64{richRandom()}
	p.reject(a, mode+"/forward-unpaired", &mismatch, "INPUT_REQUEST_INVALID")
	for _, d := range offline {
		saved := cursors[d.record.Index]
		p.start(d, &saved)
		want := map[int]lifeUpdate{}
		for _, item := range p.all[startIndex:] {
			v := item.values[d.record.UserID]
			want[v.ID] = v
		}
		got := map[int]lifeUpdate{}
		for _, v := range d.recovered {
			if v.Kind != "message" {
				p.t.Fatalf("unexpected recovery kind %s", v.Kind)
			}
			if _, ok := got[v.ID]; ok {
				p.t.Fatal("duplicate difference message")
			}
			got[v.ID] = v
		}
		if !reflect.DeepEqual(got, want) || d.cursor.Pts != saved.Pts+len(want) {
			p.t.Fatalf("offline metadata/cursor mismatch device %d got %+v want %+v", d.record.Index, got, want)
		}
		p.record("rich_recovery_checked", map[string]any{"device": d.record.Index, "from": saved, "next": d.cursor, "messages": len(want)})
	}
	p.checkpoint(mode + "/complete")
}
func (p *richProbe) pages(d *lifeDevice) {
	owner := d.record.UserID
	all := make([]lifeUpdate, 0, len(p.all))
	for _, item := range p.all {
		all = append(all, item.values[owner])
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID > all[j].ID })
	low, high := all[len(all)-1].ID-1, all[0].ID+1
	check := func(label string, req bin.Encoder, want []lifeUpdate, count int) {
		ms, container := p.messages(d, label, req)
		got := make([]lifeUpdate, 0, len(ms))
		for _, m := range ms {
			got = append(got, lifeMessage(m))
		}
		if !reflect.DeepEqual(got, want) {
			p.t.Fatalf("%s page mismatch got %+v want %+v", label, got, want)
		}
		if count > len(want) {
			v, ok := container.(*tg.MessagesMessagesSlice)
			if !ok || v.Count != count || v.Inexact {
				p.t.Fatalf("%s slice/count mismatch: %T %+v", label, container, container)
			}
		} else {
			if _, ok := container.(*tg.MessagesMessages); !ok {
				p.t.Fatalf("%s expected full container", label)
			}
		}
		ids := make([]int, 0, len(want))
		for _, v := range want {
			ids = append(ids, v.ID)
		}
		p.record("page_checked", map[string]any{"label": label, "device": d.record.Index, "expected_ids": ids, "expected_count": count})
		p.cases = append(p.cases, map[string]any{"label": label, "kind": "page", "device": d.record.Index})
	}
	prefix := fmt.Sprintf("pages/%d/", d.record.Index)
	for _, search := range []bool{false, true} {
		kind := "history"
		if search {
			kind = "search"
		}
		cursor := 0
		for start := 0; start <= len(all); start += 3 {
			end := min(start+3, len(all))
			want := all[start:end]
			count := len(want)
			if end < len(all) {
				count++
			}
			if search && start == 0 {
				count = len(all)
			}
			if search {
				check(prefix+kind+fmt.Sprint(start), &tg.MessagesSearchRequest{Peer: p.peer(d), Q: p.id, Filter: &tg.InputMessagesFilterEmpty{}, OffsetID: cursor, Limit: 3, MinID: low, MaxID: high}, want, count)
			} else {
				check(prefix+kind+fmt.Sprint(start), &tg.MessagesGetHistoryRequest{Peer: p.peer(d), OffsetID: cursor, Limit: 3, MinID: low, MaxID: high}, want, count)
			}
			if end > start {
				cursor = want[len(want)-1].ID
			}
			if end == len(all) {
				break
			}
		}
	}
	check(prefix+"history-empty", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), OffsetID: all[len(all)-1].ID, Limit: 3, MinID: low}, []lifeUpdate{}, 0)
	check(prefix+"search-empty", &tg.MessagesSearchRequest{Peer: p.peer(d), Q: p.id + "-absent", Filter: &tg.InputMessagesFilterEmpty{}, Limit: 3, MinID: low}, []lifeUpdate{}, 0)
	check(prefix+"history-skip", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), AddOffset: 2, Limit: 3, MinID: low}, all[2:5], 4)
	anchor := all[7].ID
	check(prefix+"history-around", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), OffsetID: anchor, AddOffset: -2, Limit: 5, MinID: low, MaxID: high}, all[5:10], 5)
	check(prefix+"history-forward", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), OffsetID: anchor, AddOffset: -3, Limit: 3, MinID: low, MaxID: high}, all[4:7], 3)
	check(prefix+"history-exclusive", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), MinID: all[8].ID, MaxID: all[2].ID, Limit: 20}, all[3:8], 5)
	check(prefix+"history-limit0", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), MinID: low, Limit: 0}, all, len(all))
	check(prefix+"history-limit500", &tg.MessagesGetHistoryRequest{Peer: p.peer(d), MinID: low, Limit: 500}, all, len(all))
	check(prefix+"search-empty-query", &tg.MessagesSearchRequest{Peer: p.peer(d), Filter: &tg.InputMessagesFilterEmpty{}, MinID: low, MaxID: high, Limit: 500}, all, len(all))
	check(prefix+"search-limit0", &tg.MessagesSearchRequest{Peer: p.peer(d), Q: p.id, Filter: &tg.InputMessagesFilterEmpty{}, MinID: low, Limit: 0}, all, len(all))
}
func TestRealPrivateReplyForward(t *testing.T) {
	base := newPrivateLifecycleProbe(t, "ordinary private reply/forward and bounded pagination; not capacity")
	p := &richProbe{privateLifecycleProbe: base, primary: map[int64]*lifeDevice{}}
	var a, b *lifeDevice
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			p.primary[d.record.UserID] = d
			if d.record.AccountIndex == 0 {
				a = d
			} else {
				b = d
			}
		}
	}
	if a == nil || b == nil {
		t.Fatal("missing primary devices")
	}
	// Establish an owner-local sequence offset through a real Saved Messages send.
	// Never edit counters directly or normalize the assertions to identical IDs.
	p.ownerOffset(a)
	p.mode("online", a, b)
	p.mode("offline", a, b)
	before := p.checkpoint("pagination/before")
	mark, start := p.mark(), time.Now()
	for _, d := range p.devices {
		p.pages(d)
	}
	p.checkLive(mark, start, map[int][]lifeUpdate{}, "pagination/read-only")
	p.checkPTS(before, nil, "pagination/after")
	p.snapshot("final")
	if len(p.all) != 16 {
		t.Fatalf("message count %d", len(p.all))
	}
}

func (p *richProbe) ownerOffset(a *lifeDevice) {
	label := "owner-offset"
	before := p.checkpoint(label + "/before")
	mark, start := p.mark(), time.Now()
	item := p.item(a, label)
	var out tg.UpdatesBox
	p.invoke(a, label, &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerSelf{}, Message: item.body, RandomID: item.random}, &out, "ok")
	echo := lifeUpdates(out.Updates)
	if len(echo) != 1 || echo[0].ID <= 0 || echo[0].Pts != before[a.record.Index].Pts+1 || echo[0].Count != 1 {
		p.t.Fatal("Saved Messages offset echo ID/PTS")
	}
	v := lifeUpdate{Kind: "message", ID: echo[0].ID, Peer: a.record.UserID, From: a.record.UserID, Text: item.body, Out: true}
	if got := p.fetch(a, v.ID, label+"/fetch"); !reflect.DeepEqual(got, v) {
		p.t.Fatalf("Saved Messages offset metadata got %+v want %+v", got, v)
	}
	want := map[int][]lifeUpdate{}
	v.Kind, v.Pts, v.Count = "new", echo[0].Pts, 1
	for _, d := range p.devices {
		if d != a && d.record.UserID == a.record.UserID {
			want[d.record.Index] = []lifeUpdate{v}
		}
	}
	p.checkLive(mark, start, want, label)
	p.checkPTS(before, map[int64]int{a.record.UserID: 1}, label+"/after")
	p.record("owner_offset_checked", map[string]any{"device": a.record.Index, "random_id": item.random, "message": v})
	p.snapshot(label)
}
