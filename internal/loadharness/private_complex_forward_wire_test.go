//go:build darwin || linux

package loadharness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type complexResult struct {
	request  bin.Encoder
	messages map[int64]*tg.Message
}

func TestComplexCaseEvidencePreservesOwnedMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	w, err := newBoundedEventWriter(path, EventLimits{LineBytes: 2 << 20, SegmentBytes: 16 << 20, TotalBytes: 128 << 20})
	if err != nil {
		t.Fatal(err)
	}
	p := &privateLifecycleProbe{events: w, epoch: time.Now()}
	p.record("complex_case", map[string]any{"messages": map[int64]*tg.Message{9007199254740993: {ID: 7, Message: "🌕 quote", PeerID: &tg.PeerUser{UserID: 9007199254740993}}}, "delta": map[int64]int{9007199254740993: 1}})
	if err := w.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"9007199254740993"`) || !strings.Contains(string(b), `"UserID":9007199254740993`) || !strings.Contains(string(b), "🌕 quote") {
		t.Fatal("owner identity or full payload lost")
	}
}

func TestRealPrivateComplexForward(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_COMPLEX_FORWARD") != "1" {
		t.Skip("requires owned isolated complex-forward fixture")
	}
	p := newPrivateLifecycleProbe(t, "complex reply/forward baseline; preserve unexpected successful commits, no capacity acceptance")
	primary := map[int64]*lifeDevice{}
	for _, d := range p.devices {
		if d.record.DeviceIndex == 0 {
			primary[d.record.UserID] = d
		}
	}
	a, b := p.devices[0], p.devices[1]
	if a.record.DeviceIndex != 0 || b.record.DeviceIndex != 0 || len(primary) != 2 {
		t.Fatal("requires owned primary pair")
	}
	var resume struct {
		Verified   bool          `json:"verified"`
		Run        string        `json:"run_id"`
		Owner      int64         `json:"owner"`
		ID         int           `json:"id"`
		Body       string        `json:"body"`
		InitialPTS map[int64]int `json:"initial_pts"`
	}
	if path := os.Getenv("TELESRV_LOAD_COMPLEX_RESUME"); path != "" {
		raw, err := os.ReadFile(path)
		p.require(err)
		p.require(json.Unmarshal(raw, &resume))
		if !resume.Verified || !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(resume.Run) || resume.Owner != a.record.UserID || resume.ID <= 0 || !strings.HasPrefix(resume.Body, "telesrv-lifecycle/"+resume.Run+"/") || !reflect.DeepEqual(resume.InitialPTS, p.initial) {
			t.Fatal("resume proof or PTS mismatch")
		}
		p.record("resumed_source", map[string]any{"new_connection_run": p.id, "source_run": resume.Run, "owner": resume.Owner, "id": resume.ID, "body": resume.Body, "pts": resume.InitialPTS})
		p.id = resume.Run
		p.snapshot("resumed-source-before")
	}
	initial := p.checkpoint("complex-start")
	failures, newMessages := 0, 0
	write := func(caller *lifeDevice, label string, request bin.Encoder, saved, fresh bool, wantClass string, verify func(map[int64]*tg.Message) bool) complexResult {
		before := p.checkpoint(label + "/before")
		mark, start := p.mark(), time.Now()
		p.record("complex_intent", map[string]any{"label": label, "device": caller.record.Index, "request": request, "fresh": fresh, "saved": saved, "expected_class": wantClass})
		ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
		var out tg.UpdatesBox
		err := caller.client.Invoke(ctx, request, &out)
		cancel()
		class := classifyError(err)
		if e, ok := tgerr.As(err); ok {
			class = e.Type
		}
		after := p.checkpoint(label + "/after")
		delta := map[int64]int{}
		for owner, d := range primary {
			delta[owner] = after[d.record.Index].Pts - before[d.record.Index].Pts
		}
		wantLive := map[int][]lifeUpdate{}
		result := complexResult{request: request, messages: map[int64]*tg.Message{}}
		if class == "ok" {
			var id int
			for _, v := range lifeUpdates(out.Updates) {
				if v.Kind == "new" || v.Kind == "sent" {
					if id != 0 {
						t.Fatal("single-message action returned multiple messages")
					}
					id = v.ID
				}
			}
			if id == 0 {
				t.Fatal("successful send/forward has no ID")
			}
			m := p.mediaGet(caller, id)
			result.messages[caller.record.UserID] = m
			if fresh {
				newMessages++
				if newMessages > 12 {
					t.Fatal("complex fixture message budget")
				}
			}
			if !saved && fresh {
				other := primary[p.peer(caller).UserID]
				var h tg.MessagesMessagesBox
				p.mediaCall(other, label+"/recipient", &tg.MessagesGetHistoryRequest{Peer: p.peer(other), Limit: 1}, &h, "ok")
				r := mediaMessage(h.Messages)
				if r == nil || r.Message != m.Message {
					t.Fatal("recipient does not match committed message")
				}
				result.messages[other.record.UserID] = r
			}
			for owner, d := range primary {
				want := 0
				if fresh && (owner == caller.record.UserID || !saved) {
					want = 1
				}
				if delta[owner] != want {
					t.Fatalf("%s actual commit PTS delta=%v", label, delta)
				}
				if want == 0 {
					continue
				}
				msg := result.messages[owner]
				for _, device := range p.devices {
					if device.record.UserID == owner && device != caller {
						v := lifeMessage(msg)
						v.Kind = "new"
						v.Pts = after[d.record.Index].Pts
						v.Count = 1
						wantLive[device.record.Index] = []lifeUpdate{v}
					}
				}
			}
		} else {
			for _, n := range delta {
				if n != 0 {
					t.Fatal("unreconciled rejected action committed PTS")
				}
			}
		}
		p.checkLive(mark, start, wantLive, label)
		pass := class == wantClass
		if class == "ok" && wantClass == "ok" && verify != nil {
			pass = pass && verify(result.messages)
		}
		if !pass {
			failures++
		}
		p.record("complex_case", map[string]any{"label": label, "device": caller.record.Index, "generation": caller.generation, "request": request, "response": out.Updates, "messages": result.messages, "class": class, "expected_class": wantClass, "saved": saved, "fresh": fresh, "delta": delta, "pass": pass, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds()})
		p.cases = append(p.cases, map[string]any{"label": label, "pass": pass})
		p.snapshot(label)
		return result
	}
	body := func(label string) string { return "telesrv-lifecycle/" + p.id + "/" + label + " 🌕 quote" }
	send := func(peer tg.InputPeerClass, text string) *tg.MessagesSendMessageRequest {
		return &tg.MessagesSendMessageRequest{Peer: peer, Message: text, RandomID: richRandom()}
	}
	quote := func(m *tg.Message, peer tg.InputPeerClass) *tg.InputReplyToMessage {
		r := &tg.InputReplyToMessage{ReplyToMsgID: m.ID}
		if peer != nil {
			r.SetReplyToPeerID(peer)
		}
		r.SetQuoteText("quote")
		r.SetQuoteOffset(len(utf16.Encode([]rune(m.Message[:strings.Index(m.Message, "quote")]))))
		r.SetQuoteEntities([]tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 5}})
		return r
	}
	var seed complexResult
	if resume.Verified {
		m := p.mediaGet(a, resume.ID)
		if m.Message != resume.Body || lifePeer(m.PeerID) != resume.Owner || !m.Out {
			t.Fatal("resumed source changed")
		}
		seed = complexResult{messages: map[int64]*tg.Message{resume.Owner: m}}
		p.cases = append(p.cases, map[string]any{"label": "saved-source-resumed", "pass": true})
	} else {
		seed = write(a, "saved-source", send(&tg.InputPeerSelf{}, body("saved-source")), true, true, "ok", nil)
	}
	protectedReq := send(&tg.InputPeerSelf{}, body("protected-source"))
	protectedReq.Noforwards = true
	protected := write(a, "protected-source", protectedReq, true, true, "ok", nil)
	target := write(b, "target-source", send(p.peer(b), body("target-source")), false, true, "ok", nil)
	seedA := seed.messages[a.record.UserID]
	crossReq := send(p.peer(a), body("cross-reply"))
	crossReq.SetReplyTo(quote(seedA, &tg.InputPeerSelf{}))
	cross := write(a, "cross-private-reply", crossReq, false, true, "ok", func(ms map[int64]*tg.Message) bool {
		for owner, m := range ms {
			r, ok := m.ReplyTo.(*tg.MessageReplyHeader)
			if !ok {
				return false
			}
			f, hasFrom := r.GetReplyFrom()
			if !hasFrom || lifePeer(f.FromID) != a.record.UserID || f.Date != seedA.Date || !r.Quote || r.QuoteText != "quote" || r.QuoteOffset != crossReq.ReplyTo.(*tg.InputReplyToMessage).QuoteOffset {
				return false
			}
			_, hasID := r.GetReplyToMsgID()
			if owner != a.record.UserID && hasID {
				return false
			}
			if owner == a.record.UserID && (!hasID || r.ReplyToMsgID != seedA.ID) {
				return false
			}
		}
		return true
	})
	bad := send(p.peer(a), body("protected-cross"))
	bad.SetReplyTo(quote(protected.messages[a.record.UserID], &tg.InputPeerSelf{}))
	write(a, "protected-cross-rejected", bad, false, true, "CHAT_FORWARDS_RESTRICTED", nil)
	fwdReq := &tg.MessagesForwardMessagesRequest{FromPeer: p.peer(a), ToPeer: &tg.InputPeerSelf{}, ID: []int{cross.messages[a.record.UserID].ID}, RandomID: []int64{richRandom()}}
	write(a, "forward-reply-source-without-target", fwdReq, true, true, "ok", func(ms map[int64]*tg.Message) bool {
		m := ms[a.record.UserID]
		return m.ReplyTo == nil && m.Message == crossReq.Message
	})
	targetReq := &tg.MessagesForwardMessagesRequest{FromPeer: &tg.InputPeerSelf{}, ToPeer: p.peer(a), ID: []int{seedA.ID}, RandomID: []int64{richRandom()}}
	targetReq.SetReplyTo(quote(target.messages[a.record.UserID], nil))
	forwarded := write(a, "forward-explicit-target-reply", targetReq, false, true, "ok", func(ms map[int64]*tg.Message) bool {
		for owner, m := range ms {
			r, ok := m.ReplyTo.(*tg.MessageReplyHeader)
			if !ok || r.ReplyToMsgID != target.messages[owner].ID || r.QuoteText != "quote" {
				return false
			}
		}
		return true
	})
	// Delete only this run's newly created Saved source, preserving all older
	// fixture messages. Exact retries must resolve their committed output first.
	before := p.checkpoint("source-delete/before")
	mark, start := p.mark(), time.Now()
	var affected tg.MessagesAffectedMessages
	p.invoke(a, "source-delete", &tg.MessagesDeleteMessagesRequest{ID: []int{seedA.ID}}, &affected, "ok")
	if affected.Pts != before[a.record.Index].Pts+1 || affected.PtsCount != 1 {
		t.Fatal("Saved delete PTS")
	}
	want := map[int][]lifeUpdate{}
	for _, d := range p.devices {
		if d.record.UserID == a.record.UserID && d != a {
			want[d.record.Index] = []lifeUpdate{{Kind: "delete", IDs: []int{seedA.ID}, Pts: affected.Pts, Count: 1}}
		}
	}
	p.checkLive(mark, start, want, "source-delete")
	p.checkPTS(before, map[int64]int{a.record.UserID: 1}, "source-delete/after")
	p.snapshot("source-delete")
	p.cases = append(p.cases, map[string]any{"label": "source-delete", "pass": true})
	write(a, "forward-exact-replay-after-source-delete", targetReq, false, false, "ok", func(ms map[int64]*tg.Message) bool {
		return reflect.DeepEqual(lifeMessage(ms[a.record.UserID]), lifeMessage(forwarded.messages[a.record.UserID]))
	})
	freshForward := *targetReq
	freshForward.RandomID = []int64{richRandom()}
	write(a, "forward-deleted-source-rejected", &freshForward, false, true, "MESSAGE_ID_INVALID", nil)
	freshReply := send(p.peer(a), body("deleted-reply"))
	freshReply.SetReplyTo(quote(seedA, &tg.InputPeerSelf{}))
	write(a, "reply-deleted-source-rejected", freshReply, false, true, "REPLY_MESSAGE_ID_INVALID", nil)
	badHash := send(&tg.InputPeerSelf{}, body("bad-hash"))
	wrong := *p.peer(a)
	wrong.AccessHash ^= 1
	badHash.SetReplyTo(quote(target.messages[a.record.UserID], &wrong))
	write(a, "reply-wrong-source-hash-rejected", badHash, true, true, "REPLY_MESSAGE_ID_INVALID", nil)
	write(a, "reply-exact-replay-after-source-delete", crossReq, false, false, "ok", func(ms map[int64]*tg.Message) bool {
		return reflect.DeepEqual(lifeMessage(ms[a.record.UserID]), lifeMessage(cross.messages[a.record.UserID]))
	})
	final := p.checkpoint("complex-final")
	total := 0
	for owner, d := range primary {
		n := final[d.record.Index].Pts - initial[d.record.Index].Pts
		total += n
		if n < 0 {
			t.Fatal("PTS regression")
		}
		_ = owner
	}
	if total > 24 {
		t.Fatal("PTS budget exceeded")
	}
	p.snapshot("final")
	p.record("complex_summary", map[string]any{"cases": len(p.cases), "failures": failures, "new_messages": newMessages, "pts_delta": total, "source_id": seedA.ID, "source_owner": a.record.UserID, "partial_failure_not_run": true})
	if failures != 0 {
		t.Errorf("complex reply/forward mismatches: %d; unexpected commits preserved", failures)
	}
}
