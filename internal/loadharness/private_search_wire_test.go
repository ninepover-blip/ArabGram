//go:build darwin || linux

package loadharness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type searchWireBox struct {
	Owner   int64  `json:"owner"`
	ID      int    `json:"id"`
	Peer    int64  `json:"peer"`
	From    int64  `json:"from"`
	Date    int    `json:"date"`
	Body    string `json:"body"`
	Deleted bool   `json:"deleted"`
	Pinned  bool   `json:"pinned"`
}

func (p *privateLifecycleProbe) searchSourceBoxes(source, label string) []searchWireBox {
	p.t.Helper()
	if !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(source) {
		p.t.Fatal("invalid immutable source marker")
	}
	var owners []int64
	for owner := range p.initial {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
	if len(owners) != 2 {
		p.t.Fatal("search requires the two owned fixture accounts")
	}
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	query := fmt.Sprintf(`BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms';
SELECT json_agg(row_to_json(b)) FROM (
 SELECT owner_user_id AS owner,box_id AS id,peer_id AS peer,from_user_id AS "from",
 message_date AS date,body,deleted,pinned FROM message_boxes
 WHERE owner_user_id IN (%d,%d) AND body LIKE 'telesrv-lifecycle/%s/%%'
 ORDER BY owner_user_id,box_id DESC
) b; COMMIT;`, owners[0], owners[1], source)
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	body, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", p.spec.Postgres.User, "-d", p.spec.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", query)
	p.require(err)
	var boxes []searchWireBox
	p.require(json.Unmarshal(body, &boxes))
	if len(boxes) != 33 {
		p.t.Fatalf("source boxes=%d, want preserved 33", len(boxes))
	}
	p.record("search_source", map[string]any{"label": label, "source_run_id": source, "boxes": boxes})
	return boxes
}

func TestRealPrivateSearchBoundaries(t *testing.T) {
	sourcePath := os.Getenv("TELESRV_LOAD_SEARCH_SOURCE_REPORT")
	if sourcePath == "" {
		t.Skip("requires a sealed prior marker report and isolated lifecycle fixture")
	}
	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	var prior struct {
		Pass bool   `json:"pass"`
		Run  string `json:"run_id"`
	}
	if err = json.Unmarshal(raw, &prior); err != nil || !prior.Pass || !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(prior.Run) {
		t.Fatal("source report must identify an existing successful marker run")
	}
	p := newPrivateLifecycleProbe(t, "read-only private search count/sender/input contract; no new messages or capacity acceptance")
	// checkpoint queries refresh d.cursor. Recovery must retain the exact cursor
	// obtained before any search, including date/qts/seq, rather than that refresh.
	originalCursors := make(map[int]tg.UpdatesState, len(p.devices))
	for _, d := range p.devices {
		originalCursors[d.record.Index] = d.cursor
	}
	boxes := p.searchSourceBoxes(prior.Run, "before")
	initial := p.checkpoint("search-before")
	mark := p.mark()
	failures, calls := 0, 0
	check := func(d *lifeDevice, label string, req bin.Encoder, wantIDs []int, wantCount int, wantError string, countOnly bool) {
		calls++
		if calls > 240 {
			t.Fatal("search read quota exceeded")
		}
		started := time.Now()
		ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
		var out tg.MessagesMessagesBox
		err := d.client.Invoke(ctx, req, &out)
		cancel()
		class := classifyError(err)
		if rpcErr, ok := tgerr.As(err); ok {
			class = rpcErr.Type
		}
		var ids []int
		count, users := 0, 0
		known := err != nil
		switch v := out.Messages.(type) {
		case *tg.MessagesMessages:
			known = true
			count, users = len(v.Messages), len(v.Users)
			for _, m := range v.Messages {
				ids = append(ids, lifeMessage(m).ID)
			}
		case *tg.MessagesMessagesSlice:
			known = true
			count, users = v.Count, len(v.Users)
			for _, m := range v.Messages {
				ids = append(ids, lifeMessage(m).ID)
			}
		}
		pass := known && class == wantError
		if wantError == "ok" {
			pass = pass && reflect.DeepEqual(append([]int{}, ids...), append([]int{}, wantIDs...)) && count == wantCount && (!countOnly || users == 0)
		}
		p.record("search_case", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation,
			"source_run_id": prior.Run, "method": fmt.Sprintf("%T", req), "request": req,
			"started_at": started.UTC(), "started_relative_ns": started.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(started).Nanoseconds(),
			"class": class, "response_type": fmt.Sprintf("%T", out.Messages), "response": out.Messages,
			"ids": ids, "count": count, "users": users, "expected_ids": wantIDs, "expected_count": wantCount,
			"expected_error": wantError, "count_only": countOnly, "pass": pass})
		p.cases = append(p.cases, map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "pass": pass})
		if !pass {
			failures++
		}
		p.pause(20 * time.Millisecond)
	}
	for _, d := range p.devices {
		peer := p.peer(d)
		var all, self, other, global []int
		for _, b := range boxes {
			if b.Owner != d.record.UserID || b.Deleted {
				continue
			}
			global = append(global, b.ID)
			if b.Peer != peer.UserID {
				continue
			}
			all = append(all, b.ID)
			if b.From == d.record.UserID {
				self = append(self, b.ID)
			} else if b.From == peer.UserID {
				other = append(other, b.ID)
			}
		}
		if len(all) != 16 || len(self) == 0 || len(other) == 0 {
			t.Fatal("source does not contain both senders' preserved private messages")
		}
		request := func(limit int) *tg.MessagesSearchRequest {
			return &tg.MessagesSearchRequest{Peer: peer, Q: prior.Run, Filter: &tg.InputMessagesFilterEmpty{}, Limit: limit}
		}
		check(d, "plain-page", request(500), all, len(all), "ok", false)
		check(d, "plain-count", request(0), nil, len(all), "ok", true)
		r := request(0)
		r.OffsetID, r.AddOffset = all[7], 3
		check(d, "count-with-page-offset", r, nil, len(all), "ok", true)
		for _, from := range []struct {
			name string
			peer tg.InputPeerClass
			ids  []int
		}{{"self", &tg.InputPeerSelf{}, self}, {"other", peer, other}} {
			r = request(50)
			r.SetFromID(from.peer)
			check(d, "from-"+from.name+"-page", r, from.ids, len(from.ids), "ok", false)
			r = request(0)
			r.SetFromID(from.peer)
			check(d, "from-"+from.name+"-count", r, nil, len(from.ids), "ok", true)
		}
		r = request(0)
		r.Peer = &tg.InputPeerEmpty{}
		check(d, "global-private-count", r, nil, len(global), "ok", true)
		r = request(0)
		r.Q += "-absent"
		check(d, "empty-count", r, nil, 0, "ok", true)
		for _, filter := range []struct {
			name string
			wire tg.MessagesFilterClass
		}{{"pinned", &tg.InputMessagesFilterPinned{}}, {"photo", &tg.InputMessagesFilterPhotos{}}, {"document", &tg.InputMessagesFilterDocument{}}} {
			r = request(0)
			r.Filter = filter.wire
			r.SetFromID(&tg.InputPeerSelf{})
			check(d, filter.name+"-filtered-empty-count", r, nil, 0, "ok", true)
		}
		for _, bad := range []struct {
			name string
			peer tg.InputPeerClass
		}{{"empty", &tg.InputPeerEmpty{}}, {"zero", &tg.InputPeerUser{}}, {"negative", &tg.InputPeerUser{UserID: -1}}, {"hash", &tg.InputPeerUser{UserID: peer.UserID, AccessHash: peer.AccessHash ^ 1}}} {
			r = request(0)
			r.SetFromID(bad.peer)
			check(d, "invalid-from-"+bad.name, r, nil, 0, "FROM_PEER_INVALID", true)
		}
		for _, bad := range []struct {
			name string
			peer tg.InputPeerClass
		}{{"zero", &tg.InputPeerUser{}}, {"negative", &tg.InputPeerUser{UserID: -1}}, {"hash", &tg.InputPeerUser{UserID: peer.UserID, AccessHash: peer.AccessHash ^ 1}}} {
			r = request(0)
			r.Peer = bad.peer
			check(d, "invalid-peer-"+bad.name, r, nil, 0, "PEER_ID_INVALID", true)
		}
		check(d, "negative-search-limit", request(-1), nil, 0, "LIMIT_INVALID", false)
		r = request(0)
		r.OffsetID = -1
		check(d, "negative-search-offset", r, nil, 0, "MSG_ID_INVALID", true)
		low, high := all[len(all)-1]-1, all[0]+1
		check(d, "history-bounded-page", &tg.MessagesGetHistoryRequest{Peer: peer, MinID: low, MaxID: high, Limit: 500}, all, len(all), "ok", false)
		check(d, "history-invalid-peer-hash", &tg.MessagesGetHistoryRequest{Peer: &tg.InputPeerUser{UserID: peer.UserID, AccessHash: peer.AccessHash ^ 1}, MinID: low, MaxID: high, Limit: 50}, nil, 0, "PEER_ID_INVALID", false)
		check(d, "history-negative-limit", &tg.MessagesGetHistoryRequest{Peer: peer, Limit: -1}, nil, 0, "LIMIT_INVALID", false)
	}
	p.checkPTS(initial, map[int64]int{}, "search-after")
	for _, d := range p.devices {
		if d.record.DeviceIndex != 1 {
			continue
		}
		cursor := originalCursors[d.record.Index]
		p.require(p.stop(d))
		p.start(d, &cursor)
		if len(d.recovered) != 0 || d.cursor.Pts != cursor.Pts {
			t.Fatal("read-only search changed recovery state")
		}
	}
	if len(p.liveSince(mark)) != 0 {
		t.Fatal("read-only search produced a business update")
	}
	if !reflect.DeepEqual(boxes, p.searchSourceBoxes(prior.Run, "after")) {
		t.Fatal("search mutated preserved marker boxes")
	}
	p.snapshot("final")
	p.record("search_summary", map[string]any{"calls": calls, "failures": failures, "source_run_id": prior.Run, "zero_new_messages": true})
	if failures != 0 {
		t.Errorf("search contract mismatches: %d/%d (complete raw cases retained)", failures, calls)
	}
}
