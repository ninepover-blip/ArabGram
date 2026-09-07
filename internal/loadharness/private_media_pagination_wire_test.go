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
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

type mediaReadBox struct {
	Owner     int64    `json:"owner"`
	ID        int      `json:"id"`
	Peer      int64    `json:"peer"`
	Sender    int64    `json:"sender"`
	Date      int      `json:"date"`
	Body      string   `json:"body"`
	Pinned    bool     `json:"pinned"`
	SavedPeer int64    `json:"saved_peer"`
	Kind      string   `json:"kind"`
	Tags      []string `json:"tags"`
}

func (p *privateLifecycleProbe) mediaReadFacts(source, label string) []mediaReadBox {
	p.t.Helper()
	if !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(source) {
		p.t.Fatal("invalid media source")
	}
	var owners []int64
	for owner := range p.initial {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
	if len(owners) != 2 {
		p.t.Fatal("requires two owned accounts")
	}
	var container string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			container = c.ContainerID
		}
	}
	query := fmt.Sprintf(`BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms';
WITH cap AS (
 SELECT owner_user_id,box_id,row_number() OVER (PARTITION BY owner_user_id ORDER BY box_id DESC) AS n
 FROM message_boxes WHERE owner_user_id IN (%d,%d) AND peer_id IN (%d,%d)
 AND peer_id<>owner_user_id AND peer_type='user' AND NOT deleted
)
SELECT json_agg(row_to_json(x)) FROM (
 SELECT m.owner_user_id AS owner,m.box_id AS id,m.peer_id AS peer,m.from_user_id AS sender,
 m.message_date AS date,m.body,m.pinned,m.saved_peer_id AS saved_peer,COALESCE(m.media->>'kind','') AS kind,
 ARRAY(SELECT t.reaction_type||':'||t.reaction_value FROM saved_message_reaction_tags t
 WHERE t.user_id=m.owner_user_id AND t.message_box_id=m.box_id ORDER BY t.reaction_type,t.reaction_value) AS tags
 FROM message_boxes m WHERE m.owner_user_id IN (%d,%d) AND NOT m.deleted AND
 (m.body LIKE 'telesrv-lifecycle/%s/%%' OR EXISTS (
 SELECT 1 FROM cap WHERE cap.owner_user_id=m.owner_user_id AND cap.box_id=m.box_id AND cap.n<=550))
 ORDER BY m.owner_user_id,m.box_id DESC
) x; COMMIT;`, owners[0], owners[1], owners[0], owners[1], owners[0], owners[1], source)
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	body, err := environmentCommand(ctx, p.spec.DockerBinary, nil, 2<<20, "exec", container, "psql", "-U", p.spec.Postgres.User, "-d", p.spec.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", query)
	p.require(err)
	var boxes []mediaReadBox
	p.require(json.Unmarshal(body, &boxes))
	if len(boxes) != 1104 {
		p.t.Fatalf("media/cap source=%d want1104", len(boxes))
	}
	for _, b := range boxes {
		if !strings.HasPrefix(b.Body, "telesrv") {
			p.t.Fatal("cap source outside owned load messages")
		}
	}
	p.record("media_read_source", map[string]any{"label": label, "source_run_id": source, "sql": query, "boxes": boxes})
	return boxes
}

// The independent wire oracle treats offset_id as a boundary immediately after
// all IDs >= it. Intersect the requested window with the list; do not shift a
// truncated window to fill it. TDesktop After uses messageId+1/add=-limit.
func mediaReadWindow(ids []int, offset, add, limit int) []int {
	pivot := 0
	if offset > 0 {
		for pivot < len(ids) && ids[pivot] >= offset {
			pivot++
		}
	}
	start, end := pivot+add, pivot+add+limit
	if start < 0 {
		start = 0
	}
	if start > len(ids) {
		start = len(ids)
	}
	if end < start {
		end = start
	}
	if end > len(ids) {
		end = len(ids)
	}
	return append([]int{}, ids[start:end]...)
}

func TestRealPrivateMediaPagination(t *testing.T) {
	path := os.Getenv("TELESRV_LOAD_MEDIA_SOURCE_REPORT")
	if path == "" {
		t.Skip("requires successful isolated media fixture report")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var prior struct {
		Pass bool   `json:"pass"`
		Run  string `json:"run_id"`
	}
	if json.Unmarshal(raw, &prior) != nil || !prior.Pass || !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(prior.Run) {
		t.Fatal("invalid source report")
	}
	p := newPrivateLifecycleProbe(t, "read-only nonempty media/Saved/pin and pagination boundaries; no capacity acceptance")
	original := map[int]tg.UpdatesState{}
	for _, d := range p.devices {
		original[d.record.Index] = d.cursor
	}
	boxes := p.mediaReadFacts(prior.Run, "before")
	initial := p.checkpoint("media-read-before")
	mark := p.mark()
	calls, failures := 0, 0
	check := func(d *lifeDevice, label string, req bin.Encoder, wantIDs []int, wantCount int, countOnly bool) {
		calls++
		if calls > 360 {
			t.Fatal("media read quota exceeded")
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
		var out tg.MessagesMessagesBox
		err := d.client.Invoke(ctx, req, &out)
		cancel()
		ids := []int{}
		count, users := 0, 0
		known := false
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
		pass := known && err == nil && reflect.DeepEqual(ids, append([]int{}, wantIDs...)) && count == wantCount && (!countOnly || users == 0)
		// Preserve the full typed response without exceeding the evidence writer's
		// per-record traversal budget for 500 rich message objects. Each chunk has
		// an exact index and total; the case header retains all non-message fields.
		var messages []tg.MessageClass
		var header tg.MessagesMessagesClass = out.Messages
		switch v := out.Messages.(type) {
		case *tg.MessagesMessages:
			copy := *v
			messages = v.Messages
			copy.Messages = nil
			header = &copy
		case *tg.MessagesMessagesSlice:
			copy := *v
			messages = v.Messages
			copy.Messages = nil
			header = &copy
		}
		chunks := (len(messages) + 24) / 25
		for i := 0; i < chunks; i++ {
			end := min((i+1)*25, len(messages))
			p.record("media_read_messages", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "index": i, "chunks": chunks, "messages": messages[i*25 : end]})
		}
		p.record("media_read_case", map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "source_run_id": prior.Run, "method": fmt.Sprintf("%T", req), "request": req, "started_at": start.UTC(), "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds(), "class": classifyError(err), "response_type": fmt.Sprintf("%T", out.Messages), "response_header": header, "message_chunks": chunks, "ids": ids, "count": count, "users": users, "expected_ids": wantIDs, "expected_count": wantCount, "count_only": countOnly, "pass": pass})
		p.cases = append(p.cases, map[string]any{"label": label, "device": d.record.Index, "generation": d.generation, "pass": pass})
		if !pass {
			failures++
		}
		p.pause(20 * time.Millisecond)
	}
	for _, d := range p.devices {
		peer := p.peer(d)
		var ordinary, capRows []mediaReadBox
		for _, b := range boxes {
			if b.Owner == d.record.UserID && b.Peer == peer.UserID {
				capRows = append(capRows, b)
				if strings.Contains(b.Body, prior.Run) {
					ordinary = append(ordinary, b)
				}
			}
		}
		if len(ordinary) != 12 || len(capRows) != 550 {
			t.Fatal("source cardinality changed")
		}
		search := func(label string, r *tg.MessagesSearchRequest, rows []mediaReadBox) {
			limit := r.Limit
			media := false
			pinned := false
			switch r.Filter.(type) {
			case *tg.InputMessagesFilterPhotos, *tg.InputMessagesFilterDocument:
				media = true
			case *tg.InputMessagesFilterPinned:
				pinned = true
			}
			if media && limit > 100 {
				limit = 100
			}
			if limit > 500 {
				limit = 500
			}
			var ids []int
			for _, b := range rows {
				if r.MinID > 0 && b.ID <= r.MinID || r.MaxID > 0 && b.ID >= r.MaxID || r.MinDate > 0 && b.Date <= r.MinDate || r.MaxDate > 0 && b.Date >= r.MaxDate {
					continue
				}
				if r.FromID != nil {
					self := false
					switch r.FromID.(type) {
					case *tg.InputPeerSelf:
						self = true
					}
					if self && b.Sender != d.record.UserID || !self && b.Sender != peer.UserID {
						continue
					}
				}
				ids = append(ids, b.ID)
			}
			page := mediaReadWindow(ids, r.OffsetID, r.AddOffset, limit)
			count := len(ids)
			if !media && !pinned && r.Limit != 0 && !(r.OffsetID == 0 && r.MinDate == 0 && r.MaxDate == 0 && r.AddOffset >= 0) {
				count = len(page)
				if r.AddOffset >= 0 && len(mediaReadWindow(ids, r.OffsetID, r.AddOffset, limit+1)) > limit {
					count++
				}
			}
			if r.Limit == 0 {
				page = nil
			}
			check(d, label, r, page, count, r.Limit == 0)
		}
		for _, kind := range []string{"document", "photo"} {
			var rows, saved []mediaReadBox
			for _, b := range boxes {
				if b.Owner == d.record.UserID && b.Kind == kind && strings.Contains(b.Body, prior.Run) {
					if b.Peer == peer.UserID {
						rows = append(rows, b)
					} else if b.Peer == d.record.UserID {
						saved = append(saved, b)
					}
				}
			}
			if len(rows) != 6 || len(saved) != 1 || saved[0].SavedPeer != peer.UserID {
				t.Fatal("media source mismatch")
			}
			newReq := func(limit int) *tg.MessagesSearchRequest {
				var f tg.MessagesFilterClass = &tg.InputMessagesFilterDocument{}
				if kind == "photo" {
					f = &tg.InputMessagesFilterPhotos{}
				}
				return &tg.MessagesSearchRequest{Peer: peer, Q: prior.Run, Filter: f, Limit: limit}
			}
			for _, lim := range []int{0, 100} {
				search(fmt.Sprintf("%s-base-%d", kind, lim), newReq(lim), rows)
			}
			for _, self := range []bool{true, false} {
				for _, lim := range []int{0, 3} {
					r := newReq(lim)
					if self {
						r.SetFromID(&tg.InputPeerSelf{})
					} else {
						r.SetFromID(peer)
					}
					search(fmt.Sprintf("%s-from-%t-%d", kind, self, lim), r, rows)
				}
			}
			for _, field := range []string{"ids", "dates"} {
				for _, lim := range []int{0, 100} {
					r := newReq(lim)
					if field == "ids" {
						r.MinID, r.MaxID = rows[4].ID, rows[1].ID
					} else {
						r.MinDate, r.MaxDate = rows[4].Date, rows[1].Date
					}
					search(fmt.Sprintf("%s-strict-%s-%d", kind, field, lim), r, rows)
				}
			}
			for _, tc := range []struct {
				name            string
				off, add, limit int
			}{
				{"before", rows[2].ID, 0, 2}, {"around", rows[2].ID, -2, 4}, {"after", rows[4].ID + 1, -2, 2}, {"gap-forward", rows[4].ID, -4, 2},
				{"after-newest", rows[0].ID + 1, -2, 2}, {"around-newest", rows[0].ID + 1, -2, 4}, {"positive-empty", 0, 100, 2}, {"count-offset", rows[2].ID, -2, 0},
			} {
				r := newReq(tc.limit)
				r.OffsetID, r.AddOffset = tc.off, tc.add
				search(kind+"-"+tc.name, r, rows)
			}
			for _, lim := range []int{0, 10} {
				r := newReq(lim)
				r.Peer = &tg.InputPeerSelf{}
				r.SetSavedPeerID(peer)
				search(fmt.Sprintf("%s-saved-peer-%d", kind, lim), r, saved)
			}
			for _, tag := range []string{"👍", "❤"} {
				for _, lim := range []int{0, 10} {
					r := newReq(lim)
					r.Peer = &tg.InputPeerSelf{}
					r.SetSavedPeerID(peer)
					r.SetSavedReaction([]tg.ReactionClass{&tg.ReactionEmoji{Emoticon: tag}})
					var tagged []mediaReadBox
					for _, b := range saved {
						for _, v := range b.Tags {
							if v == "emoji:"+tag {
								tagged = append(tagged, b)
								break
							}
						}
					}
					search(fmt.Sprintf("%s-saved-tag-%s-%d", kind, tag, lim), r, tagged)
				}
			}
		}
		plain := func(limit int) *tg.MessagesSearchRequest {
			return &tg.MessagesSearchRequest{Peer: peer, Q: prior.Run, Filter: &tg.InputMessagesFilterEmpty{}, Limit: limit}
		}
		for _, tc := range []struct {
			name            string
			off, add, limit int
		}{
			{"before", ordinary[5].ID, 0, 3}, {"around", ordinary[5].ID, -2, 4}, {"after", ordinary[8].ID + 1, -3, 3}, {"gap-forward", ordinary[8].ID, -5, 2}, {"after-newest", ordinary[0].ID + 1, -2, 2}, {"around-newest", ordinary[0].ID + 1, -2, 4}, {"empty-total", 0, 100, 3},
		} {
			r := plain(tc.limit)
			r.OffsetID, r.AddOffset = tc.off, tc.add
			search("plain-"+tc.name, r, ordinary)
			var ids []int
			for _, b := range ordinary {
				ids = append(ids, b.ID)
			}
			page := mediaReadWindow(ids, tc.off, tc.add, tc.limit)
			count := len(page)
			if tc.add >= 0 && len(mediaReadWindow(ids, tc.off, tc.add, tc.limit+1)) > tc.limit {
				count++
			}
			check(d, "history-"+tc.name, &tg.MessagesGetHistoryRequest{Peer: peer, OffsetID: tc.off, AddOffset: tc.add, Limit: tc.limit, MinID: ordinary[len(ordinary)-1].ID - 1, MaxID: ordinary[0].ID + 1}, page, count, false)
		}
		var pinned []mediaReadBox
		for _, b := range ordinary {
			if b.Pinned {
				pinned = append(pinned, b)
			}
		}
		if len(pinned) != 1 {
			t.Fatal("pin fixture mismatch")
		}
		for _, tc := range []struct {
			name            string
			off, add, limit int
		}{{"count", 0, 0, 0}, {"page", 0, 0, 10}, {"empty-total", 0, 100, 10}, {"after-empty", pinned[0].ID + 1, -1, 1}} {
			r := plain(tc.limit)
			r.Filter = &tg.InputMessagesFilterPinned{}
			r.OffsetID, r.AddOffset = tc.off, tc.add
			search("pinned-"+tc.name, r, pinned)
		}
		var capIDs []int
		for _, b := range capRows {
			capIDs = append(capIDs, b.ID)
		}
		for _, lim := range []int{500, 501} {
			r := plain(lim)
			r.Q = "telesrv"
			r.MinID, r.MaxID = capIDs[549]-1, capIDs[0]+1
			search(fmt.Sprintf("search-cap-%d", lim), r, capRows)
		}
		for _, lim := range []int{50, 500} {
			check(d, fmt.Sprintf("history-cap-%d", lim), &tg.MessagesGetHistoryRequest{Peer: peer, MinID: capIDs[549] - 1, MaxID: capIDs[0] + 1, Limit: lim}, capIDs[:50], 51, false)
		}
	}
	p.checkPTS(initial, map[int64]int{}, "media-read-after")
	for _, d := range p.devices {
		if d.record.DeviceIndex == 1 {
			cursor := original[d.record.Index]
			p.require(p.stop(d))
			p.start(d, &cursor)
			if len(d.recovered) != 0 || d.cursor.Pts != cursor.Pts {
				t.Fatal("read-only recovery changed")
			}
		}
	}
	if len(p.liveSince(mark)) != 0 {
		t.Fatal("read-only media produced business update")
	}
	if !reflect.DeepEqual(boxes, p.mediaReadFacts(prior.Run, "after")) {
		t.Fatal("media reads mutated fixture")
	}
	p.snapshot("final")
	p.record("media_read_summary", map[string]any{"calls": calls, "failures": failures, "source_run_id": prior.Run, "zero_new_messages": true})
	if failures != 0 {
		t.Errorf("media/pagination mismatches: %d/%d; complete cases retained", failures, calls)
	}
}
