//go:build darwin || linux

package loadharness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type lifeReaction struct {
	Emoji  string `json:"emoji"`
	Count  int    `json:"count"`
	Chosen bool   `json:"chosen"`
}
type lifeUpdate struct {
	PeerBlocked  *tg.UpdatePeerBlocked  `json:"peer_blocked,omitempty"`
	PeerSettings *tg.UpdatePeerSettings `json:"peer_settings,omitempty"`
	Service      *tg.MessageService     `json:"service,omitempty"`
	Draft        tg.DraftMessageClass   `json:"draft,omitempty"`
	TopMsgID     int                    `json:"top_msg_id,omitempty"`
	Media        tg.MessageMediaClass   `json:"media,omitempty"`
	Silent       bool                   `json:"silent,omitempty"`
	NoForwards   bool                   `json:"noforwards,omitempty"`
	Pinned       bool                   `json:"pinned,omitempty"`
	Tags         bool                   `json:"tags,omitempty"`
	Reply        *lifeReply             `json:"reply,omitempty"`
	Forward      *lifeForward           `json:"forward,omitempty"`
	Kind         string                 `json:"kind"`
	ID           int                    `json:"id"`
	Peer         int64                  `json:"peer"`
	From         int64                  `json:"from"`
	Text         string                 `json:"text"`
	Out          bool                   `json:"out"`
	Pts          int                    `json:"pts"`
	Count        int                    `json:"pts_count"`
	IDs          []int                  `json:"ids,omitempty"`
	MaxID        int                    `json:"max_id"`
	Unread       int                    `json:"unread"`
	Reactions    []lifeReaction         `json:"reactions,omitempty"`
}

func lifePeer(peer tg.PeerClass) int64 {
	if v, ok := peer.(*tg.PeerUser); ok {
		return v.UserID
	}
	return 0
}
func lifeReactions(r tg.MessageReactions) []lifeReaction {
	var out []lifeReaction
	for _, v := range r.Results {
		if emoji, ok := v.Reaction.(*tg.ReactionEmoji); ok {
			_, chosen := v.GetChosenOrder()
			out = append(out, lifeReaction{emoji.Emoticon, v.Count, chosen})
		}
	}
	return out
}
func lifeMessage(m tg.MessageClass) lifeUpdate {
	switch v := m.(type) {
	case *tg.Message:
		return lifeUpdate{Kind: "message", ID: v.ID, Peer: lifePeer(v.PeerID), From: lifePeer(v.FromID), Text: v.Message, Out: v.Out, Reactions: lifeReactions(v.Reactions), Silent: v.Silent, NoForwards: v.Noforwards, Reply: lifeReplyHeader(v.ReplyTo), Forward: lifeForwardHeader(v), Media: v.Media}
	case *tg.MessageService:
		return lifeUpdate{Kind: "service", ID: v.ID, Peer: lifePeer(v.PeerID), From: lifePeer(v.FromID), Out: v.Out, Silent: v.Silent, Reply: lifeReplyHeader(v.ReplyTo), Service: v}
	case *tg.MessageEmpty:
		return lifeUpdate{Kind: "empty", ID: v.ID}
	default:
		return lifeUpdate{Kind: fmt.Sprintf("%T", m)}
	}
}
func lifeOne(u tg.UpdateClass) (lifeUpdate, bool) {
	var v lifeUpdate
	switch x := u.(type) {
	case *tg.UpdateNewMessage:
		v = lifeMessage(x.Message)
		v.Kind = "new"
		v.Pts = x.Pts
		v.Count = x.PtsCount
	case *tg.UpdateEditMessage:
		v = lifeMessage(x.Message)
		v.Kind = "edit"
		v.Pts = x.Pts
		v.Count = x.PtsCount
	case *tg.UpdateDeleteMessages:
		v = lifeUpdate{Kind: "delete", IDs: x.Messages, Pts: x.Pts, Count: x.PtsCount}
	case *tg.UpdateReadHistoryInbox:
		v = lifeUpdate{Kind: "read_inbox", Peer: lifePeer(x.Peer), MaxID: x.MaxID, Pts: x.Pts, Count: x.PtsCount, Unread: x.StillUnreadCount}
	case *tg.UpdateReadHistoryOutbox:
		v = lifeUpdate{Kind: "read_outbox", Peer: lifePeer(x.Peer), MaxID: x.MaxID, Pts: x.Pts, Count: x.PtsCount}
	case *tg.UpdateMessageReactions:
		v = lifeUpdate{Kind: "reaction", Peer: lifePeer(x.Peer), ID: x.MsgID, Reactions: lifeReactions(x.Reactions), Tags: x.Reactions.ReactionsAsTags}
	case *tg.UpdatePinnedMessages:
		v = lifeUpdate{Kind: "pinned", Peer: lifePeer(x.Peer), IDs: x.Messages, Pinned: x.Pinned, Pts: x.Pts, Count: x.PtsCount}
	case *tg.UpdateDraftMessage:
		v = lifeUpdate{Kind: "draft", Peer: lifePeer(x.Peer), Draft: x.Draft, TopMsgID: x.TopMsgID}
	case *tg.UpdatePeerBlocked:
		v = lifeUpdate{Kind: "peer_blocked", Peer: lifePeer(x.PeerID), PeerBlocked: x}
	case *tg.UpdatePeerSettings:
		v = lifeUpdate{Kind: "peer_settings", Peer: lifePeer(x.Peer), PeerSettings: x}
	default:
		return v, false
	}
	return v, true
}
func lifeUpdates(u tg.UpdatesClass) []lifeUpdate {
	var input []tg.UpdateClass
	switch x := u.(type) {
	case *tg.Updates:
		input = x.Updates
	case *tg.UpdatesCombined:
		input = x.Updates
	case *tg.UpdateShort:
		input = []tg.UpdateClass{x.Update}
	case *tg.UpdateShortSentMessage:
		return []lifeUpdate{{Kind: "sent", ID: x.ID, Pts: x.Pts, Count: x.PtsCount}}
	}
	var out []lifeUpdate
	for _, x := range input {
		if v, ok := lifeOne(x); ok {
			out = append(out, v)
		}
	}
	return out
}
func lifeMessages(m tg.MessagesMessagesClass) []lifeUpdate {
	var input []tg.MessageClass
	switch x := m.(type) {
	case *tg.MessagesMessages:
		input = x.Messages
	case *tg.MessagesMessagesSlice:
		input = x.Messages
	}
	var out []lifeUpdate
	for _, x := range input {
		out = append(out, lifeMessage(x))
	}
	return out
}
func lifeResult(out bin.Decoder) any {
	switch x := out.(type) {
	case *tg.UpdatesBox:
		return lifeUpdates(x.Updates)
	case *tg.MessagesMessagesBox:
		return lifeMessages(x.Messages)
	case *tg.UpdatesState:
		return *x
	case *tg.MessagesAffectedMessages:
		return *x
	default:
		return map[string]string{"type": fmt.Sprintf("%T", out)}
	}
}

func lifeRequest(in bin.Encoder) map[string]any {
	v := map[string]any{}
	peer := func(p tg.InputPeerClass) {
		if x, ok := p.(*tg.InputPeerUser); ok {
			v["peer_user"] = x.UserID
		}
	}
	switch x := in.(type) {
	case *tg.MessagesSendMessageRequest:
		peer(x.Peer)
		v["message"], v["random_id"] = x.Message, x.RandomID
		v["wire"] = x
	case *tg.MessagesEditMessageRequest:
		peer(x.Peer)
		v["id"], v["message"] = x.ID, x.Message
	case *tg.MessagesReadHistoryRequest:
		peer(x.Peer)
		v["max_id"] = x.MaxID
	case *tg.MessagesDeleteMessagesRequest:
		v["ids"], v["revoke"] = x.ID, x.Revoke
	case *tg.MessagesSendReactionRequest:
		peer(x.Peer)
		v["id"], v["big"] = x.MsgID, x.Big
		var reactions []string
		for _, r := range x.Reaction {
			if emoji, ok := r.(*tg.ReactionEmoji); ok {
				reactions = append(reactions, emoji.Emoticon)
			}
		}
		v["reactions"] = reactions
	case *tg.MessagesGetHistoryRequest:
		peer(x.Peer)
		v["limit"] = x.Limit
		v["wire"] = x
	case *tg.MessagesSearchRequest:
		v["wire"] = x
	case *tg.MessagesForwardMessagesRequest:
		v["wire"] = x
	case *tg.MessagesGetMessagesRequest:
		var ids []int
		for _, id := range x.ID {
			if n, ok := id.(*tg.InputMessageID); ok {
				ids = append(ids, n.ID)
			}
		}
		v["ids"] = ids
	}
	return v
}

type lifeObservation struct {
	Device     int        `json:"device"`
	Generation int        `json:"generation"`
	At         time.Time  `json:"at"`
	RelativeNS int64      `json:"relative_ns"`
	Update     lifeUpdate `json:"update"`
}
type lifeDevice struct {
	record     SessionRecord
	storage    *EncryptedFileStorage
	client     *telegram.Client
	cancel     context.CancelFunc
	done       chan error
	generation int
	cursor     tg.UpdatesState
	recovered  []lifeUpdate
}
type privateLifecycleProbe struct {
	t                     *testing.T
	ctx                   context.Context
	epoch                 time.Time
	id, out, manifestPath string
	manifest              *Manifest
	public                *rsa.PublicKey
	events                *eventWriter
	safety                *loadSafety
	environment           *environmentGuard
	resources             *resourceGuard
	metrics               *serverMetricsCollector
	stopMetrics           context.CancelFunc
	metricsDone           chan struct{}
	spec                  environmentSpec
	devices               []*lifeDevice
	mu                    sync.Mutex
	observations          []lifeObservation
	cases                 []map[string]any
	initial               map[int64]int
}

func (p *privateLifecycleProbe) record(kind string, fields map[string]any) {
	// The bounded evidence writer deliberately rejects non-string map keys.
	// Project the finite device/owner indexes instead of weakening that guard.
	for key, value := range fields {
		switch v := value.(type) {
		case map[int]tg.UpdatesState:
			m := make(map[string]tg.UpdatesState, len(v))
			for index, state := range v {
				m[fmt.Sprint(index)] = state
			}
			fields[key] = m
		case map[int][]lifeUpdate:
			m := make(map[string][]lifeUpdate, len(v))
			for index, updates := range v {
				m[fmt.Sprint(index)] = updates
			}
			fields[key] = m
		case map[int64]int:
			m := make(map[string]int, len(v))
			for owner, id := range v {
				m[fmt.Sprint(owner)] = id
			}
			fields[key] = m
		case map[int64]*tg.Message:
			m := make(map[string]*tg.Message, len(v))
			for owner, message := range v {
				m[fmt.Sprint(owner)] = message
			}
			fields[key] = m
		}
	}
	fields["type"] = kind
	fields["at"] = time.Now().UTC()
	fields["relative_ns"] = time.Since(p.epoch).Nanoseconds()
	p.events.write(fields)
}
func (p *privateLifecycleProbe) require(err error) {
	p.t.Helper()
	if err != nil {
		p.t.Fatal(err)
	}
}
func (p *privateLifecycleProbe) pause(d time.Duration) {
	p.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		p.t.Fatal("probe canceled", p.safety.report())
	case <-timer.C:
	}
}
func (p *privateLifecycleProbe) start(d *lifeDevice, recoverCursor *tg.UpdatesState) {
	p.t.Helper()
	if d.done != nil {
		p.t.Fatal("client generation overlap")
	}
	d.generation++
	generation := d.generation
	ctx, cancel := context.WithCancel(p.ctx)
	d.cancel = cancel
	d.done = make(chan error, 1)
	ready := make(chan error, 1)
	client, err := newClient(p.manifest.Endpoint, p.public, d.storage, clientHooks{Logger: physicalProbeLog{probe: &physicalProbe{events: p.events}, index: d.record.Index}, Update: telegram.UpdateHandlerFunc(func(_ context.Context, u tg.UpdatesClass) error {
		p.record("live_wire", map[string]any{"device": d.record.Index, "generation": generation, "response": u})
		for _, v := range lifeUpdates(u) {
			o := lifeObservation{d.record.Index, generation, time.Now().UTC(), time.Since(p.epoch).Nanoseconds(), v}
			p.mu.Lock()
			if len(p.observations) >= 4096 {
				p.mu.Unlock()
				p.safety.invariantFailure("lifecycle_observation_limit")
				return errors.New("observation limit")
			}
			p.observations = append(p.observations, o)
			p.mu.Unlock()
			p.record("live", map[string]any{"observation": o})
		}
		return nil
	})})
	p.require(err)
	d.client = client
	p.record("client_start", map[string]any{"device": d.record.Index, "generation": generation, "user": d.record.UserID, "recover_cursor": recoverCursor})
	go func() {
		e := client.Run(ctx, func(runCtx context.Context) error {
			op, stop := context.WithTimeout(runCtx, 5*time.Second)
			status, e := client.Auth().Status(op)
			stop()
			if e != nil || !status.Authorized || status.User == nil || status.User.ID != d.record.UserID {
				return errors.New("authorization identity mismatch")
			}
			if recoverCursor == nil {
				op, stop := context.WithTimeout(runCtx, 5*time.Second)
				state, e := client.API().UpdatesGetState(op)
				stop()
				if e != nil {
					return e
				}
				d.cursor = *state
				p.record("initial_cursor", map[string]any{"device": d.record.Index, "cursor": state})
			} else {
				state := *recoverCursor
				d.recovered = nil
				empty := false
				for page := 0; page < 32; page++ {
					op, stop := context.WithTimeout(runCtx, 5*time.Second)
					diff, e := client.API().UpdatesGetDifference(op, &tg.UpdatesGetDifferenceRequest{Pts: state.Pts, Date: state.Date, Qts: state.Qts})
					stop()
					if e != nil {
						return e
					}
					next, last, e := deliveryDifferenceState(state, diff)
					if e != nil {
						return e
					}
					var updates []tg.UpdateClass
					var messages []tg.MessageClass
					switch x := diff.(type) {
					case *tg.UpdatesDifference:
						updates = x.OtherUpdates
						messages = x.NewMessages
					case *tg.UpdatesDifferenceSlice:
						updates = x.OtherUpdates
						messages = x.NewMessages
					}
					var values []lifeUpdate
					for _, u := range updates {
						if v, ok := lifeOne(u); ok {
							values = append(values, v)
						}
					}
					for _, m := range messages {
						values = append(values, lifeMessage(m))
					}
					d.recovered = append(d.recovered, values...)
					p.record("difference_wire", map[string]any{"device": d.record.Index, "generation": generation, "page": page, "from": state, "response": diff})
					p.record("difference", map[string]any{"device": d.record.Index, "generation": generation, "page": page, "from": state, "next": next, "empty": last, "updates": values, "wire_type": fmt.Sprintf("%T", diff)})
					state = next
					if last {
						empty = true
						break
					}
				}
				if !empty {
					return errors.New("difference page limit")
				}
				d.cursor = state
			}
			ready <- nil
			<-runCtx.Done()
			return runCtx.Err()
		})
		d.done <- e
		select {
		case ready <- e:
		default:
		}
	}()
	select {
	case e := <-ready:
		p.require(e)
	case <-p.ctx.Done():
		p.t.Fatal("client startup canceled")
	}
	p.record("client_ready", map[string]any{"device": d.record.Index, "generation": generation, "cursor": d.cursor})
}
func (p *privateLifecycleProbe) stop(d *lifeDevice) error {
	if d.done == nil {
		return nil
	}
	d.cancel()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	select {
	case e := <-d.done:
		d.done = nil
		p.record("client_joined", map[string]any{"device": d.record.Index, "generation": d.generation, "class": classifyError(e)})
		if e != nil && !errors.Is(e, context.Canceled) {
			return e
		}
		return nil
	case <-timer.C:
		return errors.New("client join timeout")
	}
}
func (p *privateLifecycleProbe) peer(d *lifeDevice) *tg.InputPeerUser {
	for _, other := range p.devices {
		if other.record.UserID != d.record.UserID {
			return &tg.InputPeerUser{UserID: other.record.UserID, AccessHash: other.record.AccessHash}
		}
	}
	panic("peer missing")
}
func (p *privateLifecycleProbe) invoke(d *lifeDevice, label string, in bin.Encoder, out bin.Decoder, want string) {
	p.t.Helper()
	start := time.Now()
	op, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	err := d.client.Invoke(op, in, out)
	cancel()
	class := classifyError(err)
	if v, ok := tgerr.As(err); ok {
		class = v.Type
	}
	p.record("rpc", map[string]any{"device": d.record.Index, "generation": d.generation, "label": label, "method": fmt.Sprintf("%T", in), "request": lifeRequest(in), "started_at": start.UTC(), "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "duration_ns": time.Since(start).Nanoseconds(), "class": class, "result": lifeResult(out)})
	if class != want {
		p.t.Fatalf("%s device %d: expected %s, got %s", label, d.record.Index, want, class)
	}
}
func (p *privateLifecycleProbe) checkpoint(label string) map[int]tg.UpdatesState {
	states := map[int]tg.UpdatesState{}
	for _, d := range p.devices {
		if d.done != nil {
			var state tg.UpdatesState
			p.invoke(d, label, &tg.UpdatesGetStateRequest{}, &state, "ok")
			d.cursor = state
			states[d.record.Index] = state
		}
	}
	p.record("checkpoint", map[string]any{"label": label, "states": states})
	return states
}
func (p *privateLifecycleProbe) checkPTS(before map[int]tg.UpdatesState, delta map[int64]int, label string) {
	after := p.checkpoint(label)
	for _, d := range p.devices {
		old, ok := before[d.record.Index]
		if !ok || d.done == nil {
			continue
		}
		if after[d.record.Index].Pts != old.Pts+delta[d.record.UserID] {
			p.t.Fatalf("%s pts delta device %d: %d -> %d, want +%d", label, d.record.Index, old.Pts, after[d.record.Index].Pts, delta[d.record.UserID])
		}
	}
}
func (p *privateLifecycleProbe) mark() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.observations)
}
func (p *privateLifecycleProbe) liveSince(mark int) []lifeObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]lifeObservation(nil), p.observations[mark:]...)
}
func (p *privateLifecycleProbe) checkLive(mark int, start time.Time, want map[int][]lifeUpdate, label string) {
	p.t.Helper()
	p.pause(max(0, 2*time.Second-time.Since(start)))
	got := map[int][]lifeUpdate{}
	var maxNS int64
	for _, o := range p.liveSince(mark) {
		got[o.Device] = append(got[o.Device], o.Update)
		lag := o.RelativeNS - start.Sub(p.epoch).Nanoseconds()
		if lag > maxNS {
			maxNS = lag
		}
		if lag < 0 || lag > int64(time.Second) {
			p.t.Fatalf("%s live timing outside [0,1s]: %d", label, lag)
		}
	}
	if !reflect.DeepEqual(got, want) {
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(want)
		p.t.Fatalf("%s live mismatch\ngot %s\nwant %s", label, a, b)
	}
	p.record("live_checked", map[string]any{"label": label, "started_relative_ns": start.Sub(p.epoch).Nanoseconds(), "expected": want, "max_latency_ns": maxNS, "window_ns": time.Since(start).Nanoseconds()})
}
func (p *privateLifecycleProbe) fetch(d *lifeDevice, id int, label string) lifeUpdate {
	var out tg.MessagesMessagesBox
	p.invoke(d, label, &tg.MessagesGetMessagesRequest{ID: []tg.InputMessageClass{&tg.InputMessageID{ID: id}}}, &out, "ok")
	values := lifeMessages(out.Messages)
	if len(values) != 1 {
		p.t.Fatal("getMessages cardinality")
	}
	return values[0]
}
func (p *privateLifecycleProbe) snapshot(label string) {
	var predicates string
	for user, pts := range p.initial {
		if predicates != "" {
			predicates += " OR "
		}
		predicates += fmt.Sprintf("(user_id=%d AND pts>%d)", user, pts)
	}
	if predicates == "" {
		predicates = "false"
	}
	q := fmt.Sprintf(`BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms'; SELECT json_build_object(
'watermarks',(SELECT json_agg(json_build_object('user',user_id,'pts',contiguous_pts) ORDER BY user_id) FROM user_update_watermarks),
'counts',json_build_object('messages',(SELECT count(*) FROM private_messages),'boxes',(SELECT count(*) FROM message_boxes),'events',(SELECT count(*) FROM user_update_events)),
'dialogs',(SELECT json_agg(row_to_json(d)) FROM (SELECT user_id,peer_id,top_message_id,read_inbox_max_id,read_outbox_max_id,unread_count,unread_reactions_count FROM dialogs ORDER BY user_id,peer_id) d),
'messages',(SELECT json_agg(row_to_json(m)) FROM (SELECT id,sender_user_id,recipient_user_id,random_id,body,edit_date,sender_box_id,recipient_box_id,sender_pts,recipient_pts,message_date FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%' ORDER BY id) m),
'boxes',(SELECT json_agg(row_to_json(b)) FROM (SELECT owner_user_id,box_id,private_message_id,peer_id,body,pts,deleted,outgoing,edit_date,reaction_unread,silent,noforwards,reply_to_msg_id,reply_to_peer_id,quote_text,quote_offset,quote_entities,fwd_from_peer_id,fwd_date FROM message_boxes WHERE private_message_id IN (SELECT id FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%') ORDER BY owner_user_id,box_id) b),
'events',(SELECT json_agg(row_to_json(e)) FROM (SELECT user_id,pts,pts_count,event_type,message_box_id,message_ids,peer_id,max_id,still_unread_count FROM user_update_events WHERE %s ORDER BY user_id,pts) e),
'reactions',(SELECT json_agg(row_to_json(r)) FROM (SELECT private_message_id,user_id,reaction_type,reaction_value,chosen_order FROM private_message_reactions WHERE private_message_id IN (SELECT id FROM private_messages WHERE body LIKE 'telesrv-lifecycle/%s/%%') ORDER BY private_message_id,user_id,reaction_value) r),
'queues',json_build_object('pts',(SELECT count(*) FROM dispatch_outbox),'absolute',(SELECT count(*) FROM edge_delivery_outbox),'channel',(SELECT count(*) FROM channel_delivery_events))); COMMIT;`, p.id, p.id, predicates, p.id)
	var name string
	for _, c := range p.spec.Containers {
		if c.ID == p.spec.Postgres.Container {
			name = c.ContainerID
		}
	}
	op, stop := context.WithTimeout(p.ctx, 5*time.Second)
	data, e := environmentCommand(op, p.spec.DockerBinary, nil, 2<<20, "exec", name, "psql", "-U", p.spec.Postgres.User, "-d", p.spec.Postgres.Database, "-Atq", "-v", "ON_ERROR_STOP=1", "-c", q)
	stop()
	p.require(e)
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	p.require(decoder.Decode(&value))
	p.record("pg_snapshot", map[string]any{"label": label, "snapshot": value})
}

func newPrivateLifecycleProbe(t *testing.T, scope string) *privateLifecycleProbe {
	fixture, out, specPath, targets := lifecycleFixture(t)
	if e := os.Mkdir(out, 0700); e != nil {
		t.Fatal(e)
	}
	manifestPath := filepath.Join(fixture, "pool/manifest.json")
	manifest, e := LoadManifest(manifestPath)
	if e != nil {
		t.Fatal(e)
	}
	key, e := LoadSessionKey(filepath.Join(fixture, "pool/session.key"))
	if e != nil {
		t.Fatal(e)
	}
	public, e := loadManifestPublicKey(manifestPath, manifest.Endpoint, "")
	if e != nil {
		t.Fatal(e)
	}
	parent, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(stop)
	ctx, safety := newLoadSafety(parent)
	events, e := newBoundedEventWriter(filepath.Join(out, "events.ndjson"), EventLimits{LineBytes: 2 << 20, SegmentBytes: 16 << 20, TotalBytes: 128 << 20})
	if e != nil {
		t.Fatal(e)
	}
	events.onFailure = safety.eventsFailure
	var random [8]byte
	if _, e = rand.Read(random[:]); e != nil {
		t.Fatal(e)
	}
	p := &privateLifecycleProbe{t: t, ctx: ctx, epoch: time.Now(), id: hex.EncodeToString(random[:]), out: out, manifestPath: manifestPath, manifest: manifest, public: public, events: events, safety: safety, initial: map[int64]int{}}
	t.Cleanup(func() {
		for _, d := range p.devices {
			if e := p.stop(d); e != nil {
				t.Error(e)
			}
		}
		safety.workersStopped()
		if p.stopMetrics != nil {
			p.stopMetrics()
			<-p.metricsDone
		}
		end, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if p.metrics != nil {
			_, _ = p.metrics.scrape(end)
			p.record("metrics", map[string]any{"metrics_targets": p.metrics.samples()})
		}
		if p.environment != nil {
			p.environment.finish(end)
		}
		if p.resources != nil {
			p.resources.finish(end)
		}
		p.record("finished", map[string]any{"failed": t.Failed(), "safety": safety.report()})
		if e := events.finalize(end); e != nil {
			t.Error(e)
		}
		r := map[string]any{"probe_version": 1, "scope": scope, "run_id": p.id, "pass": !t.Failed() && safety.report().Source == "", "cases": p.cases, "event_evidence": events.report(), "safety_stop": safety.report(), "initial_pts": p.initial}
		if p.metrics != nil {
			r["server_metrics_targets"] = p.metrics.reports()
		}
		if p.environment != nil {
			r["environment"] = p.environment.report()
		}
		if p.resources != nil {
			r["resources"] = p.resources.report()
		}
		data, _ := json.MarshalIndent(r, "", "  ")
		if e := writeNewEvidenceReport(filepath.Join(out, "report.json"), data); e != nil {
			t.Error(e)
		}
	})
	p.spec, _, e = loadEnvironmentSpec(specPath, targets)
	p.require(e)
	p.environment, e = newEnvironmentGuard(parent, specPath, targets, safety, events)
	p.require(e)
	p.resources, e = newResourceGuard(parent, ResourceLimits{HeapBytes: 512 << 20, RSSBytes: 768 << 20, OpenFiles: 8192}, filepath.Join(out, "report.json"), filepath.Join(out, "events.ndjson"), manifestPath, safety, events, nil)
	p.require(e)
	p.metrics = newServerMetricsCollector(targets)
	p.metrics.onIssue = safety.metricsFailure
	_, e = p.metrics.scrape(ctx)
	p.require(e)
	p.record("metrics", map[string]any{"metrics_targets": p.metrics.samples()})
	mctx, mcancel := context.WithCancel(parent)
	p.stopMetrics = mcancel
	p.metricsDone = make(chan struct{})
	go func() {
		defer close(p.metricsDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-mctx.Done():
				return
			case <-tick.C:
				_, _ = p.metrics.scrape(mctx)
				p.record("metrics", map[string]any{"metrics_targets": p.metrics.samples()})
			}
		}
	}()
	for _, r := range manifest.Sessions {
		p.devices = append(p.devices, &lifeDevice{record: r, storage: &EncryptedFileStorage{Path: filepath.Join(filepath.Dir(manifestPath), r.SessionFile), Key: key}})
	}
	for _, d := range p.devices {
		p.start(d, nil)
		p.initial[d.record.UserID] = d.cursor.Pts
	}
	p.snapshot("baseline")
	return p
}

func TestRealPrivateLifecycle(t *testing.T) {
	p := newPrivateLifecycleProbe(t, "private lifecycle correctness; not a capacity run")
	var sender, receiver, offline *lifeDevice
	for _, d := range p.devices {
		if d.record.AccountIndex == 0 && d.record.DeviceIndex == 0 {
			sender = d
		}
		if d.record.AccountIndex == 1 && d.record.DeviceIndex == 0 {
			receiver = d
		}
		if d.record.AccountIndex == 1 && d.record.DeviceIndex == 1 {
			offline = d
		}
	}
	if sender == nil || receiver == nil || offline == nil {
		t.Fatal("fixture roles missing")
	}
	for _, mode := range []string{"online", "offline"} {
		for _, kind := range []string{"edit", "read", "reaction", "delete_local", "delete_revoke"} {
			p.scenario(mode, kind, sender, receiver, offline)
		}
	}
	p.snapshot("final")
	if len(p.cases) != 10 {
		t.Fatal("incomplete scenarios")
	}
}

func (p *privateLifecycleProbe) scenario(mode, kind string, sender, receiver, secondary *lifeDevice) {
	p.t.Helper()
	label := mode + "/" + kind
	body := "telesrv-lifecycle/" + p.id + "/" + label
	before := p.checkpoint(label + "/before_send")
	var random [8]byte
	_, e := rand.Read(random[:])
	p.require(e)
	rid := int64(binary.LittleEndian.Uint64(random[:]) & math.MaxInt64)
	mark, start := p.mark(), time.Now()
	var sent tg.UpdatesBox
	p.invoke(sender, label+"/send", &tg.MessagesSendMessageRequest{Peer: p.peer(sender), RandomID: rid, Message: body}, &sent, "ok")
	var senderID int
	for _, u := range lifeUpdates(sent.Updates) {
		if u.Kind == "new" || u.Kind == "sent" {
			senderID = u.ID
		}
	}
	if senderID <= 0 {
		p.t.Fatal("send response missing ID")
	}
	var history tg.MessagesMessagesBox
	p.invoke(receiver, label+"/read_owner_id", &tg.MessagesGetHistoryRequest{Peer: p.peer(receiver), Limit: 1}, &history, "ok")
	messages := lifeMessages(history.Messages)
	if len(messages) != 1 || messages[0].Text != body {
		p.t.Fatal("recipient history does not contain sent message")
	}
	receiverID := messages[0].ID
	ids := map[int64]int{sender.record.UserID: senderID, receiver.record.UserID: receiverID}
	want := map[int][]lifeUpdate{}
	for _, d := range p.devices {
		if d == sender {
			continue
		}
		u := lifeUpdate{Kind: "new", ID: ids[d.record.UserID], Peer: p.peer(d).UserID, From: sender.record.UserID, Text: body, Out: d.record.UserID == sender.record.UserID, Pts: before[d.record.Index].Pts + 1, Count: 1}
		want[d.record.Index] = []lifeUpdate{u}
	}
	p.checkLive(mark, start, want, label+"/send")
	p.checkPTS(before, map[int64]int{sender.record.UserID: 1, receiver.record.UserID: 1}, label+"/sent")
	p.snapshot(label + "/sent")
	state := p.checkpoint(label + "/before_mutation")
	saved := secondary.cursor
	if mode == "offline" {
		p.require(p.stop(secondary))
	}
	caller := sender
	if kind == "read" || kind == "reaction" {
		caller = receiver
	}
	expected := map[int][]lifeUpdate{}
	all := map[int]lifeUpdate{}
	delta := map[int64]int{}
	for _, d := range p.devices {
		u := lifeUpdate{}
		switch kind {
		case "edit":
			u = lifeUpdate{Kind: "edit", ID: ids[d.record.UserID], Peer: p.peer(d).UserID, From: sender.record.UserID, Text: body + "/edited", Out: d.record.UserID == sender.record.UserID, Pts: state[d.record.Index].Pts + 1, Count: 1}
			delta[d.record.UserID] = 1
		case "read":
			u = lifeUpdate{Kind: "read_outbox", Peer: p.peer(d).UserID, MaxID: ids[d.record.UserID], Pts: state[d.record.Index].Pts + 1, Count: 1}
			if d.record.UserID == receiver.record.UserID {
				u.Kind = "read_inbox"
			}
			delta[d.record.UserID] = 1
		case "reaction":
			u = lifeUpdate{Kind: "reaction", ID: ids[d.record.UserID], Peer: p.peer(d).UserID, Reactions: []lifeReaction{{"👍", 1, d.record.UserID == receiver.record.UserID}}}
		default:
			if kind == "delete_local" && d.record.UserID != sender.record.UserID {
				continue
			}
			u = lifeUpdate{Kind: "delete", IDs: []int{ids[d.record.UserID]}, Pts: state[d.record.Index].Pts + 1, Count: 1}
			delta[d.record.UserID] = 1
		}
		all[d.record.Index] = u
		if d != caller && d.done != nil {
			expected[d.record.Index] = []lifeUpdate{u}
		}
	}
	mark, start = p.mark(), time.Now()
	var response tg.UpdatesBox
	var affected tg.MessagesAffectedMessages
	switch kind {
	case "edit":
		req := &tg.MessagesEditMessageRequest{Peer: p.peer(caller), ID: senderID}
		req.SetMessage(body + "/edited")
		p.invoke(caller, label+"/mutation", req, &response, "ok")
	case "read":
		p.invoke(caller, label+"/mutation", &tg.MessagesReadHistoryRequest{Peer: p.peer(caller), MaxID: math.MaxInt32}, &affected, "ok")
	case "reaction":
		req := &tg.MessagesSendReactionRequest{Peer: p.peer(caller), MsgID: receiverID}
		req.SetReaction([]tg.ReactionClass{&tg.ReactionEmoji{Emoticon: "👍"}})
		p.invoke(caller, label+"/mutation", req, &response, "ok")
	default:
		p.invoke(caller, label+"/mutation", &tg.MessagesDeleteMessagesRequest{Revoke: kind == "delete_revoke", ID: []int{senderID}}, &affected, "ok")
	}
	if kind == "edit" || kind == "reaction" {
		if !reflect.DeepEqual(lifeUpdates(response.Updates), []lifeUpdate{all[caller.record.Index]}) {
			p.t.Fatalf("%s response projection mismatch: %+v", label, lifeUpdates(response.Updates))
		}
	} else if affected.Pts != all[caller.record.Index].Pts || affected.PtsCount != 1 {
		p.t.Fatalf("%s affected response mismatch", label)
	}
	p.checkLive(mark, start, expected, label+"/mutation")
	p.checkPTS(state, delta, label+"/mutated")
	p.snapshot(label + "/mutated")
	if mode == "offline" {
		p.start(secondary, &saved)
		var recovered []lifeUpdate
		if u, ok := all[secondary.record.Index]; ok && kind != "reaction" {
			recovered = []lifeUpdate{u}
		}
		if !reflect.DeepEqual(secondary.recovered, recovered) {
			p.t.Fatalf("%s offline recovery mismatch: %+v vs %+v", label, secondary.recovered, recovered)
		}
	}
	for _, d := range p.devices {
		m := p.fetch(d, ids[d.record.UserID], label+"/refetch")
		deleted := kind == "delete_revoke" || (kind == "delete_local" && d.record.UserID == sender.record.UserID)
		if deleted {
			if m.Kind != "empty" {
				p.t.Fatal("deleted owner box remains visible")
			}
			continue
		}
		text := body
		if kind == "edit" {
			text += "/edited"
		}
		if m.Kind != "message" || m.Text != text || m.Peer != p.peer(d).UserID || m.Out != (d.record.UserID == sender.record.UserID) {
			p.t.Fatalf("%s owner message projection mismatch", label)
		}
		if kind == "reaction" && !reflect.DeepEqual(m.Reactions, all[d.record.Index].Reactions) {
			p.t.Fatalf("%s reaction refetch mismatch", label)
		}
	}
	p.snapshot(label + "/recovered")
	if kind == "reaction" {
		p.reactionReplay(label, caller, ids, receiver.record.UserID)
	} else {
		before = p.checkpoint(label + "/before_noop")
		mark, start = p.mark(), time.Now()
		switch kind {
		case "edit":
			req := &tg.MessagesEditMessageRequest{Peer: p.peer(caller), ID: senderID}
			req.SetMessage(body + "/edited")
			p.invoke(caller, label+"/noop", req, &response, "MESSAGE_NOT_MODIFIED")
		case "read":
			p.invoke(caller, label+"/noop", &tg.MessagesReadHistoryRequest{Peer: p.peer(caller), MaxID: math.MaxInt32}, &affected, "ok")
			if affected.PtsCount != 0 {
				p.t.Fatal("noop read consumed PTS")
			}
		default:
			p.invoke(caller, label+"/noop", &tg.MessagesDeleteMessagesRequest{Revoke: kind == "delete_revoke", ID: []int{senderID}}, &affected, "ok")
			if affected.PtsCount != 0 {
				p.t.Fatal("noop delete consumed PTS")
			}
		}
		p.checkLive(mark, start, map[int][]lifeUpdate{}, label+"/noop")
		p.checkPTS(before, nil, label+"/noop_done")
		p.snapshot(label + "/noop_done")
		if kind == "edit" {
			before = p.checkpoint(label + "/before_forbidden")
			mark, start = p.mark(), time.Now()
			req := &tg.MessagesEditMessageRequest{Peer: p.peer(receiver), ID: receiverID}
			req.SetMessage(body + "/forbidden")
			p.invoke(receiver, label+"/forbidden", req, &response, "MESSAGE_AUTHOR_REQUIRED")
			p.checkLive(mark, start, map[int][]lifeUpdate{}, label+"/forbidden")
			p.checkPTS(before, nil, label+"/forbidden_done")
			p.snapshot(label + "/forbidden_done")
		}
	}
	p.cases = append(p.cases, map[string]any{"label": label, "marker": body, "random_id": rid, "sender": sender.record.UserID, "recipient": receiver.record.UserID, "owner_ids": ids, "offline_device": secondary.record.Index, "pass": true})
	p.record("case_pass", p.cases[len(p.cases)-1])
}
func (p *privateLifecycleProbe) reactionReplay(label string, caller *lifeDevice, ids map[int64]int, reactor int64) {
	for _, clear := range []bool{false, true} {
		name := label + "/repeat"
		if clear {
			name = label + "/clear"
		}
		before := p.checkpoint(name + "/before")
		mark, start := p.mark(), time.Now()
		req := &tg.MessagesSendReactionRequest{Peer: p.peer(caller), MsgID: ids[caller.record.UserID]}
		var reactions []tg.ReactionClass
		if !clear {
			reactions = []tg.ReactionClass{&tg.ReactionEmoji{Emoticon: "👍"}}
		}
		req.SetReaction(reactions)
		var response tg.UpdatesBox
		p.invoke(caller, name, req, &response, "ok")
		want := map[int][]lifeUpdate{}
		for _, d := range p.devices {
			u := lifeUpdate{Kind: "reaction", ID: ids[d.record.UserID], Peer: p.peer(d).UserID}
			if !clear {
				u.Reactions = []lifeReaction{{"👍", 1, d.record.UserID == reactor}}
			}
			if d == caller {
				if !reflect.DeepEqual(lifeUpdates(response.Updates), []lifeUpdate{u}) {
					p.t.Fatal("reaction repeat/clear response mismatch")
				}
			} else {
				want[d.record.Index] = []lifeUpdate{u}
			}
		}
		p.checkLive(mark, start, want, name)
		p.checkPTS(before, nil, name+"/done")
		p.snapshot(name + "/done")
	}
}
