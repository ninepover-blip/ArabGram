package memory

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPrivatePinAtomicMemory(t *testing.T) {
	ctx := context.Background()
	s := NewMessageStore(NewDialogStore())
	sent, err := s.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: 11, RecipientUserID: 12, RandomID: 1, Message: "pin", Date: 1700000000})
	if err != nil {
		t.Fatal(err)
	}
	req := domain.PinPrivateMessageRequest{OwnerUserID: 11, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 12}, MessageID: sent.SenderMessage.ID, Pinned: true, Date: 1700000001, OriginAuthKeyID: [8]byte{1}, OriginSessionID: 99}
	beforeUID, beforePts := s.nextUID, s.nextPts[11]
	events := s.updateEvents
	s.updateEvents = nil
	res, err := s.PinPrivateMessage(ctx, req)
	if !errors.Is(err, store.ErrDeliveryOutboxRequired) || res.Changed() || s.m[11][0].Pinned || s.m[12][0].Pinned || s.nextUID != beforeUID || s.nextPts[11] != beforePts {
		t.Fatalf("missing boundary mutated state: %+v %v", res, err)
	}
	s.updateEvents = events
	// Invalid allocator state must fail receipt validation before pin/counter writes.
	s.nextUID = 0
	res, err = s.PinPrivateMessage(ctx, req)
	if err == nil || res.Changed() || s.m[11][0].Pinned || s.m[12][0].Pinned || s.nextUID != 0 || s.nextPts[11] != beforePts || len(events.events[11]) != 1 {
		t.Fatalf("failed receipt mutated state: %+v %v", res, err)
	}
	s.nextUID = beforeUID
	var wg sync.WaitGroup
	results := make(chan domain.PinPrivateMessageResult, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.PinPrivateMessage(ctx, req)
			if e != nil {
				t.Error(e)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	changed := 0
	var first domain.PinPrivateMessageResult
	for r := range results {
		if r.Changed() {
			changed++
			first = r
		} else if r.ServiceMessage.SenderMessage.ID != 0 {
			t.Errorf("no-op service: %+v", r)
		}
	}
	if changed != 1 || len(s.m[11]) != 2 || len(s.m[12]) != 2 {
		t.Fatalf("concurrent pin commits=%d boxes=%d/%d", changed, len(s.m[11]), len(s.m[12]))
	}
	for _, user := range []int64{11, 12} {
		es := events.events[user]
		if len(es) != 3 || es[1].Type != domain.UpdateEventPinnedMessages || es[2].Type != domain.UpdateEventNewMessage || es[2].Pts != es[1].Pts+1 || len(events.dispatches[user]) != 3 {
			t.Fatalf("event/outbox contract for %d: %+v", user, es)
		}
		for _, d := range events.dispatches[user][1:] {
			if (user == 11 && (d.ExcludeAuthKeyID != req.OriginAuthKeyID || d.ExcludeSessionID != 99)) || (user == 12 && (d.ExcludeAuthKeyID != [8]byte{} || d.ExcludeSessionID != 0)) {
				t.Fatalf("wrong origin exclusion: %+v", d)
			}
		}
	}
	req.Pinned = false
	if _, err := s.PinPrivateMessage(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Pinned = true
	again, err := s.PinPrivateMessage(ctx, req)
	if err != nil || again.ServiceMessage.SenderMessage.ID == first.ServiceMessage.SenderMessage.ID || again.ServiceMessage.SenderMessage.RandomID == first.ServiceMessage.SenderMessage.RandomID {
		t.Fatalf("same-date repin must create fresh service: %+v %v", again, err)
	}
	req.Pinned = false
	if _, err := s.PinPrivateMessage(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Pinned, req.PmOneside = true, true
	if _, err := s.PinPrivateMessage(ctx, req); err != nil {
		t.Fatal(err)
	}
	peerClear := domain.PinPrivateMessageRequest{OwnerUserID: 12, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 11}, MessageID: sent.RecipientMessage.ID, Date: req.Date}
	clear, err := s.PinPrivateMessage(ctx, peerClear)
	if err != nil || len(clear.Updated) != 1 || clear.Updated[0].UserID != 11 || s.m[11][0].Pinned {
		t.Fatalf("peer-only unpin: %+v %v", clear, err)
	}
	before := append([]domain.UpdateEvent(nil), events.events[11]...)
	clear, err = s.PinPrivateMessage(ctx, peerClear)
	if err != nil || clear.Changed() || !reflect.DeepEqual(before, events.events[11]) {
		t.Fatalf("repeat clear mutated state: %+v %v", clear, err)
	}
}

func TestPrivatePinDeliveryPolicyMemory(t *testing.T) {
	ctx := context.Background()
	s := NewMessageStore(NewDialogStore())
	sent, err := s.SendPrivateText(ctx, domain.SendPrivateTextRequest{SenderUserID: 11, RecipientUserID: 12, RandomID: 1, Message: "pin", Date: 1700000000})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{OwnerUserID: 11, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 12}, MessageID: sent.SenderMessage.ID, Pinned: true, RecipientBlocked: true, Date: 1700000001})
	if err != nil || len(res.Updated) != 2 || res.ServiceMessage.SenderMessage.ID == 0 || res.ServiceMessage.RecipientMessage.ID != 0 || s.nextPts[11] != 3 || s.nextPts[12] != 2 {
		t.Fatalf("blocked recipient service policy: %+v %v", res, err)
	}
	events := s.updateEvents
	s.updateEvents = nil
	clear := domain.UnpinAllPrivateMessagesRequest{OwnerUserID: 11, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 12}, Date: 1700000002}
	res, err = s.UnpinAllPrivateMessages(ctx, clear)
	if !errors.Is(err, store.ErrDeliveryOutboxRequired) || res.Changed() || !s.m[11][0].Pinned || !s.m[12][0].Pinned {
		t.Fatalf("missing unpin boundary mutated state: %+v %v", res, err)
	}
	s.updateEvents = events
	res, err = s.UnpinAllPrivateMessages(ctx, clear)
	if err != nil || len(res.Updated) != 2 || len(events.events[11]) != 4 || len(events.dispatches[11]) != 4 || len(events.events[12]) != 3 || len(events.dispatches[12]) != 3 {
		t.Fatalf("unpin all durable effects: %+v %v", res, err)
	}
}
