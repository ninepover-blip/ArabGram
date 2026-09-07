package rpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/egress"
	"telesrv/internal/store"
)

func TestOutboxEventDeletedNewMessageUsesDeletePtsUpdate(t *testing.T) {
	msg := domain.Message{
		ID:          547,
		OwnerUserID: 1000000001,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000002},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000301,
		Body:        "deleted before dispatch",
		Pts:         2035,
		Deleted:     true,
	}
	updates := tgUpdateForOutboxEvent(domain.UpdateEvent{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
	})
	if updates == nil || len(updates.Updates) != 1 {
		t.Fatalf("updates = %+v, want one delete pts update", updates)
	}
	del, ok := updates.Updates[0].(*tg.UpdateDeleteMessages)
	if !ok {
		t.Fatalf("update = %T, want UpdateDeleteMessages instead of UpdateNewMessage", updates.Updates[0])
	}
	if del.Pts != msg.Pts || del.PtsCount != 1 || len(del.Messages) != 1 || del.Messages[0] != msg.ID {
		t.Fatalf("delete update = %+v, want message %d at pts=%d", del, msg.ID, msg.Pts)
	}
}

func TestRouterBuildOutboxUpdatesProjectsSenderPerViewerAndCaches(t *testing.T) {
	const (
		senderUserID = int64(1000000001)
		viewerUserID = int64(1000000002)
	)
	projected := domain.User{
		ID:        senderUserID,
		FirstName: "Sender",
		PhotoID:   9301,
		PhotoDCID: 2,
	}
	users := &countingOutboxUsersService{users: map[int64]domain.User{senderUserID: projected}}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	requests := make([]egress.OutboxUpdateRequest, 0, 2)
	for i, pts := range []int{7, 8} {
		msg := domain.Message{
			ID:          10 + i,
			OwnerUserID: viewerUserID,
			Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
			From:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
			Date:        1700000300 + i,
			Body:        "hello",
			Pts:         pts,
		}
		requests = append(requests, egress.OutboxUpdateRequest{
			TargetUserID: viewerUserID,
			Event: domain.UpdateEvent{
				UserID:   viewerUserID,
				Type:     domain.UpdateEventNewMessage,
				Pts:      pts,
				PtsCount: 1,
				Date:     msg.Date,
				Message:  msg,
				Users:    []domain.User{{ID: senderUserID, FirstName: "Stale"}},
			},
		})
	}

	updates, err := router.buildOutboxUpdates(context.Background(), requests)
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if len(updates) != len(requests) {
		t.Fatalf("updates count = %d, want %d", len(updates), len(requests))
	}
	for i, update := range updates {
		if update == nil || len(update.Users) != 1 {
			t.Fatalf("updates[%d].Users = %+v, want projected sender", i, update)
		}
		user, ok := update.Users[0].(*tg.User)
		if !ok {
			t.Fatalf("updates[%d].Users[0] = %T, want *tg.User", i, update.Users[0])
		}
		if user.FirstName != "Sender" {
			t.Fatalf("updates[%d] user first_name = %q, want projected Sender", i, user.FirstName)
		}
		photo, ok := user.Photo.(*tg.UserProfilePhoto)
		if !ok || photo.PhotoID != projected.PhotoID || photo.DCID != projected.PhotoDCID {
			t.Fatalf("updates[%d] user photo = %#v, want photo_id=%d dc=%d", i, user.Photo, projected.PhotoID, projected.PhotoDCID)
		}
	}
	if users.sparseCalls != 1 || len(users.calls) != 0 {
		t.Fatalf("user projection calls = sparse %d scalar %+v, want one sparse call", users.sparseCalls, users.calls)
	}
	if !reflect.DeepEqual(users.sparseRequest[viewerUserID], []int64{senderUserID}) {
		t.Fatalf("sparse request = %+v, want viewer=%d ids=[%d]", users.sparseRequest, viewerUserID, senderUserID)
	}

	// A later Egress projection call for the same receiver/sender pair must use
	// the bounded cross-call cache. The per-call viewerPeerCache is new, so this
	// specifically proves the stable projection survived outside that object.
	if _, err := router.buildOutboxUpdates(context.Background(), requests); err != nil {
		t.Fatalf("buildOutboxUpdates cached: %v", err)
	}
	if users.sparseCalls != 1 || len(users.calls) != 0 {
		t.Fatalf("cached user projection calls = sparse %d scalar %+v, want unchanged", users.sparseCalls, users.calls)
	}

	// Target and viewer invalidations are independent cache dimensions. Each
	// must force exactly one new sparse projection and never a scalar fallback.
	router.InvalidateRPCProjectionReadModelForUser(senderUserID)
	if _, err := router.buildOutboxUpdates(context.Background(), requests); err != nil {
		t.Fatalf("buildOutboxUpdates after target invalidation: %v", err)
	}
	if users.sparseCalls != 2 || len(users.calls) != 0 {
		t.Fatalf("target invalidation calls = sparse %d scalar %+v, want 2/0", users.sparseCalls, users.calls)
	}
	router.InvalidateRPCProjectionReadModelForViewer(viewerUserID)
	if _, err := router.buildOutboxUpdates(context.Background(), requests); err != nil {
		t.Fatalf("buildOutboxUpdates after viewer invalidation: %v", err)
	}
	if users.sparseCalls != 3 || len(users.calls) != 0 {
		t.Fatalf("viewer invalidation calls = sparse %d scalar %+v, want 3/0", users.sparseCalls, users.calls)
	}
}

func TestOutboxProjectorBatchesPresenceOnceWithoutScalarFallback(t *testing.T) {
	const (
		senderA = int64(1000000201)
		senderB = int64(1000000202)
		viewerA = int64(1000000211)
		viewerB = int64(1000000212)
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{
		senderA: {ID: senderA, FirstName: "Sender A", LastSeenAt: 100},
		senderB: {ID: senderB, FirstName: "Sender B", LastSeenAt: 200},
	}}
	presence := &outboxBatchPresenceProvider{records: map[int64][]EdgeLocationRecord{
		senderA: {{UserID: senderA, ReceivesUpdates: true}},
	}}
	projector, err := NewOutboxProjector(Config{}, OutboxProjectionDeps{
		Users: users, Presence: presence,
	}, zaptest.NewLogger(t), clock.System)
	if err != nil {
		t.Fatalf("NewOutboxProjector: %v", err)
	}
	requests := []egress.OutboxUpdateRequest{
		outboxNewMessageRequest(viewerA, senderA, 41),
		outboxNewMessageRequest(viewerB, senderB, 42),
	}
	updates, err := projector.router.buildOutboxUpdates(context.Background(), requests)
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if presence.calls != 1 || !reflect.DeepEqual(presence.userIDs, []int64{senderA, senderB}) {
		t.Fatalf("batch presence calls=%d ids=%v, want one sorted union", presence.calls, presence.userIDs)
	}
	byA := tgUsersByIDForProjectionParity(updates[0].Users)[senderA]
	if byA == nil {
		t.Fatal("online sender missing from first update")
	}
	if _, ok := byA.Status.(*tg.UserStatusOnline); !ok {
		t.Fatalf("online sender status=%T, want UserStatusOnline", byA.Status)
	}
	byB := tgUsersByIDForProjectionParity(updates[1].Users)[senderB]
	if byB == nil {
		t.Fatal("offline sender missing from second update")
	}
	if status, ok := byB.Status.(*tg.UserStatusOffline); !ok || status.WasOnline != 200 {
		t.Fatalf("offline sender status=%#v, want was_online=200", byB.Status)
	}
}

func TestNewOutboxProjectorRequiresBatchPresence(t *testing.T) {
	users := &countingOutboxUsersService{users: map[int64]domain.User{}}
	if _, err := NewOutboxProjector(Config{}, OutboxProjectionDeps{Users: users}, zaptest.NewLogger(t), clock.System); !errors.Is(err, ErrOutboxPresenceBatchUnavailable) {
		t.Fatalf("NewOutboxProjector err=%v, want ErrOutboxPresenceBatchUnavailable", err)
	}
}

type outboxBatchPresenceProvider struct {
	calls   int
	userIDs []int64
	records map[int64][]EdgeLocationRecord
	err     error
}

func (p *outboxBatchPresenceProvider) UserLocationRecordsForUsers(_ context.Context, userIDs []int64) (map[int64][]EdgeLocationRecord, error) {
	p.calls++
	p.userIDs = append([]int64(nil), userIDs...)
	if p.err != nil {
		return nil, p.err
	}
	out := make(map[int64][]EdgeLocationRecord, len(userIDs))
	for _, userID := range userIDs {
		out[userID] = append([]EdgeLocationRecord(nil), p.records[userID]...)
	}
	return out, nil
}

func outboxNewMessageRequest(viewerID, senderID int64, pts int) egress.OutboxUpdateRequest {
	message := domain.Message{
		ID: pts, OwnerUserID: viewerID,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: senderID},
		From: domain.Peer{Type: domain.PeerTypeUser, ID: senderID},
		Date: 1700000000 + pts, Body: "presence batch", Pts: pts,
	}
	return egress.OutboxUpdateRequest{
		TargetUserID: viewerID,
		Event: domain.UpdateEvent{
			UserID: viewerID, Type: domain.UpdateEventNewMessage,
			Pts: pts, PtsCount: 1, Date: message.Date, Message: message,
		},
	}
}

func TestRouterBuildOutboxUpdatesReplacesRawOnlyUserEnvelopeWithoutScalarFallback(t *testing.T) {
	const (
		senderUserID  = int64(1000000101)
		rawOnlyUserID = int64(1000000102)
		viewerUserID  = int64(1000000103)
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{
		senderUserID:  {ID: senderUserID, FirstName: "Projected sender"},
		rawOnlyUserID: {ID: rawOnlyUserID, FirstName: "Projected raw-only"},
	}}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	message := domain.Message{
		ID:          31,
		OwnerUserID: viewerUserID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
		Date:        1700000500,
		Pts:         31,
	}

	updates, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: viewerUserID,
		Event: domain.UpdateEvent{
			UserID: viewerUserID, Type: domain.UpdateEventNewMessage,
			Pts: message.Pts, PtsCount: 1, Date: message.Date, Message: message,
			Users: []domain.User{
				{ID: senderUserID, AccessHash: 9001, Phone: "raw-sender-phone", FirstName: "Raw sender"},
				{ID: rawOnlyUserID, AccessHash: 9002, Phone: "raw-only-phone", FirstName: "Raw only"},
			},
		},
	}})
	if err != nil || len(updates) != 1 || updates[0] == nil {
		t.Fatalf("buildOutboxUpdates = %+v, %v; want one update", updates, err)
	}
	projected := tgUsersByIDForProjectionParity(updates[0].Users)
	if len(projected) != 2 {
		t.Fatalf("projected users = %+v, want sender and raw-only user", updates[0].Users)
	}
	for id, wantName := range map[int64]string{
		senderUserID:  "Projected sender",
		rawOnlyUserID: "Projected raw-only",
	} {
		user := projected[id]
		if user == nil || user.FirstName != wantName {
			t.Fatalf("projected user %d = %#v, want name %q", id, user, wantName)
		}
		if user.Phone != "" || user.AccessHash != 0 {
			t.Fatalf("raw account fields leaked for user %d: phone=%q access_hash=%d", id, user.Phone, user.AccessHash)
		}
	}
	if users.sparseCalls != 1 || len(users.calls) != 0 {
		t.Fatalf("user projection calls = sparse %d scalar %+v, want one sparse call and zero scalar fallbacks", users.sparseCalls, users.calls)
	}
	if !reflect.DeepEqual(users.sparseRequest[viewerUserID], []int64{senderUserID, rawOnlyUserID}) {
		t.Fatalf("sparse request = %+v, want viewer=%d ids=[%d %d]", users.sparseRequest, viewerUserID, senderUserID, rawOnlyUserID)
	}
}

func TestRouterBuildOutboxUpdatesProjectsUsernamesOncePerClaim(t *testing.T) {
	const (
		viewerUserID  = int64(1000000003)
		senderAUserID = int64(1000000001)
		senderBUserID = int64(1000000002)
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{
		senderAUserID: {ID: senderAUserID, FirstName: "Sender A", Username: "sender_a"},
		senderBUserID: {ID: senderBUserID, FirstName: "Sender B", Username: "sender_b"},
	}}
	registry := newFakeUsernameRegistry()
	registry.byPeer[domain.Peer{Type: domain.PeerTypeUser, ID: senderAUserID}] = []domain.Username{
		{Username: "sender_a", Editable: true, Active: true, SortOrder: 0},
		{Username: "sender_a_collectible", Active: true, SortOrder: 1, CollectibleID: 11},
	}
	registry.byPeer[domain.Peer{Type: domain.PeerTypeUser, ID: senderBUserID}] = []domain.Username{
		{Username: "sender_b", Editable: true, Active: true, SortOrder: 0},
		{Username: "sender_b_collectible", Active: true, SortOrder: 1, CollectibleID: 12},
	}
	router := New(Config{}, Deps{Users: users, Usernames: registry}, zaptest.NewLogger(t), clock.System)

	senderIDs := []int64{senderAUserID, senderBUserID, senderAUserID}
	requests := make([]egress.OutboxUpdateRequest, 0, len(senderIDs))
	for i, senderID := range senderIDs {
		msg := domain.Message{
			ID:          20 + i,
			OwnerUserID: viewerUserID,
			Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: senderID},
			From:        domain.Peer{Type: domain.PeerTypeUser, ID: senderID},
			Date:        1700000400 + i,
			Body:        "hello",
			Pts:         20 + i,
		}
		requests = append(requests, egress.OutboxUpdateRequest{
			TargetUserID: viewerUserID,
			Event: domain.UpdateEvent{
				UserID:   viewerUserID,
				Type:     domain.UpdateEventNewMessage,
				Pts:      msg.Pts,
				PtsCount: 1,
				Date:     msg.Date,
				Message:  msg,
			},
		})
	}

	updates, err := router.buildOutboxUpdates(context.Background(), requests)
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if len(updates) != len(requests) {
		t.Fatalf("updates count = %d, want %d", len(updates), len(requests))
	}
	for i, update := range updates {
		if update == nil || len(update.Users) != 1 {
			t.Fatalf("updates[%d].Users = %+v, want one sender", i, update)
		}
		user, ok := update.Users[0].(*tg.User)
		if !ok {
			t.Fatalf("updates[%d].Users[0] = %T, want *tg.User", i, update.Users[0])
		}
		wantScalar := "sender_a"
		wantCollectible := "sender_a_collectible"
		if senderIDs[i] == senderBUserID {
			wantScalar = "sender_b"
			wantCollectible = "sender_b_collectible"
		}
		assertVectorOnlyUsernames(t, fmt.Sprintf("updates[%d]", i), user, []string{wantScalar, wantCollectible})
	}
	if registry.batchCalls != 1 || registry.peerCalls != 0 {
		t.Fatalf("registry reads = batch %d / peer %d, want one batch read for the whole claim", registry.batchCalls, registry.peerCalls)
	}
}

func TestRouterBuildOutboxUpdatesSeparatesViewerCache(t *testing.T) {
	const (
		senderUserID       = int64(1000000001)
		secondSenderUserID = int64(1000000004)
	)
	users := &viewerSpecificOutboxUsersService{}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	requests := []egress.OutboxUpdateRequest{
		{
			TargetUserID: 1000000002,
			Event: domain.UpdateEvent{
				UserID:   1000000002,
				Type:     domain.UpdateEventNewMessage,
				Pts:      7,
				PtsCount: 1,
				Date:     1700000307,
				Message: domain.Message{
					ID:          10,
					OwnerUserID: 1000000002,
					Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
					From:        domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
					Date:        1700000307,
					Pts:         7,
				},
			},
		},
		{
			TargetUserID: 1000000003,
			Event: domain.UpdateEvent{
				UserID:   1000000003,
				Type:     domain.UpdateEventNewMessage,
				Pts:      8,
				PtsCount: 1,
				Date:     1700000308,
				Message: domain.Message{
					ID:          11,
					OwnerUserID: 1000000003,
					Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: secondSenderUserID},
					From:        domain.Peer{Type: domain.PeerTypeUser, ID: secondSenderUserID},
					Date:        1700000308,
					Pts:         8,
				},
			},
		},
	}

	updates, err := router.buildOutboxUpdates(context.Background(), requests)
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if len(updates) != 2 || updates[0] == nil || updates[1] == nil {
		t.Fatalf("updates = %+v, want two updates", updates)
	}
	firstUser, ok := updates[0].Users[0].(*tg.User)
	if !ok {
		t.Fatalf("updates[0].Users[0] = %T, want *tg.User", updates[0].Users[0])
	}
	secondUser, ok := updates[1].Users[0].(*tg.User)
	if !ok {
		t.Fatalf("updates[1].Users[0] = %T, want *tg.User", updates[1].Users[0])
	}
	if firstUser.FirstName != "viewer2" || secondUser.FirstName != "viewer3" {
		t.Fatalf("projected users = %q/%q, want viewer-specific names", firstUser.FirstName, secondUser.FirstName)
	}
	if users.sparseCalls != 1 || len(users.calls) != 0 {
		t.Fatalf("user projection calls = sparse %d scalar %+v, want one sparse call", users.sparseCalls, users.calls)
	}
	if !reflect.DeepEqual(users.sparseRequest[1000000002], []int64{senderUserID}) || !reflect.DeepEqual(users.sparseRequest[1000000003], []int64{secondSenderUserID}) {
		t.Fatalf("sparse request = %+v, want only each viewer's sender", users.sparseRequest)
	}
}

func TestOnlineOutboxAndOfflineDifferenceKeepEventPtsAndViewerUserEnvelopeEquivalent(t *testing.T) {
	const (
		viewerID = int64(1000000041)
		peerID   = int64(1000000042)
		eventPts = 73
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{
		viewerID: {ID: viewerID, AccessHash: 4101, Phone: "15550000041", FirstName: "Me"},
		peerID: {
			ID: peerID, AccessHash: 4201, Phone: "15559990042",
			FirstName: "Viewer alias", LastName: "Projected", Contact: true,
			PhotoID: 9042, PhotoDCID: 4, PhotoStripped: []byte{1, 2, 3}, PhotoPersonal: true,
			Status: domain.UserStatus{Kind: domain.UserStatusOnline, Expires: 1700000999},
		},
	}}
	message := domain.Message{
		ID: 19, OwnerUserID: viewerID, Out: true,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: peerID},
		From: domain.Peer{Type: domain.PeerTypeUser, ID: viewerID},
		Date: 1700000900, Body: "same durable event",
	}
	event := domain.UpdateEvent{
		UserID: viewerID, Type: domain.UpdateEventNewMessage,
		Pts: eventPts, PtsCount: 1, Date: message.Date, Message: message,
	}
	state := domain.UpdateState{Pts: eventPts, Date: message.Date}
	updatesStore := &captureUpdates{
		state:        state,
		currentState: &state,
		difference:   &domain.UpdateDifference{State: state, Events: []domain.UpdateEvent{event}},
	}
	router := New(Config{}, Deps{Users: users, Updates: updatesStore}, zaptest.NewLogger(t), clock.System)

	online, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: viewerID,
		Event:        event,
	}})
	if err != nil || len(online) != 1 || online[0] == nil {
		t.Fatalf("online outbox=%+v err=%v, want one update", online, err)
	}
	var onlineNew *tg.UpdateNewMessage
	for _, update := range online[0].Updates {
		if candidate, ok := update.(*tg.UpdateNewMessage); ok {
			onlineNew = candidate
			break
		}
	}
	if onlineNew == nil || onlineNew.Pts != event.Pts || onlineNew.PtsCount != event.PtsCount {
		t.Fatalf("online update=%#v, want pts=%d pts_count=%d", onlineNew, event.Pts, event.PtsCount)
	}

	ctx := WithUserID(WithAuthKeyID(context.Background(), [8]byte{4, 1}), viewerID)
	offlineClass, err := router.onUpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{
		Pts:  event.Pts - event.PtsCount,
		Date: message.Date - 1,
	})
	if err != nil {
		t.Fatalf("offline difference: %v", err)
	}
	offline, ok := offlineClass.(*tg.UpdatesDifference)
	if !ok || offline.State.Pts != event.Pts || len(offline.NewMessages) != 1 {
		t.Fatalf("offline=%#v, want one message ending at pts=%d", offlineClass, event.Pts)
	}
	if updatesStore.observedCalls != 1 || updatesStore.observedRequest.Pts+onlineNew.PtsCount != offline.State.Pts {
		t.Fatalf("difference observed=%+v calls=%d, want request pts plus event pts_count to reach %d", updatesStore.observedRequest, updatesStore.observedCalls, offline.State.Pts)
	}
	if !reflect.DeepEqual(onlineNew.Message, offline.NewMessages[0]) {
		t.Fatalf("online/offline message mismatch:\nonline=%#v\noffline=%#v", onlineNew.Message, offline.NewMessages[0])
	}
	onlineUsers := tgUsersByIDForProjectionParity(online[0].Users)
	offlineUsers := tgUsersByIDForProjectionParity(offline.Users)
	for _, id := range []int64{viewerID, peerID} {
		if !reflect.DeepEqual(onlineUsers[id], offlineUsers[id]) {
			t.Fatalf("user %d projection mismatch:\nonline=%#v\noffline=%#v", id, onlineUsers[id], offlineUsers[id])
		}
	}
	if onlineUsers[viewerID] == nil || !onlineUsers[viewerID].Self {
		t.Fatalf("viewer user=%#v, want self in both paths", onlineUsers[viewerID])
	}
	peer := onlineUsers[peerID]
	if peer == nil || peer.FirstName != "Viewer alias" || peer.Phone != "15559990042" || !peer.Contact {
		t.Fatalf("peer projection=%#v, want alias/phone/contact overlay", peer)
	}
	photo, ok := peer.Photo.(*tg.UserProfilePhoto)
	if !ok || photo.PhotoID != 9042 || !photo.Personal {
		t.Fatalf("peer photo=%#v, want viewer personal photo 9042", peer.Photo)
	}
}

func tgUsersByIDForProjectionParity(users []tg.UserClass) map[int64]*tg.User {
	out := make(map[int64]*tg.User, len(users))
	for _, item := range users {
		if user, ok := item.(*tg.User); ok {
			out[user.ID] = user
		}
	}
	return out
}

func TestRouterBuildOutboxUpdatesSplitsOnlyMembershipCapacityErrors(t *testing.T) {
	const (
		viewerA = int64(1000000002)
		viewerB = int64(1000000003)
		senderA = int64(1000000011)
		senderB = int64(1000000012)
	)
	base := &countingOutboxUsersService{users: map[int64]domain.User{
		senderA: {ID: senderA, FirstName: "Sender A"},
		senderB: {ID: senderB, FirstName: "Sender B"},
	}}
	users := &capacitySplittingOutboxUsersService{countingOutboxUsersService: base, maxEdges: 1}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	requests := []egress.OutboxUpdateRequest{
		{TargetUserID: viewerA, Event: domain.UpdateEvent{
			UserID: viewerA, Type: domain.UpdateEventNewMessage, Pts: 7, PtsCount: 1,
			Message: domain.Message{ID: 10, OwnerUserID: viewerA,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: senderA},
				From: domain.Peer{Type: domain.PeerTypeUser, ID: senderA}},
		}},
		{TargetUserID: viewerB, Event: domain.UpdateEvent{
			UserID: viewerB, Type: domain.UpdateEventNewMessage, Pts: 8, PtsCount: 1,
			Message: domain.Message{ID: 11, OwnerUserID: viewerB,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: senderB},
				From: domain.Peer{Type: domain.PeerTypeUser, ID: senderB}},
		}},
	}
	updates, err := router.buildOutboxUpdates(context.Background(), requests)
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if len(updates) != 2 || updates[0] == nil || updates[1] == nil {
		t.Fatalf("updates = %+v, want two complete updates", updates)
	}
	first, firstOK := updates[0].Users[0].(*tg.User)
	second, secondOK := updates[1].Users[0].(*tg.User)
	if !firstOK || !secondOK || first.ID != senderA || second.ID != senderB {
		t.Fatalf("projected users = %#v / %#v, want senders %d / %d", updates[0].Users, updates[1].Users, senderA, senderB)
	}
	if len(users.sparseRequests) != 3 {
		t.Fatalf("sparse calls = %d, want full failure plus two split batches", len(users.sparseRequests))
	}
	for i, request := range users.sparseRequests[1:] {
		if sparseOutboxEdgeCount(request) != 1 {
			t.Fatalf("split request %d = %+v, want one edge", i, request)
		}
	}
	if len(base.calls) != 0 {
		t.Fatalf("scalar ByIDs calls = %+v, want zero fallback", base.calls)
	}
}

func TestResolveSparseOutboxUsersDoesNotSplitOtherErrors(t *testing.T) {
	boom := errors.New("projection unavailable")
	resolver := &failingSparseOutboxResolver{err: boom}
	projected, err := resolveSparseOutboxUsers(context.Background(), resolver, map[int64][]int64{
		1001: {2001},
		1002: {2002},
	})
	if !errors.Is(err, boom) || projected != nil {
		t.Fatalf("resolveSparseOutboxUsers = %+v, %v; want nil, boom", projected, err)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want no split for non-capacity error", resolver.calls)
	}
}

func TestResolveSparseOutboxUsersSplitsEveryProjectionCapacityError(t *testing.T) {
	capacityErrors := map[string]error{
		"privacy_memberships": store.ErrActiveChannelMemberPairsLimit,
		"owner_union":         appusers.ErrBatchUsersLimit,
		"sparse_cells":        appusers.ErrBatchViewerCells,
	}
	for name, capacityErr := range capacityErrors {
		t.Run(name, func(t *testing.T) {
			base := &countingOutboxUsersService{users: map[int64]domain.User{
				2001: {ID: 2001, FirstName: "one"},
				2002: {ID: 2002, FirstName: "two"},
			}}
			resolver := &capacitySplittingOutboxUsersService{
				countingOutboxUsersService: base,
				maxEdges:                   1,
				capacityErr:                capacityErr,
			}
			projected, err := resolveSparseOutboxUsers(context.Background(), resolver, map[int64][]int64{
				1001: {2001},
				1002: {2002},
			})
			if err != nil || len(projected[1001]) != 1 || len(projected[1002]) != 1 {
				t.Fatalf("projected=%+v err=%v, want both split results", projected, err)
			}
			if len(resolver.sparseRequests) != 3 || len(base.calls) != 0 {
				t.Fatalf("sparse calls=%d scalar=%+v, want one failed batch plus two sparse halves", len(resolver.sparseRequests), base.calls)
			}
		})
	}
}

func TestRouterBuildOutboxUpdatesFailsClosedAtSparseRecoveryCallBudget(t *testing.T) {
	const viewerID = int64(1000000050)
	base := &countingOutboxUsersService{users: make(map[int64]domain.User)}
	peers := make([]domain.Peer, 100)
	for i := range peers {
		id := int64(1000001000 + i)
		peers[i] = domain.Peer{Type: domain.PeerTypeUser, ID: id}
		base.users[id] = domain.User{ID: id, FirstName: "projected"}
	}
	users := &capacitySplittingOutboxUsersService{
		countingOutboxUsersService: base,
		maxEdges:                   1,
		capacityErr:                store.ErrActiveChannelMemberPairsLimit,
	}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)

	updates, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: viewerID,
		Event: domain.UpdateEvent{
			UserID: viewerID,
			Type:   domain.UpdateEventPinnedDialogs,
			Peers:  peers,
		},
	}})
	if updates != nil || !errors.Is(err, ErrUserProjectionCapacityRecoveryLimit) {
		t.Fatalf("updates=%+v err=%v, want nil recovery-limit error", updates, err)
	}
	if errors.Is(err, store.ErrActiveChannelMemberPairsLimit) {
		t.Fatalf("recovery-limit error must not retain capacity identity: %v", err)
	}
	if len(users.sparseRequests) != maxSparseOutboxRecoveryCalls {
		t.Fatalf("sparse resolver calls=%d, want hard limit %d", len(users.sparseRequests), maxSparseOutboxRecoveryCalls)
	}
	if len(base.calls) != 0 {
		t.Fatalf("scalar ByIDs calls=%+v, want zero fallback", base.calls)
	}
}

func TestResolveSparseOutboxUsersRejectsAttemptedEdgeOverflowBeforeResolver(t *testing.T) {
	makeIDs := func(count int) []int64 {
		ids := make([]int64, count)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		return ids
	}

	t.Run("initial request", func(t *testing.T) {
		resolver := &failingSparseOutboxResolver{err: errors.New("must not be called")}
		projected, err := resolveSparseOutboxUsers(context.Background(), resolver, map[int64][]int64{
			1001: makeIDs(maxSparseOutboxAttemptedUserEdges + 1),
		})
		if projected != nil || !errors.Is(err, ErrUserProjectionCapacityRecoveryLimit) {
			t.Fatalf("projected=%+v err=%v, want nil recovery-limit error", projected, err)
		}
		if resolver.calls != 0 {
			t.Fatalf("resolver calls=%d, want rejection before first call", resolver.calls)
		}
		if isSparseProjectionCapacityError(err) {
			t.Fatalf("recovery-limit error must be terminal, got capacity identity: %v", err)
		}
	})

	t.Run("cumulative recursive edges", func(t *testing.T) {
		resolver := &failingSparseOutboxResolver{err: store.ErrActiveChannelMemberPairsLimit}
		projected, err := resolveSparseOutboxUsers(context.Background(), resolver, map[int64][]int64{
			1001: makeIDs(300000),
		})
		if projected != nil || !errors.Is(err, ErrUserProjectionCapacityRecoveryLimit) {
			t.Fatalf("projected=%+v err=%v, want nil recovery-limit error", projected, err)
		}
		if resolver.calls != 2 {
			t.Fatalf("resolver calls=%d, want root and first half before shared edge limit", resolver.calls)
		}
		if errors.Is(err, store.ErrActiveChannelMemberPairsLimit) {
			t.Fatalf("recovery-limit error must not retain capacity identity: %v", err)
		}
	})
}

func TestRouterBuildOutboxUpdatesRejectsIncompleteSparseProjection(t *testing.T) {
	const (
		viewerUserID  = int64(1000000010)
		missingUserID = int64(1000000011)
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{}}
	router := New(Config{}, Deps{Users: users}, zaptest.NewLogger(t), clock.System)
	updates, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: viewerUserID,
		Event: domain.UpdateEvent{UserID: viewerUserID, Type: domain.UpdateEventNewMessage, Pts: 3, PtsCount: 1,
			Message: domain.Message{ID: 3, OwnerUserID: viewerUserID,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: missingUserID},
				From: domain.Peer{Type: domain.PeerTypeUser, ID: missingUserID}}},
	}})
	if !errors.Is(err, ErrSparseOutboxUserProjectionIncomplete) {
		t.Fatalf("buildOutboxUpdates err = %v, want ErrSparseOutboxUserProjectionIncomplete", err)
	}
	if updates != nil {
		t.Fatalf("updates = %+v, want fail-closed nil", updates)
	}
	if users.sparseCalls != 1 || len(users.calls) != 0 {
		t.Fatalf("projection calls = sparse %d scalar %+v, want one sparse call and no fallback", users.sparseCalls, users.calls)
	}
}

func TestRouterBuildOutboxUpdatesAllowsOnlyExplicitNoopToStayEmpty(t *testing.T) {
	router := New(Config{}, Deps{}, zaptest.NewLogger(t), clock.System)
	updates, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: 1000000030,
		Event:        domain.UpdateEvent{UserID: 1000000030, Type: domain.UpdateEventNoop, Pts: 9, PtsCount: 1},
	}})
	if err != nil || len(updates) != 1 || updates[0] != nil {
		t.Fatalf("explicit noop updates=%+v err=%v, want one nil entry without error", updates, err)
	}

	updates, err = router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: 1000000030,
		Event:        domain.UpdateEvent{UserID: 1000000030, Type: domain.UpdateEventReadHistoryInbox, Pts: 10, PtsCount: 1},
	}})
	if !errors.Is(err, ErrOutboxUpdateProjectionEmpty) || updates != nil {
		t.Fatalf("invalid non-noop updates=%+v err=%v, want ErrOutboxUpdateProjectionEmpty", updates, err)
	}
}

func TestRouterBuildOutboxUpdatesPreparesPollBeforeSparseProjection(t *testing.T) {
	const (
		viewerUserID   = int64(1000000020)
		reloadedUserID = int64(1000000021)
	)
	users := &countingOutboxUsersService{users: map[int64]domain.User{
		reloadedUserID: {ID: reloadedUserID, FirstName: "Reloaded sender"},
	}}
	messages := &captureMessages{list: domain.MessageList{Messages: []domain.Message{{
		ID: 44, OwnerUserID: viewerUserID,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: reloadedUserID},
		From: domain.Peer{Type: domain.PeerTypeUser, ID: reloadedUserID},
		Media: &domain.MessageMedia{Kind: domain.MessageMediaKindPoll, Poll: &domain.MessagePoll{
			ID: 9001, Question: "q", Answers: []domain.MessagePollAnswer{{Text: "a", Option: []byte{1}}},
		}},
	}}}}
	router := New(Config{}, Deps{Users: users, Messages: messages}, zaptest.NewLogger(t), clock.System)
	_, err := router.buildOutboxUpdates(context.Background(), []egress.OutboxUpdateRequest{{
		TargetUserID: viewerUserID,
		Event: domain.UpdateEvent{UserID: viewerUserID, Type: domain.UpdateEventMessagePoll, Pts: 4, PtsCount: 1,
			Message: domain.Message{ID: 44, OwnerUserID: viewerUserID,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1000000099}}},
	}})
	if err != nil {
		t.Fatalf("buildOutboxUpdates: %v", err)
	}
	if messages.getMessagesCalls != 1 {
		t.Fatalf("GetMessages calls = %d, want one prepare read", messages.getMessagesCalls)
	}
	if !reflect.DeepEqual(users.sparseRequest[viewerUserID], []int64{reloadedUserID}) || len(users.calls) != 0 {
		t.Fatalf("projection request = %+v scalar=%+v, want authoritative poll sender only", users.sparseRequest, users.calls)
	}
}

func TestChannelMessageUpdatesIncludesActionUsers(t *testing.T) {
	const (
		viewerUserID = int64(1000000002)
		senderUserID = int64(1000000001)
		actionUserID = int64(1000000003)
		channelID    = int64(2000000001)
		messageID    = 41
		messageDate  = 1700000330
		messagePts   = 17
	)
	msg := domain.ChannelMessage{
		ID:           messageID,
		ChannelID:    channelID,
		SenderUserID: senderUserID,
		From:         domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
		Date:         messageDate,
		Action: &domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatAddUser,
			UserIDs: []int64{actionUserID},
		},
		Pts: messagePts,
	}
	router := New(Config{}, Deps{
		Users: mapUsersService{users: map[int64]domain.User{
			senderUserID: {ID: senderUserID, FirstName: "Sender"},
			actionUserID: {ID: actionUserID, FirstName: "Invitee", PhotoID: 9302, PhotoDCID: 2},
		}},
	}, zaptest.NewLogger(t), clock.System)

	updates := router.channelMessageUpdatesWithPeerCache(context.Background(), viewerUserID, domain.SendChannelMessageResult{
		Channel: domain.Channel{ID: channelID, AccessHash: 44, Title: "Group", Megagroup: true, Date: messageDate},
		Message: msg,
		Event: domain.ChannelUpdateEvent{
			ChannelID: channelID,
			Type:      domain.ChannelUpdateNewMessage,
			Pts:       messagePts,
			PtsCount:  1,
			Date:      messageDate,
			Message:   msg,
		},
	}, 0, newViewerPeerCache(router))
	got := map[int64]*tg.User{}
	for _, user := range updates.Users {
		if u, ok := user.(*tg.User); ok {
			got[u.ID] = u
		}
	}
	if _, ok := got[senderUserID]; !ok {
		t.Fatalf("sender user missing from updates.users: %+v", updates.Users)
	}
	actionUser, ok := got[actionUserID]
	if !ok {
		t.Fatalf("action user missing from updates.users: %+v", updates.Users)
	}
	if photo, ok := actionUser.Photo.(*tg.UserProfilePhoto); !ok || photo.PhotoID != 9302 {
		t.Fatalf("action user photo = %#v, want projected profile photo", actionUser.Photo)
	}
}

type outboxUsersCall struct {
	viewerUserID int64
	ids          []int64
}

func sameOutboxUsersCalls(got, want []outboxUsersCall) bool {
	if len(got) != len(want) {
		return false
	}
	used := make([]bool, len(want))
	for _, call := range got {
		found := false
		for i, expected := range want {
			if used[i] || call.viewerUserID != expected.viewerUserID || !reflect.DeepEqual(call.ids, expected.ids) {
				continue
			}
			used[i] = true
			found = true
			break
		}
		if !found {
			return false
		}
	}
	return true
}

type countingOutboxUsersService struct {
	users         map[int64]domain.User
	calls         []outboxUsersCall
	sparseCalls   int
	sparseRequest map[int64][]int64
}

type capacitySplittingOutboxUsersService struct {
	*countingOutboxUsersService
	maxEdges       int
	capacityErr    error
	sparseRequests []map[int64][]int64
}

type failingSparseOutboxResolver struct {
	calls int
	err   error
}

func (r *failingSparseOutboxResolver) ByIDsForViewerUserIDs(context.Context, map[int64][]int64) (map[int64][]domain.User, error) {
	r.calls++
	return nil, r.err
}

func (s *capacitySplittingOutboxUsersService) ByIDsForViewerUserIDs(_ context.Context, requested map[int64][]int64) (map[int64][]domain.User, error) {
	s.sparseRequests = append(s.sparseRequests, cloneOutboxSparseRequest(requested))
	if sparseOutboxEdgeCount(requested) > s.maxEdges {
		capacityErr := s.capacityErr
		if capacityErr == nil {
			capacityErr = store.ErrActiveChannelMemberPairsLimit
		}
		return nil, fmt.Errorf("%w: test capacity", capacityErr)
	}
	out := make(map[int64][]domain.User, len(requested))
	for viewerID, ids := range requested {
		for _, id := range ids {
			if user, ok := s.users[id]; ok {
				out[viewerID] = append(out[viewerID], user)
			}
		}
	}
	return out, nil
}

func sparseOutboxEdgeCount(requested map[int64][]int64) int {
	total := 0
	for _, ids := range requested {
		total += len(ids)
	}
	return total
}

func (s *countingOutboxUsersService) ByIDsForViewerUserIDs(_ context.Context, requested map[int64][]int64) (map[int64][]domain.User, error) {
	s.sparseCalls++
	s.sparseRequest = cloneOutboxSparseRequest(requested)
	out := make(map[int64][]domain.User, len(requested))
	for viewerID, ids := range requested {
		for _, id := range ids {
			if user, ok := s.users[id]; ok {
				out[viewerID] = append(out[viewerID], user)
			}
		}
	}
	return out, nil
}

func (s *countingOutboxUsersService) Self(_ context.Context, userID int64) (domain.User, error) {
	if u, ok := s.users[userID]; ok {
		return u, nil
	}
	return domain.User{}, nil
}

func (s *countingOutboxUsersService) ByID(_ context.Context, _ int64, userID int64) (domain.User, bool, error) {
	u, ok := s.users[userID]
	return u, ok, nil
}

func (s *countingOutboxUsersService) ByIDs(_ context.Context, viewerUserID int64, userIDs []int64) ([]domain.User, error) {
	s.calls = append(s.calls, outboxUsersCall{viewerUserID: viewerUserID, ids: append([]int64(nil), userIDs...)})
	out := make([]domain.User, 0, len(userIDs))
	for _, id := range userIDs {
		if u, ok := s.users[id]; ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func (s *countingOutboxUsersService) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	user := s.users[userID]
	user.ID = userID
	user = testUserWithEmojiStatus(user, status)
	return user, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s *countingOutboxUsersService) UpdatePersonalChannelWithDelivery(_ context.Context, userID, channelID int64, _ store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	user := s.users[userID]
	user.ID, user.PersonalChannelID = userID, channelID
	return user, nil
}

type viewerSpecificOutboxUsersService struct {
	calls         []outboxUsersCall
	sparseCalls   int
	sparseRequest map[int64][]int64
}

func (s *viewerSpecificOutboxUsersService) ByIDsForViewerUserIDs(_ context.Context, requested map[int64][]int64) (map[int64][]domain.User, error) {
	s.sparseCalls++
	s.sparseRequest = cloneOutboxSparseRequest(requested)
	out := make(map[int64][]domain.User, len(requested))
	for viewerID, ids := range requested {
		for _, id := range ids {
			out[viewerID] = append(out[viewerID], domain.User{ID: id, FirstName: viewerSpecificName(viewerID)})
		}
	}
	return out, nil
}

func cloneOutboxSparseRequest(in map[int64][]int64) map[int64][]int64 {
	out := make(map[int64][]int64, len(in))
	for viewerID, ids := range in {
		out[viewerID] = append([]int64(nil), ids...)
	}
	return out
}

func (s *viewerSpecificOutboxUsersService) Self(_ context.Context, userID int64) (domain.User, error) {
	return domain.User{ID: userID, FirstName: "self"}, nil
}

func (s *viewerSpecificOutboxUsersService) ByID(_ context.Context, currentUserID, userID int64) (domain.User, bool, error) {
	return domain.User{ID: userID, FirstName: viewerSpecificName(currentUserID)}, true, nil
}

func (s *viewerSpecificOutboxUsersService) ByIDs(_ context.Context, viewerUserID int64, userIDs []int64) ([]domain.User, error) {
	s.calls = append(s.calls, outboxUsersCall{viewerUserID: viewerUserID, ids: append([]int64(nil), userIDs...)})
	out := make([]domain.User, 0, len(userIDs))
	for _, userID := range userIDs {
		out = append(out, domain.User{ID: userID, FirstName: viewerSpecificName(viewerUserID)})
	}
	return out, nil
}

func (s *viewerSpecificOutboxUsersService) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	user := testUserWithEmojiStatus(domain.User{ID: userID}, status)
	return user, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s *viewerSpecificOutboxUsersService) UpdatePersonalChannelWithDelivery(_ context.Context, userID, channelID int64, _ store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	return domain.User{ID: userID, PersonalChannelID: channelID}, nil
}

func viewerSpecificName(viewerUserID int64) string {
	switch viewerUserID {
	case 1000000002:
		return "viewer2"
	case 1000000003:
		return "viewer3"
	default:
		return "viewer"
	}
}
