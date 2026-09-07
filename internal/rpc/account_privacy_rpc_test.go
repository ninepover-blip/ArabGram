package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	appprivacy "telesrv/internal/app/privacy"
	appupdates "telesrv/internal/app/updates"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestAccountPrivacyAllKeysRoundTripWithoutAdvancingPts(t *testing.T) {
	ctx := context.Background()
	const userID int64 = 8101
	authKeyID := [8]byte{8, 1}
	sessionID := int64(81)
	privacyStore := memory.NewPrivacyStore()
	delivery := attachPrivacyDeliveryOutbox(privacyStore)
	privacy := appprivacy.NewService(privacyStore, memory.NewContactStore())
	events := memory.NewUpdateEventStore()
	updates := appupdates.NewService(memory.NewUpdateStateStore(), events)
	sessions := &captureSessions{}
	router := New(Config{}, Deps{
		Privacy:        privacy,
		Updates:        updates,
		Sessions:       sessions,
		DeliveryOutbox: delivery,
	}, zaptest.NewLogger(t), clock.System)
	requestCtx := WithSessionID(WithAuthKeyID(WithUserID(ctx, userID), authKeyID), sessionID)

	keys := []struct {
		name   string
		input  tg.InputPrivacyKeyClass
		domain domain.PrivacyKey
		wire   func(tg.PrivacyKeyClass) bool
	}{
		{"status_timestamp", &tg.InputPrivacyKeyStatusTimestamp{}, domain.PrivacyKeyStatusTimestamp, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyStatusTimestamp); return ok }},
		{"chat_invite", &tg.InputPrivacyKeyChatInvite{}, domain.PrivacyKeyChatInvite, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyChatInvite); return ok }},
		{"phone_call", &tg.InputPrivacyKeyPhoneCall{}, domain.PrivacyKeyPhoneCall, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyPhoneCall); return ok }},
		{"phone_p2p", &tg.InputPrivacyKeyPhoneP2P{}, domain.PrivacyKeyPhoneP2P, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyPhoneP2P); return ok }},
		{"forwards", &tg.InputPrivacyKeyForwards{}, domain.PrivacyKeyForwards, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyForwards); return ok }},
		{"profile_photo", &tg.InputPrivacyKeyProfilePhoto{}, domain.PrivacyKeyProfilePhoto, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyProfilePhoto); return ok }},
		{"phone_number", &tg.InputPrivacyKeyPhoneNumber{}, domain.PrivacyKeyPhoneNumber, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyPhoneNumber); return ok }},
		{"added_by_phone", &tg.InputPrivacyKeyAddedByPhone{}, domain.PrivacyKeyAddedByPhone, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyAddedByPhone); return ok }},
		{"voice_messages", &tg.InputPrivacyKeyVoiceMessages{}, domain.PrivacyKeyVoiceMessages, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyVoiceMessages); return ok }},
		{"about", &tg.InputPrivacyKeyAbout{}, domain.PrivacyKeyAbout, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyAbout); return ok }},
		{"birthday", &tg.InputPrivacyKeyBirthday{}, domain.PrivacyKeyBirthday, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyBirthday); return ok }},
		{"star_gifts_auto_save", &tg.InputPrivacyKeyStarGiftsAutoSave{}, domain.PrivacyKeyStarGiftsAutoSave, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyStarGiftsAutoSave); return ok }},
		{"no_paid_messages", &tg.InputPrivacyKeyNoPaidMessages{}, domain.PrivacyKeyNoPaidMessages, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeyNoPaidMessages); return ok }},
		{"saved_music", &tg.InputPrivacyKeySavedMusic{}, domain.PrivacyKeySavedMusic, func(v tg.PrivacyKeyClass) bool { _, ok := v.(*tg.PrivacyKeySavedMusic); return ok }},
	}

	for i, test := range keys {
		t.Run(test.name, func(t *testing.T) {
			gotKey, ok := domainPrivacyKeyFromInput(test.input)
			if !ok || gotKey != test.domain {
				t.Fatalf("input key maps to %q/%v, want %q/true", gotKey, ok, test.domain)
			}
			if !test.wire(tgPrivacyKey(test.domain)) {
				t.Fatalf("domain key %q projected as %T", test.domain, tgPrivacyKey(test.domain))
			}
			set, err := router.onAccountSetPrivacy(requestCtx, &tg.AccountSetPrivacyRequest{
				Key:   test.input,
				Rules: []tg.InputPrivacyRuleClass{&tg.InputPrivacyValueDisallowAll{}},
			})
			if err != nil {
				t.Fatalf("setPrivacy: %v", err)
			}
			if len(set.Rules) != 1 {
				t.Fatalf("setPrivacy rules=%d, want 1", len(set.Rules))
			}
			if _, ok := set.Rules[0].(*tg.PrivacyValueDisallowAll); !ok {
				t.Fatalf("setPrivacy rule=%T, want disallowAll", set.Rules[0])
			}
			get, err := router.onAccountGetPrivacy(requestCtx, test.input)
			if err != nil {
				t.Fatalf("getPrivacy: %v", err)
			}
			if len(get.Rules) != 1 {
				t.Fatalf("getPrivacy rules=%d, want 1", len(get.Rules))
			}
			if _, ok := get.Rules[0].(*tg.PrivacyValueDisallowAll); !ok {
				t.Fatalf("getPrivacy rule=%T, want disallowAll", get.Rules[0])
			}
			items := delivery.Snapshot()
			if len(items) != i+1 {
				t.Fatalf("delivery outbox items=%d, want %d", len(items), i+1)
			}
			item := items[len(items)-1]
			if item.TargetUserID != userID || item.ExcludeAuthKeyID != authKeyID || item.ExcludeSessionID != sessionID {
				t.Fatalf("delivery outbox target/exclusion=%+v, want user %d excluding %x/%d", item, userID, authKeyID, sessionID)
			}
			pushed := lastQueuedDeliveryUpdates(t, delivery)
			if len(pushed.Updates) != 1 {
				t.Fatalf("delivery payload updates=%d, want one updatePrivacy", len(pushed.Updates))
			}
			privacyUpdate, ok := pushed.Updates[0].(*tg.UpdatePrivacy)
			if !ok {
				t.Fatalf("delivery payload update=%T, want updatePrivacy(%q)", pushed.Updates[0], test.domain)
			}
			if !test.wire(privacyUpdate.Key) {
				t.Fatalf("delivery payload key=%T, want %q", privacyUpdate.Key, test.domain)
			}
		})
	}
	if pushedUserIDs := sessions.pushedUserIDs(); len(pushedUserIDs) != 0 {
		t.Fatalf("direct online privacy pushes=%v, want durable delivery only", pushedUserIDs)
	}

	recorded, err := events.ListAfter(ctx, userID, 0, 100)
	if err != nil {
		t.Fatalf("list account update events: %v", err)
	}
	if len(recorded) != 0 {
		t.Fatalf("account update events=%+v, want none for privacy changes", recorded)
	}
	state, err := updates.CurrentState(ctx, userID)
	if err != nil {
		t.Fatalf("current update state: %v", err)
	}
	if state.Pts != 0 {
		t.Fatalf("privacy changes advanced pts to %d, want 0", state.Pts)
	}

	difference, err := updates.GetDifference(ctx, [8]byte{8, 2}, userID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("getDifference: %v", err)
	}
	if difference.State.Pts != 0 || len(difference.Events) != 0 {
		t.Fatalf("difference after privacy changes=%+v, want empty pts=0", difference)
	}

	// A real message-box update immediately after privacy changes must still
	// receive pts=1. This catches both hidden privacy allocations and gaps left
	// behind by synthetic bookkeeping events.
	message := domain.Message{
		ID:          1,
		OwnerUserID: userID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 8102},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 8102},
		Date:        1700000000,
		Body:        "after privacy",
	}
	event, state, err := updates.RecordNewMessage(ctx, authKeyID, userID, message)
	if err != nil {
		t.Fatalf("record adjacent message update: %v", err)
	}
	if event.Pts != 1 || event.PtsCount != 1 || state.Pts != 1 {
		t.Fatalf("adjacent message event/state=%+v/%+v, want first pts=1", event, state)
	}
	difference, err = updates.GetDifference(ctx, [8]byte{8, 2}, userID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("getDifference after message: %v", err)
	}
	if difference.State.Pts != 1 || len(difference.Events) != 1 || difference.Events[0].Type != domain.UpdateEventNewMessage {
		t.Fatalf("difference after adjacent message=%+v, want one contiguous new_message at pts=1", difference)
	}
}

func TestAccountSetPrivacyRequiresDeliveryOutbox(t *testing.T) {
	ctx := context.Background()
	const userID int64 = 8111
	privacyStore := memory.NewPrivacyStore()
	privacy := appprivacy.NewService(privacyStore, memory.NewContactStore())
	router := New(Config{}, Deps{
		Privacy: privacy,
	}, zaptest.NewLogger(t), clock.System)

	reqCtx := WithSessionID(WithAuthKeyID(WithUserID(ctx, userID), [8]byte{1, 1}), 11)
	if set, err := router.onAccountSetPrivacy(reqCtx, &tg.AccountSetPrivacyRequest{
		Key:   &tg.InputPrivacyKeyPhoneNumber{},
		Rules: []tg.InputPrivacyRuleClass{&tg.InputPrivacyValueDisallowAll{}},
	}); set != nil || err == nil {
		t.Fatalf("setPrivacy without delivery outbox = %T/%v, want failure", set, err)
	}
	if _, found, err := privacyStore.GetPrivacyRules(ctx, userID, domain.PrivacyKeyPhoneNumber); err != nil || found {
		t.Fatalf("privacy mutated without delivery outbox: found=%v err=%v", found, err)
	}
}

func TestAccountSetPrivacyRequiresPrivacyDeliveryAggregate(t *testing.T) {
	ctx := context.Background()
	const userID int64 = 8112
	privacyStore := memory.NewPrivacyStore()
	delivery := memory.NewDeliveryOutboxStore()
	privacy := appprivacy.NewService(privacyStore, memory.NewContactStore())
	router := New(Config{}, Deps{
		Privacy:        privacy,
		DeliveryOutbox: delivery,
	}, zaptest.NewLogger(t), clock.System)

	reqCtx := WithSessionID(WithAuthKeyID(WithUserID(ctx, userID), [8]byte{1, 2}), 12)
	if set, err := router.onAccountSetPrivacy(reqCtx, &tg.AccountSetPrivacyRequest{
		Key:   &tg.InputPrivacyKeyPhoneNumber{},
		Rules: []tg.InputPrivacyRuleClass{&tg.InputPrivacyValueDisallowAll{}},
	}); set != nil || err == nil {
		t.Fatalf("setPrivacy without aggregate delivery store = %T/%v, want failure", set, err)
	}
	if _, found, err := privacyStore.GetPrivacyRules(ctx, userID, domain.PrivacyKeyPhoneNumber); err != nil || found {
		t.Fatalf("privacy mutated without aggregate delivery store: found=%v err=%v", found, err)
	}
	if items := delivery.Snapshot(); len(items) != 0 {
		t.Fatalf("delivery outbox mutated without aggregate delivery store: %+v", items)
	}
}
