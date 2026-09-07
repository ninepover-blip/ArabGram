package edgecontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
)

func TestHandleSessionControlCommandAppliesSessionMutations(t *testing.T) {
	local := &captureFullController{}
	raw := [8]byte{1}
	business := [8]byte{2}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:         "cmd-bind-auth",
		Kind:              SessionControlBindAuthKeySession,
		RawAuthKeyID:      raw,
		SessionID:         10,
		BusinessAuthKeyID: business,
	})
	if ack.Error != "" || ack.Affected != 1 {
		t.Fatalf("bind auth ack = %+v", ack)
	}
	if local.boundAuthRaw != raw || local.boundAuthSession != 10 || local.boundAuthBusiness != business {
		t.Fatalf("bind auth captured raw=%v session=%d business=%v", local.boundAuthRaw, local.boundAuthSession, local.boundAuthBusiness)
	}

	ack = HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:    "cmd-bind-user",
		Kind:         SessionControlBindUser,
		RawAuthKeyID: raw,
		SessionID:    10,
		UserID:       42,
	})
	if ack.Error != "" || ack.Affected != 1 {
		t.Fatalf("bind user ack = %+v", ack)
	}
	if local.boundUserRaw != raw || local.boundUserSession != 10 || local.boundUserID != 42 {
		t.Fatalf("bind user captured raw=%v session=%d user=%d", local.boundUserRaw, local.boundUserSession, local.boundUserID)
	}

	ack = HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:       "cmd-receives",
		Kind:            SessionControlSetReceivesUpdates,
		RawAuthKeyID:    raw,
		SessionID:       10,
		ReceivesUpdates: true,
	})
	if ack.Error != "" || ack.Affected != 1 {
		t.Fatalf("receives ack = %+v", ack)
	}
	if local.receivesRaw != raw || local.receivesSession != 10 || !local.receives {
		t.Fatalf("receives captured raw=%v session=%d receives=%v", local.receivesRaw, local.receivesSession, local.receives)
	}

	ack = HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:    "cmd-layer",
		Kind:         SessionControlSetClientLayer,
		RawAuthKeyID: raw,
		SessionID:    10,
		Layer:        228,
	})
	if ack.Error != "" || ack.Affected != 1 {
		t.Fatalf("layer ack = %+v", ack)
	}
	if local.layerRaw != raw || local.layerSession != 10 || local.layer != 228 {
		t.Fatalf("layer captured raw=%v session=%d layer=%d", local.layerRaw, local.layerSession, local.layer)
	}
}

func TestHandleSessionControlCommandAppliesLifecycleFences(t *testing.T) {
	local := &captureFullController{activationBeginToken: 71, bootstrapBeginToken: 81}
	raw := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}

	activation := HandleSessionControlCommand(local, SessionControlCommand{
		Kind: SessionControlBeginUpdatesActivation, RawAuthKeyID: raw, SessionID: 10,
	})
	if activation.Error != "" || activation.Affected != 1 || activation.LifecycleToken != 71 {
		t.Fatalf("begin activation ack = %+v", activation)
	}
	ended := HandleSessionControlCommand(local, SessionControlCommand{
		Kind: SessionControlEndUpdatesActivation, RawAuthKeyID: raw, SessionID: 10, LifecycleToken: 71,
	})
	if ended.Error != "" || ended.Affected != 1 || local.activationEndToken != 71 {
		t.Fatalf("end activation ack=%+v token=%d", ended, local.activationEndToken)
	}

	bootstrap := HandleSessionControlCommand(local, SessionControlCommand{
		Kind: SessionControlBeginBootstrapProbe, RawAuthKeyID: raw, SessionID: 10,
	})
	if bootstrap.Error != "" || bootstrap.Affected != 1 || bootstrap.LifecycleToken != 81 {
		t.Fatalf("begin bootstrap ack = %+v", bootstrap)
	}
	completed := HandleSessionControlCommand(local, SessionControlCommand{
		Kind: SessionControlEndBootstrapProbe, RawAuthKeyID: raw, SessionID: 10,
		LifecycleToken: 81, LifecycleSuccess: true,
	})
	if completed.Error != "" || completed.Affected != 1 || local.bootstrapEndToken != 81 || !local.bootstrapSuccess {
		t.Fatalf("end bootstrap ack=%+v token=%d success=%v", completed, local.bootstrapEndToken, local.bootstrapSuccess)
	}
}

func TestSessionControlFabricRoutesLifecycleFenceToOwningEdge(t *testing.T) {
	raw := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {{InstanceID: "edge-a", RawAuthKeyID: raw, SessionID: 10, LocationRevision: 1}},
	}}
	bus := &captureSessionBus{affected: 1, lifecycleToken: 99}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core-a", Registry: registry, Bus: bus,
	})

	token, ok := fabric.BeginSessionBootstrapProbe(raw, 10)
	if !ok || token != 99 {
		t.Fatalf("begin bootstrap = (%d,%v), want (99,true)", token, ok)
	}
	fabric.EndSessionBootstrapProbe(raw, 10, token, true)
	if len(bus.sent) != 2 || bus.sent[0].target != "edge-a" ||
		bus.sent[0].cmd.Kind != SessionControlBeginBootstrapProbe ||
		bus.sent[1].cmd.Kind != SessionControlEndBootstrapProbe ||
		bus.sent[1].cmd.LifecycleToken != 99 || !bus.sent[1].cmd.LifecycleSuccess {
		t.Fatalf("lifecycle commands = %+v", bus.sent)
	}
}

func TestSessionControlFabricRoutesBindUserToOwningRemoteEdge(t *testing.T) {
	raw := [8]byte{1}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-a", RawAuthKeyID: raw, SessionID: 10},
			{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 11},
			{InstanceID: "core", RawAuthKeyID: raw, SessionID: 10},
		},
	}}
	bus := &captureSessionBus{affected: 1}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
		Bus:        bus,
	})

	fabric.BindUserForAuthKey(raw, 10, 42)

	if len(bus.sent) != 2 {
		t.Fatalf("sent commands = %d, want 2", len(bus.sent))
	}
	targets := map[string]bool{}
	for _, got := range bus.sent {
		targets[got.target] = true
		if got.cmd.Kind != SessionControlBindUser || got.cmd.RawAuthKeyID != raw || got.cmd.SessionID != 10 || got.cmd.UserID != 42 {
			t.Fatalf("command = %+v", got.cmd)
		}
	}
	if !targets["edge-a"] || !targets["edge-b"] {
		t.Fatalf("targets = %+v, want edge-a and edge-b", targets)
	}
}

func TestSessionControlFabricCloseBusinessAuthKeyUsesRawIndexFallback(t *testing.T) {
	business := [8]byte{5}
	registry := &captureLocationRegistry{
		business: map[[8]byte][]LocationRecord{
			business: {
				{InstanceID: "edge-b", BusinessAuthKeyID: business, RawAuthKeyID: business, SessionID: 21},
			},
		},
		raw: map[[8]byte][]LocationRecord{
			business: {
				{InstanceID: "edge-b", RawAuthKeyID: business, SessionID: 21},
				{InstanceID: "edge-c", RawAuthKeyID: business, SessionID: 31},
				{InstanceID: "edge-a", RawAuthKeyID: business, SessionID: 41},
			},
		},
	}
	bus := &captureSessionBus{affected: 1}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	affected := fabric.CloseSessionsForBusinessAuthKey(business)

	if affected != 2 {
		t.Fatalf("affected = %d, want two remote acks", affected)
	}
	if len(bus.sent) != 2 {
		t.Fatalf("sent commands = %d, want deduped edge-b and edge-c", len(bus.sent))
	}
	targets := map[string]bool{}
	for _, sent := range bus.sent {
		targets[sent.target] = true
		if sent.cmd.Kind != SessionControlCloseBusinessAuthKey || sent.cmd.AuthKeyID != business {
			t.Fatalf("command = %+v, want close business auth key %v", sent.cmd, business)
		}
	}
	if !targets["edge-b"] || !targets["edge-c"] || targets["edge-a"] {
		t.Fatalf("targets = %+v, want remote edge-b and edge-c only", targets)
	}
}

func TestSessionControlFabricRoutesRawLayerToRemoteEdges(t *testing.T) {
	raw := [8]byte{7}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-a", RawAuthKeyID: raw, SessionID: 10},
			{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 11},
			{InstanceID: "edge-a", RawAuthKeyID: raw, SessionID: 12},
		},
	}}
	bus := &captureSessionBus{affected: 3}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "edge-local",
		Registry:   registry,
		Bus:        bus,
	})

	affected := fabric.SeedInheritedLayerForRawAuthKey(raw, 227)

	if affected != 6 {
		t.Fatalf("affected = %d, want two remote acks * 3", affected)
	}
	if len(bus.sent) != 2 {
		t.Fatalf("remote commands = %d, want deduped 2", len(bus.sent))
	}
	for _, sent := range bus.sent {
		if sent.cmd.Kind != SessionControlSeedRawLayer || sent.cmd.RawAuthKeyID != raw || sent.cmd.Layer != 227 {
			t.Fatalf("remote seed command = %+v", sent.cmd)
		}
	}
}

func TestSessionControlFabricDoesNotCountErroredRemoteAck(t *testing.T) {
	raw := [8]byte{8}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 11},
		},
	}}
	bus := &captureSessionBus{affected: 3, ackErr: "remote mutation failed after partial work"}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "edge-local",
		Registry:   registry,
		Bus:        bus,
	})

	affected := fabric.SeedInheritedLayerForRawAuthKey(raw, 228)

	if affected != 0 {
		t.Fatalf("affected = %d, want no counted remote ack", affected)
	}
	if len(bus.sent) != 1 {
		t.Fatalf("remote commands = %d, want 1 attempted command", len(bus.sent))
	}
}

func TestSessionControlFabricDoesNotCountRemoteSendFailure(t *testing.T) {
	raw := [8]byte{9}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 11},
		},
	}}
	bus := &captureSessionBus{affected: 3, err: errors.New("redis publish timeout")}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "edge-local",
		Registry:   registry,
		Bus:        bus,
	})

	affected := fabric.SeedInheritedLayerForRawAuthKey(raw, 228)

	if affected != 0 {
		t.Fatalf("affected = %d, want no counted remote send failure", affected)
	}
	if len(bus.sent) != 1 {
		t.Fatalf("remote commands = %d, want 1 attempted command", len(bus.sent))
	}
}

func TestSessionControlFabricBoundedCloseReportsRemoteFailure(t *testing.T) {
	business := [8]byte{10}
	for _, test := range []struct {
		name    string
		bus     *captureSessionBus
		wantErr string
	}{
		{
			name:    "errored ack",
			bus:     &captureSessionBus{affected: 1, ackErr: "remote close failed"},
			wantErr: "remote close failed",
		},
		{
			name:    "send failure",
			bus:     &captureSessionBus{affected: 1, err: errors.New("redis publish timeout")},
			wantErr: "redis publish timeout",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := &captureLocationRegistry{business: map[[8]byte][]LocationRecord{
				business: {
					{InstanceID: "edge-b", BusinessAuthKeyID: business, RawAuthKeyID: business, SessionID: 11},
				},
			}}
			fabric := NewSessionControlFabric(SessionControlFabricConfig{
				InstanceID: "edge-a",
				Registry:   registry,
				Bus:        test.bus,
			})

			affected, err := fabric.CloseSessionsForBusinessAuthKeyBounded(context.Background(), business)
			if affected != 0 || err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("bounded close affected=%d err=%v, want affected=0 containing %q", affected, err, test.wantErr)
			}
			if len(test.bus.sent) != 1 {
				t.Fatalf("remote commands=%d, want one attempted close", len(test.bus.sent))
			}
		})
	}
}

func TestSessionControlFabricRoutesTransientPushToOwningRemoteEdge(t *testing.T) {
	userID := int64(42)
	excludeRaw := [8]byte{9}
	registry := &captureLocationRegistry{users: map[int64][]LocationRecord{
		userID: {
			{InstanceID: "edge-a", UserID: userID, RawAuthKeyID: excludeRaw, SessionID: 10, ReceivesUpdates: true},
			{InstanceID: "edge-b", UserID: userID, RawAuthKeyID: [8]byte{2}, SessionID: 20, ReceivesUpdates: true},
			{InstanceID: "edge-c", UserID: userID, RawAuthKeyID: [8]byte{3}, SessionID: 30, ReceivesUpdates: false},
			{InstanceID: "core", UserID: userID, RawAuthKeyID: [8]byte{4}, SessionID: 40, ReceivesUpdates: true},
		},
	}}
	bus := &captureSessionBus{affected: 2}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
		Bus:        bus,
	})
	update := &tg.UpdateShort{Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}}, Date: 1}

	sent, err := fabric.PushToUserTransientExceptAuthKeySession(context.Background(), userID, excludeRaw, 10, proto.MessageFromServer, update, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("PushToUserTransientExceptAuthKeySession: %v", err)
	}
	if sent != 2 {
		t.Fatalf("sent = %d, want remote ack count 2", sent)
	}
	if len(bus.sent) != 1 {
		t.Fatalf("commands = %d, want only owning remote Edge", len(bus.sent))
	}
	got := bus.sent[0]
	if got.target != "edge-b" {
		t.Fatalf("target = %q, want edge-b", got.target)
	}
	if got.cmd.Kind != SessionControlPushUserTransient ||
		got.cmd.TargetUserID != userID ||
		got.cmd.RawAuthKeyID != excludeRaw ||
		got.cmd.ExceptSessionID != 10 ||
		got.cmd.MessageType != proto.MessageFromServer ||
		got.cmd.DeliveryTimeout != 50*time.Millisecond {
		t.Fatalf("command = %+v", got.cmd)
	}
	if decoded, err := DecodeDeliveryUpdate(got.cmd.UpdateBytes); err != nil {
		t.Fatalf("decode live push update: %v", err)
	} else if _, ok := decoded.(*tg.UpdateShort); !ok {
		t.Fatalf("decoded update = %T, want *tg.UpdateShort", decoded)
	}
}

func TestHandleSessionControlCommandAppliesTransientPush(t *testing.T) {
	local := &captureFullController{}
	updateBytes, err := EncodeDeliveryUpdate(&tg.UpdateShort{
		Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}},
		Date:   1,
	})
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	excludeRaw := [8]byte{7}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:       "cmd-push-transient",
		Kind:            SessionControlPushUserTransient,
		TargetUserID:    42,
		RawAuthKeyID:    excludeRaw,
		ExceptSessionID: 55,
		MessageType:     proto.MessageFromServer,
		UpdateBytes:     updateBytes,
		DeliveryTimeout: 25 * time.Millisecond,
	})
	if ack.Error != "" || ack.Affected != 3 {
		t.Fatalf("transient push ack = %+v", ack)
	}
	if local.pushKind != SessionControlPushUserTransient ||
		local.pushUserID != 42 ||
		local.pushRaw != excludeRaw ||
		local.pushSession != 55 ||
		local.pushType != proto.MessageFromServer ||
		local.pushTimeout != 25*time.Millisecond {
		t.Fatalf("local push capture kind=%s user=%d raw=%x session=%d type=%v timeout=%s",
			local.pushKind, local.pushUserID, local.pushRaw, local.pushSession, local.pushType, local.pushTimeout)
	}
	if _, ok := local.pushMessage.(*tg.UpdateShort); !ok {
		t.Fatalf("local push message = %T, want *tg.UpdateShort", local.pushMessage)
	}
}

func TestSessionControlFabricRoutesBusinessAuthKeyPushToOwningRemoteEdges(t *testing.T) {
	userID := int64(42)
	business := [8]byte{5}
	registry := &captureLocationRegistry{business: map[[8]byte][]LocationRecord{
		business: {
			{InstanceID: "edge-a", UserID: userID, BusinessAuthKeyID: business, RawAuthKeyID: [8]byte{1}, SessionID: 10, ReceivesUpdates: true, Layer: 228},
			{InstanceID: "edge-b", UserID: userID, BusinessAuthKeyID: business, RawAuthKeyID: [8]byte{2}, SessionID: 20, ReceivesUpdates: true, Layer: 228},
			{InstanceID: "edge-c", UserID: userID, BusinessAuthKeyID: business, RawAuthKeyID: [8]byte{3}, SessionID: 30, ReceivesUpdates: false, Layer: 228},
			{InstanceID: "edge-d", UserID: userID + 1, BusinessAuthKeyID: business, RawAuthKeyID: [8]byte{4}, SessionID: 40, ReceivesUpdates: true, Layer: 228},
			{InstanceID: "edge-e", UserID: userID, RawAuthKeyID: business, SessionID: 50, ReceivesUpdates: true, Layer: 227},
		},
	}}
	bus := &captureSessionBus{affected: 1}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
		Bus:        bus,
	})
	update := &tg.UpdateShort{Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}}, Date: 1}

	sent, err := fabric.PushToUserAuthKeyTransientAtLeastLayer(context.Background(), userID, business, 228, proto.MessageFromServer, update, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("PushToUserAuthKeyTransientAtLeastLayer: %v", err)
	}
	if sent != 2 {
		t.Fatalf("sent = %d, want edge-a and edge-b acks", sent)
	}
	if len(bus.sent) != 2 {
		t.Fatalf("commands = %d, want two eligible owning Edges", len(bus.sent))
	}
	targets := map[string]bool{}
	for _, got := range bus.sent {
		targets[got.target] = true
		if got.cmd.Kind != SessionControlPushUserAuthKeyTransientAtLeastLayer ||
			got.cmd.TargetUserID != userID ||
			got.cmd.BusinessAuthKeyID != business ||
			got.cmd.Layer != 228 ||
			got.cmd.DeliveryTimeout != 40*time.Millisecond {
			t.Fatalf("command = %+v", got.cmd)
		}
	}
	if !targets["edge-a"] || !targets["edge-b"] || targets["edge-c"] || targets["edge-d"] || targets["edge-e"] {
		t.Fatalf("targets = %+v, want edge-a and edge-b only", targets)
	}
}

func TestSessionControlFabricRoutesSemanticTransientOnlyToCompatibleProfiles(t *testing.T) {
	const userID int64 = 42
	registry := &captureLocationRegistry{users: map[int64][]LocationRecord{
		userID: {
			{InstanceID: "edge-227", UserID: userID, ReceivesUpdates: true, Layer: 227},
			{InstanceID: "edge-228", UserID: userID, ReceivesUpdates: true, Layer: 228},
			{InstanceID: "edge-229", UserID: userID, ReceivesUpdates: true, Layer: 229},
			{InstanceID: "edge-unknown", UserID: userID, ReceivesUpdates: true},
		},
	}}
	bus := &captureSessionBus{affected: 1}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{InstanceID: "egress", Registry: registry, Bus: bus})
	update := &tg.UpdateShort{Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}}, Date: 1}

	sent, err := fabric.PushToUserTransientCompatible(
		context.Background(), userID, tlprofile.SemanticTypeUpdateNewEphemeralMessage,
		proto.MessageFromServer, update, 40*time.Millisecond,
	)
	if err != nil || sent != 2 {
		t.Fatalf("semantic push sent=%d err=%v", sent, err)
	}
	if len(bus.sent) != 2 {
		t.Fatalf("targets=%+v, want exact compatible profiles only", bus.sent)
	}
	targets := map[string]bool{}
	for _, sent := range bus.sent {
		targets[sent.target] = true
		cmd := sent.cmd
		if cmd.Semantic != tlprofile.SemanticTypeUpdateNewEphemeralMessage || cmd.Layer != 0 || cmd.Kind != SessionControlPushUserTransientAtLeastLayer {
			t.Fatalf("semantic command=%+v", cmd)
		}
	}
	if !targets["edge-228"] || !targets["edge-229"] || targets["edge-227"] || targets["edge-unknown"] {
		t.Fatalf("targets=%+v, want edge-228 and edge-229", targets)
	}
}

func TestHandleSessionControlCommandAppliesSemanticTransientPush(t *testing.T) {
	local := &captureFullController{}
	updateBytes, err := EncodeDeliveryUpdate(&tg.UpdateShort{Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}}, Date: 1})
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID: "semantic-welcome", Kind: SessionControlPushUserTransientAtLeastLayer,
		TargetUserID: 42, Semantic: tlprofile.SemanticTypeUpdateNewEphemeralMessage,
		MessageType: proto.MessageFromServer, UpdateBytes: updateBytes, DeliveryTimeout: 25 * time.Millisecond,
	})
	if ack.Error != "" || ack.Affected != 4 {
		t.Fatalf("semantic push ack=%+v", ack)
	}
	if local.pushSemantic != tlprofile.SemanticTypeUpdateNewEphemeralMessage || local.pushUserID != 42 {
		t.Fatalf("semantic capture=%#x user=%d", local.pushSemantic, local.pushUserID)
	}
}

func TestHandleSessionControlCommandAppliesBusinessAuthKeyPush(t *testing.T) {
	local := &captureFullController{}
	business := [8]byte{6}
	updateBytes, err := EncodeDeliveryUpdate(&tg.UpdateShort{
		Update: &tg.UpdateUserTyping{UserID: 1001, Action: &tg.SendMessageTypingAction{}},
		Date:   1,
	})
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:         "cmd-push-business",
		Kind:              SessionControlPushUserAuthKeyTransientAtLeastLayer,
		TargetUserID:      42,
		BusinessAuthKeyID: business,
		Layer:             228,
		MessageType:       proto.MessageFromServer,
		UpdateBytes:       updateBytes,
		DeliveryTimeout:   25 * time.Millisecond,
	})
	if ack.Error != "" || ack.Affected != 4 {
		t.Fatalf("business push ack = %+v", ack)
	}
	if local.pushKind != SessionControlPushUserAuthKeyTransientAtLeastLayer ||
		local.pushUserID != 42 ||
		local.pushBusiness != business ||
		local.pushLayer != 228 ||
		local.pushTimeout != 25*time.Millisecond {
		t.Fatalf("local business push capture kind=%s user=%d business=%x layer=%d timeout=%s",
			local.pushKind, local.pushUserID, local.pushBusiness, local.pushLayer, local.pushTimeout)
	}
}

func TestSessionControlFabricOnlineUserIDsUseLocationRegistry(t *testing.T) {
	registry := &captureLocationRegistry{users: map[int64][]LocationRecord{
		10: {{InstanceID: "edge-a", UserID: 10, ReceivesUpdates: true}},
		20: {{InstanceID: "edge-b", UserID: 20, ReceivesUpdates: false}},
		30: {{InstanceID: "edge-c", UserID: 30, ReceivesUpdates: true}},
	}}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
	})

	if !fabric.IsUserOnline(10) {
		t.Fatal("user 10 should be online via registry")
	}
	if fabric.IsUserOnline(20) {
		t.Fatal("user 20 has no receives-updates session and should be offline")
	}
	got := fabric.OnlineUserIDsForCandidates([]int64{20, 10, 10, 30}, 1)
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("online candidates limit = %v, want [10]", got)
	}
	got = fabric.OnlineUserIDsForCandidates([]int64{20, 10, 10, 30}, 0)
	if len(got) != 2 || got[0] != 10 || got[1] != 30 {
		t.Fatalf("online candidates = %v, want [10 30]", got)
	}
	if registry.listUsersCalls != 2 {
		t.Fatalf("ListUsers calls = %d, want 2 (candidate checks must be batched)", registry.listUsersCalls)
	}
}

func TestSessionControlFabricRoutesChannelStateAndReadsRegistryIndexes(t *testing.T) {
	raw := [8]byte{1}
	registry := &captureLocationRegistry{
		raw: map[[8]byte][]LocationRecord{
			raw: {{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 10, UserID: 42, LocationRevision: 1}},
		},
		channelMembers: map[int64][]LocationRecord{
			77: {
				{InstanceID: "edge-b", UserID: 1001},
				{InstanceID: "edge-c", UserID: 1002},
			},
		},
		channelSubscriptions: map[int64][]LocationRecord{
			88: {{InstanceID: "edge-d", UserID: 1003, ChannelSubscriptions: []ChannelSubscriptionLocation{{ChannelID: 88, ExpiresAtUnixNano: time.Now().Add(time.Minute).UnixNano()}}}},
		},
		channelIDs: []int64{88, 77},
	}
	bus := &captureSessionBus{membershipSyncID: 19, membershipSyncDisposition: ChannelMembershipSyncAcquired}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
		Bus:        bus,
	})

	fabric.TrackChannelInterest(raw, 10, 42, []int64{77, 88})
	if len(bus.sent) != 1 || bus.sent[0].target != "edge-b" || bus.sent[0].cmd.Kind != SessionControlTrackChannelInterest {
		t.Fatalf("track channel command = %+v", bus.sent)
	}
	syncID, disposition, err := fabric.BeginSessionChannelMembershipSync(context.Background(), raw, 10, 42)
	if err != nil || disposition != ChannelMembershipSyncAcquired || syncID != 19 {
		t.Fatalf("begin membership sync id=%d disposition=%q err=%v, want 19/acquired/nil", syncID, disposition, err)
	}
	if err := fabric.AppendSessionChannelMembershipSync(context.Background(), raw, 10, 42, syncID, []int64{77}); err != nil {
		t.Fatalf("append membership sync: %v", err)
	}
	bus.affected = 1
	if synced, err := fabric.CommitSessionChannelMembershipSync(context.Background(), raw, 10, 42, syncID); err != nil || !synced {
		t.Fatalf("commit membership sync synced=%v err=%v", synced, err)
	}
	if len(bus.sent) != 4 || bus.sent[1].cmd.Kind != SessionControlBeginChannelMembershipSync || bus.sent[2].cmd.Kind != SessionControlAppendChannelMembershipSync || bus.sent[3].cmd.Kind != SessionControlCommitChannelMembershipSync || bus.sent[2].cmd.MembershipSyncID != 19 {
		t.Fatalf("membership sync commands = %+v", bus.sent)
	}

	members := fabric.OnlineChannelMemberUserIDsExcluding(77, map[int64]struct{}{1001: {}}, 10)
	if len(members) != 1 || members[0] != 1002 {
		t.Fatalf("members = %v, want [1002]", members)
	}
	subscribers := fabric.OnlineChannelSubscriberUserIDs(88, 10)
	if len(subscribers) != 1 || subscribers[0] != 1003 {
		t.Fatalf("subscribers = %v, want [1003]", subscribers)
	}
	ids := fabric.OnlineChannelIDsSnapshot()
	if len(ids) != 2 || ids[0] != 77 || ids[1] != 88 {
		t.Fatalf("channel ids = %v, want [77 88]", ids)
	}
}

func TestSessionControlFabricMembershipSyncPrefersNewestReconnectLocation(t *testing.T) {
	raw := [8]byte{1}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-stale", RawAuthKeyID: raw, SessionID: 10, UserID: 42, LocationRevision: 100},
			{InstanceID: "edge-current", RawAuthKeyID: raw, SessionID: 10, UserID: 42, LocationRevision: 101},
		},
	}}
	bus := &captureSessionBus{membershipSyncID: 19, membershipSyncDisposition: ChannelMembershipSyncAcquired}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{InstanceID: "core", Registry: registry, Bus: bus})

	syncID, disposition, err := fabric.BeginSessionChannelMembershipSync(context.Background(), raw, 10, 42)
	if err != nil || syncID != 19 || disposition != ChannelMembershipSyncAcquired {
		t.Fatalf("begin membership sync id=%d disposition=%q err=%v, want 19/acquired/nil", syncID, disposition, err)
	}
	if len(bus.sent) != 1 || bus.sent[0].target != "edge-current" {
		t.Fatalf("membership sync targets = %+v, want only newest reconnect location", bus.sent)
	}
}

func TestSessionControlFabricMembershipSyncRejectsLocationWithoutRevision(t *testing.T) {
	raw := [8]byte{1}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-unversioned", RawAuthKeyID: raw, SessionID: 10, UserID: 42, UpdatedAtUnixNano: time.Now().UnixNano()},
		},
	}}
	bus := &captureSessionBus{membershipSyncID: 19, membershipSyncDisposition: ChannelMembershipSyncAcquired}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{InstanceID: "core", Registry: registry, Bus: bus})

	if _, _, err := fabric.BeginSessionChannelMembershipSync(context.Background(), raw, 10, 42); err == nil || !strings.Contains(err.Error(), "target count 0") {
		t.Fatalf("begin membership sync err = %v, want missing authoritative location", err)
	}
	if len(bus.sent) != 0 {
		t.Fatalf("unversioned location must not receive session control: %+v", bus.sent)
	}
}

func TestSessionControlFabricMembershipSyncFailsClosedOnNewestLocationTie(t *testing.T) {
	raw := [8]byte{1}
	registry := &captureLocationRegistry{raw: map[[8]byte][]LocationRecord{
		raw: {
			{InstanceID: "edge-a", RawAuthKeyID: raw, SessionID: 10, UserID: 42, LocationRevision: 100},
			{InstanceID: "edge-b", RawAuthKeyID: raw, SessionID: 10, UserID: 42, LocationRevision: 100},
		},
	}}
	bus := &captureSessionBus{membershipSyncID: 19, membershipSyncDisposition: ChannelMembershipSyncAcquired}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{InstanceID: "core", Registry: registry, Bus: bus})

	if _, _, err := fabric.BeginSessionChannelMembershipSync(context.Background(), raw, 10, 42); err == nil || !strings.Contains(err.Error(), "target count 2") {
		t.Fatalf("begin membership sync err = %v, want ambiguous target count", err)
	}
	if len(bus.sent) != 0 {
		t.Fatalf("ambiguous membership sync must not send: %+v", bus.sent)
	}
}

func TestSessionControlFabricPushesUsingSuppliedLocations(t *testing.T) {
	registry := &captureLocationRegistry{}
	bus := &captureSessionBus{affected: 5}
	fabric := NewSessionControlFabric(SessionControlFabricConfig{
		InstanceID: "core",
		Registry:   registry,
		Bus:        bus,
	})
	raw := [8]byte{7}

	sent, err := fabric.PushToUserLocationBatches(context.Background(), []LocationTargetedUserPush{
		{
			TargetUserID: 42,
			Locations: []LocationRecord{
				{InstanceID: "core", UserID: 42, ReceivesUpdates: true},
				{InstanceID: "edge-b", UserID: 42, RawAuthKeyID: raw, SessionID: 11, ReceivesUpdates: true},
				{InstanceID: "edge-b", UserID: 42, RawAuthKeyID: raw, SessionID: 12, ReceivesUpdates: true},
				{InstanceID: "edge-c", UserID: 42, RawAuthKeyID: raw, SessionID: 13, ReceivesUpdates: false},
				{InstanceID: "edge-d", UserID: 99, ReceivesUpdates: true},
			},
			ExcludeAuthKeyID: raw,
			ExcludeSessionID: 99,
			MessageType:      proto.MessageFromServer,
			Update:           &tg.Updates{Date: 1},
			ChannelDelivery:  ChannelDeliveryWatermark{Kind: ChannelDeliveryPayload, ChannelID: 1001, MinPts: 5, MaxPts: 5},
		},
		{
			TargetUserID: 43,
			Locations: []LocationRecord{
				{InstanceID: "edge-b", UserID: 43, RawAuthKeyID: [8]byte{8}, SessionID: 21, ReceivesUpdates: true},
			},
			MessageType: proto.MessageFromServer,
			Update:      &tg.Updates{Date: 2},
		},
	})
	if err != nil {
		t.Fatalf("PushToUserLocationBatches err = %v", err)
	}
	if sent != 5 {
		t.Fatalf("sent = %d, want bus affected count 5", sent)
	}
	if registry.listUserCalls != 0 {
		t.Fatalf("ListUser calls = %d, want 0 (locations already supplied)", registry.listUserCalls)
	}
	if len(bus.sent) != 1 || bus.sent[0].target != "edge-b" {
		t.Fatalf("bus sent = %+v, want one edge-b command", bus.sent)
	}
	if got := bus.sent[0].cmd; got.Kind != SessionControlPushUserBatch || len(got.UserPushes) != 2 {
		t.Fatalf("command = %+v, want one batch command with two user pushes", got)
	} else if got.UserPushes[0].TargetUserID != 42 || got.UserPushes[0].RawAuthKeyID != raw || got.UserPushes[0].ExceptSessionID != 99 {
		t.Fatalf("first batch entry = %+v, want user 42 with origin exclusion", got.UserPushes[0])
	} else if got.UserPushes[0].ChannelDelivery != (ChannelDeliveryWatermark{Kind: ChannelDeliveryPayload, ChannelID: 1001, MinPts: 5, MaxPts: 5}) {
		t.Fatalf("first batch delivery = %+v, want channel payload watermark", got.UserPushes[0].ChannelDelivery)
	}
}

func TestHandleSessionControlCommandAppliesUserPushBatch(t *testing.T) {
	local := &captureFullController{}
	raw := [8]byte{7}
	first, err := EncodeDeliveryUpdate(&tg.Updates{Date: 1})
	if err != nil {
		t.Fatalf("encode first update: %v", err)
	}
	second, err := EncodeDeliveryUpdate(&tg.Updates{Date: 2})
	if err != nil {
		t.Fatalf("encode second update: %v", err)
	}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID: "cmd-user-push-batch",
		Kind:      SessionControlPushUserBatch,
		UserPushes: []SessionControlUserPush{
			{TargetUserID: 42, RawAuthKeyID: raw, ExceptSessionID: 99, MessageType: proto.MessageFromServer, UpdateBytes: first},
			{TargetUserID: 43, MessageType: proto.MessageFromServer, UpdateBytes: second},
		},
	})
	if ack.Error != "" || ack.Affected != 6 {
		t.Fatalf("batch ack = %+v, want affected 6 without error", ack)
	}
	if len(local.pushUserIDs) != 2 || local.pushUserIDs[0] != 42 || local.pushUserIDs[1] != 43 {
		t.Fatalf("batch pushed users = %v, want [42 43]", local.pushUserIDs)
	}
	if local.pushRaw != [8]byte{} || local.pushSession != 0 {
		t.Fatalf("last push exclusion = raw %x session %d, want zero exclusion for second entry", local.pushRaw, local.pushSession)
	}
}

func TestHandleSessionControlCommandAppliesChannelDeliveryBatch(t *testing.T) {
	local := &captureFullController{}
	raw := [8]byte{7}
	updateBytes, err := EncodeDeliveryUpdate(&tg.Updates{Date: 1})
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	delivery := ChannelDeliveryWatermark{Kind: ChannelDeliveryPayload, ChannelID: 1001, MinPts: 5, MaxPts: 6}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID: "cmd-channel-push-batch",
		Kind:      SessionControlPushUserBatch,
		UserPushes: []SessionControlUserPush{{
			TargetUserID:    42,
			RawAuthKeyID:    raw,
			ExceptSessionID: 99,
			MessageType:     proto.MessageFromServer,
			UpdateBytes:     updateBytes,
			ChannelDelivery: delivery,
		}},
	})
	if ack.Error != "" || ack.Affected != 2 {
		t.Fatalf("channel delivery batch ack = %+v, want affected 2 without error", ack)
	}
	if local.pushKind != SessionControlPushUserBatch || local.pushUserID != 42 || local.pushRaw != raw || local.pushSession != 99 {
		t.Fatalf("channel delivery push = kind %s user %d raw %x session %d", local.pushKind, local.pushUserID, local.pushRaw, local.pushSession)
	}
	if local.pushDelivery != delivery {
		t.Fatalf("channel delivery = %+v, want %+v", local.pushDelivery, delivery)
	}
}

func TestHandleSessionControlCommandAppliesChannelState(t *testing.T) {
	local := &captureFullController{membershipSyncID: 33}
	raw := [8]byte{9}

	ack := HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:    "cmd-track-channel",
		Kind:         SessionControlTrackChannelInterest,
		RawAuthKeyID: raw,
		SessionID:    10,
		UserID:       42,
		ChannelIDs:   []int64{77, 88},
	})
	if ack.Error != "" || ack.Affected != 1 {
		t.Fatalf("track ack = %+v", ack)
	}
	if local.channelKind != SessionControlTrackChannelInterest || local.channelRaw != raw || local.channelSession != 10 || local.channelUserID != 42 || len(local.channelIDs) != 2 {
		t.Fatalf("track capture = %+v", local)
	}

	ack = HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:    "cmd-begin",
		Kind:         SessionControlBeginChannelMembershipSync,
		RawAuthKeyID: raw,
		SessionID:    10,
		UserID:       42,
	})
	if ack.Error != "" || ack.MembershipSyncID != 33 {
		t.Fatalf("begin membership ack = %+v", ack)
	}

	ack = HandleSessionControlCommand(local, SessionControlCommand{
		CommandID:        "cmd-oversized-membership-page",
		Kind:             SessionControlAppendChannelMembershipSync,
		RawAuthKeyID:     raw,
		SessionID:        10,
		UserID:           42,
		MembershipSyncID: 33,
		ChannelIDs:       make([]int64, MaxChannelMembershipSyncPage+1),
	})
	if ack.Error == "" {
		t.Fatal("oversized membership page was accepted")
	}
}

type captureFullController struct {
	FullController

	boundAuthRaw      [8]byte
	boundAuthSession  int64
	boundAuthBusiness [8]byte

	boundUserRaw     [8]byte
	boundUserSession int64
	boundUserID      int64

	receivesRaw     [8]byte
	receivesSession int64
	receives        bool

	layerRaw     [8]byte
	layerSession int64
	layer        int

	activationBeginToken uint64
	activationEndToken   uint64
	bootstrapBeginToken  uint64
	bootstrapEndToken    uint64
	bootstrapSuccess     bool

	seedRawKey      [8]byte
	seedRawLayer    int
	seedRawAffected int

	closeBusinessKey      [8]byte
	closeBusinessAffected int

	pushKind     SessionControlKind
	pushUserID   int64
	pushRaw      [8]byte
	pushSession  int64
	pushBusiness [8]byte
	pushLayer    int
	pushSemantic tlprofile.SemanticID
	pushType     proto.MessageType
	pushMessage  tg.UpdatesClass
	pushTimeout  time.Duration
	pushUserIDs  []int64
	pushDelivery ChannelDeliveryWatermark

	channelKind      SessionControlKind
	channelRaw       [8]byte
	channelSession   int64
	channelUserID    int64
	channelID        int64
	channelIDs       []int64
	membershipSyncID int64
	channelSyncID    int64
	channelTTL       time.Duration
}

func (c *captureFullController) BeginSessionUpdatesActivation([8]byte, int64) (uint64, bool) {
	return c.activationBeginToken, c.activationBeginToken != 0
}

func (c *captureFullController) EndSessionUpdatesActivation(_ [8]byte, _ int64, token uint64) {
	c.activationEndToken = token
}

func (c *captureFullController) BeginSessionBootstrapProbe([8]byte, int64) (uint64, bool) {
	return c.bootstrapBeginToken, c.bootstrapBeginToken != 0
}

func (c *captureFullController) EndSessionBootstrapProbe(_ [8]byte, _ int64, token uint64, success bool) {
	c.bootstrapEndToken = token
	c.bootstrapSuccess = success
}

func (c *captureFullController) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	c.boundAuthRaw = rawAuthKeyID
	c.boundAuthSession = sessionID
	c.boundAuthBusiness = authKeyID
}

func (c *captureFullController) BindAuthKeyForRawAuthKey(rawAuthKeyID [8]byte, authKeyID [8]byte) int {
	c.boundAuthRaw = rawAuthKeyID
	c.boundAuthBusiness = authKeyID
	return 1
}

func (c *captureFullController) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	c.boundUserRaw = rawAuthKeyID
	c.boundUserSession = sessionID
	c.boundUserID = userID
}

func (c *captureFullController) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool) {
	c.receivesRaw = rawAuthKeyID
	c.receivesSession = sessionID
	c.receives = receives
}

func (c *captureFullController) SetClientLayerForAuthKey(rawAuthKeyID [8]byte, sessionID int64, layer int) {
	c.layerRaw = rawAuthKeyID
	c.layerSession = sessionID
	c.layer = layer
}

func (c *captureFullController) SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	c.seedRawKey = rawAuthKeyID
	c.seedRawLayer = layer
	return c.seedRawAffected
}

func (c *captureFullController) CloseSessionsForBusinessAuthKey(authKeyID [8]byte) int {
	c.closeBusinessKey = authKeyID
	return c.closeBusinessAffected
}

func (c *captureFullController) PushToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	c.capturePush(SessionControlPushUser, userID, excludeAuthKeyID, excludeSessionID, t, msg, 0)
	return 3, nil
}

func (c *captureFullController) PushToUserExceptAuthKeySessionBounded(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.capturePush(SessionControlPushUserBounded, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
	return 3, nil
}

func (c *captureFullController) PushChannelUpdateToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, delivery ChannelDeliveryWatermark) (int, error) {
	c.capturePush(SessionControlPushUserBatch, userID, excludeAuthKeyID, excludeSessionID, t, msg, 0)
	c.pushDelivery = delivery
	return 2, nil
}

func (c *captureFullController) PushToUserTransientExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.capturePush(SessionControlPushUserTransient, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
	return 3, nil
}

func (c *captureFullController) PushToUserAuthKey(_ context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	c.captureBusinessPush(SessionControlPushUserAuthKey, userID, businessAuthKeyID, 0, t, msg, 0)
	return 4, nil
}

func (c *captureFullController) PushToUserAuthKeyTransient(_ context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserAuthKeyTransient, userID, businessAuthKeyID, 0, t, msg, timeout)
	return 4, nil
}

func (c *captureFullController) PushToUserExceptBusinessAuthKey(_ context.Context, userID int64, excludeBusinessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserExceptBusinessAuthKey, userID, excludeBusinessAuthKeyID, 0, t, msg, timeout)
	return 4, nil
}

func (c *captureFullController) PushToUserTransientAtLeastLayer(_ context.Context, userID int64, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserTransientAtLeastLayer, userID, [8]byte{}, minLayer, t, msg, timeout)
	return 4, nil
}

func (c *captureFullController) PushToUserAuthKeyTransientAtLeastLayer(_ context.Context, userID int64, businessAuthKeyID [8]byte, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserAuthKeyTransientAtLeastLayer, userID, businessAuthKeyID, minLayer, t, msg, timeout)
	return 4, nil
}

func (c *captureFullController) PushToUserTransientCompatible(_ context.Context, userID int64, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserTransientAtLeastLayer, userID, [8]byte{}, 0, t, msg, timeout)
	c.pushSemantic = semantic
	return 4, nil
}

func (c *captureFullController) PushToUserAuthKeyTransientCompatible(_ context.Context, userID int64, businessAuthKeyID [8]byte, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	c.captureBusinessPush(SessionControlPushUserAuthKeyTransientAtLeastLayer, userID, businessAuthKeyID, 0, t, msg, timeout)
	c.pushSemantic = semantic
	return 4, nil
}

func (c *captureFullController) capturePush(kind SessionControlKind, userID int64, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) {
	c.pushKind = kind
	c.pushUserID = userID
	c.pushUserIDs = append(c.pushUserIDs, userID)
	c.pushRaw = rawAuthKeyID
	c.pushSession = sessionID
	c.pushType = t
	c.pushMessage = msg
	c.pushTimeout = timeout
}

func (c *captureFullController) captureBusinessPush(kind SessionControlKind, userID int64, businessAuthKeyID [8]byte, layer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) {
	c.pushKind = kind
	c.pushUserID = userID
	c.pushBusiness = businessAuthKeyID
	c.pushLayer = layer
	c.pushType = t
	c.pushMessage = msg
	c.pushTimeout = timeout
}

func (c *captureFullController) TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64) {
	c.channelKind = SessionControlTrackChannelInterest
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	c.channelIDs = append([]int64(nil), channelIDs...)
}

func (c *captureFullController) ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64) {
	c.channelKind = SessionControlClearChannelInterest
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
}

func (c *captureFullController) RefreshChannelSubscription(rawAuthKeyID [8]byte, sessionID, userID, channelID int64, ttl time.Duration) {
	c.channelKind = SessionControlRefreshChannelSubscription
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	c.channelID = channelID
	c.channelTTL = ttl
}

func (c *captureFullController) BeginSessionChannelMembershipSync(_ context.Context, rawAuthKeyID [8]byte, sessionID, userID int64) (int64, ChannelMembershipSyncDisposition, error) {
	c.channelKind = SessionControlBeginChannelMembershipSync
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	return c.membershipSyncID, ChannelMembershipSyncAcquired, nil
}

func (c *captureFullController) AppendSessionChannelMembershipSync(_ context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64, channelIDs []int64) error {
	c.channelKind = SessionControlAppendChannelMembershipSync
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	c.channelIDs = append([]int64(nil), channelIDs...)
	c.channelSyncID = syncID
	return nil
}

func (c *captureFullController) CommitSessionChannelMembershipSync(_ context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) (bool, error) {
	c.channelKind = SessionControlCommitChannelMembershipSync
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	c.channelSyncID = syncID
	return true, nil
}

func (c *captureFullController) AbortSessionChannelMembershipSync(_ context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) {
	c.channelKind = SessionControlAbortChannelMembershipSync
	c.channelRaw = rawAuthKeyID
	c.channelSession = sessionID
	c.channelUserID = userID
	c.channelSyncID = syncID
}

func (c *captureFullController) AddUserChannelMembership(userID, channelID int64) {
	c.channelKind = SessionControlAddUserChannelMembership
	c.channelUserID = userID
	c.channelID = channelID
}

func (c *captureFullController) RemoveUserChannelMembership(userID, channelID int64) {
	c.channelKind = SessionControlRemoveUserChannelMembership
	c.channelUserID = userID
	c.channelID = channelID
}

type captureLocationRegistry struct {
	raw                  map[[8]byte][]LocationRecord
	business             map[[8]byte][]LocationRecord
	users                map[int64][]LocationRecord
	channelInterests     map[int64][]LocationRecord
	channelMembers       map[int64][]LocationRecord
	channelSubscriptions map[int64][]LocationRecord
	channelIDs           []int64
	listUserCalls        int
	listUsersCalls       int
}

func (r *captureLocationRegistry) Heartbeat(context.Context, LocationRecord, time.Duration) error {
	return nil
}

func (r *captureLocationRegistry) Remove(context.Context, LocationRecord) error {
	return nil
}

func (r *captureLocationRegistry) ListUser(_ context.Context, userID int64) ([]LocationRecord, error) {
	r.listUserCalls++
	return append([]LocationRecord(nil), r.users[userID]...), nil
}

func (r *captureLocationRegistry) ListUsers(_ context.Context, userIDs []int64) (map[int64][]LocationRecord, error) {
	r.listUsersCalls++
	out := make(map[int64][]LocationRecord, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, ok := out[userID]; ok {
			continue
		}
		out[userID] = append([]LocationRecord(nil), r.users[userID]...)
	}
	return out, nil
}

func (r *captureLocationRegistry) ListBusinessAuthKey(_ context.Context, authKeyID [8]byte) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.business[authKeyID]...), nil
}

func (r *captureLocationRegistry) ListInstance(context.Context, string) ([]LocationRecord, error) {
	return nil, nil
}

func (r *captureLocationRegistry) ListRawAuthKey(_ context.Context, rawAuthKeyID [8]byte) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.raw[rawAuthKeyID]...), nil
}

func (r *captureLocationRegistry) ListChannelInterest(_ context.Context, channelID int64) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.channelInterests[channelID]...), nil
}

func (r *captureLocationRegistry) ListChannelMember(_ context.Context, channelID int64) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.channelMembers[channelID]...), nil
}

func (r *captureLocationRegistry) ListChannelSubscription(_ context.Context, channelID int64) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.channelSubscriptions[channelID]...), nil
}

func (r *captureLocationRegistry) ListOnlineChannelIDsSnapshot(context.Context) ([]int64, error) {
	return append([]int64(nil), r.channelIDs...), nil
}

type sentSessionControl struct {
	target string
	cmd    SessionControlCommand
}

type captureSessionBus struct {
	sent                      []sentSessionControl
	affected                  int
	membershipSyncID          int64
	membershipSyncDisposition ChannelMembershipSyncDisposition
	ackErr                    string
	err                       error
	lifecycleToken            uint64
}

func (b *captureSessionBus) SendSessionControl(_ context.Context, targetInstanceID string, cmd SessionControlCommand) (SessionControlAck, error) {
	b.sent = append(b.sent, sentSessionControl{target: targetInstanceID, cmd: cmd})
	return SessionControlAck{CommandID: cmd.CommandID, Affected: b.affected, MembershipSyncID: b.membershipSyncID, MembershipSyncDisposition: b.membershipSyncDisposition, LifecycleToken: b.lifecycleToken, Error: b.ackErr}, b.err
}

func (b *captureSessionBus) SubscribeSessionControls(context.Context, string, SessionControlCommandHandler) error {
	return nil
}
