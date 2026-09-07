package edgecontrol

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
)

const (
	defaultSessionControlTimeout         = 2 * time.Second
	defaultSessionControlBatchMaxEntries = 128
	defaultSessionControlBatchMaxBytes   = 1 << 20
)

var sessionControlSeq atomic.Uint64
var ErrControlFabricControllerRequired = errors.New("edgecontrol: local session controller is required")
var ErrSessionControlFabricRequired = errors.New("edgecontrol: session-control fabric is required")
var ErrSessionControlFabricDependenciesRequired = errors.New("edgecontrol: session-control fabric dependencies are required")

// EncodeDeliveryUpdate and DecodeDeliveryUpdate are the transient
// session-control codecs. Durable DeliveryFabric treats TL as opaque bytes;
// its encode/decode boundaries live in RPC and MTProto Edge respectively.
func EncodeDeliveryUpdate(update tg.UpdatesClass) ([]byte, error) {
	if update == nil {
		return nil, fmt.Errorf("nil delivery update")
	}
	var b bin.Buffer
	if err := update.Encode(&b); err != nil {
		return nil, err
	}
	return b.Raw(), nil
}

func DecodeDeliveryUpdate(raw []byte) (tg.UpdatesClass, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty delivery update")
	}
	return tg.DecodeUpdates(&bin.Buffer{Buf: raw})
}

type SessionControlFabricConfig struct {
	InstanceID     string
	Registry       LocationRegistry
	Bus            SessionControlCommandBus
	CommandTimeout time.Duration
}

type SessionControlFabric struct {
	instanceID     string
	registry       LocationRegistry
	bus            SessionControlCommandBus
	commandTimeout time.Duration
}

func NewSessionControlFabric(cfg SessionControlFabricConfig) *SessionControlFabric {
	return &SessionControlFabric{
		instanceID:     cfg.InstanceID,
		registry:       cfg.Registry,
		bus:            cfg.Bus,
		commandTimeout: cfg.CommandTimeout,
	}
}

type controlFabricController struct {
	FullController
	fabric *SessionControlFabric
}

func NewControlFabricController(controller FullController, fabric *SessionControlFabric) (FullController, error) {
	if controller == nil {
		return nil, ErrControlFabricControllerRequired
	}
	if fabric == nil {
		return nil, ErrSessionControlFabricRequired
	}
	if fabric.instanceID == "" || fabric.registry == nil || fabric.bus == nil {
		return nil, ErrSessionControlFabricDependenciesRequired
	}
	return &controlFabricController{FullController: controller, fabric: fabric}, nil
}

func (c *controlFabricController) CloseSessionsForBusinessAuthKey(authKeyID [8]byte) int {
	return c.fabric.CloseSessionsForBusinessAuthKey(authKeyID)
}

func (c *controlFabricController) CloseSessionsForBusinessAuthKeyBounded(ctx context.Context, authKeyID [8]byte) (int, error) {
	return c.fabric.CloseSessionsForBusinessAuthKeyBounded(ctx, authKeyID)
}

func (c *controlFabricController) CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int {
	return c.fabric.CloseSessionsForRawAuthKeyExcept(authKeyID, exceptSessionID)
}

func (c *controlFabricController) CloseSessionsForRawAuthKeyExceptBounded(ctx context.Context, authKeyID [8]byte, exceptSessionID int64) (int, error) {
	return c.fabric.CloseSessionsForRawAuthKeyExceptBounded(ctx, authKeyID, exceptSessionID)
}

func (c *controlFabricController) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	c.fabric.BindAuthKeyForSession(rawAuthKeyID, sessionID, authKeyID)
}

func (c *controlFabricController) BindAuthKeyForRawAuthKey(rawAuthKeyID [8]byte, authKeyID [8]byte) int {
	return c.fabric.BindAuthKeyForRawAuthKey(rawAuthKeyID, authKeyID)
}

func (c *controlFabricController) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	c.fabric.BindUserForAuthKey(rawAuthKeyID, sessionID, userID)
}

func (c *controlFabricController) UnbindAuthKey(authKeyID [8]byte) int {
	return c.fabric.UnbindAuthKey(authKeyID)
}

func (c *controlFabricController) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool) {
	c.fabric.SetReceivesUpdatesForAuthKey(rawAuthKeyID, sessionID, receives)
}

func (c *controlFabricController) BeginSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64) (uint64, bool) {
	return c.fabric.BeginSessionUpdatesActivation(rawAuthKeyID, sessionID)
}

func (c *controlFabricController) EndSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64, token uint64) {
	c.fabric.EndSessionUpdatesActivation(rawAuthKeyID, sessionID, token)
}

func (c *controlFabricController) BeginSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64) (uint64, bool) {
	return c.fabric.BeginSessionBootstrapProbe(rawAuthKeyID, sessionID)
}

func (c *controlFabricController) EndSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64, token uint64, success bool) {
	c.fabric.EndSessionBootstrapProbe(rawAuthKeyID, sessionID, token, success)
}

func (c *controlFabricController) SetClientLayerForAuthKey(rawAuthKeyID [8]byte, sessionID int64, layer int) {
	c.fabric.SetClientLayerForAuthKey(rawAuthKeyID, sessionID, layer)
}

func (c *controlFabricController) SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	return c.fabric.SeedInheritedLayerForRawAuthKey(rawAuthKeyID, layer)
}

func (c *controlFabricController) SeedInheritedLayerForBusinessAuthKey(authKeyID [8]byte, layer int) int {
	return c.fabric.SeedInheritedLayerForBusinessAuthKey(authKeyID, layer)
}

func (c *controlFabricController) RefreshInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	return c.fabric.RefreshInheritedLayerForRawAuthKey(rawAuthKeyID, layer)
}

func (c *controlFabricController) ClearInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte) int {
	return c.fabric.ClearInheritedLayerForRawAuthKey(rawAuthKeyID)
}

func (c *controlFabricController) PushToSessionForAuthKey(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	return c.fabric.PushToSessionForAuthKey(ctx, rawAuthKeyID, sessionID, t, msg)
}

func (c *controlFabricController) PushToSessionForAuthKeyImmediate(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	return c.fabric.PushToSessionForAuthKeyImmediate(ctx, rawAuthKeyID, sessionID, t, msg)
}

func (c *controlFabricController) PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	return c.fabric.PushToUserExceptAuthKeySession(ctx, userID, excludeAuthKeyID, excludeSessionID, t, msg)
}

func (c *controlFabricController) PushToUserExceptAuthKeySessionBounded(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserExceptAuthKeySessionBounded(ctx, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (c *controlFabricController) PushToUserTransientExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserTransientExceptAuthKeySession(ctx, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (c *controlFabricController) PushToUserAuthKey(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	return c.fabric.PushToUserAuthKey(ctx, userID, businessAuthKeyID, t, msg)
}

func (c *controlFabricController) PushToUserAuthKeyTransient(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserAuthKeyTransient(ctx, userID, businessAuthKeyID, t, msg, timeout)
}

func (c *controlFabricController) PushToUserExceptBusinessAuthKey(ctx context.Context, userID int64, excludeBusinessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserExceptBusinessAuthKey(ctx, userID, excludeBusinessAuthKeyID, t, msg, timeout)
}

func (c *controlFabricController) PushToUserTransientAtLeastLayer(ctx context.Context, userID int64, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserTransientAtLeastLayer(ctx, userID, minLayer, t, msg, timeout)
}

func (c *controlFabricController) PushToUserAuthKeyTransientAtLeastLayer(ctx context.Context, userID int64, businessAuthKeyID [8]byte, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserAuthKeyTransientAtLeastLayer(ctx, userID, businessAuthKeyID, minLayer, t, msg, timeout)
}

func (c *controlFabricController) PushToUserTransientCompatible(ctx context.Context, userID int64, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserTransientCompatible(ctx, userID, semantic, t, msg, timeout)
}

func (c *controlFabricController) PushToUserAuthKeyTransientCompatible(ctx context.Context, userID int64, businessAuthKeyID [8]byte, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return c.fabric.PushToUserAuthKeyTransientCompatible(ctx, userID, businessAuthKeyID, semantic, t, msg, timeout)
}

func (c *controlFabricController) IsUserOnline(userID int64) bool {
	return c.fabric.IsUserOnline(userID)
}

func (c *controlFabricController) OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64 {
	return c.fabric.OnlineUserIDsForCandidates(candidateUserIDs, limit)
}

func (c *controlFabricController) TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64) {
	c.fabric.TrackChannelInterest(rawAuthKeyID, sessionID, userID, channelIDs)
}

func (c *controlFabricController) ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64) {
	c.fabric.ClearChannelInterest(rawAuthKeyID, sessionID, userID)
}

func (c *controlFabricController) OnlineChannelUserIDs(channelID int64, limit int) []int64 {
	return c.fabric.OnlineChannelUserIDs(channelID, limit)
}

func (c *controlFabricController) BeginSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID int64) (int64, ChannelMembershipSyncDisposition, error) {
	return c.fabric.BeginSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID)
}

func (c *controlFabricController) AppendSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64, channelIDs []int64) error {
	return c.fabric.AppendSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID, syncID, channelIDs)
}

func (c *controlFabricController) CommitSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) (bool, error) {
	return c.fabric.CommitSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID, syncID)
}

func (c *controlFabricController) AbortSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) {
	c.fabric.AbortSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID, syncID)
}

func (c *controlFabricController) AddUserChannelMembership(userID, channelID int64) {
	c.fabric.AddUserChannelMembership(userID, channelID)
}

func (c *controlFabricController) RemoveUserChannelMembership(userID, channelID int64) {
	c.fabric.RemoveUserChannelMembership(userID, channelID)
}

func (c *controlFabricController) OnlineChannelMemberUserIDs(channelID int64, limit int) []int64 {
	return c.fabric.OnlineChannelMemberUserIDs(channelID, limit)
}

func (c *controlFabricController) RefreshChannelSubscription(rawAuthKeyID [8]byte, sessionID, userID, channelID int64, ttl time.Duration) {
	c.fabric.RefreshChannelSubscription(rawAuthKeyID, sessionID, userID, channelID, ttl)
}

func (c *controlFabricController) OnlineChannelSubscriberUserIDs(channelID int64, limit int) []int64 {
	return c.fabric.OnlineChannelSubscriberUserIDs(channelID, limit)
}

func (c *controlFabricController) OnlineChannelSubscriberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64 {
	return c.fabric.OnlineChannelSubscriberUserIDsExcluding(channelID, exclude, limit)
}

func (c *controlFabricController) OnlineChannelMemberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64 {
	return c.fabric.OnlineChannelMemberUserIDsExcluding(channelID, exclude, limit)
}

func (c *controlFabricController) OnlineChannelIDsSnapshot() []int64 {
	return c.fabric.OnlineChannelIDsSnapshot()
}

func (c *controlFabricController) UserLocationRecordsForUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error) {
	return c.fabric.UserLocationRecordsForUsers(ctx, userIDs)
}

func (c *controlFabricController) PushToUserLocationBatches(ctx context.Context, pushes []LocationTargetedUserPush) (int, error) {
	return c.fabric.PushToUserLocationBatches(ctx, pushes)
}

func (f *SessionControlFabric) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	if f == nil {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:              SessionControlBindAuthKeySession,
		RawAuthKeyID:      rawAuthKeyID,
		SessionID:         sessionID,
		BusinessAuthKeyID: authKeyID,
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) BindAuthKeyForRawAuthKey(rawAuthKeyID [8]byte, authKeyID [8]byte) int {
	affected := 0
	if f == nil {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:              SessionControlBindRawAuthKey,
		RawAuthKeyID:      rawAuthKeyID,
		BusinessAuthKeyID: authKeyID,
	}, f.rawAuthKeyTargets(rawAuthKeyID))
}

func (f *SessionControlFabric) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	if f == nil {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlBindUser,
		RawAuthKeyID: rawAuthKeyID,
		SessionID:    sessionID,
		UserID:       userID,
	}, f.rawAuthKeyTargets(rawAuthKeyID))
}

func (f *SessionControlFabric) UnbindAuthKey(authKeyID [8]byte) int {
	affected := 0
	if f == nil {
		return affected
	}
	if f.registry == nil || f.bus == nil || authKeyID == ([8]byte{}) {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:      SessionControlUnbindAuthKey,
		AuthKeyID: authKeyID,
	}, remoteControlTargets(f.instanceID, f.businessAuthKeyRecords(authKeyID)))
}

func (f *SessionControlFabric) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool) {
	if f == nil {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:            SessionControlSetReceivesUpdates,
		RawAuthKeyID:    rawAuthKeyID,
		SessionID:       sessionID,
		ReceivesUpdates: receives,
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) BeginSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64) (uint64, bool) {
	return f.beginSessionLifecycle(rawAuthKeyID, sessionID, SessionControlBeginUpdatesActivation)
}

func (f *SessionControlFabric) EndSessionUpdatesActivation(rawAuthKeyID [8]byte, sessionID int64, token uint64) {
	f.endSessionLifecycle(rawAuthKeyID, sessionID, token, false, SessionControlEndUpdatesActivation)
}

func (f *SessionControlFabric) BeginSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64) (uint64, bool) {
	return f.beginSessionLifecycle(rawAuthKeyID, sessionID, SessionControlBeginBootstrapProbe)
}

func (f *SessionControlFabric) EndSessionBootstrapProbe(rawAuthKeyID [8]byte, sessionID int64, token uint64, success bool) {
	f.endSessionLifecycle(rawAuthKeyID, sessionID, token, success, SessionControlEndBootstrapProbe)
}

func (f *SessionControlFabric) beginSessionLifecycle(rawAuthKeyID [8]byte, sessionID int64, kind SessionControlKind) (uint64, bool) {
	if f == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 {
		return 0, false
	}
	targets := f.rawSessionTargets(rawAuthKeyID, sessionID)
	if len(targets) != 1 {
		return 0, false
	}
	ack, err := f.sendLivePushCommand(context.Background(), targets[0], SessionControlCommand{
		Kind: kind, RawAuthKeyID: rawAuthKeyID, SessionID: sessionID,
	})
	if err != nil || ack.Error != "" || ack.Affected != 1 || ack.LifecycleToken == 0 {
		return 0, false
	}
	return ack.LifecycleToken, true
}

func (f *SessionControlFabric) endSessionLifecycle(rawAuthKeyID [8]byte, sessionID int64, token uint64, success bool, kind SessionControlKind) {
	if f == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 || token == 0 {
		return
	}
	targets := f.rawSessionTargets(rawAuthKeyID, sessionID)
	if len(targets) != 1 {
		return
	}
	_, _ = f.sendLivePushCommand(context.Background(), targets[0], SessionControlCommand{
		Kind: kind, RawAuthKeyID: rawAuthKeyID, SessionID: sessionID,
		LifecycleToken: token, LifecycleSuccess: success,
	})
}

func (f *SessionControlFabric) SetClientLayerForAuthKey(rawAuthKeyID [8]byte, sessionID int64, layer int) {
	if f == nil {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlSetClientLayer,
		RawAuthKeyID: rawAuthKeyID,
		SessionID:    sessionID,
		Layer:        layer,
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	affected := 0
	if f == nil {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlSeedRawLayer,
		RawAuthKeyID: rawAuthKeyID,
		Layer:        layer,
	}, f.rawAuthKeyTargets(rawAuthKeyID))
}

func (f *SessionControlFabric) SeedInheritedLayerForBusinessAuthKey(authKeyID [8]byte, layer int) int {
	affected := 0
	if f == nil {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:      SessionControlSeedBusinessLayer,
		AuthKeyID: authKeyID,
		Layer:     layer,
	}, remoteControlTargets(f.instanceID, f.businessAuthKeyRecords(authKeyID)))
}

func (f *SessionControlFabric) RefreshInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	affected := 0
	if f == nil {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlRefreshRawLayer,
		RawAuthKeyID: rawAuthKeyID,
		Layer:        layer,
	}, f.rawAuthKeyTargets(rawAuthKeyID))
}

func (f *SessionControlFabric) ClearInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte) int {
	affected := 0
	if f == nil {
		return affected
	}
	return affected + f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlClearRawLayer,
		RawAuthKeyID: rawAuthKeyID,
	}, f.rawAuthKeyTargets(rawAuthKeyID))
}

func (f *SessionControlFabric) PushToSessionForAuthKey(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	return f.pushSession(ctx, SessionControlPushSession, rawAuthKeyID, sessionID, t, msg)
}

func (f *SessionControlFabric) PushToSessionForAuthKeyImmediate(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	return f.pushSession(ctx, SessionControlPushSessionImmediate, rawAuthKeyID, sessionID, t, msg)
}

func (f *SessionControlFabric) pushSession(ctx context.Context, kind SessionControlKind, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	if f == nil || msg == nil {
		return nil
	}
	targets := f.rawSessionTargets(rawAuthKeyID, sessionID)
	if len(targets) == 0 {
		return nil
	}
	updateBytes, err := EncodeDeliveryUpdate(msg)
	if err != nil {
		return err
	}
	_, remoteErr := f.sendLivePushToTargets(ctx, SessionControlCommand{
		Kind:         kind,
		RawAuthKeyID: rawAuthKeyID,
		SessionID:    sessionID,
		MessageType:  t,
		UpdateBytes:  updateBytes,
	}, targets)
	if remoteErr != nil {
		return remoteErr
	}
	return nil
}

func (f *SessionControlFabric) PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	return f.pushUser(ctx, SessionControlPushUser, userID, excludeAuthKeyID, excludeSessionID, t, msg, 0)
}

func (f *SessionControlFabric) PushToUserExceptAuthKeySessionBounded(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushUser(ctx, SessionControlPushUserBounded, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (f *SessionControlFabric) PushToUserTransientExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushUser(ctx, SessionControlPushUserTransient, userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (f *SessionControlFabric) pushUser(ctx context.Context, kind SessionControlKind, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	if f == nil || userID == 0 || msg == nil {
		return 0, nil
	}
	sent := 0
	targets, err := f.liveUserTargets(userID, excludeAuthKeyID, excludeSessionID)
	if err != nil {
		return sent, err
	}
	if len(targets) == 0 {
		return sent, nil
	}
	updateBytes, err := EncodeDeliveryUpdate(msg)
	if err != nil {
		return sent, err
	}
	remoteSent, err := f.sendLivePushToTargets(ctx, SessionControlCommand{
		Kind:            kind,
		RawAuthKeyID:    excludeAuthKeyID,
		ExceptSessionID: excludeSessionID,
		TargetUserID:    userID,
		MessageType:     t,
		UpdateBytes:     updateBytes,
		DeliveryTimeout: timeout,
	}, targets)
	sent += remoteSent
	return sent, err
}

func (f *SessionControlFabric) PushToUserLocationBatches(ctx context.Context, pushes []LocationTargetedUserPush) (int, error) {
	if f == nil || len(pushes) == 0 {
		return 0, nil
	}
	byTarget, err := f.sessionControlPushBatchesByTarget(pushes)
	if err != nil {
		return 0, err
	}
	if len(byTarget) == 0 {
		return 0, nil
	}
	targets := make([]string, 0, len(byTarget))
	for target := range byTarget {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	sent := 0
	var firstErr error
	for _, target := range targets {
		for _, chunk := range splitSessionControlUserPushes(byTarget[target]) {
			ack, err := f.sendLivePushCommand(ctx, target, SessionControlCommand{
				Kind:       SessionControlPushUserBatch,
				UserPushes: chunk,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			sent += ack.Affected
			if ack.Error != "" && firstErr == nil {
				firstErr = fmt.Errorf("edge session control batch push: %s", ack.Error)
			}
		}
	}
	return sent, firstErr
}

func (f *SessionControlFabric) sessionControlPushBatchesByTarget(pushes []LocationTargetedUserPush) (map[string][]SessionControlUserPush, error) {
	byTarget := make(map[string][]SessionControlUserPush)
	for _, push := range pushes {
		if push.TargetUserID == 0 || push.Update == nil || len(push.Locations) == 0 {
			continue
		}
		updateBytes, err := EncodeDeliveryUpdate(push.Update)
		if err != nil {
			return nil, err
		}
		entry := SessionControlUserPush{
			TargetUserID:    push.TargetUserID,
			RawAuthKeyID:    push.ExcludeAuthKeyID,
			ExceptSessionID: push.ExcludeSessionID,
			MessageType:     push.MessageType,
			UpdateBytes:     updateBytes,
			ChannelDelivery: push.ChannelDelivery,
		}
		targets := remoteDeliveryTargets(f.instanceID, push.TargetUserID, push.ExcludeAuthKeyID, push.ExcludeSessionID, push.Locations)
		for _, target := range targets {
			byTarget[target] = append(byTarget[target], entry)
		}
	}
	return byTarget, nil
}

func splitSessionControlUserPushes(entries []SessionControlUserPush) [][]SessionControlUserPush {
	if len(entries) == 0 {
		return nil
	}
	chunks := make([][]SessionControlUserPush, 0, (len(entries)+defaultSessionControlBatchMaxEntries-1)/defaultSessionControlBatchMaxEntries)
	start := 0
	bytesInChunk := 0
	for i, entry := range entries {
		entryBytes := len(entry.UpdateBytes) + 64
		if i > start && (i-start >= defaultSessionControlBatchMaxEntries || bytesInChunk+entryBytes > defaultSessionControlBatchMaxBytes) {
			chunks = append(chunks, entries[start:i])
			start = i
			bytesInChunk = 0
		}
		bytesInChunk += entryBytes
	}
	chunks = append(chunks, entries[start:])
	return chunks
}

func (f *SessionControlFabric) PushToUserAuthKey(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	return f.pushBusinessAuthKey(ctx, SessionControlPushUserAuthKey, userID, businessAuthKeyID, 0, 0, t, msg, 0)
}

func (f *SessionControlFabric) PushToUserAuthKeyTransient(ctx context.Context, userID int64, businessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushBusinessAuthKey(ctx, SessionControlPushUserAuthKeyTransient, userID, businessAuthKeyID, 0, 0, t, msg, timeout)
}

func (f *SessionControlFabric) PushToUserAuthKeyTransientAtLeastLayer(ctx context.Context, userID int64, businessAuthKeyID [8]byte, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushBusinessAuthKey(ctx, SessionControlPushUserAuthKeyTransientAtLeastLayer, userID, businessAuthKeyID, minLayer, 0, t, msg, timeout)
}

func (f *SessionControlFabric) PushToUserAuthKeyTransientCompatible(ctx context.Context, userID int64, businessAuthKeyID [8]byte, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushBusinessAuthKey(ctx, SessionControlPushUserAuthKeyTransientAtLeastLayer, userID, businessAuthKeyID, 0, semantic, t, msg, timeout)
}

func (f *SessionControlFabric) pushBusinessAuthKey(ctx context.Context, kind SessionControlKind, userID int64, businessAuthKeyID [8]byte, minLayer int, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	if f == nil || userID == 0 || businessAuthKeyID == ([8]byte{}) || msg == nil {
		return 0, nil
	}
	sent := 0
	targets := remoteBusinessAuthKeyPushTargets(f.instanceID, userID, businessAuthKeyID, minLayer, semantic, f.businessAuthKeyRecords(businessAuthKeyID))
	if len(targets) == 0 {
		return sent, nil
	}
	updateBytes, err := EncodeDeliveryUpdate(msg)
	if err != nil {
		return sent, err
	}
	remoteSent, err := f.sendLivePushToTargets(ctx, SessionControlCommand{
		Kind:              kind,
		TargetUserID:      userID,
		BusinessAuthKeyID: businessAuthKeyID,
		Layer:             minLayer,
		Semantic:          semantic,
		MessageType:       t,
		UpdateBytes:       updateBytes,
		DeliveryTimeout:   timeout,
	}, targets)
	sent += remoteSent
	return sent, err
}

func (f *SessionControlFabric) PushToUserExceptBusinessAuthKey(ctx context.Context, userID int64, excludeBusinessAuthKeyID [8]byte, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	if f == nil || userID == 0 || msg == nil {
		return 0, nil
	}
	sent := 0
	records, err := f.userLocationRecords(userID)
	if err != nil {
		return sent, err
	}
	targets := remoteExceptBusinessAuthKeyPushTargets(f.instanceID, userID, excludeBusinessAuthKeyID, records)
	if len(targets) == 0 {
		return sent, nil
	}
	updateBytes, err := EncodeDeliveryUpdate(msg)
	if err != nil {
		return sent, err
	}
	remoteSent, err := f.sendLivePushToTargets(ctx, SessionControlCommand{
		Kind:              SessionControlPushUserExceptBusinessAuthKey,
		TargetUserID:      userID,
		BusinessAuthKeyID: excludeBusinessAuthKeyID,
		MessageType:       t,
		UpdateBytes:       updateBytes,
		DeliveryTimeout:   timeout,
	}, targets)
	sent += remoteSent
	return sent, err
}

func (f *SessionControlFabric) PushToUserTransientAtLeastLayer(ctx context.Context, userID int64, minLayer int, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushUserTransientCompatible(ctx, userID, minLayer, 0, t, msg, timeout)
}

func (f *SessionControlFabric) PushToUserTransientCompatible(ctx context.Context, userID int64, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	return f.pushUserTransientCompatible(ctx, userID, 0, semantic, t, msg, timeout)
}

func (f *SessionControlFabric) pushUserTransientCompatible(ctx context.Context, userID int64, minLayer int, semantic tlprofile.SemanticID, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	if f == nil || userID == 0 || msg == nil {
		return 0, nil
	}
	sent := 0
	records, err := f.userLocationRecords(userID)
	if err != nil {
		return sent, err
	}
	targets := remoteLayerPushTargets(f.instanceID, userID, minLayer, semantic, records)
	if len(targets) == 0 {
		return sent, nil
	}
	updateBytes, err := EncodeDeliveryUpdate(msg)
	if err != nil {
		return sent, err
	}
	remoteSent, err := f.sendLivePushToTargets(ctx, SessionControlCommand{
		Kind:            SessionControlPushUserTransientAtLeastLayer,
		TargetUserID:    userID,
		Layer:           minLayer,
		Semantic:        semantic,
		MessageType:     t,
		UpdateBytes:     updateBytes,
		DeliveryTimeout: timeout,
	}, targets)
	sent += remoteSent
	return sent, err
}

func (f *SessionControlFabric) IsUserOnline(userID int64) bool {
	if f == nil || userID == 0 {
		return false
	}
	records, err := f.userLocationRecords(userID)
	if err != nil {
		return false
	}
	for _, record := range records {
		if record.UserID == userID && record.ReceivesUpdates {
			return true
		}
	}
	return false
}

func (f *SessionControlFabric) OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64 {
	if f == nil || len(candidateUserIDs) == 0 {
		return nil
	}
	if f.registry != nil {
		if batch, ok := f.registry.(BatchUserLocationRegistry); ok {
			recordsByUser, err := batch.ListUsers(context.Background(), candidateUserIDs)
			if err != nil {
				return nil
			}
			return onlineUserIDsForLocationRecords(candidateUserIDs, limit, recordsByUser)
		}
	}
	out := make([]int64, 0, len(candidateUserIDs))
	seen := make(map[int64]struct{}, len(candidateUserIDs))
	for _, userID := range candidateUserIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if !f.IsUserOnline(userID) {
			continue
		}
		out = append(out, userID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (f *SessionControlFabric) UserLocationRecordsForUsers(ctx context.Context, userIDs []int64) (map[int64][]LocationRecord, error) {
	if f == nil || f.registry == nil || len(userIDs) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if batch, ok := f.registry.(BatchUserLocationRegistry); ok {
		return batch.ListUsers(ctx, userIDs)
	}
	return nil, nil
}

func onlineUserIDsForLocationRecords(candidateUserIDs []int64, limit int, recordsByUser map[int64][]LocationRecord) []int64 {
	if len(candidateUserIDs) == 0 {
		return nil
	}
	out := make([]int64, 0, len(candidateUserIDs))
	seen := make(map[int64]struct{}, len(candidateUserIDs))
	for _, userID := range candidateUserIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if !hasReceivesUpdatesLocation(userID, recordsByUser[userID]) {
			continue
		}
		out = append(out, userID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (f *SessionControlFabric) TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64) {
	if f == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 || userID == 0 {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlTrackChannelInterest,
		RawAuthKeyID: rawAuthKeyID,
		SessionID:    sessionID,
		UserID:       userID,
		ChannelIDs:   append([]int64(nil), channelIDs...),
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64) {
	if f == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 || userID == 0 {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlClearChannelInterest,
		RawAuthKeyID: rawAuthKeyID,
		SessionID:    sessionID,
		UserID:       userID,
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) RefreshChannelSubscription(rawAuthKeyID [8]byte, sessionID, userID, channelID int64, ttl time.Duration) {
	if f == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 || userID == 0 || channelID == 0 {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:            SessionControlRefreshChannelSubscription,
		RawAuthKeyID:    rawAuthKeyID,
		SessionID:       sessionID,
		UserID:          userID,
		ChannelID:       channelID,
		SubscriptionTTL: ttl,
	}, f.rawSessionTargets(rawAuthKeyID, sessionID))
}

func (f *SessionControlFabric) BeginSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID int64) (int64, ChannelMembershipSyncDisposition, error) {
	ack, err := f.sendChannelMembershipSyncCommand(ctx, rawAuthKeyID, sessionID, SessionControlCommand{
		Kind:      SessionControlBeginChannelMembershipSync,
		UserID:    userID,
		SessionID: sessionID,
	})
	if err != nil {
		return 0, "", err
	}
	switch ack.MembershipSyncDisposition {
	case ChannelMembershipSyncAcquired, ChannelMembershipSyncInProgress, ChannelMembershipSyncPrepared:
		return ack.MembershipSyncID, ack.MembershipSyncDisposition, nil
	default:
		return 0, "", fmt.Errorf("channel membership sync returned invalid disposition %q", ack.MembershipSyncDisposition)
	}
}

func (f *SessionControlFabric) AppendSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64, channelIDs []int64) error {
	if len(channelIDs) > MaxChannelMembershipSyncPage {
		return fmt.Errorf("channel membership sync page has %d ids, max %d", len(channelIDs), MaxChannelMembershipSyncPage)
	}
	_, err := f.sendChannelMembershipSyncCommand(ctx, rawAuthKeyID, sessionID, SessionControlCommand{
		Kind:             SessionControlAppendChannelMembershipSync,
		UserID:           userID,
		SessionID:        sessionID,
		MembershipSyncID: syncID,
		ChannelIDs:       append([]int64(nil), channelIDs...),
	})
	return err
}

func (f *SessionControlFabric) CommitSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) (bool, error) {
	ack, err := f.sendChannelMembershipSyncCommand(ctx, rawAuthKeyID, sessionID, SessionControlCommand{
		Kind:             SessionControlCommitChannelMembershipSync,
		UserID:           userID,
		SessionID:        sessionID,
		MembershipSyncID: syncID,
	})
	if err != nil {
		return false, err
	}
	return ack.Affected > 0, nil
}

func (f *SessionControlFabric) AbortSessionChannelMembershipSync(ctx context.Context, rawAuthKeyID [8]byte, sessionID, userID, syncID int64) {
	_, _ = f.sendChannelMembershipSyncCommand(ctx, rawAuthKeyID, sessionID, SessionControlCommand{
		Kind:             SessionControlAbortChannelMembershipSync,
		UserID:           userID,
		SessionID:        sessionID,
		MembershipSyncID: syncID,
	})
}

func (f *SessionControlFabric) sendChannelMembershipSyncCommand(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, cmd SessionControlCommand) (SessionControlAck, error) {
	if f == nil || f.registry == nil || f.bus == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 {
		return SessionControlAck{}, fmt.Errorf("channel membership sync fabric unavailable")
	}
	targets := f.rawSessionTargets(rawAuthKeyID, sessionID)
	if len(targets) != 1 {
		return SessionControlAck{}, fmt.Errorf("channel membership sync target count %d", len(targets))
	}
	cmd.RawAuthKeyID = rawAuthKeyID
	cmd.SessionID = sessionID
	ack, err := f.sendLivePushCommand(ctx, targets[0], cmd)
	if err != nil {
		return SessionControlAck{}, err
	}
	if ack.Error != "" {
		return SessionControlAck{}, fmt.Errorf("edge channel membership sync: %s", ack.Error)
	}
	return ack, nil
}

func (f *SessionControlFabric) AddUserChannelMembership(userID, channelID int64) {
	if f == nil || userID == 0 || channelID == 0 {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlAddUserChannelMembership,
		UserID:       userID,
		ChannelID:    channelID,
		TargetUserID: userID,
	}, f.userControlTargets(userID))
}

func (f *SessionControlFabric) RemoveUserChannelMembership(userID, channelID int64) {
	if f == nil || userID == 0 || channelID == 0 {
		return
	}
	f.sendSessionControlToTargets(SessionControlCommand{
		Kind:         SessionControlRemoveUserChannelMembership,
		UserID:       userID,
		ChannelID:    channelID,
		TargetUserID: userID,
	}, f.userControlTargets(userID))
}

func (f *SessionControlFabric) OnlineChannelUserIDs(channelID int64, limit int) []int64 {
	return f.onlineChannelUsersFromRegistry(channelID, limit, nil, func(reg ChannelLocationRegistry, ctx context.Context) ([]LocationRecord, error) {
		return reg.ListChannelInterest(ctx, channelID)
	})
}

func (f *SessionControlFabric) OnlineChannelMemberUserIDs(channelID int64, limit int) []int64 {
	return f.OnlineChannelMemberUserIDsExcluding(channelID, nil, limit)
}

func (f *SessionControlFabric) OnlineChannelMemberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64 {
	return f.onlineChannelUsersFromRegistry(channelID, limit, exclude, func(reg ChannelLocationRegistry, ctx context.Context) ([]LocationRecord, error) {
		return reg.ListChannelMember(ctx, channelID)
	})
}

func (f *SessionControlFabric) OnlineChannelSubscriberUserIDs(channelID int64, limit int) []int64 {
	return f.OnlineChannelSubscriberUserIDsExcluding(channelID, nil, limit)
}

func (f *SessionControlFabric) OnlineChannelSubscriberUserIDsExcluding(channelID int64, exclude map[int64]struct{}, limit int) []int64 {
	return f.onlineChannelUsersFromRegistry(channelID, limit, exclude, func(reg ChannelLocationRegistry, ctx context.Context) ([]LocationRecord, error) {
		return reg.ListChannelSubscription(ctx, channelID)
	})
}

func (f *SessionControlFabric) OnlineChannelIDsSnapshot() []int64 {
	if f == nil {
		return nil
	}
	out := []int64(nil)
	registry, ok := f.registry.(ChannelLocationRegistry)
	if ok {
		ids, err := registry.ListOnlineChannelIDsSnapshot(context.Background())
		if err == nil {
			out = append(out, ids...)
		}
	}
	return dedupeSortInt64s(out)
}

func (f *SessionControlFabric) onlineChannelUsersFromRegistry(channelID int64, limit int, exclude map[int64]struct{}, load func(ChannelLocationRegistry, context.Context) ([]LocationRecord, error)) []int64 {
	if channelID == 0 {
		return nil
	}
	out := make([]int64, 0, positiveLimitOrLen(limit, 0))
	seen := make(map[int64]struct{})
	appendUser := func(userID int64) bool {
		if userID == 0 {
			return false
		}
		if _, ok := exclude[userID]; ok {
			return false
		}
		if _, ok := seen[userID]; ok {
			return false
		}
		seen[userID] = struct{}{}
		out = append(out, userID)
		return limit > 0 && len(out) >= limit
	}
	if f == nil || f.registry == nil || load == nil {
		return out
	}
	registry, ok := f.registry.(ChannelLocationRegistry)
	if !ok {
		return out
	}
	records, err := load(registry, context.Background())
	if err != nil {
		return out
	}
	for _, record := range records {
		if record.UserID == 0 {
			continue
		}
		if appendUser(record.UserID) {
			return out
		}
	}
	return out
}

func (f *SessionControlFabric) CloseSessionsForBusinessAuthKey(authKeyID [8]byte) int {
	affected, _ := f.CloseSessionsForBusinessAuthKeyBounded(context.Background(), authKeyID)
	return affected
}

func (f *SessionControlFabric) CloseSessionsForBusinessAuthKeyBounded(ctx context.Context, authKeyID [8]byte) (int, error) {
	if f == nil || f.registry == nil || f.bus == nil {
		return 0, ErrSessionControlFabricDependenciesRequired
	}
	if authKeyID == ([8]byte{}) {
		return 0, nil
	}
	records, lookupErr := f.businessAuthKeyRecordsBounded(ctx, authKeyID)
	affected, deliveryErr := f.sendSessionControlToTargetsBounded(ctx, SessionControlCommand{
		Kind:      SessionControlCloseBusinessAuthKey,
		AuthKeyID: authKeyID,
	}, remoteControlTargets(f.instanceID, records))
	return affected, errors.Join(lookupErr, deliveryErr)
}

func (f *SessionControlFabric) CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int {
	affected, _ := f.CloseSessionsForRawAuthKeyExceptBounded(context.Background(), authKeyID, exceptSessionID)
	return affected
}

func (f *SessionControlFabric) CloseSessionsForRawAuthKeyExceptBounded(ctx context.Context, authKeyID [8]byte, exceptSessionID int64) (int, error) {
	if f == nil || f.registry == nil || f.bus == nil {
		return 0, ErrSessionControlFabricDependenciesRequired
	}
	if authKeyID == ([8]byte{}) {
		return 0, nil
	}
	rawRegistry, ok := f.registry.(RawAuthKeyLocationRegistry)
	if !ok {
		return 0, fmt.Errorf("edgecontrol: raw auth-key location registry is required")
	}
	records, err := rawRegistry.ListRawAuthKey(ctx, authKeyID)
	if err != nil {
		return 0, fmt.Errorf("list raw auth-key edge locations: %w", err)
	}
	return f.sendSessionControlToTargetsBounded(ctx, SessionControlCommand{
		Kind:            SessionControlCloseRawAuthKey,
		AuthKeyID:       authKeyID,
		ExceptSessionID: exceptSessionID,
	}, remoteControlTargets(f.instanceID, records))
}

func (f *SessionControlFabric) rawSessionTargets(rawAuthKeyID [8]byte, sessionID int64) []string {
	records := f.rawAuthKeyRecords(rawAuthKeyID)
	filtered := make([]LocationRecord, 0, len(records))
	var newestRevision int64
	for _, record := range records {
		if record.SessionID != sessionID || record.LocationRevision <= 0 {
			continue
		}
		if newestRevision == 0 || record.LocationRevision > newestRevision {
			filtered = filtered[:0]
			filtered = append(filtered, record)
			newestRevision = record.LocationRevision
			continue
		}
		if record.LocationRevision == newestRevision {
			filtered = append(filtered, record)
		}
	}
	return remoteControlTargets(f.instanceID, filtered)
}

func (f *SessionControlFabric) rawAuthKeyTargets(rawAuthKeyID [8]byte) []string {
	return remoteControlTargets(f.instanceID, f.rawAuthKeyRecords(rawAuthKeyID))
}

func (f *SessionControlFabric) rawAuthKeyRecords(rawAuthKeyID [8]byte) []LocationRecord {
	if f == nil || f.registry == nil || f.bus == nil || rawAuthKeyID == ([8]byte{}) {
		return nil
	}
	rawRegistry, ok := f.registry.(RawAuthKeyLocationRegistry)
	if !ok {
		return nil
	}
	records, err := rawRegistry.ListRawAuthKey(context.Background(), rawAuthKeyID)
	if err != nil {
		return nil
	}
	return records
}

func (f *SessionControlFabric) businessAuthKeyRecords(authKeyID [8]byte) []LocationRecord {
	records, _ := f.businessAuthKeyRecordsBounded(context.Background(), authKeyID)
	return records
}

func (f *SessionControlFabric) businessAuthKeyRecordsBounded(ctx context.Context, authKeyID [8]byte) ([]LocationRecord, error) {
	if f == nil || f.registry == nil || f.bus == nil {
		return nil, ErrSessionControlFabricDependenciesRequired
	}
	if authKeyID == ([8]byte{}) {
		return nil, nil
	}
	var records []LocationRecord
	var lookupErr error
	if businessRecords, err := f.registry.ListBusinessAuthKey(ctx, authKeyID); err == nil {
		records = append(records, businessRecords...)
	} else {
		lookupErr = errors.Join(lookupErr, fmt.Errorf("list business auth-key edge locations: %w", err))
	}
	rawRegistry, ok := f.registry.(RawAuthKeyLocationRegistry)
	if !ok {
		return records, lookupErr
	}
	if rawRecords, err := rawRegistry.ListRawAuthKey(ctx, authKeyID); err == nil {
		records = append(records, rawRecords...)
	} else {
		lookupErr = errors.Join(lookupErr, fmt.Errorf("list raw auth-key edge locations: %w", err))
	}
	return records, lookupErr
}

func (f *SessionControlFabric) liveUserTargets(userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64) ([]string, error) {
	if f == nil || f.registry == nil || f.bus == nil || userID == 0 {
		return nil, nil
	}
	records, err := f.userLocationRecords(userID)
	if err != nil {
		return nil, err
	}
	return remoteDeliveryTargets(f.instanceID, userID, excludeAuthKeyID, excludeSessionID, records), nil
}

func (f *SessionControlFabric) userControlTargets(userID int64) []string {
	if f == nil || f.registry == nil || f.bus == nil || userID == 0 {
		return nil
	}
	records, err := f.userLocationRecords(userID)
	if err != nil {
		return nil
	}
	return remoteControlTargets(f.instanceID, records)
}

func (f *SessionControlFabric) userLocationRecords(userID int64) ([]LocationRecord, error) {
	if f == nil || f.registry == nil || userID == 0 {
		return nil, nil
	}
	return f.registry.ListUser(context.Background(), userID)
}

func (f *SessionControlFabric) sendSessionControlToTargets(template SessionControlCommand, targets []string) int {
	affected, _ := f.sendSessionControlToTargetsBounded(context.Background(), template, targets)
	return affected
}

func (f *SessionControlFabric) sendSessionControlToTargetsBounded(ctx context.Context, template SessionControlCommand, targets []string) (int, error) {
	affected := 0
	var firstErr error
	for _, target := range targets {
		cmd := template
		cmd.CommandID = nextSessionControlCommandID(f.instanceID)
		cmd.SourceInstanceID = f.instanceID
		cmd.TargetInstanceID = target
		sendCtx, cancel := context.WithTimeout(ctx, f.sessionControlTimeout())
		ack, err := f.bus.SendSessionControl(sendCtx, target, cmd)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("send session control to %s: %w", target, err)
			}
			continue
		}
		if ack.Error != "" {
			if firstErr == nil {
				firstErr = fmt.Errorf("edge %s session control: %s", target, ack.Error)
			}
			continue
		}
		affected += ack.Affected
	}
	return affected, firstErr
}

func (f *SessionControlFabric) sendLivePushToTargets(ctx context.Context, template SessionControlCommand, targets []string) (int, error) {
	affected := 0
	var firstErr error
	for _, target := range targets {
		ack, err := f.sendLivePushCommand(ctx, target, template)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ack.Error != "" {
			if firstErr == nil {
				firstErr = fmt.Errorf("edge session control push: %s", ack.Error)
			}
			continue
		}
		affected += ack.Affected
	}
	return affected, firstErr
}

func (f *SessionControlFabric) sendLivePushCommand(ctx context.Context, target string, template SessionControlCommand) (SessionControlAck, error) {
	cmd := template
	cmd.CommandID = nextSessionControlCommandID(f.instanceID)
	cmd.SourceInstanceID = f.instanceID
	cmd.TargetInstanceID = target
	sendCtx := ctx
	cancel := func() {}
	timeout := cmd.DeliveryTimeout
	if timeout <= 0 {
		timeout = f.sessionControlTimeout()
	}
	if timeout > 0 {
		sendCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	return f.bus.SendSessionControl(sendCtx, target, cmd)
}

func (f *SessionControlFabric) sessionControlTimeout() time.Duration {
	if f.commandTimeout > 0 {
		return f.commandTimeout
	}
	return defaultSessionControlTimeout
}

func remoteControlTargets(localInstanceID string, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.InstanceID == "" || record.InstanceID == localInstanceID {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func remoteBusinessAuthKeyPushTargets(localInstanceID string, userID int64, businessAuthKeyID [8]byte, minLayer int, semantic tlprofile.SemanticID, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.InstanceID == "" || record.InstanceID == localInstanceID || record.UserID != userID || !record.ReceivesUpdates {
			continue
		}
		if record.BusinessAuthKeyID != businessAuthKeyID && record.RawAuthKeyID != businessAuthKeyID {
			continue
		}
		if !locationSupportsCompatibility(record, minLayer, semantic) {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func remoteExceptBusinessAuthKeyPushTargets(localInstanceID string, userID int64, excludeBusinessAuthKeyID [8]byte, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.InstanceID == "" || record.InstanceID == localInstanceID || record.UserID != userID || !record.ReceivesUpdates {
			continue
		}
		if excludeBusinessAuthKeyID != ([8]byte{}) && (record.BusinessAuthKeyID == excludeBusinessAuthKeyID || record.RawAuthKeyID == excludeBusinessAuthKeyID) {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func remoteLayerPushTargets(localInstanceID string, userID int64, minLayer int, semantic tlprofile.SemanticID, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.InstanceID == "" || record.InstanceID == localInstanceID || record.UserID != userID || !record.ReceivesUpdates {
			continue
		}
		if !locationSupportsCompatibility(record, minLayer, semantic) {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func locationSupportsCompatibility(record LocationRecord, minLayer int, semantic tlprofile.SemanticID) bool {
	if semantic != 0 {
		profile, ok := tlprofile.ResolveProfile(record.Layer)
		if !ok {
			return false
		}
		_, ok = tlprofile.WireID(profile, semantic)
		return ok
	}
	return minLayer <= 0 || record.Layer >= minLayer
}

func positiveLimitOrLen(limit, length int) int {
	if limit > 0 && limit < length {
		return limit
	}
	if length > 0 {
		return length
	}
	return 0
}

func dedupeSortInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	out := values[:0]
	for _, value := range values {
		if value == 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func hasReceivesUpdatesLocation(userID int64, records []LocationRecord) bool {
	for _, record := range records {
		if record.UserID == userID && record.ReceivesUpdates {
			return true
		}
	}
	return false
}

func HandleSessionControlCommand(local FullController, cmd SessionControlCommand) SessionControlAck {
	return HandleSessionControlCommandContext(context.Background(), local, cmd)
}

func HandleSessionControlCommandContext(ctx context.Context, local FullController, cmd SessionControlCommand) SessionControlAck {
	ack := SessionControlAck{
		CommandID:        cmd.CommandID,
		SourceInstanceID: cmd.SourceInstanceID,
		TargetInstanceID: cmd.TargetInstanceID,
	}
	if local == nil {
		ack.Error = "local edge controller is not configured"
		return ack
	}
	switch cmd.Kind {
	case SessionControlCloseBusinessAuthKey:
		ack.Affected = local.CloseSessionsForBusinessAuthKey(cmd.AuthKeyID)
	case SessionControlCloseRawAuthKey:
		ack.Affected = local.CloseSessionsForRawAuthKeyExcept(cmd.AuthKeyID, cmd.ExceptSessionID)
	case SessionControlBindAuthKeySession:
		local.BindAuthKeyForSession(cmd.RawAuthKeyID, cmd.SessionID, cmd.BusinessAuthKeyID)
		ack.Affected = 1
	case SessionControlBindRawAuthKey:
		ack.Affected = local.BindAuthKeyForRawAuthKey(cmd.RawAuthKeyID, cmd.BusinessAuthKeyID)
	case SessionControlBindUser:
		local.BindUserForAuthKey(cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID)
		ack.Affected = 1
	case SessionControlUnbindAuthKey:
		ack.Affected = local.UnbindAuthKey(cmd.AuthKeyID)
	case SessionControlSetReceivesUpdates:
		local.SetReceivesUpdatesForAuthKey(cmd.RawAuthKeyID, cmd.SessionID, cmd.ReceivesUpdates)
		ack.Affected = 1
	case SessionControlBeginUpdatesActivation:
		token, ok := local.BeginSessionUpdatesActivation(cmd.RawAuthKeyID, cmd.SessionID)
		if ok {
			ack.Affected = 1
			ack.LifecycleToken = token
		}
	case SessionControlEndUpdatesActivation:
		local.EndSessionUpdatesActivation(cmd.RawAuthKeyID, cmd.SessionID, cmd.LifecycleToken)
		ack.Affected = 1
	case SessionControlBeginBootstrapProbe:
		token, ok := local.BeginSessionBootstrapProbe(cmd.RawAuthKeyID, cmd.SessionID)
		if ok {
			ack.Affected = 1
			ack.LifecycleToken = token
		}
	case SessionControlEndBootstrapProbe:
		local.EndSessionBootstrapProbe(cmd.RawAuthKeyID, cmd.SessionID, cmd.LifecycleToken, cmd.LifecycleSuccess)
		ack.Affected = 1
	case SessionControlSetClientLayer:
		local.SetClientLayerForAuthKey(cmd.RawAuthKeyID, cmd.SessionID, cmd.Layer)
		ack.Affected = 1
	case SessionControlSeedRawLayer:
		ack.Affected = local.SeedInheritedLayerForRawAuthKey(cmd.RawAuthKeyID, cmd.Layer)
	case SessionControlSeedBusinessLayer:
		ack.Affected = local.SeedInheritedLayerForBusinessAuthKey(cmd.AuthKeyID, cmd.Layer)
	case SessionControlRefreshRawLayer:
		ack.Affected = local.RefreshInheritedLayerForRawAuthKey(cmd.RawAuthKeyID, cmd.Layer)
	case SessionControlClearRawLayer:
		ack.Affected = local.ClearInheritedLayerForRawAuthKey(cmd.RawAuthKeyID)
	case SessionControlPushSession:
		update, err := DecodeDeliveryUpdate(cmd.UpdateBytes)
		if err != nil {
			ack.Error = err.Error()
			return ack
		}
		if err := local.PushToSessionForAuthKey(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.MessageType, update); err != nil {
			ack.Error = err.Error()
			return ack
		}
		ack.Affected = 1
	case SessionControlPushSessionImmediate:
		update, err := DecodeDeliveryUpdate(cmd.UpdateBytes)
		if err != nil {
			ack.Error = err.Error()
			return ack
		}
		if err := local.PushToSessionForAuthKeyImmediate(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.MessageType, update); err != nil {
			ack.Error = err.Error()
			return ack
		}
		ack.Affected = 1
	case SessionControlPushUserBatch:
		if len(cmd.UserPushes) == 0 {
			ack.Error = "empty user push batch"
			return ack
		}
		var firstErr error
		for _, entry := range cmd.UserPushes {
			if entry.TargetUserID == 0 || len(entry.UpdateBytes) == 0 {
				if firstErr == nil {
					firstErr = fmt.Errorf("invalid user push batch entry")
				}
				continue
			}
			update, err := DecodeDeliveryUpdate(entry.UpdateBytes)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			messageType := entry.MessageType
			if messageType == proto.MessageUnknown {
				messageType = cmd.MessageType
			}
			if messageType == proto.MessageUnknown {
				messageType = proto.MessageFromServer
			}
			var sent int
			if entry.ChannelDelivery.Present() {
				if !entry.ChannelDelivery.Valid() {
					err = fmt.Errorf("invalid channel delivery watermark")
				} else if pusher, ok := local.(ChannelDeliverySessionPusher); ok {
					sent, err = pusher.PushChannelUpdateToUserExceptAuthKeySession(ctx, entry.TargetUserID, entry.RawAuthKeyID, entry.ExceptSessionID, messageType, update, entry.ChannelDelivery)
				} else {
					err = fmt.Errorf("channel delivery pusher unavailable")
				}
			} else {
				sent, err = local.PushToUserExceptAuthKeySession(ctx, entry.TargetUserID, entry.RawAuthKeyID, entry.ExceptSessionID, messageType, update)
			}
			ack.Affected += sent
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			ack.Error = firstErr.Error()
		}
	case SessionControlPushUser, SessionControlPushUserBounded, SessionControlPushUserTransient, SessionControlPushUserAuthKey, SessionControlPushUserAuthKeyTransient, SessionControlPushUserExceptBusinessAuthKey, SessionControlPushUserTransientAtLeastLayer, SessionControlPushUserAuthKeyTransientAtLeastLayer:
		update, err := DecodeDeliveryUpdate(cmd.UpdateBytes)
		if err != nil {
			ack.Error = err.Error()
			return ack
		}
		switch cmd.Kind {
		case SessionControlPushUserBounded:
			ack.Affected, err = local.PushToUserExceptAuthKeySessionBounded(ctx, cmd.TargetUserID, cmd.RawAuthKeyID, cmd.ExceptSessionID, cmd.MessageType, update, cmd.DeliveryTimeout)
		case SessionControlPushUserTransient:
			ack.Affected, err = local.PushToUserTransientExceptAuthKeySession(ctx, cmd.TargetUserID, cmd.RawAuthKeyID, cmd.ExceptSessionID, cmd.MessageType, update, cmd.DeliveryTimeout)
		case SessionControlPushUserAuthKey:
			ack.Affected, err = local.PushToUserAuthKey(ctx, cmd.TargetUserID, cmd.BusinessAuthKeyID, cmd.MessageType, update)
		case SessionControlPushUserAuthKeyTransient:
			ack.Affected, err = local.PushToUserAuthKeyTransient(ctx, cmd.TargetUserID, cmd.BusinessAuthKeyID, cmd.MessageType, update, cmd.DeliveryTimeout)
		case SessionControlPushUserExceptBusinessAuthKey:
			ack.Affected, err = local.PushToUserExceptBusinessAuthKey(ctx, cmd.TargetUserID, cmd.BusinessAuthKeyID, cmd.MessageType, update, cmd.DeliveryTimeout)
		case SessionControlPushUserTransientAtLeastLayer:
			if cmd.Semantic != 0 {
				if pusher, ok := any(local).(SemanticTransientPusher); ok {
					ack.Affected, err = pusher.PushToUserTransientCompatible(ctx, cmd.TargetUserID, cmd.Semantic, cmd.MessageType, update, cmd.DeliveryTimeout)
				} else {
					err = fmt.Errorf("semantic transient pusher unavailable")
				}
			} else {
				ack.Affected, err = local.PushToUserTransientAtLeastLayer(ctx, cmd.TargetUserID, cmd.Layer, cmd.MessageType, update, cmd.DeliveryTimeout)
			}
		case SessionControlPushUserAuthKeyTransientAtLeastLayer:
			if cmd.Semantic != 0 {
				if pusher, ok := any(local).(SemanticTransientPusher); ok {
					ack.Affected, err = pusher.PushToUserAuthKeyTransientCompatible(ctx, cmd.TargetUserID, cmd.BusinessAuthKeyID, cmd.Semantic, cmd.MessageType, update, cmd.DeliveryTimeout)
				} else {
					err = fmt.Errorf("semantic transient pusher unavailable")
				}
			} else {
				ack.Affected, err = local.PushToUserAuthKeyTransientAtLeastLayer(ctx, cmd.TargetUserID, cmd.BusinessAuthKeyID, cmd.Layer, cmd.MessageType, update, cmd.DeliveryTimeout)
			}
		default:
			ack.Affected, err = local.PushToUserExceptAuthKeySession(ctx, cmd.TargetUserID, cmd.RawAuthKeyID, cmd.ExceptSessionID, cmd.MessageType, update)
		}
		if err != nil {
			ack.Error = err.Error()
		}
	case SessionControlTrackChannelInterest:
		local.TrackChannelInterest(cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID, cmd.ChannelIDs)
		ack.Affected = 1
	case SessionControlClearChannelInterest:
		local.ClearChannelInterest(cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID)
		ack.Affected = 1
	case SessionControlRefreshChannelSubscription:
		local.RefreshChannelSubscription(cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID, cmd.ChannelID, cmd.SubscriptionTTL)
		ack.Affected = 1
	case SessionControlBeginChannelMembershipSync:
		syncID, disposition, err := local.BeginSessionChannelMembershipSync(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID)
		if err != nil {
			ack.Error = err.Error()
			return ack
		}
		ack.MembershipSyncID = syncID
		ack.MembershipSyncDisposition = disposition
		if disposition == ChannelMembershipSyncPrepared {
			ack.Affected = 1
		}
	case SessionControlAppendChannelMembershipSync:
		if len(cmd.ChannelIDs) > MaxChannelMembershipSyncPage {
			ack.Error = fmt.Sprintf("channel membership sync page has %d ids, max %d", len(cmd.ChannelIDs), MaxChannelMembershipSyncPage)
			return ack
		}
		if err := local.AppendSessionChannelMembershipSync(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID, cmd.MembershipSyncID, cmd.ChannelIDs); err != nil {
			ack.Error = err.Error()
			return ack
		}
		ack.Affected = len(cmd.ChannelIDs)
	case SessionControlCommitChannelMembershipSync:
		synced, err := local.CommitSessionChannelMembershipSync(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID, cmd.MembershipSyncID)
		if err != nil {
			ack.Error = err.Error()
			return ack
		}
		if synced {
			ack.Affected = 1
		}
	case SessionControlAbortChannelMembershipSync:
		local.AbortSessionChannelMembershipSync(ctx, cmd.RawAuthKeyID, cmd.SessionID, cmd.UserID, cmd.MembershipSyncID)
		ack.Affected = 1
	case SessionControlAddUserChannelMembership:
		local.AddUserChannelMembership(cmd.UserID, cmd.ChannelID)
		ack.Affected = 1
	case SessionControlRemoveUserChannelMembership:
		local.RemoveUserChannelMembership(cmd.UserID, cmd.ChannelID)
		ack.Affected = 1
	default:
		ack.Error = fmt.Sprintf("unknown session control kind %q", cmd.Kind)
	}
	return ack
}

func RunSessionControlSubscriber(ctx context.Context, bus SessionControlCommandBus, instanceID string, local FullController) {
	if bus == nil || instanceID == "" || local == nil {
		return
	}
	for {
		err := bus.SubscribeSessionControls(ctx, instanceID, func(ctx context.Context, cmd SessionControlCommand) SessionControlAck {
			return HandleSessionControlCommandContext(ctx, local, cmd)
		})
		if ctx.Err() != nil {
			return
		}
		_ = err
		select {
		case <-ctx.Done():
			return
		case <-time.After(deliverySubscribeRetry):
		}
	}
}

func nextSessionControlCommandID(instanceID string) string {
	if instanceID == "" {
		instanceID = "edge"
	}
	return fmt.Sprintf("%s:%d:%d", instanceID, time.Now().UnixNano(), sessionControlSeq.Add(1))
}
