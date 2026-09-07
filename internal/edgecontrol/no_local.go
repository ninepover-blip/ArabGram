package edgecontrol

import (
	"context"
	"errors"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
)

var ErrNoLocalSessionController = errors.New("edgecontrol: core role has no local MTProto sessions")

// NoLocalController is the Core-side placeholder for the EdgeControl interface.
// It intentionally owns no MTProto sessions. Write/push operations fail closed;
// production Core wiring must use ControlFabricController over the Redis
// session-control fabric for remote Edge effects.
type NoLocalController struct{}

func NewNoLocalController() *NoLocalController { return &NoLocalController{} }

func (*NoLocalController) BindAuthKeyForSession([8]byte, int64, [8]byte) {}
func (*NoLocalController) AuthKeyIDForSession([8]byte, int64) ([8]byte, bool) {
	return [8]byte{}, false
}
func (*NoLocalController) BindUserForAuthKey([8]byte, int64, int64) {}
func (*NoLocalController) UserIDResolvedForAuthKey([8]byte, int64) (int64, bool) {
	return 0, false
}
func (*NoLocalController) UnbindAuthKey([8]byte) int                         { return 0 }
func (*NoLocalController) SetReceivesUpdatesForAuthKey([8]byte, int64, bool) {}
func (*NoLocalController) PushToSessionForAuthKey(context.Context, [8]byte, int64, proto.MessageType, tg.UpdatesClass) error {
	return ErrNoLocalSessionController
}
func (*NoLocalController) BindAuthKeyForRawAuthKey([8]byte, [8]byte) int { return 0 }
func (*NoLocalController) AuthKeyExpiresAtForSession([8]byte, int64) (int, bool) {
	return 0, false
}
func (*NoLocalController) PushToSessionForAuthKeyImmediate(context.Context, [8]byte, int64, proto.MessageType, tg.UpdatesClass) error {
	return ErrNoLocalSessionController
}
func (*NoLocalController) ReceivesUpdatesForAuthKey([8]byte, int64) bool { return false }
func (*NoLocalController) BeginSessionUpdatesActivation([8]byte, int64) (uint64, bool) {
	return 0, false
}
func (*NoLocalController) EndSessionUpdatesActivation([8]byte, int64, uint64) {}
func (*NoLocalController) BeginSessionBootstrapProbe([8]byte, int64) (uint64, bool) {
	return 0, false
}
func (*NoLocalController) EndSessionBootstrapProbe([8]byte, int64, uint64, bool) {}
func (*NoLocalController) SetClientLayerForAuthKey([8]byte, int64, int)          {}
func (*NoLocalController) SeedInheritedLayerForRawAuthKey([8]byte, int) int {
	return 0
}
func (*NoLocalController) SeedInheritedLayerForBusinessAuthKey([8]byte, int) int {
	return 0
}
func (*NoLocalController) RefreshInheritedLayerForRawAuthKey([8]byte, int) int {
	return 0
}
func (*NoLocalController) ClearInheritedLayerForRawAuthKey([8]byte) int { return 0 }
func (*NoLocalController) ExplicitLayerEvidenceForAuthKey([8]byte, int64) (int, int64, bool) {
	return 0, 0, false
}
func (*NoLocalController) CloseSessionsForBusinessAuthKey([8]byte) int { return 0 }
func (*NoLocalController) CloseSessionsForRawAuthKeyExcept([8]byte, int64) int {
	return 0
}
func (*NoLocalController) PushToUserExceptAuthKeySession(context.Context, int64, [8]byte, int64, proto.MessageType, tg.UpdatesClass) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserExceptAuthKeySessionBounded(context.Context, int64, [8]byte, int64, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserTransientExceptAuthKeySession(context.Context, int64, [8]byte, int64, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserAuthKey(context.Context, int64, [8]byte, proto.MessageType, tg.UpdatesClass) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserAuthKeyTransient(context.Context, int64, [8]byte, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserExceptBusinessAuthKey(context.Context, int64, [8]byte, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserTransientAtLeastLayer(context.Context, int64, int, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) PushToUserAuthKeyTransientAtLeastLayer(context.Context, int64, [8]byte, int, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	return 0, ErrNoLocalSessionController
}
func (*NoLocalController) IsUserOnline(int64) bool { return false }
func (*NoLocalController) OnlineUserIDsForCandidates([]int64, int) []int64 {
	return nil
}
func (*NoLocalController) TrackChannelInterest([8]byte, int64, int64, []int64) {}
func (*NoLocalController) ClearChannelInterest([8]byte, int64, int64)          {}
func (*NoLocalController) OnlineChannelUserIDs(int64, int) []int64             { return nil }
func (*NoLocalController) BeginSessionChannelMembershipSync(context.Context, [8]byte, int64, int64) (int64, ChannelMembershipSyncDisposition, error) {
	return 0, "", ErrNoLocalSessionController
}
func (*NoLocalController) AppendSessionChannelMembershipSync(context.Context, [8]byte, int64, int64, int64, []int64) error {
	return ErrNoLocalSessionController
}
func (*NoLocalController) CommitSessionChannelMembershipSync(context.Context, [8]byte, int64, int64, int64) (bool, error) {
	return false, ErrNoLocalSessionController
}
func (*NoLocalController) AbortSessionChannelMembershipSync(context.Context, [8]byte, int64, int64, int64) {
}
func (*NoLocalController) AddUserChannelMembership(int64, int64)         {}
func (*NoLocalController) RemoveUserChannelMembership(int64, int64)      {}
func (*NoLocalController) OnlineChannelMemberUserIDs(int64, int) []int64 { return nil }
func (*NoLocalController) OnlineChannelMemberUserIDsExcluding(int64, map[int64]struct{}, int) []int64 {
	return nil
}
func (*NoLocalController) RefreshChannelSubscription([8]byte, int64, int64, int64, time.Duration) {
}
func (*NoLocalController) OnlineChannelSubscriberUserIDs(int64, int) []int64 {
	return nil
}
func (*NoLocalController) OnlineChannelSubscriberUserIDsExcluding(int64, map[int64]struct{}, int) []int64 {
	return nil
}
func (*NoLocalController) OnlineChannelIDsSnapshot() []int64 { return nil }

var _ FullController = (*NoLocalController)(nil)
