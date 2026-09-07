package rpc

import (
	"context"
	"fmt"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"
)

// pushTransientChannelLocationBatches is reserved for channel-scoped updates
// that have no durable ChannelUpdateEvent/PTS recovery fact (for example
// typing, call signalling, and authoritative-reload cache invalidation).
// Channel PTS updates are exclusively delivered by telesrv-egress.
func (r *Router) pushTransientChannelLocationBatches(ctx context.Context, targetUserIDs []int64, excludeAuthKeyID [8]byte, excludeSessionID int64, build func(int64) *tg.Updates, fields ...zap.Field) bool {
	if len(targetUserIDs) == 0 {
		return true
	}
	locationProvider, hasLocations := r.deps.Sessions.(UserLocationBatchProvider)
	batchPusher, hasBatchPusher := r.deps.Sessions.(BatchLocationTargetedSessionPusher)
	if !hasLocations || !hasBatchPusher {
		logFields := append([]zap.Field{}, fields...)
		logFields = append(logFields,
			zap.Bool("has_location_batch_provider", hasLocations),
			zap.Bool("has_batch_pusher", hasBatchPusher),
			zap.Int("targets", len(targetUserIDs)),
		)
		r.log.Warn("transient channel delivery dependencies unavailable", logFields...)
		return false
	}
	locationsByUser, err := locationProvider.UserLocationRecordsForUsers(ctx, targetUserIDs)
	if err != nil {
		logFields := append([]zap.Field{}, fields...)
		logFields = append(logFields, zap.Int("targets", len(targetUserIDs)), zap.Error(err))
		r.log.Warn("transient channel location prefetch failed", logFields...)
		return false
	}

	seen := make(map[int64]struct{}, len(targetUserIDs))
	batchPushes := make([]LocationTargetedUserPush, 0, len(targetUserIDs))
	for _, userID := range targetUserIDs {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		locations := locationsByUser[userID]
		if !transientChannelHasDeliverableLocation(userID, locations, excludeAuthKeyID, excludeSessionID) {
			continue
		}
		updates := build(userID)
		if updates == nil {
			continue
		}
		if err := validateTransientChannelUpdates(updates); err != nil {
			logFields := append([]zap.Field{}, fields...)
			logFields = append(logFields, zap.Int64("target_user_id", userID), zap.Error(err))
			r.log.Error("transient channel update contains durable channel PTS", logFields...)
			return false
		}
		batchPushes = append(batchPushes, LocationTargetedUserPush{
			TargetUserID:     userID,
			Locations:        locations,
			ExcludeAuthKeyID: excludeAuthKeyID,
			ExcludeSessionID: excludeSessionID,
			MessageType:      proto.MessageFromServer,
			Update:           updates,
		})
	}
	if len(batchPushes) == 0 {
		return true
	}
	sent, err := batchPusher.PushToUserLocationBatches(ctx, batchPushes)
	if err != nil {
		logFields := append([]zap.Field{}, fields...)
		logFields = append(logFields, zap.Int("pushes", len(batchPushes)), zap.Int("sent", sent), zap.Error(err))
		r.log.Warn("transient channel batch push failed", logFields...)
		return false
	}
	return true
}

type transientChannelInvariantError struct {
	index int
	cause error
}

func (e *transientChannelInvariantError) Error() string {
	if e == nil {
		return "transient channel invariant violation"
	}
	return fmt.Sprintf("transient channel update %d violates the recovery domain: %v", e.index, e.cause)
}

func (e *transientChannelInvariantError) Unwrap() error { return e.cause }

// validateTransientChannelUpdates rejects the complete container when any update
// belongs to the durable channel PTS domain. Core must never become a second
// delivery implementation by removing PTS updates and pushing the remainder.
func validateTransientChannelUpdates(updates *tg.Updates) error {
	if updates == nil {
		return &transientChannelInvariantError{index: -1, cause: fmt.Errorf("nil updates container")}
	}
	for i, update := range updates.Updates {
		_, _, _, channelPTS, err := tg.IsChannelPtsUpdate(update)
		if err != nil {
			return &transientChannelInvariantError{index: i, cause: err}
		}
		if channelPTS {
			return &transientChannelInvariantError{index: i, cause: fmt.Errorf("channel PTS update %T", update)}
		}
	}
	return nil
}

func transientChannelHasDeliverableLocation(userID int64, locations []EdgeLocationRecord, excludeAuthKeyID [8]byte, excludeSessionID int64) bool {
	for _, location := range locations {
		if location.UserID != userID || !location.ReceivesUpdates || location.InstanceID == "" {
			continue
		}
		if excludeSessionID != 0 && location.RawAuthKeyID == excludeAuthKeyID && location.SessionID == excludeSessionID {
			continue
		}
		return true
	}
	return false
}
