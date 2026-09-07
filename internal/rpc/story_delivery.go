package rpc

import (
	"context"
	"fmt"

	"telesrv/internal/store"
)

// storyMutationDeliveryEffects projects the store-frozen audience into account
// PTS effects. The request origin is excluded only for the actor's synchronous
// result; every other recipient remains eligible on all sessions.
func storyMutationDeliveryEffects(ctx context.Context) store.DeliveryEffectsBuilder[store.StoryMutationSnapshot] {
	excludeAuthKeyID := rawAuthKeyIDForOrigin(ctx)
	excludeSessionID, _ := SessionIDFrom(ctx)
	return func(snapshot store.StoryMutationSnapshot) ([]store.DeliveryEffect, error) {
		effects := make([]store.DeliveryEffect, 0, len(snapshot.Required))
		for i, required := range snapshot.Required {
			authKeyID, sessionID := [8]byte{}, int64(0)
			if required.ExcludeOrigin {
				if required.TargetUserID != snapshot.ActorUserID {
					return nil, fmt.Errorf("story delivery requirement %d excludes a non-actor target", i)
				}
				authKeyID, sessionID = excludeAuthKeyID, excludeSessionID
			}
			effects = append(effects, store.AccountPTSDeliveryEffect(required.TargetUserID, required.Event, authKeyID, sessionID))
		}
		return effects, nil
	}
}
