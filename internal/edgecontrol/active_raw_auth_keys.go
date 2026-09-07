package edgecontrol

import (
	"context"
	"errors"
)

var ErrNoActiveRawAuthKeyRegistry = errors.New("edgecontrol: missing active raw auth-key registry")

// DistributedActiveRawAuthKeys adapts the Edge location registry into the
// maintenance worker's active raw-key snapshot boundary. Core must use this in
// split deployments; NoLocalController intentionally has no MTProto sessions.
type DistributedActiveRawAuthKeys struct {
	Registry ActiveRawAuthKeyRegistry
}

func NewDistributedActiveRawAuthKeys(registry ActiveRawAuthKeyRegistry) *DistributedActiveRawAuthKeys {
	return &DistributedActiveRawAuthKeys{Registry: registry}
}

func (p *DistributedActiveRawAuthKeys) ActiveRawAuthKeySnapshot(ctx context.Context) ([][8]byte, error) {
	if p == nil || p.Registry == nil {
		return nil, ErrNoActiveRawAuthKeyRegistry
	}
	return p.Registry.ListActiveRawAuthKeyIDs(ctx)
}
