package store

import (
	"context"
	"encoding/binary"
	"hash/fnv"

	"telesrv/internal/domain"
)

// ReadModelVersionStore exposes durable read-model version tokens to app services.
type ReadModelVersionStore interface {
	ReadModelHash(ctx context.Context, model string, ownerUserID int64, peerType domain.PeerType, peerID int64) (int64, bool, error)
	ReadModelHashes(ctx context.Context, keys []ReadModelKey) (map[ReadModelKey]int64, error)
}

type ReadModelVersionCache interface {
	InvalidateReadModel(ReadModelKey)
	FlushReadModelCache()
}

type ReadModelVersionCacheUpdater interface {
	UpdateReadModelHash(ReadModelKey, int64)
}

type ReadModelKey struct {
	Model       string
	OwnerUserID int64
	PeerType    domain.PeerType
	PeerID      int64
}

// ReadModelInvalidation is a rebuildable cross-process cache signal. Version
// and Hash are copied from PostgreSQL's authoritative read_model_versions row;
// Redis transports the signal but never owns either value.
type ReadModelInvalidation struct {
	Key     ReadModelKey
	Version int64
	Hash    int64
}

// NewSequencedReadModelInvalidation derives a stable, non-zero cache generation
// from an already-durable ordered sequence (account PTS for dialog_light). It is
// rebuildable coordination only: the sequence's owning durable event/outbox row
// remains the crash-recovery truth.
func NewSequencedReadModelInvalidation(key ReadModelKey, sequence int64) ReadModelInvalidation {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key.Model))
	_, _ = h.Write([]byte{0})
	var raw [8]byte
	for _, value := range []int64{key.OwnerUserID, key.PeerID, sequence} {
		binary.BigEndian.PutUint64(raw[:], uint64(value))
		_, _ = h.Write(raw[:])
	}
	_, _ = h.Write([]byte(key.PeerType))
	hash := int64(h.Sum64() & 0x7fffffffffffffff)
	if hash == 0 {
		hash = 1
	}
	return ReadModelInvalidation{Key: key, Version: sequence, Hash: hash}
}

type ReadModelInvalidationPublisher interface {
	PublishReadModelInvalidations(context.Context, []ReadModelInvalidation) error
}
