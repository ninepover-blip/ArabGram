package redisstore

import (
	"context"
	"encoding/binary"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestLayerAwareTempAuthKeyBindingReturnsRedisEffectiveTuple(t *testing.T) {
	base := &captureTempAuthKeyBindingStore{
		result: domain.TempAuthKeyBindingResult{Layer: 225, LayerObservationID: 11},
	}
	layers := &captureLayerIdentityBindingStore{
		value: store.AuthKeyLayerDefault{Layer: 227, ObservationID: 22},
		found: true,
	}
	bindings, err := NewLayerAwareTempAuthKeyBindingStore(base, layers)
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.TempAuthKeyBinding{
		TempAuthKeyID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		PermAuthKeyID: 91,
		ExpiresAt:     2_000_000_000,
	}
	got, err := bindings.SaveWithState(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if got.Layer != 227 || got.LayerObservationID != 22 {
		t.Fatalf("effective tuple = %+v, want Redis (227,22)", got)
	}
	var permanent [8]byte
	binary.LittleEndian.PutUint64(permanent[:], uint64(binding.PermAuthKeyID))
	if base.saved != 1 || layers.bound != 1 || layers.raw != binding.TempAuthKeyID ||
		layers.effective != permanent || layers.expiresAt != binding.ExpiresAt {
		t.Fatalf("identity bind base=%d layer=%d raw=%x effective=%x expiry=%d",
			base.saved, layers.bound, layers.raw, layers.effective, layers.expiresAt)
	}
}

type captureTempAuthKeyBindingStore struct {
	result domain.TempAuthKeyBindingResult
	saved  int
}

func (s *captureTempAuthKeyBindingStore) Save(ctx context.Context, binding domain.TempAuthKeyBinding) error {
	_, err := s.SaveWithState(ctx, binding)
	return err
}

func (s *captureTempAuthKeyBindingStore) SaveWithState(context.Context, domain.TempAuthKeyBinding) (domain.TempAuthKeyBindingResult, error) {
	s.saved++
	return s.result, nil
}

func (*captureTempAuthKeyBindingStore) GetByTemp(context.Context, [8]byte) (domain.TempAuthKeyBinding, bool, error) {
	return domain.TempAuthKeyBinding{}, false, nil
}

func (*captureTempAuthKeyBindingStore) DeleteExpired(context.Context, int64, int) (int, error) {
	return 0, nil
}

type captureLayerIdentityBindingStore struct {
	store.AuthKeySessionLayerStore
	value     store.AuthKeyLayerDefault
	found     bool
	bound     int
	raw       [8]byte
	effective [8]byte
	expiresAt int
}

func (s *captureLayerIdentityBindingStore) BindAuthKeyLayerIdentity(_ context.Context, raw, effective [8]byte, expiresAt int) error {
	s.bound++
	s.raw = raw
	s.effective = effective
	s.expiresAt = expiresAt
	return nil
}

func (s *captureLayerIdentityBindingStore) GetAuthKeyLayerDefault(context.Context, [8]byte) (store.AuthKeyLayerDefault, bool, error) {
	return s.value, s.found, nil
}

var _ store.TempAuthKeyBindingStore = (*captureTempAuthKeyBindingStore)(nil)
var _ layerIdentityBindingStore = (*captureLayerIdentityBindingStore)(nil)
