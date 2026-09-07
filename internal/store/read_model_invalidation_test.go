package store

import (
	"testing"

	"telesrv/internal/domain"
)

func TestNewSequencedReadModelInvalidationIsStableAndSequenceSensitive(t *testing.T) {
	key := ReadModelKey{Model: "dialog_light", OwnerUserID: 42, PeerType: domain.PeerTypeUser, PeerID: 84}
	a := NewSequencedReadModelInvalidation(key, 7)
	b := NewSequencedReadModelInvalidation(key, 7)
	c := NewSequencedReadModelInvalidation(key, 8)
	if a != b || a.Key != key || a.Version != 7 || a.Hash == 0 {
		t.Fatalf("stable invalidation = %+v/%+v", a, b)
	}
	if c.Hash == a.Hash || c.Version != 8 {
		t.Fatalf("sequence did not change generation: a=%+v c=%+v", a, c)
	}
}
