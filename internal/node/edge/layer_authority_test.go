package edge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

type edgeLayerTestStore struct {
	store.AuthKeySessionLayerStore
}

func (*edgeLayerTestStore) ResolveCachedAuthKeyLayerIdentity(_ context.Context, raw [8]byte) (store.AuthKeyLayerIdentity, bool, error) {
	return store.AuthKeyLayerIdentity{EffectiveAuthKeyID: raw}, true, nil
}

type edgeLayerTestPrimer struct {
	calls int
	err   error
}

func (p *edgeLayerTestPrimer) PrimeAuthKeyLayerIdentity(context.Context, [8]byte) error {
	p.calls++
	return p.err
}

func TestRequiredAuthKeySessionLayerStoreFailsClosedAndBindsOnce(t *testing.T) {
	ctx := context.Background()
	proxy := &requiredAuthKeySessionLayerStore{}
	if _, _, err := proxy.GetAuthKeyLayerDefault(ctx, [8]byte{1}); !errors.Is(err, store.ErrAuthKeySessionLayerStoreRequired) {
		t.Fatalf("pre-bind error=%v", err)
	}
	keys := memory.NewAuthKeyStore()
	authKeyID := [8]byte{1, 2, 3}
	if err := keys.Save(ctx, store.AuthKeyData{ID: authKeyID}); err != nil {
		t.Fatal(err)
	}
	delegate := &edgeLayerTestStore{AuthKeySessionLayerStore: keys}
	if err := proxy.Bind(delegate); err != nil {
		t.Fatal(err)
	}
	msgID := proto.NewMessageIDGen(time.Now).New(proto.MessageFromClient)
	if value, applied, err := proxy.AdvanceSessionLayer(ctx, authKeyID, 77, 228, msgID); err != nil || !applied || value.Layer != 228 {
		t.Fatalf("bound advance=%+v applied=%v err=%v", value, applied, err)
	}
	if err := proxy.Bind(delegate); err == nil {
		t.Fatal("second authority bind succeeded")
	}
}

func TestEdgeAuthKeyLayerIdentitySourceUsesAuthenticatedPermanentKey(t *testing.T) {
	ctx := context.Background()
	keys := memory.NewAuthKeyStore()
	authKeyID := [8]byte{9, 8, 7}
	if err := keys.Save(ctx, store.AuthKeyData{ID: authKeyID}); err != nil {
		t.Fatal(err)
	}
	primer := &edgeLayerTestPrimer{}
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer redisClient.Close()
	source, err := newEdgeAuthKeyLayerIdentitySource(keys, primer, redisClient)
	if err != nil {
		t.Fatal(err)
	}
	identity, found, err := source.ResolveAuthKeyLayerIdentity(ctx, authKeyID)
	if err != nil || !found || identity.EffectiveAuthKeyID != authKeyID || identity.RawExpiresAt != 0 {
		t.Fatalf("identity=%+v found=%v err=%v", identity, found, err)
	}
	if primer.calls != 0 {
		t.Fatalf("permanent key triggered %d Core primers", primer.calls)
	}
}
