package postgres

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestCollectiblePhoneOwnerCacheNegativeHitAndExactInvalidationPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	from := createTestUser(t, ctx, users, "+1781"+suffix+"01", "PhoneCacheFrom", "")
	to := createTestUser(t, ctx, users, "+1781"+suffix+"02", "PhoneCacheTo", "")
	store := NewCollectiblePhoneStore(pool)

	// Prime both positive-key candidates as negative entries. Duplicate and
	// invalid IDs must not expand the database batch.
	got, err := store.OwnedCollectiblePhones(ctx, []int64{from.ID, to.ID, from.ID, 0, -1})
	if err != nil || len(got) != 0 {
		t.Fatalf("prime owner cache = %+v, err=%v", got, err)
	}

	numericSuffix, err := strconv.ParseUint(suffix, 16, 32)
	if err != nil {
		t.Fatalf("parse random phone suffix: %v", err)
	}
	phone := fmt.Sprintf("888%010d", numericSuffix)
	var assetID int64
	err = pool.QueryRow(ctx, `INSERT INTO collectible_phones
(phone,tier,status,owner_user_id,purchase_date,currency,amount,crypto_currency,crypto_amount,url,
 original_owner_user_id,transfer_count,version,created_at,updated_at)
VALUES($1,'standard','owned',$2,$3,'USD',100,'TON',1000,$4,$2,0,1,$3,$3)
RETURNING id`, phone, from.ID, time.Now().UTC(), "https://fragment.com/number/"+phone).Scan(&assetID)
	if err != nil {
		t.Fatalf("insert collectible phone: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM collectible_phones WHERE id=$1`, assetID) })

	// The insert is durable, but an already-built process cache changes only
	// through the committed invalidation event.
	got, err = store.OwnedCollectiblePhones(ctx, []int64{from.ID})
	if err != nil || len(got) != 0 {
		t.Fatalf("negative cache was bypassed before invalidation: %+v err=%v", got, err)
	}
	rpcProjections := &fakeRPCProjectionReadModelCache{}
	listener := NewReadModelChangeListener("", ReadModelCacheSet{
		CollectiblePhones: store,
		RPCProjections:    rpcProjections,
	}, nil)
	listener.handlePayload(fmt.Sprintf(`{"model":"collectible_phone","peer_type":"user","peer_id":%d,"version":1}`, from.ID))
	if len(rpcProjections.users) != 1 || rpcProjections.users[0] != from.ID {
		t.Fatalf("receiver projection invalidations after insert = %+v, want [%d]", rpcProjections.users, from.ID)
	}
	got, err = store.OwnedCollectiblePhones(ctx, []int64{from.ID})
	if err != nil || got[from.ID].ID != assetID {
		t.Fatalf("owner after insert invalidation = %+v err=%v, want asset %d", got, err, assetID)
	}

	if _, err := pool.Exec(ctx, `UPDATE collectible_phones
SET owner_user_id=$2, original_owner_user_id=$2, version=version+1, updated_at=now()
WHERE id=$1`, assetID, to.ID); err != nil {
		t.Fatalf("transfer collectible phone: %v", err)
	}
	var fromVersion, toVersion int64
	if err := pool.QueryRow(ctx, `SELECT version FROM read_model_versions
WHERE model='collectible_phone' AND owner_user_id=0 AND peer_type='user' AND peer_id=$1`, from.ID).Scan(&fromVersion); err != nil {
		t.Fatalf("old owner read-model version: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT version FROM read_model_versions
WHERE model='collectible_phone' AND owner_user_id=0 AND peer_type='user' AND peer_id=$1`, to.ID).Scan(&toVersion); err != nil {
		t.Fatalf("new owner read-model version: %v", err)
	}
	if fromVersion < 2 || toVersion < 1 {
		t.Fatalf("owner transition versions old/new=%d/%d, want >=2/>=1", fromVersion, toVersion)
	}

	listener.handlePayload(fmt.Sprintf(`{"model":"collectible_phone","peer_type":"user","peer_id":%d,"version":%d}`, from.ID, fromVersion))
	listener.handlePayload(fmt.Sprintf(`{"model":"collectible_phone","peer_type":"user","peer_id":%d,"version":%d}`, to.ID, toVersion))
	if len(rpcProjections.users) != 3 || rpcProjections.users[1] != from.ID || rpcProjections.users[2] != to.ID {
		t.Fatalf("receiver projection invalidations after transfer = %+v, want old/new owners", rpcProjections.users)
	}
	got, err = store.OwnedCollectiblePhones(ctx, []int64{from.ID, to.ID})
	if err != nil {
		t.Fatalf("owner cache after transfer invalidation: %v", err)
	}
	if _, exists := got[from.ID]; exists || got[to.ID].ID != assetID || got[to.ID].OwnerUserID != to.ID {
		t.Fatalf("owner cache after transfer = %+v, want only user %d asset %d", got, to.ID, assetID)
	}

	listener.flush("test reconnect")
	got, err = store.OwnedCollectiblePhones(ctx, []int64{from.ID, to.ID})
	if err != nil || got[to.ID].ID != assetID {
		t.Fatalf("owner cache after reconnect flush = %+v err=%v", got, err)
	}
}
