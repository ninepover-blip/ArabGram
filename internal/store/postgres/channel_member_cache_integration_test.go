package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// TestChannelMemberCacheInvalidatesOnReadModelNotify 验证：channel_members 触发器(0120)
// → 统一 read-model LISTEN/NOTIFY → ChannelMemberCache 失效 → 下一次读回填新成员态。
func TestChannelMemberCacheInvalidatesOnReadModelNotify(t *testing.T) {
	pool := testPool(t)
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 881,
		Phone:      "+1888" + suffix + "01",
		FirstName:  "MemberCacheOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}

	rowCache := NewChannelRowCache(1000)
	memberCache := NewChannelMemberCache(1000)
	channels := newTestChannelStore(pool,
		WithChannelRowCache(rowCache),
		WithChannelMemberCache(memberCache))

	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "MemberCacheChan " + suffix,
		Megagroup:     true,
		Date:          1700001700,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	if _, err := channels.GetChannel(ctx, owner.ID, channelID); err != nil {
		t.Fatalf("warm GetChannel: %v", err)
	}
	if _, ok := rowCache.get(channelID); !ok {
		t.Fatalf("GetChannel 后频道行应已缓存")
	}
	if _, ok := memberCache.get(channelID, owner.ID); !ok {
		t.Fatalf("GetChannel 后成员行应已缓存")
	}

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listener := NewReadModelChangeListener(dsn, ReadModelCacheSet{
		ChannelRows:    rowCache,
		ChannelMembers: memberCache,
	}, nil)
	go listener.Run(lctx)
	if !waitUntil(2*time.Second, func() bool {
		_, rowOK := rowCache.get(channelID)
		_, memberOK := memberCache.get(channelID, owner.ID)
		return !rowOK && !memberOK
	}) {
		t.Fatalf("read model listener 未在预期内连接并 flush 缓存")
	}

	if view, err := channels.GetChannel(ctx, owner.ID, channelID); err != nil {
		t.Fatalf("re-warm GetChannel: %v", err)
	} else if view.Self.Rank != "" {
		t.Fatalf("初始 rank = %q, want empty", view.Self.Rank)
	}
	if _, ok := memberCache.get(channelID, owner.ID); !ok {
		t.Fatalf("re-warm 后成员行应已缓存")
	}

	const newRank = "cache-rank"
	if _, err := pool.Exec(ctx, `
UPDATE channel_members
SET rank = $3, updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID, newRank); err != nil {
		t.Fatalf("update member rank: %v", err)
	}

	if !waitUntil(3*time.Second, func() bool {
		_, ok := memberCache.get(channelID, owner.ID)
		return !ok
	}) {
		t.Fatalf("UPDATE channel_members 后成员缓存未在预期内被 read-model NOTIFY 失效")
	}

	view, err := channels.GetChannel(ctx, owner.ID, channelID)
	if err != nil {
		t.Fatalf("post-invalidation GetChannel: %v", err)
	}
	if view.Self.Rank != newRank {
		t.Fatalf("失效后 rank = %q, want %q", view.Self.Rank, newRank)
	}
}

func TestChannelDialogLightInvalidatesOnChannelMemberAndDialogNotify(t *testing.T) {
	pool := testPool(t)
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner := createTestUser(t, ctx, users, "+1888"+suffix+"91", "DialogLightChannelOwner", "")
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "DialogLightChannel " + suffix,
		Megagroup:     true,
		Date:          1700003300,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: created.Channel.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", created.Channel.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	versionBeforeMember, hashBeforeMember := readDialogLightVersionHash(t, ctx, pool, owner.ID, peer)
	dialogCache := &fakeDialogReadModelCache{}
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listener := NewReadModelChangeListener(dsn, ReadModelCacheSet{Dialogs: dialogCache}, nil)
	go listener.Run(lctx)
	if !waitUntil(2*time.Second, func() bool { return dialogCache.flushCount() == 1 }) {
		t.Fatalf("read model listener 未在预期内连接并 flush dialog cache")
	}

	if _, err := pool.Exec(ctx, `
UPDATE channel_members
SET rank = $3, updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, created.Channel.ID, owner.ID, "dialog-light-rank"); err != nil {
		t.Fatalf("update channel member: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, peer) > 0
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("channel member 写入未触发 dialog_light 失效: owners=%v peers=%+v want owner=%d peer=%+v", owners, peers, owner.ID, peer)
	}
	versionAfterMember, hashAfterMember := readDialogLightVersionHash(t, ctx, pool, owner.ID, peer)
	if versionAfterMember <= versionBeforeMember {
		t.Fatalf("channel member did not bump dialog_light version: before=%d after=%d", versionBeforeMember, versionAfterMember)
	}
	if hashAfterMember == 0 || hashAfterMember == hashBeforeMember {
		t.Fatalf("channel member did not refresh dialog_light hash: before=%d after=%d", hashBeforeMember, hashAfterMember)
	}

	beforeDialogInvalidation := countDialogInvalidation(dialogCache, owner.ID, peer)
	versionBeforeDialog, hashBeforeDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, peer)
	changed, err := channels.SetChannelDialogUnreadMark(ctx, owner.ID, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("set channel unread mark: %v", err)
	}
	if !changed {
		t.Fatalf("set channel unread mark changed = false, want true")
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, peer) > beforeDialogInvalidation
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("channel dialog 写入未触发 dialog_light 失效: owners=%v peers=%+v want owner=%d peer=%+v", owners, peers, owner.ID, peer)
	}
	versionAfterDialog, hashAfterDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, peer)
	if versionAfterDialog <= versionBeforeDialog {
		t.Fatalf("channel dialog did not bump dialog_light version: before=%d after=%d", versionBeforeDialog, versionAfterDialog)
	}
	if hashAfterDialog == 0 || hashAfterDialog == hashBeforeDialog {
		t.Fatalf("channel dialog did not refresh dialog_light hash: before=%d after=%d", hashBeforeDialog, hashAfterDialog)
	}
}

// TestReadModelChangeListenerInvalidatesProjectionCachesFromNotify 验证 0120/0121
// 的 account/profile 投影触发器能穿过统一 listener 分发到对应缓存。
func TestReadModelChangeListenerInvalidatesProjectionCachesFromNotify(t *testing.T) {
	pool := testPool(t)
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 991,
		Phone:      "+1991" + suffix + "01",
		FirstName:  "ProjectionOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	target, err := users.Create(ctx, domain.User{
		AccessHash: 992,
		Phone:      "+1992" + suffix + "02",
		FirstName:  "ProjectionTarget",
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	photoID := time.Now().UnixNano()
	targetPhotoID := photoID + 1
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM account_privacy_rules WHERE owner_user_id = ANY($1::bigint[])", []int64{owner.ID, target.ID})
		_, _ = pool.Exec(ctx, "DELETE FROM profile_photos WHERE owner_peer_type = 'user' AND owner_peer_id = ANY($1::bigint[])", []int64{owner.ID, target.ID})
		_, _ = pool.Exec(ctx, "DELETE FROM photos WHERE id = ANY($1::bigint[])", []int64{photoID, targetPhotoID})
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, target.ID})
	})

	contactsCache := &fakeContactReadModelCache{}
	dialogCache := &fakeDialogReadModelCache{}
	privacyCache := &fakePrivacyReadModelCache{}
	photoCache := &fakeProfilePhotoReadModelCache{}
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listener := NewReadModelChangeListener(dsn, ReadModelCacheSet{
		Contacts:      contactsCache,
		Dialogs:       dialogCache,
		Privacy:       privacyCache,
		ProfilePhotos: photoCache,
	}, nil)
	go listener.Run(lctx)
	if !waitUntil(2*time.Second, func() bool {
		return contactsCache.flushCount() == 1 && dialogCache.flushCount() == 1 && privacyCache.flushCount() == 1 && photoCache.flushCount() == 1
	}) {
		t.Fatalf("read model listener 未在预期内连接并 flush account/dialog/photo caches")
	}

	dialogs := NewDialogStore(pool)
	dialogPeer := domain.Peer{Type: domain.PeerTypeUser, ID: target.ID}
	if err := dialogs.Upsert(ctx, owner.ID, domain.Dialog{Peer: dialogPeer, TopMessage: 1, TopMessageDate: 1700002999}); err != nil {
		t.Fatalf("upsert dialog: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > 0
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("dialog 写入未触发 dialog_light 失效: owners=%v peers=%+v want owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionBeforeContactDialog, hashBeforeContactDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)

	contacts := NewContactStore(pool)
	beforeContactDialogFanout := countDialogInvalidation(dialogCache, owner.ID, dialogPeer)
	if _, err := contacts.Upsert(ctx, owner.ID, domain.ContactInput{ContactUserID: target.ID, FirstName: "Target"}); err != nil {
		t.Fatalf("upsert contact: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return containsInt64(contactsCache.idsSnapshot(), owner.ID)
	}) {
		t.Fatalf("contacts 写入未触发 contact_account 失效: ids=%v owner=%d", contactsCache.idsSnapshot(), owner.ID)
	}
	var versionBeforeProfile, hashBeforeProfile int64
	if err := pool.QueryRow(ctx, `
SELECT version, hash
FROM read_model_versions
WHERE model = 'contact_account' AND owner_user_id = $1 AND peer_type = 'user' AND peer_id = $1`, owner.ID).Scan(&versionBeforeProfile, &hashBeforeProfile); err != nil {
		t.Fatalf("read contact_account version/hash before profile update: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > beforeContactDialogFanout
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("contact 写入未 fan-out 到 dialog_light 失效: owners=%v peers=%+v owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionAfterContactDialog, hashAfterContactDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if versionAfterContactDialog <= versionBeforeContactDialog {
		t.Fatalf("contact did not bump dialog_light version: before=%d after=%d", versionBeforeContactDialog, versionAfterContactDialog)
	}
	if hashAfterContactDialog == 0 || hashAfterContactDialog == hashBeforeContactDialog {
		t.Fatalf("contact did not refresh dialog_light hash: before=%d after=%d", hashBeforeContactDialog, hashAfterContactDialog)
	}
	beforeUserFanout := countInt64(contactsCache.idsSnapshot(), owner.ID)
	beforeUserDialogFanout := countDialogInvalidation(dialogCache, owner.ID, dialogPeer)
	versionBeforeProfileDialog, hashBeforeProfileDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if _, err := pool.Exec(ctx, "UPDATE users SET first_name = $2, updated_at = now() WHERE id = $1", target.ID, "ProjectionTargetRenamed"); err != nil {
		t.Fatalf("update target user profile field: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countInt64(contactsCache.idsSnapshot(), owner.ID) > beforeUserFanout
	}) {
		t.Fatalf("users profile 写入未 fan-out 到 contact_account 失效: ids=%v owner=%d", contactsCache.idsSnapshot(), owner.ID)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > beforeUserDialogFanout
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("users profile 写入未 fan-out 到 dialog_light 失效: owners=%v peers=%+v owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionAfterProfileDialog, hashAfterProfileDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if versionAfterProfileDialog <= versionBeforeProfileDialog {
		t.Fatalf("profile update did not bump dialog_light version: before=%d after=%d", versionBeforeProfileDialog, versionAfterProfileDialog)
	}
	if hashAfterProfileDialog == 0 || hashAfterProfileDialog == hashBeforeProfileDialog {
		t.Fatalf("profile update did not refresh dialog_light hash: before=%d after=%d", hashBeforeProfileDialog, hashAfterProfileDialog)
	}
	var versionBeforeLastSeen, hashBeforeLastSeen int64
	if err := pool.QueryRow(ctx, `
SELECT version, hash
FROM read_model_versions
WHERE model = 'contact_account' AND owner_user_id = $1 AND peer_type = 'user' AND peer_id = $1`, owner.ID).Scan(&versionBeforeLastSeen, &hashBeforeLastSeen); err != nil {
		t.Fatalf("read contact_account version before last_seen update: %v", err)
	}
	if versionBeforeLastSeen <= versionBeforeProfile {
		t.Fatalf("profile update did not bump contact_account version: before=%d after=%d", versionBeforeProfile, versionBeforeLastSeen)
	}
	if hashBeforeLastSeen == 0 || hashBeforeLastSeen == hashBeforeProfile {
		t.Fatalf("profile update did not refresh contact_account hash: before=%d after=%d", hashBeforeProfile, hashBeforeLastSeen)
	}
	if _, err := pool.Exec(ctx, "UPDATE users SET last_seen_at = GREATEST(last_seen_at, $2), updated_at = now() WHERE id = $1", target.ID, int64(1700003999)); err != nil {
		t.Fatalf("update target last_seen: %v", err)
	}
	var versionAfterLastSeen, hashAfterLastSeen int64
	if err := pool.QueryRow(ctx, `
SELECT version, hash
FROM read_model_versions
WHERE model = 'contact_account' AND owner_user_id = $1 AND peer_type = 'user' AND peer_id = $1`, owner.ID).Scan(&versionAfterLastSeen, &hashAfterLastSeen); err != nil {
		t.Fatalf("read contact_account version after last_seen update: %v", err)
	}
	if versionAfterLastSeen != versionBeforeLastSeen {
		t.Fatalf("last_seen update bumped contact_account version: before=%d after=%d", versionBeforeLastSeen, versionAfterLastSeen)
	}
	if hashAfterLastSeen != hashBeforeLastSeen {
		t.Fatalf("last_seen update refreshed contact_account hash: before=%d after=%d", hashBeforeLastSeen, hashAfterLastSeen)
	}
	if _, err := contacts.MutateBlocklist(ctx, store.BlocklistMutation{Kind: store.BlocklistBlock, OwnerUserID: owner.ID, PeerIDs: []int64{target.ID}, Date: 1700003000}, store.BlocklistDeliveryEffects); err != nil {
		t.Fatalf("block contact: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countInt64(contactsCache.idsSnapshot(), owner.ID) >= 2
	}) {
		t.Fatalf("block 写入未触发 contact_blocklist 失效: ids=%v owner=%d", contactsCache.idsSnapshot(), owner.ID)
	}

	privacy := NewPrivacyStore(pool)
	if err := privacy.SetPrivacyRules(ctx, domain.PrivacyRules{
		OwnerUserID: owner.ID,
		Key:         domain.PrivacyKeyProfilePhoto,
		Rules:       []domain.PrivacyRule{{Kind: domain.PrivacyRuleAllowContacts}},
	}); err != nil {
		t.Fatalf("set privacy: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return containsInt64(privacyCache.idsSnapshot(), owner.ID)
	}) {
		t.Fatalf("privacy 写入未触发 privacy_rules 失效: ids=%v owner=%d", privacyCache.idsSnapshot(), owner.ID)
	}

	media := NewMediaStore(pool)
	versionBeforeTargetPrivacy, hashBeforeTargetPrivacy := readContactAccountVersionHash(t, ctx, pool, owner.ID)
	versionBeforeTargetPrivacyDialog, hashBeforeTargetPrivacyDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	beforeTargetPrivacyFanout := countInt64(contactsCache.idsSnapshot(), owner.ID)
	beforeTargetPrivacyDialogFanout := countDialogInvalidation(dialogCache, owner.ID, dialogPeer)
	if err := privacy.SetPrivacyRules(ctx, domain.PrivacyRules{
		OwnerUserID: target.ID,
		Key:         domain.PrivacyKeyProfilePhoto,
		Rules:       []domain.PrivacyRule{{Kind: domain.PrivacyRuleDisallowAll}},
	}); err != nil {
		t.Fatalf("set target privacy: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countInt64(contactsCache.idsSnapshot(), owner.ID) > beforeTargetPrivacyFanout
	}) {
		t.Fatalf("target privacy 写入未 fan-out 到 contact_account 失效: ids=%v owner=%d", contactsCache.idsSnapshot(), owner.ID)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > beforeTargetPrivacyDialogFanout
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("target privacy 写入未 fan-out 到 dialog_light 失效: owners=%v peers=%+v owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionAfterTargetPrivacy, hashAfterTargetPrivacy := readContactAccountVersionHash(t, ctx, pool, owner.ID)
	if versionAfterTargetPrivacy <= versionBeforeTargetPrivacy {
		t.Fatalf("target privacy did not bump contact_account version: before=%d after=%d", versionBeforeTargetPrivacy, versionAfterTargetPrivacy)
	}
	if hashAfterTargetPrivacy == 0 || hashAfterTargetPrivacy == hashBeforeTargetPrivacy {
		t.Fatalf("target privacy did not refresh contact_account hash: before=%d after=%d", hashBeforeTargetPrivacy, hashAfterTargetPrivacy)
	}
	versionAfterTargetPrivacyDialog, hashAfterTargetPrivacyDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if versionAfterTargetPrivacyDialog <= versionBeforeTargetPrivacyDialog {
		t.Fatalf("target privacy did not bump dialog_light version: before=%d after=%d", versionBeforeTargetPrivacyDialog, versionAfterTargetPrivacyDialog)
	}
	if hashAfterTargetPrivacyDialog == 0 || hashAfterTargetPrivacyDialog == hashBeforeTargetPrivacyDialog {
		t.Fatalf("target privacy did not refresh dialog_light hash: before=%d after=%d", hashBeforeTargetPrivacyDialog, hashAfterTargetPrivacyDialog)
	}

	if err := media.PutPhoto(ctx, domain.Photo{ID: targetPhotoID, AccessHash: 994, FileReference: []byte("target-ref"), Date: 1700003003, DCID: 2}); err != nil {
		t.Fatalf("put target photo: %v", err)
	}
	versionBeforeTargetPhoto, hashBeforeTargetPhoto := readContactAccountVersionHash(t, ctx, pool, owner.ID)
	versionBeforeTargetPhotoDialog, hashBeforeTargetPhotoDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	beforeTargetPhotoFanout := countInt64(contactsCache.idsSnapshot(), owner.ID)
	beforeTargetPhotoDialogFanout := countDialogInvalidation(dialogCache, owner.ID, dialogPeer)
	if err := media.AddProfilePhotoKind(ctx, domain.PeerTypeUser, target.ID, domain.ProfilePhotoKindProfile, targetPhotoID, 1700003004); err != nil {
		t.Fatalf("add target profile photo: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countInt64(contactsCache.idsSnapshot(), owner.ID) > beforeTargetPhotoFanout
	}) {
		t.Fatalf("target profile photo 写入未 fan-out 到 contact_account 失效: ids=%v owner=%d", contactsCache.idsSnapshot(), owner.ID)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > beforeTargetPhotoDialogFanout
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("target profile photo 写入未 fan-out 到 dialog_light 失效: owners=%v peers=%+v owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionAfterTargetPhoto, hashAfterTargetPhoto := readContactAccountVersionHash(t, ctx, pool, owner.ID)
	if versionAfterTargetPhoto <= versionBeforeTargetPhoto {
		t.Fatalf("target profile photo did not bump contact_account version: before=%d after=%d", versionBeforeTargetPhoto, versionAfterTargetPhoto)
	}
	if hashAfterTargetPhoto == 0 || hashAfterTargetPhoto == hashBeforeTargetPhoto {
		t.Fatalf("target profile photo did not refresh contact_account hash: before=%d after=%d", hashBeforeTargetPhoto, hashAfterTargetPhoto)
	}
	versionAfterTargetPhotoDialog, hashAfterTargetPhotoDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if versionAfterTargetPhotoDialog <= versionBeforeTargetPhotoDialog {
		t.Fatalf("target profile photo did not bump dialog_light version: before=%d after=%d", versionBeforeTargetPhotoDialog, versionAfterTargetPhotoDialog)
	}
	if hashAfterTargetPhotoDialog == 0 || hashAfterTargetPhotoDialog == hashBeforeTargetPhotoDialog {
		t.Fatalf("target profile photo did not refresh dialog_light hash: before=%d after=%d", hashBeforeTargetPhotoDialog, hashAfterTargetPhotoDialog)
	}

	versionBeforeDraftDialog, hashBeforeDraftDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	beforeDraftDialogFanout := countDialogInvalidation(dialogCache, owner.ID, dialogPeer)
	if err := dialogs.SaveDraft(ctx, owner.ID, domain.DialogDraft{Peer: dialogPeer, Date: 1700003005, Message: "dialog light draft"}); err != nil {
		t.Fatalf("save dialog draft: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		return countDialogInvalidation(dialogCache, owner.ID, dialogPeer) > beforeDraftDialogFanout
	}) {
		owners, peers := dialogCache.entriesSnapshot()
		t.Fatalf("dialog draft 写入未触发 dialog_light 失效: owners=%v peers=%+v owner=%d peer=%+v", owners, peers, owner.ID, dialogPeer)
	}
	versionAfterDraftDialog, hashAfterDraftDialog := readDialogLightVersionHash(t, ctx, pool, owner.ID, dialogPeer)
	if versionAfterDraftDialog <= versionBeforeDraftDialog {
		t.Fatalf("draft did not bump dialog_light version: before=%d after=%d", versionBeforeDraftDialog, versionAfterDraftDialog)
	}
	if hashAfterDraftDialog == 0 || hashAfterDraftDialog == hashBeforeDraftDialog {
		t.Fatalf("draft did not refresh dialog_light hash: before=%d after=%d", hashBeforeDraftDialog, hashAfterDraftDialog)
	}

	if err := media.PutPhoto(ctx, domain.Photo{ID: photoID, AccessHash: 993, FileReference: []byte("ref"), Date: 1700003001, DCID: 2}); err != nil {
		t.Fatalf("put photo: %v", err)
	}
	if err := media.AddProfilePhotoKind(ctx, domain.PeerTypeUser, owner.ID, domain.ProfilePhotoKindProfile, photoID, 1700003002); err != nil {
		t.Fatalf("add profile photo: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool {
		for _, ownerPeer := range photoCache.ownersSnapshot() {
			if ownerPeer == (domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}) {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("profile photo 写入未触发 profile_photo 失效: owners=%+v want user %d", photoCache.ownersSnapshot(), owner.ID)
	}
}

func readContactAccountVersionHash(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ownerID int64) (int64, int64) {
	t.Helper()
	var version, hash int64
	if err := pool.QueryRow(ctx, `
SELECT version, hash
FROM read_model_versions
WHERE model = 'contact_account' AND owner_user_id = $1 AND peer_type = 'user' AND peer_id = $1`, ownerID).Scan(&version, &hash); err != nil {
		t.Fatalf("read contact_account version/hash: %v", err)
	}
	return version, hash
}

func readDialogLightVersionHash(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ownerID int64, peer domain.Peer) (int64, int64) {
	t.Helper()
	var version, hash int64
	if err := pool.QueryRow(ctx, `
SELECT version, hash
FROM read_model_versions
WHERE model = 'dialog_light' AND owner_user_id = $1 AND peer_type = $2 AND peer_id = $3`, ownerID, peer.Type, peer.ID).Scan(&version, &hash); err != nil {
		t.Fatalf("read dialog_light version/hash: %v", err)
	}
	return version, hash
}

func containsInt64(values []int64, target int64) bool {
	return countInt64(values, target) > 0
}

func countInt64(values []int64, target int64) int {
	n := 0
	for _, value := range values {
		if value == target {
			n++
		}
	}
	return n
}

func countDialogInvalidation(cache *fakeDialogReadModelCache, ownerID int64, peer domain.Peer) int {
	owners, peers := cache.entriesSnapshot()
	n := 0
	for i, owner := range owners {
		if owner == ownerID && i < len(peers) && peers[i] == peer {
			n++
		}
	}
	return n
}
