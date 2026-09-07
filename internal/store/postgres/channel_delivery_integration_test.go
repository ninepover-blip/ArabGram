package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type countingBeginDB struct {
	*pgxpool.Pool
	beginCount atomic.Int64
	queryCount atomic.Int64
}

func (db *countingBeginDB) Begin(ctx context.Context) (pgx.Tx, error) {
	db.beginCount.Add(1)
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &countingTx{Tx: tx, queryCount: &db.queryCount}, nil
}

type countingTx struct {
	pgx.Tx
	queryCount *atomic.Int64
}

func (tx *countingTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	tx.queryCount.Add(1)
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func (tx *countingTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.queryCount.Add(1)
	return tx.Tx.Query(ctx, sql, args...)
}

func (tx *countingTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	tx.queryCount.Add(1)
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func (db *countingBeginDB) resetBeginCount() {
	db.beginCount.Store(0)
}

func (db *countingBeginDB) begins() int64 {
	return db.beginCount.Load()
}

func (db *countingBeginDB) resetQueryCount() {
	db.queryCount.Store(0)
}

func (db *countingBeginDB) queries() int64 {
	return db.queryCount.Load()
}

func TestChannelDeliverySignalFreezesExactMonoforumAudiencePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 6051, Phone: "+1777601" + suffix, FirstName: "AudienceOwner"})
	if err != nil {
		t.Fatalf("create audience owner: %v", err)
	}
	subscriber, err := users.Create(ctx, domain.User{AccessHash: 6052, Phone: "+1777602" + suffix, FirstName: "AudienceSubscriber"})
	if err != nil {
		t.Fatalf("create audience subscriber: %v", err)
	}
	manager, err := users.Create(ctx, domain.User{AccessHash: 6053, Phone: "+1777603" + suffix, FirstName: "AudienceManager"})
	if err != nil {
		t.Fatalf("create audience manager: %v", err)
	}
	ordinary, err := users.Create(ctx, domain.User{AccessHash: 6054, Phone: "+1777604" + suffix, FirstName: "AudienceOrdinary"})
	if err != nil {
		t.Fatalf("create ordinary audience member: %v", err)
	}
	channels := newTestChannelStore(pool)
	parent, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "delivery-audience-" + suffix, Broadcast: true,
		MemberUserIDs: []int64{manager.ID, ordinary.ID}, Date: 1700050000,
	})
	if err != nil {
		t.Fatalf("create audience parent: %v", err)
	}
	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID: owner.ID, ChannelID: parent.Channel.ID, MemberID: manager.ID,
		AdminRights: domain.ChannelAdminRights{ManageDirectMessages: true}, Date: 1700050001,
	}); err != nil {
		t.Fatalf("grant direct-message manager: %v", err)
	}
	enabled, err := channels.SetPaidMessagesPrice(ctx, owner.ID, parent.Channel.ID, 0, true)
	if err != nil {
		t.Fatalf("enable audience monoforum: %v", err)
	}
	monoID := enabled.Channel.LinkedMonoforumID
	if monoID <= 0 {
		t.Fatal("enable audience monoforum returned no channel")
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channels WHERE id = ANY($1::bigint[])`, []int64{parent.Channel.ID, monoID})
		_, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id = ANY($1::bigint[])`, []int64{owner.ID, subscriber.ID, manager.ID, ordinary.ID})
	})

	var adminMinPTS, adminMaxPTS int
	var adminAudience string
	var adminUsers []int64
	if err := pool.QueryRow(ctx, `
SELECT min_pts, max_pts, audience_kind, audience_user_ids
FROM channel_delivery_events
WHERE channel_id = $1 AND audience_kind = 'monoforum_admins'
ORDER BY min_pts
LIMIT 1`, monoID).Scan(&adminMinPTS, &adminMaxPTS, &adminAudience, &adminUsers); err != nil {
		t.Fatalf("load monoforum admin signal: %v", err)
	}
	wantAdmins := []int64{owner.ID, manager.ID}
	slices.Sort(wantAdmins)
	if adminAudience != string(store.ChannelDeliveryAudienceMonoforumAdmins) || !slices.Equal(adminUsers, wantAdmins) {
		t.Fatalf("admin audience = %q/%v, want %q/%v", adminAudience, adminUsers, store.ChannelDeliveryAudienceMonoforumAdmins, wantAdmins)
	}

	sent, err := channels.SendMonoforumMessage(ctx, domain.SendMonoforumMessageRequest{
		MonoforumID: monoID, SenderUserID: subscriber.ID,
		SavedPeer: domain.Peer{Type: domain.PeerTypeUser, ID: subscriber.ID},
		RandomID:  7001, Message: "exact audience", Date: 1700050002,
	})
	if err != nil {
		t.Fatalf("send audience monoforum message: %v", err)
	}
	messagePayload := channelDeliveryPayloadForPTS(t, ctx, pool, monoID, sent.Event.Pts)
	wantMessageAudience := []int64{owner.ID, subscriber.ID, manager.ID}
	slices.Sort(wantMessageAudience)
	if messagePayload.AudienceKind != store.ChannelDeliveryAudienceMessageBox || !slices.Equal(messagePayload.AudienceUserIDs, wantMessageAudience) {
		t.Fatalf("message delivery payload = %+v, want frozen audience %v", messagePayload, wantMessageAudience)
	}
	if slices.Contains(messagePayload.AudienceUserIDs, ordinary.ID) {
		t.Fatalf("ordinary member %d leaked into message-box audience %v", ordinary.ID, messagePayload.AudienceUserIDs)
	}

	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID: owner.ID, ChannelID: parent.Channel.ID, MemberID: manager.ID,
		AdminRights: domain.ChannelAdminRights{}, Date: 1700050003,
	}); err != nil {
		t.Fatalf("revoke direct-message manager: %v", err)
	}
	var frozenAdminUsers []int64
	if err := pool.QueryRow(ctx, `
SELECT audience_user_ids
FROM channel_delivery_events
WHERE channel_id = $1 AND min_pts = $2 AND max_pts = $3`, monoID, adminMinPTS, adminMaxPTS).Scan(&frozenAdminUsers); err != nil {
		t.Fatalf("reload frozen admin signal: %v", err)
	}
	if !slices.Equal(frozenAdminUsers, wantAdmins) {
		t.Fatalf("admin revocation mutated frozen audience: got %v want %v", frozenAdminUsers, wantAdmins)
	}
	frozenMessage := channelDeliveryPayloadForPTS(t, ctx, pool, monoID, sent.Event.Pts)
	if !slices.Equal(frozenMessage.AudienceUserIDs, wantMessageAudience) {
		t.Fatalf("admin revocation mutated frozen message audience: got %v want %v", frozenMessage.AudienceUserIDs, wantMessageAudience)
	}

	afterRevoke, err := channels.SendMonoforumMessage(ctx, domain.SendMonoforumMessageRequest{
		MonoforumID: monoID, SenderUserID: subscriber.ID,
		SavedPeer: domain.Peer{Type: domain.PeerTypeUser, ID: subscriber.ID},
		RandomID:  7002, Message: "audience after revoke", Date: 1700050004,
	})
	if err != nil {
		t.Fatalf("send monoforum message after manager revocation: %v", err)
	}
	afterPayload := channelDeliveryPayloadForPTS(t, ctx, pool, monoID, afterRevoke.Event.Pts)
	wantAfterRevoke := []int64{owner.ID, subscriber.ID}
	slices.Sort(wantAfterRevoke)
	if !slices.Equal(afterPayload.AudienceUserIDs, wantAfterRevoke) {
		t.Fatalf("post-revocation audience = %v, want %v", afterPayload.AudienceUserIDs, wantAfterRevoke)
	}
	if slices.Contains(afterPayload.AudienceUserIDs, manager.ID) || slices.Contains(afterPayload.AudienceUserIDs, ordinary.ID) {
		t.Fatalf("revoked/ordinary users leaked into post-revocation audience %v", afterPayload.AudienceUserIDs)
	}
}

func channelDeliveryPayloadForPTS(t *testing.T, ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, channelID int64, pts int) store.ChannelDeliveryPayload {
	t.Helper()
	var payload store.ChannelDeliveryPayload
	var projection, audience string
	if err := db.QueryRow(ctx, `
SELECT min_pts, max_pts, projection_kind, audience_kind, audience_user_ids, affected_user_ids
FROM channel_delivery_events
WHERE channel_id = $1 AND max_pts = $2`, channelID, pts).Scan(
		&payload.MinPTS, &payload.MaxPTS, &projection, &audience, &payload.AudienceUserIDs, &payload.AffectedUserIDs,
	); err != nil {
		t.Fatalf("load channel delivery payload %d/%d: %v", channelID, pts, err)
	}
	payload.ProjectionKind = domain.ChannelUpdateEventType(projection)
	payload.AudienceKind = store.ChannelDeliveryAudienceKind(audience)
	return payload
}

func TestChannelDeliveryAffectedAudienceSurvivesInviteAndKickACLTransitionPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 6071, Phone: "+1777607" + suffix, FirstName: "AffectedOwner"})
	if err != nil {
		t.Fatalf("create affected owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 6072, Phone: "+1777608" + suffix, FirstName: "AffectedMember"})
	if err != nil {
		t.Fatalf("create affected member: %v", err)
	}
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "delivery-affected-" + suffix, Megagroup: true, Date: 1700070000,
	})
	if err != nil {
		t.Fatalf("create affected channel: %v", err)
	}
	channelID := created.Channel.ID
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id = ANY($1::bigint[])`, []int64{owner.ID, member.ID})
	})

	invited, err := channels.InviteToChannel(ctx, channelID, owner.ID, []int64{member.ID}, 1700070001)
	if err != nil {
		t.Fatalf("invite affected member: %v", err)
	}
	invitePayload := channelDeliveryPayloadForPTS(t, ctx, pool, channelID, invited.Event.Pts)
	if invitePayload.AudienceKind != store.ChannelDeliveryAudienceMembers {
		t.Fatalf("invite audience kind = %q, want members", invitePayload.AudienceKind)
	}
	if len(invitePayload.AffectedUserIDs) != 1 || invitePayload.AffectedUserIDs[0] != member.ID {
		t.Fatalf("invite affected users = %v, want [%d]", invitePayload.AffectedUserIDs, member.ID)
	}

	kicked, err := channels.EditChannelBanned(ctx, domain.EditChannelBannedRequest{
		UserID: owner.ID, ChannelID: channelID,
		Participant:  domain.Peer{Type: domain.PeerTypeUser, ID: member.ID},
		BannedRights: domain.ChannelBannedRights{ViewMessages: true, UntilDate: 2147483647},
		Date:         1700070002,
	})
	if err != nil {
		t.Fatalf("kick affected member: %v", err)
	}
	if kicked.ServiceEvent.Pts <= 0 {
		t.Fatalf("kick service event has no channel PTS: %+v", kicked.ServiceEvent)
	}
	kickPayload := channelDeliveryPayloadForPTS(t, ctx, pool, channelID, kicked.ServiceEvent.Pts)
	if len(kickPayload.AffectedUserIDs) != 1 || kickPayload.AffectedUserIDs[0] != member.ID {
		t.Fatalf("kick affected users = %v, want [%d]", kickPayload.AffectedUserIDs, member.ID)
	}
	if kickPayload.AudienceKind != store.ChannelDeliveryAudienceMembers {
		t.Fatalf("kick audience kind = %q, want members", kickPayload.AudienceKind)
	}
}

func TestChannelDeliveryDurableSignalAudienceAndAttemptStatePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 6101,
		Phone:      "+177761" + suffix,
		FirstName:  "ChannelDeliveryOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "channel-delivery-" + suffix,
		Megagroup:     true,
		Date:          1700100000,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID := created.Channel.ID
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence AND a.channel_id = $1`, channelID)
		_, _ = pool.Exec(ctx, `DELETE FROM channel_delivery_attempts WHERE channel_id = $1`, channelID)
		_, _ = pool.Exec(ctx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, owner.ID)
	})

	// Model a monoforum durable event directly. Production creates a separate
	// monoforum channel; this isolates the audience classification invariant.
	if _, err := pool.Exec(ctx, `UPDATE channels SET monoforum = true WHERE id = $1`, channelID); err != nil {
		t.Fatalf("mark test channel monoforum: %v", err)
	}
	secondPTS, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700100001, owner.ID)
	if err != nil {
		t.Fatalf("append monoforum event: %v", err)
	}

	rows, err := pool.Query(ctx, `
SELECT min_pts, max_pts, audience_kind, audience_user_ids
FROM channel_delivery_events
WHERE channel_id = $1
ORDER BY min_pts`, channelID)
	if err != nil {
		t.Fatalf("list durable channel signals: %v", err)
	}
	var audiences []string
	var audienceUsers [][]int64
	var ranges [][2]int
	for rows.Next() {
		var minPTS, maxPTS int
		var audience string
		var audienceUserIDs []int64
		if err := rows.Scan(&minPTS, &maxPTS, &audience, &audienceUserIDs); err != nil {
			rows.Close()
			t.Fatalf("scan durable channel signal: %v", err)
		}
		ranges = append(ranges, [2]int{minPTS, maxPTS})
		audiences = append(audiences, audience)
		audienceUsers = append(audienceUsers, audienceUserIDs)
	}
	rows.Close()
	if len(audiences) != 2 || audiences[0] != "members" || audiences[1] != "message_box" {
		t.Fatalf("channel signal audiences = %v, want [members message_box]", audiences)
	}
	if len(audienceUsers[0]) != 0 || len(audienceUsers[1]) != 1 || audienceUsers[1][0] != owner.ID {
		t.Fatalf("channel signal audience users = %v, want [[],[%d]]", audienceUsers, owner.ID)
	}
	if ranges[1][1] != secondPTS {
		t.Fatalf("second signal range = %v, want max pts %d", ranges[1], secondPTS)
	}
	var lanes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&lanes); err != nil {
		t.Fatalf("count channel lanes: %v", err)
	}
	if lanes != 1 {
		t.Fatalf("channel lane count = %d, want exactly one regardless of audience size", lanes)
	}

	delivery := NewChannelDeliveryStore(pool)
	windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind:          store.OutboxQueueChannelPTS,
		LaneLimit:          1,
		WindowSize:         8,
		WindowByteLimit:    1 << 20,
		LeaseDuration:      time.Minute,
		PhysicalDuration:   10 * time.Second,
		ClockSkewAllowance: time.Second,
		Owner:              "channel-egress-test",
	})
	if err != nil {
		t.Fatalf("claim channel window: %v", err)
	}
	if len(windows) != 1 || len(windows[0].Items) != 2 {
		t.Fatalf("claimed channel windows = %+v, want one two-item window", windows)
	}
	firstPayload, ok := windows[0].Items[0].Payload.(store.ChannelDeliveryPayload)
	if !ok || firstPayload.AudienceKind != store.ChannelDeliveryAudienceMembers {
		t.Fatalf("first channel payload = %#v, want member audience", windows[0].Items[0].Payload)
	}
	secondPayload, ok := windows[0].Items[1].Payload.(store.ChannelDeliveryPayload)
	if !ok || secondPayload.AudienceKind != store.ChannelDeliveryAudienceMessageBox || len(secondPayload.AudienceUserIDs) != 1 || secondPayload.AudienceUserIDs[0] != owner.ID {
		t.Fatalf("second channel payload = %#v, want message-box audience", windows[0].Items[1].Payload)
	}

	batchID, commandID := [16]byte{1}, [16]byte{2}
	bind, err := delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: windows[0].Items[0].Ref, SourceInstanceID: "channel-egress-source", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-a", TargetUserID: 0, BatchID: batchID, CommandID: commandID}}},
		{Ref: windows[0].Items[1].Ref, SourceInstanceID: "channel-egress-source", Targets: []store.OutboxAttemptTarget{}},
	})
	if err != nil {
		t.Fatalf("bind channel targets: %v", err)
	}
	if len(bind) != 2 || bind[0].Outcome != store.OutboxBindTargetBound || bind[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind outcomes = %+v, want Bound/Bound", bind)
	}
	now := time.Now().UTC()
	wrongSource, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: windows[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten,
		SourceInstanceID: "other-channel-egress-source",
		TargetInstanceID: "edge-a", TargetUserID: 0, BatchID: batchID, CommandID: commandID,
		EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: 8999, ObservedAt: now,
	}})
	if err != nil || len(wrongSource) != 1 || wrongSource[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("wrong-source channel evidence = %+v, %v", wrongSource, err)
	}
	evidence, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{
		{
			Ref:              windows[0].Items[0].Ref,
			Kind:             store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "channel-egress-source",
			TargetInstanceID: "edge-a",
			TargetUserID:     0,
			BatchID:          batchID,
			CommandID:        commandID,
			EligibleSessions: 2,
			WrittenSessions:  2,
			ServerMsgID:      9001,
			ObservedAt:       now,
		},
	})
	if err != nil {
		t.Fatalf("record channel evidence: %v", err)
	}
	if len(evidence) != 1 || evidence[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("evidence outcomes = %+v, want Recorded", evidence)
	}
	finalize := make([]store.OutboxFinalizeRequest, 0, 2)
	for _, item := range windows[0].Items {
		finalize = append(finalize, store.OutboxFinalizeRequest{Ref: item.Ref})
	}
	finalized, err := delivery.FinalizeAttempts(ctx, finalize)
	if err != nil {
		t.Fatalf("finalize channel attempts: %v", err)
	}
	if len(finalized.Results) != 2 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied || finalized.Results[1].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize outcomes = %+v, want Applied/Applied", finalized.Results)
	}
	var signals, targets, attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&signals); err != nil {
		t.Fatalf("count finalized channel signals: %v", err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM channel_delivery_attempt_targets t
JOIN channel_delivery_attempts a USING (item_id, lease_fence)
WHERE a.channel_id = $1`, channelID).Scan(&targets); err != nil {
		t.Fatalf("count channel target commands: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_attempts WHERE channel_id = $1`, channelID).Scan(&attempts); err != nil {
		t.Fatalf("count live channel attempts: %v", err)
	}
	if signals != 0 || targets != 0 || attempts != 0 {
		t.Fatalf("final state signals/targets/attempts = %d/%d/%d, want 0/0/0", signals, targets, attempts)
	}
	late, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref:              windows[0].Items[0].Ref,
		Kind:             store.OutboxEvidenceEdgeWritten,
		SourceInstanceID: "channel-egress-source",
		TargetInstanceID: "edge-a",
		TargetUserID:     0,
		BatchID:          batchID,
		CommandID:        commandID,
		EligibleSessions: 1,
		WrittenSessions:  1,
		ServerMsgID:      9002,
		ObservedAt:       now.Add(time.Second),
	}})
	if err != nil {
		t.Fatalf("record late channel evidence: %v", err)
	}
	if len(late) != 1 || (late[0].Outcome != store.OutboxEvidenceFenced && late[0].Outcome != store.OutboxEvidenceRejected) {
		t.Fatalf("late evidence after live attempt deletion = %+v", late)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_attempt_targets WHERE item_id = $1 AND lease_fence = $2`,
		windows[0].Items[0].Ref.ItemID, int64(windows[0].Items[0].Ref.LeaseFence)).Scan(&targets); err != nil || targets != 0 {
		t.Fatalf("channel target cascade after finalization = %d, %v", targets, err)
	}
}

func lateChannelPhysicalEvidence(ref store.OutboxAttemptRef, source, target string, batchID, commandID [16]byte, observedAt time.Time) store.OutboxAttemptEvidence {
	return store.OutboxAttemptEvidence{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: source,
		TargetInstanceID: target, TargetUserID: 0, BatchID: batchID, CommandID: commandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 9003, ObservedAt: observedAt,
	}
}

func TestChannelDeliveryFinalizeKeepsPendingWindowSuccessorLeasedPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 6141, Phone: "+1777601" + suffix, FirstName: "PendingSuccessorOwner",
	})
	if err != nil {
		t.Fatalf("create pending-successor owner: %v", err)
	}
	created, err := newTestChannelStore(pool).CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "channel-pending-successor-" + suffix,
		Megagroup: true, Date: 1700140000,
	})
	if err != nil {
		t.Fatalf("create pending-successor channel: %v", err)
	}
	channelID := created.Channel.ID
	if _, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700140001, 0); err != nil {
		t.Fatalf("append pending-successor event: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence AND a.channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempts WHERE channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, owner.ID)
	})

	delivery := NewChannelDeliveryStore(pool)
	windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Second, Owner: "channel-pending-successor-egress",
		PhysicalDuration: 100 * time.Millisecond, ClockSkewAllowance: 100 * time.Millisecond,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 2 {
		t.Fatalf("claim pending-successor window = %+v, %v", windows, err)
	}
	firstBatch, firstCommand := [16]byte{41}, [16]byte{42}
	secondBatch, secondCommand := [16]byte{43}, [16]byte{44}
	bound, err := delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: windows[0].Items[0].Ref, SourceInstanceID: "channel-pending-source", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-pending", BatchID: firstBatch, CommandID: firstCommand}}},
		{Ref: windows[0].Items[1].Ref, SourceInstanceID: "channel-pending-source", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-pending", BatchID: secondBatch, CommandID: secondCommand}}},
	})
	if err != nil || len(bound) != 2 || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind pending-successor targets = %+v, %v", bound, err)
	}
	evidence, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: windows[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten,
		SourceInstanceID: "channel-pending-source", TargetInstanceID: "edge-pending",
		BatchID: firstBatch, CommandID: firstCommand, EligibleSessions: 1, WrittenSessions: 1,
		ServerMsgID: 4100, ObservedAt: time.Now(),
	}})
	if err != nil || len(evidence) != 1 || evidence[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record first pending-successor evidence = %+v, %v", evidence, err)
	}
	finalized, err := delivery.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: windows[0].Items[0].Ref}})
	if err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize first pending-successor item = %+v, %v", finalized, err)
	}
	var laneState string
	var headItemID int64
	if err := pool.QueryRow(ctx, `SELECT state, head_item_id FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&laneState, &headItemID); err != nil {
		t.Fatalf("load pending-successor lane: %v", err)
	}
	if laneState != "leased" || headItemID != windows[0].Items[1].Ref.ItemID {
		t.Fatalf("pending successor lane state/head = %s/%d, want leased/%d", laneState, headItemID, windows[0].Items[1].Ref.ItemID)
	}
	next, ok, err := delivery.NextReadyAt(ctx, store.OutboxQueueChannelPTS, 0, nil)
	if err != nil || !ok || next.Kind != store.OutboxReadyRecoverLease || !next.ReadyAt.Equal(windows[0].Items[1].EvidenceDeadline) {
		t.Fatalf("pending successor durable wake = %+v ok=%v err=%v", next, ok, err)
	}

	// A forbidden ready+attempt row must fail closed instead of being normalized
	// on read or advertised as immediate claim work.
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_lanes
SET state = 'ready', ready_at = clock_timestamp() - interval '1 second',
    lease_owner = NULL, lease_until = NULL,
    window_tail_item_id = NULL, window_tail_sequence = NULL
WHERE channel_id = $1`, channelID); err != nil {
		t.Fatalf("create forbidden ready+bound state: %v", err)
	}
	next, ok, err = delivery.NextReadyAt(ctx, store.OutboxQueueChannelPTS, 0, nil)
	if err == nil || ok || !strings.Contains(err.Error(), "forbidden ready lane") {
		t.Fatalf("forbidden ready+bound state did not fail closed: next=%+v ok=%v err=%v", next, ok, err)
	}
	claimed, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Second, Owner: "must-not-spin-claim",
		PhysicalDuration: 100 * time.Millisecond, ClockSkewAllowance: 100 * time.Millisecond,
	})
	if err != nil || len(claimed) != 0 {
		t.Fatalf("forbidden ready+bound state was claimed = %+v, %v", claimed, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_lanes
SET state = 'leased', lease_owner = $2, lease_until = $3,
    window_tail_item_id = $4, window_tail_sequence = $5
WHERE channel_id = $1`, channelID, windows[0].Owner, windows[0].LeaseUntil,
		windows[0].Items[1].Ref.ItemID, windows[0].Items[1].Ref.Sequence); err != nil {
		t.Fatalf("restore canonical pending-successor lane: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_attempts
SET issued_at = clock_timestamp() - interval '3 seconds',
    command_not_after = clock_timestamp() - interval '2 seconds',
    evidence_deadline = clock_timestamp() - interval '1 second'
WHERE item_id = $1 AND lease_fence = $2`, windows[0].Items[1].Ref.ItemID, int64(windows[0].LeaseFence)); err != nil {
		t.Fatalf("expire pending successor: %v", err)
	}
	expired, err := delivery.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != windows[0].Items[1].Ref {
		t.Fatalf("expire pending successor = %+v, %v", expired, err)
	}
	retried, err := delivery.FinalizeAttempts(ctx, expired)
	if err != nil || len(retried.Results) != 1 || retried.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize pending-successor retry = %+v, %v", retried, err)
	}
}

func TestChannelDeliveryRecoveryNeverReplaysExpiredChannelCommandsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 6151, Phone: "+1777611" + suffix, FirstName: "RecoveryOwner"})
	if err != nil {
		t.Fatalf("create recovery owner: %v", err)
	}
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "channel-recovery-" + suffix, Megagroup: true, Date: 1700150000,
	})
	if err != nil {
		t.Fatalf("create recovery channel: %v", err)
	}
	channelID := created.Channel.ID
	if _, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700150001, 0); err != nil {
		t.Fatalf("append recovery event: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence AND a.channel_id = $1`, channelID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_delivery_attempts WHERE channel_id = $1`, channelID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, owner.ID)
	})

	delivery := NewChannelDeliveryStore(pool)
	windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute, Owner: "channel-recovery-old",
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 2 {
		t.Fatalf("claim recovery window = %+v, %v", windows, err)
	}
	oldBatchID := [16]byte{11}
	oldCommandID := [16]byte{12}
	sets := []store.OutboxAttemptTargetSet{
		{
			Ref: windows[0].Items[0].Ref, SourceInstanceID: "edge-source-stable",
			Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-shared", TargetUserID: 0, BatchID: oldBatchID, CommandID: oldCommandID}},
		},
		{Ref: windows[0].Items[1].Ref, SourceInstanceID: "edge-source-stable", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-shared", TargetUserID: 0, BatchID: oldBatchID, CommandID: oldCommandID}}},
	}
	bound, err := delivery.BindAttemptTargets(ctx, sets)
	if err != nil || len(bound) != 2 || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind recovery targets = %+v, %v", bound, err)
	}
	exact, err := delivery.RecoverBoundWindows(ctx, store.OutboxRecoverBoundRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, Mode: store.OutboxBoundRecoveryExactOwnerLive,
		LeaseDuration: time.Minute, Owner: "channel-recovery-old",
	})
	if err != nil || len(exact) != 1 || len(exact[0].Items) != 2 {
		t.Fatalf("exact-owner recovery = %+v, %v", exact, err)
	}
	if exact[0].LeaseFence != windows[0].LeaseFence || exact[0].Items[0].Ref != windows[0].Items[0].Ref ||
		!exact[0].Items[0].CommandNotAfter.Equal(windows[0].Items[0].CommandNotAfter) ||
		!exact[0].Items[0].EvidenceDeadline.Equal(windows[0].Items[0].EvidenceDeadline) ||
		exact[0].Items[0].SourceInstanceID != "edge-source-stable" || len(exact[0].Items[0].Targets) != 1 ||
		exact[0].Items[0].Targets[0].TargetUserID != 0 || exact[0].Items[0].Targets[0].BatchID != oldBatchID ||
		exact[0].Items[0].Targets[0].CommandID != oldCommandID {
		t.Fatalf("exact-owner recovery changed frozen plan: %+v", exact)
	}
	wrongOwner, err := delivery.RecoverBoundWindows(ctx, store.OutboxRecoverBoundRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, Mode: store.OutboxBoundRecoveryExactOwnerLive,
		LeaseDuration: time.Minute, Owner: "channel-recovery-other",
	})
	if err != nil || len(wrongOwner) != 0 {
		t.Fatalf("wrong-owner live recovery = %+v, %v, want empty", wrongOwner, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_lanes
SET lease_until = clock_timestamp() - interval '1 second'
WHERE channel_id = $1`, channelID); err != nil {
		t.Fatalf("expire recovery lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_attempts
SET issued_at = clock_timestamp() - interval '3 seconds',
    command_not_after = clock_timestamp() - interval '2 seconds',
    evidence_deadline = clock_timestamp() - interval '1 second'
WHERE channel_id = $1 AND lease_fence = $2`,
		channelID, int64(windows[0].LeaseFence)); err != nil {
		t.Fatalf("expire recovery evidence deadline: %v", err)
	}
	lateBeforeSweep, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{
		{
			Ref: windows[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "edge-source-stable", TargetInstanceID: "edge-shared", TargetUserID: 0,
			BatchID: oldBatchID, CommandID: oldCommandID, EligibleSessions: 1, WrittenSessions: 1,
			ServerMsgID: 499, ObservedAt: time.Now().Add(time.Hour),
		},
		{
			Ref: windows[0].Items[1].Ref, Kind: store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "edge-source-stable", TargetInstanceID: "edge-shared", TargetUserID: 0,
			BatchID: oldBatchID, CommandID: oldCommandID, EligibleSessions: 1, WrittenSessions: 1,
			ServerMsgID: 500, ObservedAt: time.Now().Add(time.Hour),
		},
	})
	if err != nil || len(lateBeforeSweep) != 2 ||
		lateBeforeSweep[0].Outcome != store.OutboxEvidenceFenced ||
		lateBeforeSweep[1].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("late channel completion before expiry sweep = %+v, %v, want Fenced/Fenced", lateBeforeSweep, err)
	}
	ownedAfterDBLease, err := delivery.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: windows[0].Items[0].Ref, Owner: windows[0].Owner,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond,
	}})
	if err != nil || len(ownedAfterDBLease) != 1 || ownedAfterDBLease[0].Outcome != store.OutboxResolutionFenced {
		t.Fatalf("DB-expired channel owner lease = %+v, %v, want Fenced", ownedAfterDBLease, err)
	}
	claimedAgain, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute, Owner: "must-not-replan",
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil {
		t.Fatalf("claim bound expired channel lane: %v", err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("normal claim replanned a bound channel lane: %+v", claimedAgain)
	}
	expired, err := delivery.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != windows[0].Items[0].Ref {
		t.Fatalf("expired recovery did not resolve exact head = %+v, %v", expired, err)
	}
	if next, ok, err := delivery.NextReadyAt(ctx, store.OutboxQueueChannelPTS, 0, nil); err != nil || !ok || next.Kind != store.OutboxReadyRecoverLease || next.Delay() > 0 {
		t.Fatalf("resolved attempt did not arm durable finalization: next=%+v ok=%v err=%v", next, ok, err)
	}
	finalizable, err := delivery.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(finalizable) != 1 || finalizable[0].Ref != windows[0].Items[0].Ref {
		t.Fatalf("recover expired finalizable channel head = %+v, %v", finalizable, err)
	}
	retried, err := delivery.FinalizeAttempts(ctx, finalizable)
	if err != nil || len(retried.Results) != 1 || retried.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize expired channel retry = %+v, %v", retried, err)
	}
	observedAt := time.Now()
	lateOld, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: windows[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "edge-source-stable",
		TargetInstanceID: "edge-shared", TargetUserID: 0,
		BatchID: oldBatchID, CommandID: oldCommandID, EligibleSessions: 1, WrittenSessions: 1,
		ServerMsgID: 501, ObservedAt: observedAt.Add(time.Second),
	}})
	if err != nil || len(lateOld) != 1 || lateOld[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("old channel evidence after retry = %+v, %v, want Fenced", lateOld, err)
	}
	fresh, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute, Owner: "channel-recovery-new",
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(fresh) != 1 || len(fresh[0].Items) != 2 {
		t.Fatalf("fresh claim after expired retry = %+v, %v", fresh, err)
	}
	if fresh[0].LeaseFence <= windows[0].LeaseFence || fresh[0].Items[0].Ref.Attempt <= windows[0].Items[0].Ref.Attempt ||
		fresh[0].Items[0].SourceInstanceID != "" || len(fresh[0].Items[0].Targets) != 0 {
		t.Fatalf("fresh claim reused expired plan identity: old=%+v fresh=%+v", windows[0], fresh[0])
	}
	newBatchID := [16]byte{21}
	newCommandID := [16]byte{22}
	noEligibleBatchID := [16]byte{23}
	noEligibleCommandID := [16]byte{24}
	bound, err = delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: fresh[0].Items[0].Ref, SourceInstanceID: "edge-source-fresh", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-shared", TargetUserID: 0, BatchID: newBatchID, CommandID: newCommandID}}},
		{Ref: fresh[0].Items[1].Ref, SourceInstanceID: "edge-source-fresh", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-empty", TargetUserID: 0, BatchID: noEligibleBatchID, CommandID: noEligibleCommandID}}},
	})
	if err != nil || len(bound) != 2 || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind fresh channel plan = %+v, %v", bound, err)
	}
	completed, err := delivery.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{
		{
			Ref: fresh[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "edge-source-fresh",
			TargetInstanceID: "edge-shared", TargetUserID: 0,
			BatchID: newBatchID, CommandID: newCommandID, EligibleSessions: 3, WrittenSessions: 3,
			ServerMsgID: 502, ObservedAt: observedAt.Add(2 * time.Second),
		},
		{
			Ref: fresh[0].Items[1].Ref, Kind: store.OutboxEvidenceEdgeNoEligible,
			SourceInstanceID: "edge-source-fresh", TargetInstanceID: "edge-empty", TargetUserID: 0,
			BatchID: noEligibleBatchID, CommandID: noEligibleCommandID,
			ObservedAt: observedAt.Add(2 * time.Second),
		},
	})
	if err != nil || len(completed) != 2 || completed[0].Outcome != store.OutboxEvidenceRecorded || completed[1].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("complete recovered channel evidence = %+v, %v", completed, err)
	}
	finalizable, err = delivery.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, AttemptLimit: 1,
	})
	if err != nil || len(finalizable) != 1 {
		t.Fatalf("recover finalizable channel attempts = %+v, %v", finalizable, err)
	}
	finalized, err := delivery.FinalizeAttempts(ctx, finalizable)
	if err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize recovered channel attempts = %+v, %v", finalized, err)
	}
	if _, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700150002, 0); err != nil {
		t.Fatalf("append post-empty channel event: %v", err)
	}
	unboundOld, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute, Owner: "channel-unbound-old",
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(unboundOld) != 1 || len(unboundOld[0].Items) != 1 {
		t.Fatalf("claim post-empty channel generation = %+v, %v", unboundOld, err)
	}
	if unboundOld[0].LeaseFence <= fresh[0].LeaseFence {
		t.Fatalf("post-empty channel fence %d reused prior generation fence %d", unboundOld[0].LeaseFence, fresh[0].LeaseFence)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_delivery_lanes
SET lease_until = clock_timestamp() - interval '1 second'
WHERE channel_id = $1`, channelID); err != nil {
		t.Fatalf("expire unbound channel generation: %v", err)
	}
	unboundFresh, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute, Owner: "channel-unbound-fresh",
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(unboundFresh) != 1 || len(unboundFresh[0].Items) != 1 {
		t.Fatalf("reclaim expired unbound channel generation = %+v, %v", unboundFresh, err)
	}
	if unboundFresh[0].LeaseFence <= unboundOld[0].LeaseFence || unboundFresh[0].Items[0].Ref.Attempt <= unboundOld[0].Items[0].Ref.Attempt {
		t.Fatalf("unbound reclaim did not allocate fresh identities: old=%+v fresh=%+v", unboundOld[0], unboundFresh[0])
	}
	var oldUnboundAttempts int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM channel_delivery_attempts
WHERE item_id = $1 AND lease_fence = $2`, unboundOld[0].Items[0].Ref.ItemID, int64(unboundOld[0].LeaseFence)).Scan(&oldUnboundAttempts); err != nil {
		t.Fatalf("load old unbound live attempt count: %v", err)
	}
	if oldUnboundAttempts != 0 {
		t.Fatalf("old unbound live attempts = %d, want 0", oldUnboundAttempts)
	}
	bound, err = delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: unboundFresh[0].Items[0].Ref, SourceInstanceID: "edge-source-unbound-fresh", Targets: []store.OutboxAttemptTarget{},
	}})
	if err != nil || len(bound) != 1 || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind reclaimed unbound channel plan = %+v, %v", bound, err)
	}
	last, err := delivery.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: unboundFresh[0].Items[0].Ref}})
	if err != nil || len(last.Results) != 1 || last.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize reclaimed unbound channel plan = %+v, %v", last, err)
	}
	if _, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700150003, 0); err != nil {
		t.Fatalf("append terminal-resync event: %v", err)
	}
	resyncWindow, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 8,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Second, Owner: "channel-terminal-resync",
		PhysicalDuration: 50 * time.Millisecond, ClockSkewAllowance: 50 * time.Millisecond,
	})
	if err != nil || len(resyncWindow) != 1 || len(resyncWindow[0].Items) != 1 {
		t.Fatalf("claim terminal-resync channel plan = %+v, %v", resyncWindow, err)
	}
	bound, err = delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: resyncWindow[0].Items[0].Ref, SourceInstanceID: "edge-source-terminal-resync",
		Targets: []store.OutboxAttemptTarget{{
			TargetInstanceID: "edge-terminal-resync", TargetUserID: 0,
			BatchID: [16]byte{31}, CommandID: [16]byte{32},
		}},
	}})
	if err != nil || len(bound) != 1 || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind terminal-resync channel plan = %+v, %v", bound, err)
	}
	resolved, err := delivery.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: resyncWindow[0].Items[0].Ref, Owner: resyncWindow[0].Owner,
		Kind: store.OutboxResolutionTerminalResync, LastError: "deterministic projection poison",
	}})
	if err != nil || len(resolved) != 1 || resolved[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("resolve terminal channel resync = %+v, %v", resolved, err)
	}
	early, err := delivery.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: resyncWindow[0].Items[0].Ref}})
	if err != nil || len(early.Results) != 1 || early.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor {
		t.Fatalf("terminal resync finalized before frozen evidence deadline = %+v, %v", early, err)
	}
	wait := time.Until(resyncWindow[0].Items[0].EvidenceDeadline) + 20*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}
	resynced, err := delivery.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: resyncWindow[0].Items[0].Ref}})
	if err != nil || len(resynced.Results) != 1 || resynced.Results[0].Outcome != store.OutboxFinalizeTerminalResync {
		t.Fatalf("finalize terminal channel resync = %+v, %v", resynced, err)
	}
	var retainedLanes, retainedEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&retainedLanes); err != nil {
		t.Fatalf("count terminal-resync channel lanes: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&retainedEvents); err != nil {
		t.Fatalf("count terminal-resync channel events: %v", err)
	}
	if retainedLanes != 0 || retainedEvents != 0 {
		t.Fatalf("terminal resync permanently halted channel lane: lanes=%d events=%d", retainedLanes, retainedEvents)
	}
}

func TestChannelDeliveryFinalizeWaitsForUncommittedAppendMarkerPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 6200, Phone: "+177761" + suffix, FirstName: "ChannelCommitFenceOwner",
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := newTestChannelStore(pool).CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "channel-delivery-commit-fence-" + suffix,
		Megagroup: true, Date: 1700190000,
	})
	if err != nil {
		t.Fatal(err)
	}
	channelID := created.Channel.ID
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence AND a.channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempts WHERE channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, owner.ID)
	})
	delivery := NewChannelDeliveryStore(pool)
	windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: 1, WindowSize: 1,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
		Owner: "channel-commit-fence-egress",
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	if bound, err := delivery.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "channel-commit-fence-source", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", bound, err)
	}
	var committedBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&committedBefore); err != nil {
		t.Fatal(err)
	}
	if committedBefore != 1 {
		t.Fatalf("committed channel signals before race=%d want=1", committedBefore)
	}

	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Rollback(ctx) }()
	pts, err := reserveChannelPts(ctx, producer, channelID, 1)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.ChannelUpdateEvent{
		ChannelID: channelID, Type: domain.ChannelUpdateNoop,
		Pts: pts, PtsCount: 1, Date: 1700190002,
	}
	if err := insertChannelEventTx(ctx, producer, event); err != nil {
		t.Fatalf("insert uncommitted channel event: %v", err)
	}
	var exclusiveAvailable bool
	if err := pool.QueryRow(ctx, `
SELECT pg_try_advisory_xact_lock(outbox_lane_advisory_key('channel_pts', $1))`, channelID).Scan(&exclusiveAvailable); err != nil {
		t.Fatal(err)
	}
	if exclusiveAvailable {
		t.Fatal("uncommitted channel append did not retain its shared commit fence")
	}
	var successorID int64
	if err := producer.QueryRow(ctx, `
SELECT id FROM channel_delivery_events WHERE channel_id = $1 AND max_pts = $2`, channelID, pts).Scan(&successorID); err != nil {
		t.Fatal(err)
	}

	type finalizeResult struct {
		batch store.OutboxFinalizeBatch
		err   error
	}
	done := make(chan finalizeResult, 1)
	go func() {
		batch, err := delivery.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
		done <- finalizeResult{batch: batch, err: err}
	}()
	select {
	case result := <-done:
		var headID, committed int64
		_ = pool.QueryRow(ctx, `SELECT head_item_id FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&headID)
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&committed)
		t.Fatalf("channel finalizer crossed an uncommitted append: batch=%+v err=%v head=%d committed=%d", result.batch, result.err, headID, committed)
	case <-time.After(50 * time.Millisecond):
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || len(result.batch.Results) != 1 || result.batch.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize = %+v err=%v", result.batch, result.err)
	}
	var headID int64
	if err := pool.QueryRow(ctx, `SELECT head_item_id FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&headID); err != nil {
		t.Fatal(err)
	}
	if headID != successorID {
		t.Fatalf("head=%d want uncommitted successor=%d", headID, successorID)
	}
}

func TestChannelDeliveryFinalizeBatchesIndependentLanesIntoBoundedTransactionsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := channelDeliveryRandomSuffix(t)
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 6299, Phone: "+177769" + suffix, FirstName: "ChannelFinalizeBatchOwner",
	})
	if err != nil {
		t.Fatal(err)
	}
	const laneCount = outboxFinalizeTransactionLanes + 1
	channelIDs := make([]int64, 0, laneCount)
	channels := newTestChannelStore(pool)
	for i := 0; i < laneCount; i++ {
		created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
			CreatorUserID: owner.ID,
			Title:         fmt.Sprintf("channel-finalize-batch-%s-%02d", suffix, i),
			Megagroup:     true,
			Date:          1700191000 + i,
		})
		if err != nil {
			t.Fatalf("create channel %d: %v", i, err)
		}
		channelIDs = append(channelIDs, created.Channel.ID)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence
  AND a.channel_id = ANY($1::bigint[])`, channelIDs)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempts WHERE channel_id = ANY($1::bigint[])`, channelIDs)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channels WHERE id = ANY($1::bigint[])`, channelIDs)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, owner.ID)
	})

	countingDB := &countingBeginDB{Pool: pool}
	delivery := NewChannelDeliveryStore(countingDB)
	windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueChannelPTS, LaneLimit: laneCount, WindowSize: 1,
		WindowByteLimit: 1 << 20, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
		Owner: "channel-finalize-batch-egress",
	})
	if err != nil || len(windows) != laneCount {
		t.Fatalf("claim lanes = %d err=%v, want %d", len(windows), err, laneCount)
	}
	sets := make([]store.OutboxAttemptTargetSet, 0, laneCount)
	evidence := make([]store.OutboxAttemptEvidence, 0, laneCount)
	finalize := make([]store.OutboxFinalizeRequest, 0, laneCount)
	for _, window := range windows {
		if len(window.Items) != 1 {
			t.Fatalf("lane %d items=%d want=1", window.StreamID, len(window.Items))
		}
		ref := window.Items[0].Ref
		sets = append(sets, store.OutboxAttemptTargetSet{
			Ref: ref, SourceInstanceID: "channel-finalize-batch-source", Targets: []store.OutboxAttemptTarget{},
		})
		evidence = append(evidence, store.OutboxAttemptEvidence{
			Ref: ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets,
			SourceInstanceID: "channel-finalize-batch-source", ObservedAt: time.Now(),
		})
		finalize = append(finalize, store.OutboxFinalizeRequest{Ref: ref})
	}
	if bound, err := delivery.BindAttemptTargets(ctx, sets); err != nil || len(bound) != laneCount {
		t.Fatalf("bind lanes = %d err=%v", len(bound), err)
	}
	if recorded, err := delivery.RecordAttemptEvidenceBatch(ctx, evidence); err != nil || len(recorded) != laneCount {
		t.Fatalf("record evidence = %d err=%v", len(recorded), err)
	}

	countingDB.resetBeginCount()
	batch, err := delivery.FinalizeAttempts(ctx, finalize)
	if err != nil || len(batch.Results) != laneCount {
		t.Fatalf("finalize lanes = %d err=%v", len(batch.Results), err)
	}
	for i, result := range batch.Results {
		if result.Outcome != store.OutboxFinalizeApplied {
			t.Fatalf("finalize result %d = %+v", i, result)
		}
	}
	if got := countingDB.begins(); got != 2 {
		t.Fatalf("finalize begin count=%d want=2 for %d lanes", got, laneCount)
	}
}

func TestChannelDeliveryConcurrentAppendFinalizeNeverOrphansLanePostgres(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := channelDeliveryRandomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 6201, Phone: "+177762" + suffix, FirstName: "ChannelRaceOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "channel-delivery-race-" + suffix,
		Megagroup:     true,
		Date:          1700200000,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID := created.Channel.ID
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempt_targets t
USING channel_delivery_attempts a
WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence AND a.channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempts WHERE channel_id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM channels WHERE id = $1`, channelID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, owner.ID)
	})

	const appended = 64
	delivery := NewChannelDeliveryStore(pool)
	producerDone := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(producerDone)
		for i := 0; i < appended; i++ {
			if _, err := appendChannelDeliveryTestEvent(ctx, pool, channelID, 1700200001+i, 0); err != nil {
				errCh <- fmt.Errorf("append event %d: %w", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			windows, err := delivery.ClaimWindows(ctx, store.OutboxClaimRequest{
				QueueKind:          store.OutboxQueueChannelPTS,
				LaneLimit:          1,
				WindowSize:         16,
				WindowByteLimit:    1 << 20,
				LeaseDuration:      time.Second,
				PhysicalDuration:   200 * time.Millisecond,
				ClockSkewAllowance: 100 * time.Millisecond,
				Owner:              "channel-race-egress",
			})
			if err != nil {
				errCh <- fmt.Errorf("claim: %w", err)
				return
			}
			if len(windows) == 0 {
				select {
				case <-producerDone:
					var remaining int
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&remaining); err != nil {
						errCh <- fmt.Errorf("count remaining: %w", err)
						return
					}
					if remaining == 0 {
						return
					}
				default:
				}
				time.Sleep(time.Millisecond)
				continue
			}
			for _, window := range windows {
				sets := make([]store.OutboxAttemptTargetSet, len(window.Items))
				evidence := make([]store.OutboxAttemptEvidence, len(window.Items))
				finalize := make([]store.OutboxFinalizeRequest, len(window.Items))
				observed := time.Now().UTC()
				for i, item := range window.Items {
					sets[i] = store.OutboxAttemptTargetSet{Ref: item.Ref, SourceInstanceID: "channel-race-source", Targets: []store.OutboxAttemptTarget{}}
					evidence[i] = store.OutboxAttemptEvidence{Ref: item.Ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets, SourceInstanceID: "channel-race-source", ObservedAt: observed}
					finalize[i] = store.OutboxFinalizeRequest{Ref: item.Ref}
				}
				if _, err := delivery.BindAttemptTargets(ctx, sets); err != nil {
					errCh <- fmt.Errorf("bind: %w", err)
					return
				}
				if _, err := delivery.RecordAttemptEvidenceBatch(ctx, evidence); err != nil {
					errCh <- fmt.Errorf("evidence: %w", err)
					return
				}
				if _, err := delivery.FinalizeAttempts(ctx, finalize); err != nil {
					errCh <- fmt.Errorf("finalize: %w", err)
					return
				}
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var signals, lanes, businessEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_events WHERE channel_id = $1`, channelID).Scan(&signals); err != nil {
		t.Fatalf("count final signals: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_delivery_lanes WHERE channel_id = $1`, channelID).Scan(&lanes); err != nil {
		t.Fatalf("count final lanes: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_update_events WHERE channel_id = $1`, channelID).Scan(&businessEvents); err != nil {
		t.Fatalf("count durable business events: %v", err)
	}
	if signals != 0 || lanes != 0 || businessEvents != appended+1 {
		t.Fatalf("final signal/lane/business counts = %d/%d/%d, want 0/0/%d", signals, lanes, businessEvents, appended+1)
	}
}

func appendChannelDeliveryTestEvent(ctx context.Context, db interface {
	Begin(context.Context) (pgx.Tx, error)
}, channelID int64, date int, savedPeerUserID int64) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	pts, err := reserveChannelPts(ctx, tx, channelID, 1)
	if err != nil {
		return 0, err
	}
	event := domain.ChannelUpdateEvent{
		ChannelID: channelID,
		Type:      domain.ChannelUpdateNoop,
		Pts:       pts,
		PtsCount:  1,
		Date:      date,
	}
	if savedPeerUserID > 0 {
		event.Message = domain.ChannelMessage{
			ChannelID: channelID,
			ID:        pts,
			SavedPeer: domain.Peer{Type: domain.PeerTypeUser, ID: savedPeerUserID},
			Pts:       pts,
		}
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return pts, nil
}

func channelDeliveryRandomSuffix(t *testing.T) string {
	t.Helper()
	var raw [5]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("generate channel delivery suffix: %v", err)
	}
	return hex.EncodeToString(raw[:])
}
