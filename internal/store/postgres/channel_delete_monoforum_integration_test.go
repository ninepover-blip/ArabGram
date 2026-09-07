package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

// TestDeleteChannelCascadesLinkedMonoforumPostgres 回归 Risk A:删除开启了 Direct Messages 的母广播
// 频道时,关联 monoforum 在同一事务内被软删并随结果返回。不级联会留下指向已删父频道的孤儿 mono。
// 门控于 TELESRV_TEST_POSTGRES_DSN。
func TestDeleteChannelCascadesLinkedMonoforumPostgres(t *testing.T) {
	pool := testPool(t) // 未设 DSN 会 t.Skip
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 91, Phone: "+1778" + suffix + "31", FirstName: "DelMonoOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) > 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	channels := newTestChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID, Title: "Del Mono BC " + suffix, Broadcast: true, Date: 1700000900,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	parentID := created.Channel.ID
	channelIDs = append(channelIDs, parentID)

	enabled, err := channels.SetPaidMessagesPrice(ctx, owner.ID, parentID, 0, true)
	if err != nil {
		t.Fatalf("enable DM: %v", err)
	}
	monoID := enabled.Channel.LinkedMonoforumID
	if monoID == 0 {
		t.Fatalf("no monoforum created")
	}
	channelIDs = append(channelIDs, monoID)

	res, err := channels.DeleteChannel(ctx, domain.DeleteChannelRequest{UserID: owner.ID, ChannelID: parentID, Date: 1700001000})
	if err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	if !res.Channel.Deleted {
		t.Fatalf("parent not marked deleted: %+v", res.Channel)
	}
	if res.LinkedMonoforum == nil || res.LinkedMonoforum.ID != monoID || !res.LinkedMonoforum.Deleted {
		t.Fatalf("result LinkedMonoforum = %+v, want deleted mono %d", res.LinkedMonoforum, monoID)
	}

	// 两行在 DB 里都必须 deleted=true。
	var parentDeleted, monoDeleted bool
	if err := pool.QueryRow(ctx, "SELECT deleted FROM channels WHERE id = $1", parentID).Scan(&parentDeleted); err != nil {
		t.Fatalf("read parent deleted: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT deleted FROM channels WHERE id = $1", monoID).Scan(&monoDeleted); err != nil {
		t.Fatalf("read mono deleted: %v", err)
	}
	if !parentDeleted || !monoDeleted {
		t.Fatalf("deleted flags parent=%v mono=%v, want both true (orphan monoforum left behind)", parentDeleted, monoDeleted)
	}

	// 删后 getDialogs 不再返回母频道或 mono。
	dialogs, err := channels.ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list dialogs after delete: %v", err)
	}
	for _, d := range dialogs.Dialogs {
		if d.Peer.ID == monoID || d.Peer.ID == parentID {
			t.Fatalf("dialogs after delete still include parent/mono: %+v", d)
		}
	}
}
