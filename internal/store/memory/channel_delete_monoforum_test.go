package memory

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

// TestDeleteChannelCascadesLinkedMonoforum 验证删除开启了 Direct Messages 的母广播频道时,关联的
// monoforum 虚拟频道被一并软删并随结果返回。不级联会留下 monoforum=true 但 linked_monoforum_id 指向
// 已删父频道的孤儿(web 客户端渲染崩 + DB 垃圾随删除累积)。
func TestDeleteChannelCascadesLinkedMonoforum(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	broadcast, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1, Title: "DM Broadcast", Broadcast: true, Date: 1_700_000_900,
	})
	if err != nil {
		t.Fatalf("create broadcast: %v", err)
	}
	parentID := broadcast.Channel.ID
	enabled, err := store.SetPaidMessagesPrice(ctx, 1, parentID, 0, true)
	if err != nil {
		t.Fatalf("enable DM: %v", err)
	}
	monoID := enabled.Channel.LinkedMonoforumID
	if monoID == 0 {
		t.Fatalf("no monoforum created")
	}

	res, err := store.DeleteChannel(ctx, domain.DeleteChannelRequest{UserID: 1, ChannelID: parentID, Date: 1_700_001_000})
	if err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	if !res.Channel.Deleted {
		t.Fatalf("parent not marked deleted: %+v", res.Channel)
	}
	if res.LinkedMonoforum == nil || res.LinkedMonoforum.ID != monoID || !res.LinkedMonoforum.Deleted {
		t.Fatalf("result LinkedMonoforum = %+v, want deleted mono %d", res.LinkedMonoforum, monoID)
	}
	if mono := store.channels[monoID]; !mono.Deleted {
		t.Fatalf("monoforum %d not soft-deleted in store: %+v", monoID, mono)
	}
	// 删后 getDialogs 不应再返回母频道或 mono。
	dialogs, err := store.ListChannelDialogs(ctx, 1, domain.DialogFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list dialogs after delete: %v", err)
	}
	for _, d := range dialogs.Dialogs {
		if d.Peer.ID == monoID || d.Peer.ID == parentID {
			t.Fatalf("dialogs after delete still include parent/mono: %+v", d)
		}
	}
}

// TestDeleteChannelNoMonoforumNoCascade 锁定不开私信的普通频道删除不受影响:LinkedMonoforum 为 nil,
// 不会误触级联。
func TestDeleteChannelNoMonoforumNoCascade(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	plain, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1, Title: "Plain Group", Megagroup: true, Date: 1_700_000_900,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	res, err := store.DeleteChannel(ctx, domain.DeleteChannelRequest{UserID: 1, ChannelID: plain.Channel.ID, Date: 1_700_001_000})
	if err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if res.LinkedMonoforum != nil {
		t.Fatalf("plain channel delete returned LinkedMonoforum %+v, want nil", res.LinkedMonoforum)
	}
}
