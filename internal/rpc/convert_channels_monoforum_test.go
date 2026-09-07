package rpc

import (
	"testing"

	"telesrv/internal/domain"
)

// TestTgChannelMonoforumSuppressesCreatorChrome 锁定频道私信(monoforum)渲染修复的核心不变量:
// monoforum 虚拟频道在 TDesktop 必须呈现为 Direct-Messages 容器而非普通群。真机 bug 的根因是
// mono 自身被投影了 Creator=true —— TDesktop 的 NeedAboutGroup 对 megagroup 看 amCreator() 决定
// 画 "You created a group" 群聊空状态,Leave/订阅数 chrome 也 key off amCreator/asMegagroup。
// DM 容器身份(本地 MonoforumAdmin 标志)由客户端从母频道 canAccessMonoforum 派生,与 mono 自身无关。
func TestTgChannelMonoforumSuppressesCreatorChrome(t *testing.T) {
	const owner = int64(1001)
	const parentID = int64(5001)
	const monoID = int64(5002)

	mono := domain.Channel{
		ID: monoID, CreatorUserID: owner, Title: "Parent",
		Megagroup: true, Monoforum: true, LinkedMonoforumID: parentID,
		// monoforum 镜像母频道 DM 启用状态;启用时才下发 linked_monoforum_id。
		BroadcastMessagesAllowed: true,
	}
	// 即便带 creator 角色的 synthetic self(admin 预览),mono 自身对象也必须 Creator=false / 无 admin_rights。
	syntheticSelf := &domain.ChannelMember{ChannelID: monoID, UserID: owner, Status: domain.ChannelMemberActive, Role: domain.ChannelRoleCreator}
	monoTg := tgChannel(owner, mono, syntheticSelf)
	if !monoTg.Megagroup || !monoTg.Monoforum {
		t.Fatalf("mono megagroup=%v monoforum=%v, want both true", monoTg.Megagroup, monoTg.Monoforum)
	}
	if monoTg.Creator {
		t.Fatalf("mono Creator=true, want false (Creator=true paints the wrong group chrome)")
	}
	if _, ok := monoTg.GetAdminRights(); ok {
		t.Fatalf("mono must carry no admin_rights (admin status derived client-side from parent)")
	}
	if monoTg.Left {
		t.Fatalf("mono Left=true, want false")
	}
	if id, ok := monoTg.GetLinkedMonoforumID(); !ok || id != parentID {
		t.Fatalf("mono linked_monoforum_id = %d ok=%v, want parent %d", id, ok, parentID)
	}

	// 母广播频道对 owner 投影:broadcast-only + Creator=true(canAccessMonoforum 走 amCreator 旁路)
	// + broadcast_messages_allowed + 反向 linked_monoforum_id。三者共同让客户端派生 MonoforumAdmin。
	parent := domain.Channel{
		ID: parentID, CreatorUserID: owner, Title: "Parent",
		Broadcast: true, BroadcastMessagesAllowed: true, LinkedMonoforumID: monoID,
	}
	parentTg := tgChannel(owner, parent, nil)
	if !parentTg.Broadcast || parentTg.Megagroup || parentTg.Monoforum {
		t.Fatalf("parent broadcast=%v mega=%v mono=%v, want broadcast only", parentTg.Broadcast, parentTg.Megagroup, parentTg.Monoforum)
	}
	if !parentTg.Creator {
		t.Fatalf("parent Creator=false for owner viewer, want true (canAccessMonoforum via amCreator)")
	}
	if !parentTg.BroadcastMessagesAllowed {
		t.Fatalf("parent broadcast_messages_allowed=false, want true")
	}
	if id, ok := parentTg.GetLinkedMonoforumID(); !ok || id != monoID {
		t.Fatalf("parent linked_monoforum_id = %d ok=%v, want mono %d", id, ok, monoID)
	}

	// 普通频道(Monoforum=false)不受影响:creator self 仍投影 Creator=true。
	normal := domain.Channel{ID: 6001, CreatorUserID: owner, Title: "Normal", Megagroup: true}
	normalTg := tgChannel(owner, normal, &domain.ChannelMember{ChannelID: 6001, UserID: owner, Status: domain.ChannelMemberActive, Role: domain.ChannelRoleCreator})
	if !normalTg.Creator {
		t.Fatalf("normal megagroup creator suppressed; monoforum guard leaked to non-monoforum channels")
	}
}

// TestChannelAdminRightsManageDirectMessagesRoundTrip 证明新映射的 manage_direct_messages(flags.17)
// 双向无损,且对未设置该权限的普通管理员保持惰性(不污染线格式)。
func TestChannelAdminRightsManageDirectMessagesRoundTrip(t *testing.T) {
	tgRights := tgChatAdminRights(domain.ChannelAdminRights{ManageDirectMessages: true, PostMessages: true})
	if !tgRights.ManageDirectMessages {
		t.Fatalf("tg ManageDirectMessages=false, want true")
	}
	if back := domainChannelAdminRights(tgRights); !back.ManageDirectMessages {
		t.Fatalf("round-trip dropped ManageDirectMessages")
	}
	if plain := tgChatAdminRights(domain.ChannelAdminRights{PinMessages: true}); plain.ManageDirectMessages {
		t.Fatalf("normal admin rights wrongly set ManageDirectMessages (not inert)")
	}
}

// TestTgChannelMonoforumDisabledHidesLink 锁定关闭 Direct Messages 时的投影:母频道与 monoforum
// **双方都**隐藏 linked_monoforum_id(都以 BroadcastMessagesAllowed 表示 DM 启用,关闭时为 false)。
// monoforum 必须也隐藏 —— 否则用户打开 monoforum 重新拉取频道对象时,mono 仍带 link →
// setMonoforumLink(parent) 触发 link&&monoforumDisabled 分支把 MonoforumDisabled 清掉,「已停用私信」
// 停用页脚消失。monoforum 自身绝不投影 broadcast_messages_allowed(它是 megagroup)。
func TestTgChannelMonoforumDisabledHidesLink(t *testing.T) {
	const owner = int64(1001)
	const parentID = int64(5001)
	const monoID = int64(5002)

	parent := domain.Channel{ID: parentID, CreatorUserID: owner, Broadcast: true, LinkedMonoforumID: monoID, BroadcastMessagesAllowed: false}
	if _, ok := tgChannel(owner, parent, nil).GetLinkedMonoforumID(); ok {
		t.Fatalf("disabled parent projected linked_monoforum_id, want hidden")
	}

	mono := domain.Channel{ID: monoID, CreatorUserID: owner, Megagroup: true, Monoforum: true, LinkedMonoforumID: parentID, BroadcastMessagesAllowed: false}
	monoTg := tgChannel(owner, mono, nil)
	if _, ok := monoTg.GetLinkedMonoforumID(); ok {
		t.Fatalf("disabled monoforum projected linked_monoforum_id, want hidden (else opening it clears MonoforumDisabled → footer disappears)")
	}
	if !monoTg.Monoforum || !monoTg.Megagroup {
		t.Fatalf("disabled mono monoforum=%v mega=%v, want both still true", monoTg.Monoforum, monoTg.Megagroup)
	}
	if monoTg.BroadcastMessagesAllowed {
		t.Fatalf("monoforum leaked broadcast_messages_allowed, want never projected (it is a megagroup)")
	}
}
