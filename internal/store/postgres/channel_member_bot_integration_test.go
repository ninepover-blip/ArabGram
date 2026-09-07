package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestChannelStoreListActiveChannelBotMembers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	bots := NewBotStore(pool)
	channels := newTestChannelStore(pool)

	owner, err := users.Create(ctx, domain.User{AccessHash: 8901, Phone: "+1888" + suffix + "01", FirstName: "BotOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{AccessHash: 8902, Phone: "+1888" + suffix + "02", FirstName: "Human"})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{AccessHash: 8903, FirstName: "MemberBot", Username: "member_" + suffix + "_bot"},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "secret_" + suffix},
		testBotLifecycleEffects,
	)
	if err != nil {
		t.Fatalf("create bot: %v", err)
	}

	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM bots WHERE bot_user_id = $1 OR owner_user_id = $2", bot.ID, owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID, bot.ID})
	})

	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Bot Members " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID, bot.ID},
		Date:          1700008900,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	list, err := channels.ListActiveChannelBotMembers(ctx, owner.ID, channelID, 0, 20)
	if err != nil {
		t.Fatalf("list active bot members: %v", err)
	}
	if list.Count != 1 || len(list.Participants) != 1 || list.Participants[0].UserID != bot.ID {
		t.Fatalf("bot members = %+v, want only bot %d", list, bot.ID)
	}

	if _, err := channels.SetParticipantsHidden(ctx, owner.ID, channelID, true); err != nil {
		t.Fatalf("hide participants: %v", err)
	}
	hidden, err := channels.ListActiveChannelBotMembers(ctx, friend.ID, channelID, 0, 20)
	if err != nil {
		t.Fatalf("list hidden bot members as ordinary member: %v", err)
	}
	if hidden.Count != 0 || len(hidden.Participants) != 0 {
		t.Fatalf("hidden bot members = %+v, want empty for ordinary member", hidden)
	}
	internalIDs, err := channels.ListActiveChannelBotMemberIDs(ctx, friend.ID, channelID, 20)
	if err != nil {
		t.Fatalf("list hidden bot member ids as ordinary member: %v", err)
	}
	if len(internalIDs) != 1 || internalIDs[0] != bot.ID {
		t.Fatalf("internal hidden bot ids = %+v, want bot %d", internalIDs, bot.ID)
	}
	ownerVisible, err := channels.ListActiveChannelBotMembers(ctx, owner.ID, channelID, 0, 20)
	if err != nil {
		t.Fatalf("list hidden bot members as owner: %v", err)
	}
	if ownerVisible.Count != 1 || len(ownerVisible.Participants) != 1 || ownerVisible.Participants[0].UserID != bot.ID {
		t.Fatalf("owner visible bot members = %+v, want bot %d", ownerVisible, bot.ID)
	}
}

func TestChannelStoreGetParticipantsHidesAnonymousAdminFromRegularMember(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	channels := newTestChannelStore(pool)

	owner, err := users.Create(ctx, domain.User{AccessHash: 8911, Phone: "+1889" + suffix + "01", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	anonymousAdmin, err := users.Create(ctx, domain.User{AccessHash: 8912, Phone: "+1889" + suffix + "02", FirstName: "Hidden"})
	if err != nil {
		t.Fatalf("create anonymous admin: %v", err)
	}
	regular, err := users.Create(ctx, domain.User{AccessHash: 8913, Phone: "+1889" + suffix + "03", FirstName: "Regular"})
	if err != nil {
		t.Fatalf("create regular: %v", err)
	}

	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, anonymousAdmin.ID, regular.ID})
	})

	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Anonymous Admin " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{anonymousAdmin.ID, regular.ID},
		Date:          1700008910,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MemberID:  anonymousAdmin.ID,
		AdminRights: domain.ChannelAdminRights{
			Anonymous:  true,
			ChangeInfo: true,
		},
		Date: 1700008911,
	}); err != nil {
		t.Fatalf("edit anonymous admin: %v", err)
	}

	recent, err := channels.GetParticipants(ctx, regular.ID, channelID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsRecent}, 0, 10)
	if err != nil {
		t.Fatalf("regular recent participants: %v", err)
	}
	if postgresParticipantListHasUser(recent.Participants, anonymousAdmin.ID) || recent.Count != 2 {
		t.Fatalf("regular recent participants = %+v count=%d, want anonymous admin hidden and visible count 2", recent.Participants, recent.Count)
	}
	admins, err := channels.GetParticipants(ctx, regular.ID, channelID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsAdmins}, 0, 10)
	if err != nil {
		t.Fatalf("regular admin participants: %v", err)
	}
	if postgresParticipantListHasUser(admins.Participants, anonymousAdmin.ID) || len(admins.Participants) != 1 || admins.Participants[0].UserID != owner.ID {
		t.Fatalf("regular admins = %+v, want only creator visible", admins.Participants)
	}
	ownerAdmins, err := channels.GetParticipants(ctx, owner.ID, channelID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsAdmins}, 0, 10)
	if err != nil {
		t.Fatalf("owner admin participants: %v", err)
	}
	if !postgresParticipantListHasUser(ownerAdmins.Participants, anonymousAdmin.ID) {
		t.Fatalf("owner admins = %+v, want anonymous admin visible to admins", ownerAdmins.Participants)
	}
}

func postgresParticipantListHasUser(participants []domain.ChannelMember, userID int64) bool {
	for _, participant := range participants {
		if participant.UserID == userID {
			return true
		}
	}
	return false
}
