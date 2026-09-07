package rpc

import (
	"testing"

	"telesrv/internal/domain"
)

// TestChannelFullCanViewParticipants 锁定:广播频道的订阅者列表仅管理员可见(官方语义),
// 非管理员订阅者拿到 can_view_participants=false——否则 DrKLO Profile 会冒出
// Subscribers/Administrators/Channel Settings 三行。超级群成员可见(隐藏成员时仅管理员)。
func TestChannelFullCanViewParticipants(t *testing.T) {
	broadcast := domain.Channel{ID: 1, Broadcast: true}
	megagroup := domain.Channel{ID: 2, Megagroup: true}
	megagroupHidden := domain.Channel{ID: 3, Megagroup: true, ParticipantsHidden: true}

	cases := []struct {
		name string
		ch   domain.Channel
		role domain.ChannelMemberRole
		want bool
	}{
		{"broadcast-subscriber", broadcast, domain.ChannelRoleMember, false},
		{"broadcast-admin", broadcast, domain.ChannelRoleAdmin, true},
		{"broadcast-creator", broadcast, domain.ChannelRoleCreator, true},
		{"megagroup-member", megagroup, domain.ChannelRoleMember, true},
		{"megagroup-admin", megagroup, domain.ChannelRoleAdmin, true},
		{"megagroup-hidden-member", megagroupHidden, domain.ChannelRoleMember, false},
		{"megagroup-hidden-admin", megagroupHidden, domain.ChannelRoleAdmin, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view := domain.ChannelView{
				Channel: tc.ch,
				Self: domain.ChannelMember{
					ChannelID: tc.ch.ID,
					Role:      tc.role,
					Status:    domain.ChannelMemberActive,
				},
			}
			if got := tgChannelFull(view).CanViewParticipants; got != tc.want {
				t.Fatalf("CanViewParticipants = %v, want %v", got, tc.want)
			}
		})
	}
}
