package postgres

import (
	"telesrv/internal/domain"
)

func canExportChannelInvite(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo))
}
