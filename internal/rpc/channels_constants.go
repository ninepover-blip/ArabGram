package rpc

const (
	maxChannelTitleLength                 = 128
	maxChannelAboutLength                 = 255
	maxChannelUsernameOrder               = 32
	maxChannelReportMessageIDs            = 100
	maxChannelSearchPostsLimit            = 50
	maxChannelSearchPostsQuery            = 256
	maxChannelPaidMessageStars            = 10000
	maxChannelBoostsToUnblockRestrictions = 8
	maxChatInviteListLimit                = 100
	maxChatInviteLinkLength               = 256
	maxChatInviteSearchLength             = 256
)

const (
	channelTransientMembers channelTransientScope = iota
	channelTransientViewers
	channelTransientExplicit
)
