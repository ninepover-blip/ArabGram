package domain

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

const (
	// MaxStoryID keeps story ids inside TL int / PostgreSQL int4 bounds.
	MaxStoryID = MaxMessageBoxID
	// MaxStoryIDs limits exact-id story RPCs and view increments.
	MaxStoryIDs = 200
	// MaxStoryListLimit bounds one active/archive/pinned story page.
	MaxStoryListLimit = 100
	// MaxStoryInteractionListLimit bounds one story viewer/reaction page.
	MaxStoryInteractionListLimit = 100
	// MaxStoryInteractionOffsetLength bounds server-generated owner interaction
	// cursors before they reach store-side parsing or query predicates.
	MaxStoryInteractionOffsetLength = 64
	// MaxStoryAlbumOffset bounds stories.getAlbumStories count-offset paging
	// before a future album store can turn malformed requests into deep OFFSET
	// work. DrKLO sends loadedObjects.size(), so normal UI paging stays small.
	MaxStoryAlbumOffset = 10000
	// MaxStoryPinnedToTop bounds stories.togglePinnedToTop. It matches the
	// client default stories_pinned_to_top_count_max used by TDesktop/DrKLO.
	MaxStoryPinnedToTop = 3
	// MaxStoryPrivacyFanoutTargets bounds one privacy-change fanout candidate set.
	MaxStoryPrivacyFanoutTargets = 5000
	// MaxStorySendAsChannels bounds stories.getChatsToSend channel candidates.
	MaxStorySendAsChannels = 200
	// MaxStoryMediaAreas bounds one story's overlay/click target vector.
	MaxStoryMediaAreas = 16
	// MaxStoryMediaAreaURLLength bounds mediaAreaUrl click targets stored in
	// one story snapshot.
	MaxStoryMediaAreaURLLength = 2048
	// MaxStoryStarGiftSlugLength bounds collectible gift slugs stored in one
	// story snapshot. TDesktop/DrKLO treat the slug as a deep-link path token.
	MaxStoryStarGiftSlugLength = 255
	// MaxStoryGeoAddressPartLength bounds optional mediaAreaGeoPoint address
	// labels stored in one story snapshot.
	MaxStoryGeoAddressPartLength = 256
	// MaxStoryWeatherEmojiLength bounds mediaAreaWeather emoji labels stored in
	// one story snapshot.
	MaxStoryWeatherEmojiLength = 32
	// DefaultStoryPeriod is the Layer 225 sendStory period when the optional
	// period flag is absent.
	DefaultStoryPeriod = 86400
	// MaxStoryViewQueryLength bounds q in stories.getStoryViewsList before it
	// reaches store-side LIKE/search predicates.
	MaxStoryViewQueryLength = 128
	// DefaultStoryCanSendRemaining is the development-stage story quota returned
	// by canSendStory until production limits are modeled.
	DefaultStoryCanSendRemaining = 100
)

var (
	ErrStoryIDInvalid     = errors.New("story id invalid")
	ErrStoryNotFound      = errors.New("story not found")
	ErrStoryPeerInvalid   = errors.New("story peer invalid")
	ErrStoryNotModified   = errors.New("story not modified")
	ErrStoryOffsetInvalid = errors.New("story offset invalid")
	ErrStoryPeriodInvalid = errors.New("story period invalid")
)

// ValidateStoryInteractionOffset validates stories.getStoryViewsList and
// stories.getStoryReactionsList keyset cursors without depending on TL types.
func ValidateStoryInteractionOffset(offset string, reactionsOnly bool) error {
	return validateStoryInteractionOffset(offset, reactionsOnly, false)
}

// ValidateStoryReactionInteractionOffset validates stories.getStoryReactionsList
// cursors. When forwardsFirst is set, reaction-list pages may legitimately use
// group 1 for ordinary reaction rows after public repost rows.
func ValidateStoryReactionInteractionOffset(offset string, forwardsFirst bool) error {
	return validateStoryInteractionOffset(offset, true, forwardsFirst)
}

func validateStoryInteractionOffset(offset string, reactionsOnly, forwardsFirst bool) error {
	if offset == "" {
		return nil
	}
	if len(offset) > MaxStoryInteractionOffsetLength {
		return ErrStoryOffsetInvalid
	}
	parts := strings.Split(offset, ":")
	if len(parts) != 3 && len(parts) != 4 {
		return ErrStoryOffsetInvalid
	}
	group, err1 := strconv.Atoi(parts[0])
	date, err2 := strconv.Atoi(parts[1])
	viewerID, err3 := strconv.ParseInt(parts[2], 10, 64)
	var messageID int
	var err4 error
	if len(parts) == 4 {
		messageID, err4 = strconv.Atoi(parts[3])
	}
	reactionGroupInvalid := reactionsOnly && group != 0 && !(forwardsFirst && group == 1)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil ||
		group < 0 || group > 1 || reactionGroupInvalid ||
		date <= 0 || viewerID == 0 || messageID < 0 || messageID > MaxMessageBoxID {
		return ErrStoryOffsetInvalid
	}
	return nil
}

// Story is a protocol-neutral Telegram story snapshot owned by one peer.
type Story struct {
	Owner            Peer
	ID               int
	RandomID         int64
	Date             int
	ExpireDate       int
	Deleted          bool
	Pinned           bool
	PinnedToTopOrder int
	Public           bool
	CloseFriends     bool
	Contacts         bool
	SelectedContacts bool
	NoForwards       bool
	Edited           bool
	Out              bool
	PrivacyRules     []PrivacyRule
	AllowUserIDs     []int64
	DisallowUserIDs  []int64
	Caption          string
	Entities         []MessageEntity
	Media            *MessageMedia
	MediaAreas       []StoryMediaArea
	Forward          *StoryForward
	Views            StoryViews
	SentReaction     *MessageReaction
}

// Active reports whether the story belongs in the active strip at now.
func (s Story) Active(now int) bool {
	return !s.Deleted && s.ExpireDate > now
}

// Interactable reports whether a viewer may create new view/reaction state.
// Expired profile-pinned stories remain visible from profiles, but ordinary
// expired stories must not accept stale interaction writes.
func (s Story) Interactable(now int) bool {
	return !s.Deleted && (s.ExpireDate > now || s.Pinned)
}

// VisibleTo reports story visibility with no relationship facts.
func (s Story) VisibleTo(viewerUserID int64) bool {
	return s.VisibleToWithFacts(viewerUserID, false, false)
}

// VisibleToWithFacts reports story visibility using already-loaded viewer facts.
func (s Story) VisibleToWithFacts(viewerUserID int64, viewerIsContact, viewerCloseFriend bool) bool {
	return s.VisibleToWithStoryFacts(viewerUserID, viewerIsContact, viewerCloseFriend, false)
}

// VisibleToWithStoryFacts reports story visibility using already-loaded viewer
// facts, including owner-side story blocklist membership.
func (s Story) VisibleToWithStoryFacts(viewerUserID int64, viewerIsContact, viewerCloseFriend, viewerStoryBlocked bool) bool {
	if viewerUserID == 0 {
		return false
	}
	if s.Owner.Type == PeerTypeUser && s.Owner.ID == viewerUserID {
		return true
	}
	if viewerStoryBlocked {
		return false
	}
	if int64InSet(s.DisallowUserIDs, viewerUserID) {
		return false
	}
	if int64InSet(s.AllowUserIDs, viewerUserID) {
		return true
	}
	if s.Public {
		return true
	}
	if s.Contacts && viewerIsContact {
		return true
	}
	if s.CloseFriends && viewerCloseFriend {
		return true
	}
	return false
}

func int64InSet(ids []int64, needle int64) bool {
	for _, id := range ids {
		if id == needle {
			return true
		}
	}
	return false
}

// StoryForward contains the protocol-neutral source of a reposted story.
type StoryForward struct {
	// Source is the durable server-side source story owner used for counters
	// and owner interaction lists. From is the client-visible clickable peer
	// and may be empty when forwards privacy requires a from_name-only header.
	Source   Peer
	From     Peer
	FromName string
	StoryID  int
	Modified bool
}

// StoryViews is the owner-visible aggregate of story views and reactions.
type StoryViews struct {
	ViewsCount     int
	ForwardsCount  int
	ReactionsCount int
	Reactions      []ChannelMessageReactionCount
	RecentViewers  []int64
	HasViewers     bool
}

// StoryView is one viewer's durable view/reaction row.
type StoryView struct {
	Owner    Peer
	StoryID  int
	ViewerID int64
	Date     int
	Reaction *MessageReaction
	// Repost is set for public repost interactions. It is protocol-neutral and
	// converted by rpc to storyViewPublicRepost.
	Repost               *Story
	PublicForward        *StoryPublicForward
	Blocked              bool
	BlockedMyStoriesFrom bool
}

// StoryPublicForward is a public channel/supergroup message that shared a story
// via messageMediaStory. It is converted by rpc to storyViewPublicForward or
// storyReactionPublicForward.
type StoryPublicForward struct {
	Message ChannelMessage
}

// StoryViewListRequest pages owner-visible story viewers.
type StoryViewListRequest struct {
	ViewerUserID   int64
	Owner          Peer
	StoryID        int
	Offset         string
	Limit          int
	Query          string
	JustContacts   bool
	ReactionsFirst bool
	ForwardsFirst  bool
}

// StoryViewList is a bounded page for stories.getStoryViewsList.
type StoryViewList struct {
	Count          int
	ViewsCount     int
	ForwardsCount  int
	ReactionsCount int
	Views          []StoryView
	NextOffset     string
}

// StoryReactionListRequest pages peers that reacted to one story.
type StoryReactionListRequest struct {
	ViewerUserID             int64
	Owner                    Peer
	StoryID                  int
	Reaction                 *MessageReaction
	Offset                   string
	Limit                    int
	ForwardsFirst            bool
	CanViewOwnerInteractions bool
}

// StoryReactionList is a bounded page for stories.getStoryReactionsList.
type StoryReactionList struct {
	Count      int
	Reactions  []StoryView
	NextOffset string
}

// StoryMessageForwardListRequest pages public channel/supergroup messages that
// shared one source story as messageMediaStory.
type StoryMessageForwardListRequest struct {
	ViewerUserID   int64
	Owner          Peer
	StoryID        int
	Offset         string
	Limit          int
	ReactionsFirst bool
	ForwardsFirst  bool
}

// StoryMessageForwardList is a bounded page of public message forwards.
type StoryMessageForwardList struct {
	Count      int
	Forwards   []StoryView
	NextOffset string
}

// StoryPublicForwardListRequest pages public repost stories for one source story.
type StoryPublicForwardListRequest struct {
	ViewerUserID int64
	Owner        Peer
	StoryID      int
	Offset       string
	Limit        int
}

// StoryPublicForwardList is a bounded page of public repost story forwards.
type StoryPublicForwardList struct {
	Count      int
	Forwards   []StoryView
	NextOffset string
}

// PeerStories is the story list for one owner peer from one viewer's perspective.
type PeerStories struct {
	Peer      Peer
	MaxReadID int
	Stories   []Story
	Users     []User
	Channels  []Channel
}

// StoryList is a bounded story page.
type StoryList struct {
	Count   int
	State   string
	HasMore bool
	// Hidden reports that this list is the viewer's hidden story source list.
	Hidden  bool
	Stories []Story
	// PinnedToTop contains the owner-visible top-pinned story IDs in display
	// order for stories.stories.pinned_to_top.
	PinnedToTop []int
	Peers       []PeerStories
	Users       []User
	Channels    []Channel
}

// StoryListCursor is the opaque getAllStories seek position after one peer.
type StoryListCursor struct {
	Set  bool
	Date int
	Peer Peer
}

// StoryListDigest summarizes a complete viewer-scoped active story source list.
type StoryListDigest struct {
	Count int
	Hash  uint64
}

// DigestStoryPeerList returns a deterministic digest for an already ordered
// complete active/hidden story peer list.
func DigestStoryPeerList(peers []PeerStories) StoryListDigest {
	h := fnv.New64a()
	fmt.Fprintf(h, "story-list|count=%d|", len(peers))
	for _, peer := range peers {
		fmt.Fprintf(h, "p=%s:%d:%d:%d|", peer.Peer.Type, peer.Peer.ID, peer.MaxReadID, len(peer.Stories))
		for _, story := range peer.Stories {
			fmt.Fprintf(
				h,
				"s=%d:%d:%d:%t:%t:%d:%t:%t:%t:%t:%t:%t:%d:%d:%#v:%#v:%#v:%#v:%#v:%#v:%#v:%#v|",
				story.ID,
				story.Date,
				story.ExpireDate,
				story.Deleted,
				story.Pinned,
				story.PinnedToTopOrder,
				story.Public,
				story.CloseFriends,
				story.Contacts,
				story.SelectedContacts,
				story.Edited,
				story.Out,
				story.Views.ViewsCount,
				story.Views.ReactionsCount,
				story.SentReaction,
				story.Caption,
				story.Entities,
				story.Media,
				story.MediaAreas,
				story.Forward,
				story.PrivacyRules,
				story.AllowUserIDs,
			)
			fmt.Fprintf(h, "disallow=%#v|noforwards=%t|", story.DisallowUserIDs, story.NoForwards)
		}
	}
	return StoryListDigest{Count: len(peers), Hash: h.Sum64()}
}

// StoryReadState is the per viewer+owner read boundary.
type StoryReadState struct {
	ViewerID  int64
	Peer      Peer
	MaxReadID int
	Date      int
}

// RecentStory is the domain counterpart of TL recentStory.
type RecentStory struct {
	Peer  Peer
	MaxID int
	Live  bool
}

// PeerStoryProjection is the per-viewer story summary embedded into peer objects.
type PeerStoryProjection struct {
	Peer   Peer
	Recent RecentStory
	Hidden bool
}

// StoryReadResult describes a readStories mutation.
type StoryReadResult struct {
	ViewerID  int64
	Peer      Peer
	MaxReadID int
	Advanced  bool
	Date      int
}

// StoryReactionResult describes one story reaction mutation.
type StoryReactionResult struct {
	ViewerID int64
	Peer     Peer
	StoryID  int
	Reaction *MessageReaction
	Story    Story
	Changed  bool
	Date     int
}

// StoryMediaAreaKind identifies a protocol-neutral story overlay area.
type StoryMediaAreaKind string

const (
	StoryMediaAreaSuggestedReaction StoryMediaAreaKind = "suggested_reaction"
	StoryMediaAreaURL               StoryMediaAreaKind = "url"
	StoryMediaAreaGeoPoint          StoryMediaAreaKind = "geo_point"
	StoryMediaAreaVenue             StoryMediaAreaKind = "venue"
	StoryMediaAreaWeather           StoryMediaAreaKind = "weather"
	StoryMediaAreaChannelPost       StoryMediaAreaKind = "channel_post"
	StoryMediaAreaStarGift          StoryMediaAreaKind = "star_gift"
)

// StoryMediaAreaCoordinates is a percentage-based story media overlay box.
type StoryMediaAreaCoordinates struct {
	X         float64
	Y         float64
	W         float64
	H         float64
	Rotation  float64
	Radius    float64
	HasRadius bool
}

// StoryGeoPointAddress is the protocol-neutral counterpart of geoPointAddress.
type StoryGeoPointAddress struct {
	CountryISO2 string
	State       string
	City        string
	Street      string
}

// StoryMediaArea is a protocol-neutral media area stored with a story snapshot.
type StoryMediaArea struct {
	Kind         StoryMediaAreaKind
	Coordinates  StoryMediaAreaCoordinates
	Dark         bool
	Flipped      bool
	Reaction     *MessageReaction
	URL          string
	Geo          *MessageGeoPoint
	GeoAddress   *StoryGeoPointAddress
	Venue        *MessageVenue
	WeatherEmoji string
	TemperatureC float64
	Color        int
	ChannelID    int64
	MsgID        int
	StarGiftSlug string
}

// StoryCreateRequest creates one owner story with a store-assigned monotonic ID.
type StoryCreateRequest struct {
	Owner            Peer
	RandomID         int64
	Date             int
	Period           int
	Pinned           bool
	Public           bool
	CloseFriends     bool
	Contacts         bool
	SelectedContacts bool
	NoForwards       bool
	PrivacyRules     []PrivacyRule
	AllowUserIDs     []int64
	DisallowUserIDs  []int64
	Caption          string
	Entities         []MessageEntity
	Media            *MessageMedia
	MediaAreas       []StoryMediaArea
	Forward          *StoryForward
}

// StoryCreateResult describes a sendStory mutation.
type StoryCreateResult struct {
	Story     Story
	Duplicate bool
}

// StoryEditRequest applies a partial story edit.
type StoryEditRequest struct {
	Owner            Peer
	ID               int
	Media            *MessageMedia
	UpdateMedia      bool
	Caption          string
	Entities         []MessageEntity
	UpdateCaption    bool
	Public           bool
	CloseFriends     bool
	Contacts         bool
	SelectedContacts bool
	PrivacyRules     []PrivacyRule
	AllowUserIDs     []int64
	DisallowUserIDs  []int64
	UpdatePrivacy    bool
	MediaAreas       []StoryMediaArea
	UpdateMediaAreas bool
}

// StoryEditResult describes a story edit and its previous visible facts.
type StoryEditResult struct {
	Story    Story
	Previous Story
}

// StoryMutationResult describes story mutations that may emit updateStory.
type StoryMutationResult struct {
	Peer     Peer
	IDs      []int
	Stories  []Story
	Previous []Story
}

// UpsertStoryRequest inserts or replaces a story snapshot.
type UpsertStoryRequest struct {
	Story Story
}
