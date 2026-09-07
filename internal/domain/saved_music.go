package domain

const (
	// MaxSavedMusicItems bounds one account's profile music list in the current
	// small-scale compatibility phase.
	MaxSavedMusicItems = 1000
)

// SaveMusicRequest mutates the current user's ordered profile music list.
type SaveMusicRequest struct {
	UserID          int64
	Document        Document
	Unsave          bool
	AfterDocumentID int64
	Date            int
}

// SavedMusicList is an ordered page of music documents pinned to a user profile.
type SavedMusicList struct {
	UserID    int64
	Documents []Document
	Count     int
}
