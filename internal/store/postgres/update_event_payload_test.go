package postgres

import (
	"testing"

	"telesrv/internal/domain"
)

func TestDecodeUpdateEventPayloadIgnoresIrrelevantColumns(t *testing.T) {
	got, err := decodeUpdateEventPayload(domain.UpdateEventNewMessage, updateEventPayloadColumns{
		messageEntities: "[]",
		media:           "{}",
		replyMarkup:     "{}",
		richMessage:     "{}",
		eventPeers:      "not-json",
		peerSettings:    "not-json",
		messageIDs:      "not-json",
		dialogFilter:    "not-json",
		filterOrder:     "not-json",
		folderPeers:     "not-json",
		story:           "not-json",
		reaction:        "not-json",
		emojiStatus:     "not-json",
	})
	if err != nil {
		t.Fatalf("decode new_message payload: %v", err)
	}
	if len(got.entities) != 0 || got.media != nil || got.markup != nil || got.rich != nil {
		t.Fatalf("unexpected decoded payload: %+v", got)
	}
}

func TestDecodeUpdateEventPayloadFailsOnRelevantColumn(t *testing.T) {
	tests := []struct {
		name      string
		eventType domain.UpdateEventType
		columns   updateEventPayloadColumns
	}{
		{name: "message", eventType: domain.UpdateEventNewMessage, columns: updateEventPayloadColumns{messageEntities: "not-json"}},
		{name: "peers", eventType: domain.UpdateEventPinnedDialogs, columns: updateEventPayloadColumns{eventPeers: "not-json"}},
		{name: "settings", eventType: domain.UpdateEventPeerSettings, columns: updateEventPayloadColumns{peerSettings: "not-json"}},
		{name: "message ids", eventType: domain.UpdateEventDeleteMessages, columns: updateEventPayloadColumns{messageIDs: "not-json"}},
		{name: "dialog filter", eventType: domain.UpdateEventDialogFilter, columns: updateEventPayloadColumns{dialogFilter: "not-json"}},
		{name: "filter order", eventType: domain.UpdateEventDialogFilterOrder, columns: updateEventPayloadColumns{filterOrder: "not-json"}},
		{name: "folder peers", eventType: domain.UpdateEventFolderPeers, columns: updateEventPayloadColumns{folderPeers: "not-json"}},
		{name: "story", eventType: domain.UpdateEventStory, columns: updateEventPayloadColumns{story: "not-json"}},
		{name: "story reaction story", eventType: domain.UpdateEventNewStoryReaction, columns: updateEventPayloadColumns{story: "not-json", reaction: "{}"}},
		{name: "story reaction reaction", eventType: domain.UpdateEventNewStoryReaction, columns: updateEventPayloadColumns{story: "{}", reaction: "not-json"}},
		{name: "emoji status", eventType: domain.UpdateEventUserEmojiStatus, columns: updateEventPayloadColumns{emojiStatus: "not-json"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeUpdateEventPayload(tt.eventType, tt.columns); err == nil {
				t.Fatal("decode succeeded with malformed relevant payload")
			}
		})
	}
}

func TestDecodeUpdateEventPayloadStoryReactionDecodesBothPayloads(t *testing.T) {
	story := domain.Story{
		Owner:      domain.Peer{Type: domain.PeerTypeUser, ID: 1001},
		ID:         17,
		Date:       1700000000,
		ExpireDate: 1700003600,
		Caption:    "immutable reaction story",
	}
	reaction := &domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: "🔥"}
	storyJSON, err := encodeEventStory(story)
	if err != nil {
		t.Fatalf("encode story: %v", err)
	}
	reactionJSON, err := encodeEventReaction(reaction)
	if err != nil {
		t.Fatalf("encode reaction: %v", err)
	}

	for _, eventType := range []domain.UpdateEventType{
		domain.UpdateEventSentStoryReaction,
		domain.UpdateEventNewStoryReaction,
	} {
		t.Run(string(eventType), func(t *testing.T) {
			got, err := decodeUpdateEventPayload(eventType, updateEventPayloadColumns{
				story:    string(storyJSON),
				reaction: string(reactionJSON),
			})
			if err != nil {
				t.Fatalf("decode story reaction: %v", err)
			}
			if got.story.ID != story.ID || got.story.Owner != story.Owner || got.story.Caption != story.Caption {
				t.Fatalf("story = %+v, want %+v", got.story, story)
			}
			if got.reaction == nil || *got.reaction != *reaction {
				t.Fatalf("reaction = %+v, want %+v", got.reaction, reaction)
			}
		})
	}
}

func BenchmarkDecodeUpdateEventPayloadNewMessageEmpty(b *testing.B) {
	columns := updateEventPayloadColumns{
		messageEntities: "[]",
		media:           "{}",
		replyMarkup:     "{}",
		richMessage:     "{}",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeUpdateEventPayload(domain.UpdateEventNewMessage, columns); err != nil {
			b.Fatal(err)
		}
	}
}
