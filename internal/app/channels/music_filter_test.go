package channels

import (
	"context"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestChannelHistoryMusicOnlyFiltersAudioDocuments(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)
	created, err := service.CreateChannel(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Music Filter",
		Megagroup:     true,
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	music := domain.Document{
		ID:         301,
		AccessHash: 3001,
		Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrAudio, AudioDuration: 200, Title: "Channel Song"}},
	}
	voice := domain.Document{
		ID:         302,
		AccessHash: 3002,
		Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrAudio, Voice: true, AudioDuration: 4}},
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Media:     &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Document: &voice, Voice: true},
		Date:      11,
	}); err != nil {
		t.Fatalf("SendMessage voice: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Media:     &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Document: &music},
		Date:      12,
	}); err != nil {
		t.Fatalf("SendMessage music: %v", err)
	}

	history, err := service.GetHistory(ctx, 1002, domain.ChannelHistoryFilter{
		ChannelID: created.Channel.ID,
		MusicOnly: true,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("GetHistory music: %v", err)
	}
	if len(history.Messages) != 1 || history.Messages[0].Media == nil || history.Messages[0].Media.Document == nil || history.Messages[0].Media.Document.ID != music.ID {
		t.Fatalf("music history = %+v, want only music document", history.Messages)
	}
}
