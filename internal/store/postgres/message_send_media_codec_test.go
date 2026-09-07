package postgres

import (
	"bytes"
	"testing"

	"telesrv/internal/domain"
)

func TestEncodePrivateSendMediaProjectionReusesIdenticalSnapshots(t *testing.T) {
	media := &domain.MessageMedia{
		Kind: domain.MessageMediaKindDocument,
		Document: &domain.Document{
			ID: 42,
		},
	}
	shared, sender, recipient, err := encodePrivateSendMediaProjection(privateSendMediaProjection{
		Shared: media, Sender: media, Recipient: media,
	})
	if err != nil {
		t.Fatalf("encode projection: %v", err)
	}
	if len(shared) == 0 || !bytes.Equal(shared, sender) || !bytes.Equal(shared, recipient) {
		t.Fatalf("encoded projections differ: shared=%q sender=%q recipient=%q", shared, sender, recipient)
	}
	if &shared[0] != &sender[0] || &shared[0] != &recipient[0] {
		t.Fatal("identical media projections did not reuse one encoded buffer")
	}
}

func TestEncodePrivateSendMediaProjectionKeepsDistinctSnapshots(t *testing.T) {
	sharedMedia := &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Document: &domain.Document{ID: 42}}
	senderMedia := &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Document: &domain.Document{ID: 43}}
	shared, sender, recipient, err := encodePrivateSendMediaProjection(privateSendMediaProjection{
		Shared: sharedMedia, Sender: senderMedia, Recipient: senderMedia,
	})
	if err != nil {
		t.Fatalf("encode projection: %v", err)
	}
	if bytes.Equal(shared, sender) {
		t.Fatalf("distinct media projections encoded identically: %q", shared)
	}
	if len(sender) == 0 || &sender[0] != &recipient[0] {
		t.Fatal("recipient did not reuse sender projection")
	}
}
