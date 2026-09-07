package files

import (
	"context"
	"fmt"
	"testing"

	"telesrv/internal/seed/appearance"
)

func TestSeedAppearanceImportsDefaultWallpaperDocuments(t *testing.T) {
	media := newFakeMediaStore()
	blobs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}
	svc := NewService(media, blobs, 2)

	stats, err := svc.SeedAppearance(context.Background())
	if err != nil {
		t.Fatalf("SeedAppearance: %v", err)
	}
	if stats.Skipped || stats.Wallpapers == 0 || stats.Documents == 0 || stats.Blobs < stats.Documents {
		t.Fatalf("SeedAppearance stats = %+v, want non-empty wallpapers/documents with >=1 blob each", stats)
	}

	var first appearance.Wallpaper
	for _, w := range appearance.Default().Wallpapers {
		if w.Document.ID != 0 {
			first = w
			break
		}
	}
	if first.Document.ID == 0 {
		t.Fatalf("no wallpaper with a document in catalog")
	}
	doc, ok, err := media.GetDocument(context.Background(), first.Document.ID)
	if err != nil || !ok {
		t.Fatalf("GetDocument(%d) = ok %v err %v", first.Document.ID, ok, err)
	}
	if doc.DCID != 2 || doc.MimeType != first.Document.MimeType || doc.Size != first.Document.Size {
		t.Fatalf("document = dc %d mime %q size %d, want dc 2 mime %q size %d",
			doc.DCID, doc.MimeType, doc.Size, first.Document.MimeType, first.Document.Size)
	}
	if len(doc.Thumbs) == 0 || doc.Thumbs[0].Type != "m" {
		t.Fatalf("document thumbs = %+v, want m thumbnail", doc.Thumbs)
	}
	if _, ok, err := media.GetFileBlob(context.Background(), fmt.Sprintf("doc:%d", first.Document.ID)); err != nil || !ok {
		t.Fatalf("main blob ok=%v err=%v, want present", ok, err)
	}
	if _, ok, err := media.GetFileBlob(context.Background(), fmt.Sprintf("doc:%d:m", first.Document.ID)); err != nil || !ok {
		t.Fatalf("thumb blob ok=%v err=%v, want present", ok, err)
	}
}

func TestSeedAppearanceSkipsUnchangedCatalogAndRepairsMissingBlob(t *testing.T) {
	ctx := context.Background()
	media := newFakeMediaStore()
	blobs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}
	svc := NewService(media, blobs, 2)

	first, err := svc.SeedAppearance(ctx)
	if err != nil {
		t.Fatalf("first SeedAppearance: %v", err)
	}
	if first.Skipped || first.Documents == 0 || first.Blobs == 0 {
		t.Fatalf("first stats = %+v, want import", first)
	}
	second, err := svc.SeedAppearance(ctx)
	if err != nil {
		t.Fatalf("second SeedAppearance: %v", err)
	}
	if !second.Skipped || second.Documents != 0 || second.Blobs != 0 {
		t.Fatalf("second stats = %+v, want unchanged catalog skip", second)
	}

	var firstDocID int64
	for _, w := range appearance.Default().Wallpapers {
		if w.Document.ID != 0 && w.Document.Path != "" {
			firstDocID = w.Document.ID
			break
		}
	}
	if firstDocID == 0 {
		t.Fatal("no wallpaper document found")
	}
	delete(media.blobs, fmt.Sprintf("doc:%d", firstDocID))
	repaired, err := svc.SeedAppearance(ctx)
	if err != nil {
		t.Fatalf("repair SeedAppearance: %v", err)
	}
	if repaired.Skipped || repaired.Documents == 0 || repaired.Blobs == 0 {
		t.Fatalf("repair stats = %+v, want missing blob to force reimport", repaired)
	}
}
