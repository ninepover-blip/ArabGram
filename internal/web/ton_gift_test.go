package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"telesrv/internal/app/stargifts"
	"telesrv/internal/domain"
)

type fakeTONGiftExports struct {
	view       stargifts.TONExportView
	found      bool
	metadata   []byte
	document   domain.Document
	challenge  stargifts.TONProofChallenge
	proof      stargifts.TONProofSubmission
	proofCalls int
}

func (f *fakeTONGiftExports) Resolve(context.Context, string) (stargifts.TONExportView, bool, error) {
	return f.view, f.found, nil
}

func (f *fakeTONGiftExports) Metadata(context.Context, int64) ([]byte, bool, error) {
	return f.metadata, len(f.metadata) > 0, nil
}

func (f *fakeTONGiftExports) Media(context.Context, int64) (domain.Document, bool, error) {
	return f.document, f.document.ID > 0, nil
}

func (f *fakeTONGiftExports) CreateProofChallenge(context.Context, string, int) (stargifts.TONProofChallenge, error) {
	return f.challenge, nil
}

func (f *fakeTONGiftExports) VerifyProof(_ context.Context, _ string, proof stargifts.TONProofSubmission, _ int) (domain.StarGiftTONExport, error) {
	f.proofCalls++
	f.proof = proof
	export := f.view.Export
	export.Status = domain.StarGiftTONExportMintQueued
	return export, nil
}

type fakeTONGiftFiles struct {
	chunk domain.FileChunk
}

func (f fakeTONGiftFiles) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	return f.chunk, len(f.chunk.Bytes) > 0, nil
}

func newTONGiftHandler(t *testing.T, base string, exports *fakeTONGiftExports, files PublicFileResolver) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{
		StickerSets: fakeResolver{}, PublicBaseURL: base, AppName: "telesrv",
		TONGiftExports: exports, TONGiftFiles: files,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

func tonGiftFixture() *fakeTONGiftExports {
	export := domain.StarGiftTONExport{
		ID: 11, UniqueGiftID: 22, OwnerUserID: 33, Network: domain.TONNetworkMainnet,
		CollectionAddress: "0:collection",
		MetadataURI:       "https://gifts.example/base/ton-gift/nft/22.json",
		Status:            domain.StarGiftTONExportAwaitingWallet, ExpiresAt: int(time.Now().Add(time.Hour).Unix()),
	}
	gift := domain.UniqueStarGift{
		ID: 22, Title: "Astral Gift", Slug: "astral-17", Num: 17,
		Model:    domain.StarGiftCollectibleAttribute{Name: "Nova"},
		Pattern:  domain.StarGiftCollectibleAttribute{Name: "Orbit"},
		Backdrop: domain.StarGiftCollectibleAttribute{Name: "Azure"},
	}
	return &fakeTONGiftExports{
		view: stargifts.TONExportView{Export: export, Gift: gift}, found: true,
		metadata:  []byte(`{"name":"Astral Gift #17"}`),
		document:  domain.Document{ID: 44, AccessHash: 55, Date: 100, MimeType: "image/png", Size: 8},
		challenge: stargifts.TONProofChallenge{Export: export, Payload: "proof-payload", Domain: "gifts.example", ExpiresAt: int(time.Now().Add(time.Minute).Unix())},
	}
}

func TestTONGiftRoutesAreOptInAndRequireFileResolver(t *testing.T) {
	if _, err := NewHandler(Config{StickerSets: fakeResolver{}, PublicBaseURL: "https://gifts.example", TONGiftExports: tonGiftFixture()}); err == nil {
		t.Fatal("TON export accepted without public file resolver")
	}
	h, err := NewHandler(Config{StickerSets: fakeResolver{}, PublicBaseURL: "https://gifts.example"})
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/ton-gift/export", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("disabled TON route status=%d, want 404", rr.Code)
	}
}

func TestTONGiftPageStateManifestAndImmutableAssets(t *testing.T) {
	exports := tonGiftFixture()
	files := fakeTONGiftFiles{chunk: domain.FileChunk{Bytes: []byte("\x89PNGdata"), MimeType: "image/png", Total: 8}}
	h := newTONGiftHandler(t, "https://gifts.example/base", exports, files)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/base/ton-gift/export", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "TON collectible export") ||
		!strings.Contains(rr.Body.String(), "/base/ton-gift/assets/tonconnect-ui-3.0.2.min.js") ||
		strings.Contains(rr.Body.String(), "unpkg.com") || strings.Contains(rr.Body.String(), "safe_token") {
		t.Fatalf("page status=%d body=%q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Cache-Control") != "no-store" || !strings.Contains(rr.Header().Get("Content-Security-Policy"), "connect-src https: wss: 'self'") {
		t.Fatalf("page security headers=%v", rr.Header())
	}

	rr = httptest.NewRecorder()
	stateRequest := httptest.NewRequest(http.MethodGet, "/base/ton-gift/export/state", nil)
	stateRequest.Header.Set("Authorization", "Bearer safe_token")
	h.ServeHTTP(rr, stateRequest)
	var state tonGiftStateResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &state); err != nil || rr.Code != http.StatusOK || state.Status != "awaiting_wallet" || state.ItemIndex != "" {
		t.Fatalf("state status=%d body=%q parsed=%+v err=%v", rr.Code, rr.Body.String(), state, err)
	}
	assertBearerCapabilityHeaders(t, rr)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/base/tonconnect-manifest.json", nil))
	if rr.Code != http.StatusOK || rr.Header().Get("Access-Control-Allow-Origin") != "*" ||
		!strings.Contains(rr.Body.String(), `"url":"https://gifts.example/base"`) ||
		!strings.Contains(rr.Body.String(), `"iconUrl":"https://gifts.example/base/ton-gift/assets/icon-180.ico"`) {
		t.Fatalf("manifest status=%d headers=%v body=%q", rr.Code, rr.Header(), rr.Body.String())
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/base/ton-gift/assets/tonconnect-ui-3.0.2.min.js", nil))
	if rr.Code != http.StatusOK || rr.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" || rr.Body.Len() < 400000 {
		t.Fatalf("SDK asset status=%d bytes=%d headers=%v", rr.Code, rr.Body.Len(), rr.Header())
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/base/ton-gift/nft/22.json", nil))
	if rr.Code != http.StatusOK || rr.Header().Get("Access-Control-Allow-Origin") != "*" || rr.Body.String() != string(exports.metadata) {
		t.Fatalf("metadata status=%d headers=%v body=%q", rr.Code, rr.Header(), rr.Body.String())
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/base/ton-gift/nft/22/media", nil))
	if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "image/png" || rr.Body.String() != "\x89PNGdata" {
		t.Fatalf("media status=%d headers=%v body=%q", rr.Code, rr.Header(), rr.Body.String())
	}
}

func TestTONGiftChallengeAndProofBoundary(t *testing.T) {
	exports := tonGiftFixture()
	h := newTONGiftHandler(t, "https://gifts.example", exports, fakeTONGiftFiles{})

	challengeRequest := httptest.NewRequest(http.MethodPost, "/ton-gift/export/challenge", strings.NewReader("{}"))
	challengeRequest.Header.Set("Content-Type", "application/json")
	challengeRequest.Header.Set("Authorization", "Bearer safe_token")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, challengeRequest)
	if rr.Code != http.StatusCreated || !strings.Contains(rr.Body.String(), `"payload":"proof-payload"`) {
		t.Fatalf("challenge status=%d body=%q", rr.Code, rr.Body.String())
	}
	assertBearerCapabilityHeaders(t, rr)

	proofBody := `{"account":{"address":"0:wallet","chain":"-239","walletStateInit":"` +
		base64.StdEncoding.EncodeToString([]byte{1, 2, 3}) + `"},"proof":{"timestamp":"` +
		strconv.FormatInt(time.Now().Unix(), 10) + `","domain":{"lengthBytes":13,"value":"gifts.example"},"signature":"` +
		base64.StdEncoding.EncodeToString(make([]byte, 64)) + `","payload":"proof-payload"}}`
	proofRequest := httptest.NewRequest(http.MethodPost, "/ton-gift/export/proof", strings.NewReader(proofBody))
	proofRequest.Header.Set("Content-Type", "application/json")
	proofRequest.Header.Set("Authorization", "Bearer safe_token")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, proofRequest)
	if rr.Code != http.StatusOK || exports.proofCalls != 1 || exports.proof.Chain != "-239" ||
		exports.proof.Payload != "proof-payload" || len(exports.proof.Proof.Signature) != 64 {
		t.Fatalf("proof status=%d calls=%d submission=%+v body=%q", rr.Code, exports.proofCalls, exports.proof, rr.Body.String())
	}

	badRequest := httptest.NewRequest(http.MethodPost, "/ton-gift/export/proof", strings.NewReader(`{"unexpected":true}`))
	badRequest.Header.Set("Content-Type", "application/json")
	badRequest.Header.Set("Authorization", "Bearer safe_token")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, badRequest)
	if rr.Code != http.StatusBadRequest || exports.proofCalls != 1 {
		t.Fatalf("unknown proof field status=%d calls=%d body=%q", rr.Code, exports.proofCalls, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	missingType := httptest.NewRequest(http.MethodPost, "/ton-gift/export/challenge", strings.NewReader("{}"))
	missingType.Header.Set("Authorization", "Bearer safe_token")
	h.ServeHTTP(rr, missingType)
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("challenge without JSON status=%d, want 415", rr.Code)
	}
}
