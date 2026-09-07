package web

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"telesrv/internal/app/stargifts"
	"telesrv/internal/domain"
)

type fakeTONGiftClaims struct {
	challenge stargifts.TONClaimChallenge
	result    domain.StarGiftTONFinalizationResult
	err       error
	created   int
	verified  int
}

func (f *fakeTONGiftClaims) Resolve(context.Context, string) (stargifts.TONExportView, domain.StarGiftTONAsset, bool, error) {
	return stargifts.TONExportView{}, domain.StarGiftTONAsset{}, true, nil
}

func (f *fakeTONGiftClaims) CreateChallenge(context.Context, string, string, int) (stargifts.TONClaimChallenge, error) {
	f.created++
	return f.challenge, f.err
}

func (f *fakeTONGiftClaims) Verify(context.Context, stargifts.TONClaimSubmission, int) (domain.StarGiftTONFinalizationResult, error) {
	f.verified++
	return f.result, f.err
}

func newTONGiftClaimHandler(t *testing.T, claims *fakeTONGiftClaims) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{
		StickerSets: fakeResolver{}, PublicBaseURL: "https://gifts.example", AppName: "telesrv",
		TONGiftExports: tonGiftFixture(), TONGiftClaims: claims, TONGiftFiles: fakeTONGiftFiles{},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

func TestTONGiftClaimRoutesAreOptInAndPageUsesLocalWalletSDK(t *testing.T) {
	if _, err := NewHandler(Config{StickerSets: fakeResolver{}, PublicBaseURL: "https://gifts.example", TONGiftClaims: &fakeTONGiftClaims{}}); err == nil {
		t.Fatal("claim route accepted without TON export/media boundary")
	}
	claims := &fakeTONGiftClaims{}
	h := newTONGiftClaimHandler(t, claims)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/ton-gift/claim", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "/ton-gift/assets/tonconnect-ui-3.0.2.min.js") ||
		strings.Contains(rr.Body.String(), "unpkg.com") || !strings.Contains(rr.Body.String(), "telegram.org/js/telegram-web-app.js") {
		t.Fatalf("claim page status=%d body=%q", rr.Code, rr.Body.String())
	}
	csp := rr.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "script-src 'self' https://telegram.org") || !strings.Contains(csp, "connect-src https: wss: 'self'") {
		t.Fatalf("claim CSP = %q", csp)
	}
}

func TestTONGiftClaimChallengeProofAndErrors(t *testing.T) {
	claims := &fakeTONGiftClaims{
		challenge: stargifts.TONClaimChallenge{
			Payload: "claim-proof", ExpiresAt: int(time.Now().Add(time.Minute).Unix()),
			Gift:          domain.UniqueStarGift{Title: "Crystal", Slug: "crystal-1", Num: 1},
			WalletAddress: "0:wallet", NFTAddress: "0:item",
		},
		result: domain.StarGiftTONFinalizationResult{
			Gift:  domain.UniqueStarGift{Title: "Crystal", Slug: "crystal-1", Host: domain.Peer{Type: domain.PeerTypeUser, ID: 44}},
			Asset: domain.StarGiftTONAsset{OwnerAddress: "0:wallet", ItemAddress: "0:item"},
		},
	}
	h := newTONGiftClaimHandler(t, claims)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ton-gift/claim/challenge", strings.NewReader(`{"init_data":"signed","gift":"crystal-1"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated || claims.created != 1 || !strings.Contains(rr.Body.String(), `"payload":"claim-proof"`) {
		t.Fatalf("challenge status=%d calls=%d body=%q", rr.Code, claims.created, rr.Body.String())
	}

	proofBody := `{"init_data":"signed","account":{"address":"0:wallet","chain":"-239","walletStateInit":"` +
		base64.StdEncoding.EncodeToString([]byte{1, 2, 3}) + `"},"proof":{"timestamp":"` +
		strconv.FormatInt(time.Now().Unix(), 10) + `","domain":{"lengthBytes":13,"value":"gifts.example"},"signature":"` +
		base64.StdEncoding.EncodeToString(make([]byte, 64)) + `","payload":"claim-proof"}}`
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/ton-gift/claim/proof", strings.NewReader(proofBody))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || claims.verified != 1 || !strings.Contains(rr.Body.String(), `"host_user_id":44`) {
		t.Fatalf("proof status=%d calls=%d body=%q", rr.Code, claims.verified, rr.Body.String())
	}

	claims.err = stargifts.ErrTONClaimNotOwner
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/ton-gift/claim/challenge", strings.NewReader(`{"init_data":"signed","gift":"crystal-1"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("not-owner status=%d body=%q", rr.Code, rr.Body.String())
	}

	claims.err = stargifts.ErrTONClaimUnauthorized
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/ton-gift/claim/challenge", strings.NewReader(`{"init_data":"signed","gift":"crystal-1"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%q", rr.Code, rr.Body.String())
	}

	claims.err = nil
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/ton-gift/claim/challenge", strings.NewReader(`{"init_data":"signed","gift":"crystal-1","extra":true}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%q", rr.Code, rr.Body.String())
	}
}
