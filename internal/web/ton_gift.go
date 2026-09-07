package web

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/ton/wallet"
	"go.uber.org/zap"

	"telesrv/internal/app/stargifts"
	"telesrv/internal/domain"
)

const (
	tonConnectUIAssetName = "tonconnect-ui-3.0.2.min.js"
	maxTONGiftMediaBytes  = 20 << 20
	maxTONProofBodyBytes  = 32 << 10
)

//go:embed static/tonconnect-ui-3.0.2.min.js static/tonconnect-ui-LICENSE static/ton-gift-icon.ico
var tonGiftStatic embed.FS

type TONGiftExportResolver interface {
	Resolve(ctx context.Context, token string) (stargifts.TONExportView, bool, error)
	Metadata(ctx context.Context, uniqueGiftID int64) ([]byte, bool, error)
	Media(ctx context.Context, uniqueGiftID int64) (domain.Document, bool, error)
	CreateProofChallenge(ctx context.Context, token string, now int) (stargifts.TONProofChallenge, error)
	VerifyProof(ctx context.Context, token string, submission stargifts.TONProofSubmission, now int) (domain.StarGiftTONExport, error)
}

type PublicFileResolver interface {
	GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error)
}

func registerTONGiftRoutes(mux *http.ServeMux, prefix string, h *handler) {
	registerBearerCapabilityRoute(mux, http.MethodGet, prefix+"/ton-gift/export", h.tonGiftExportPage)
	registerBearerCapabilityRoute(mux, http.MethodGet, prefix+"/ton-gift/export/state", h.tonGiftExportState)
	registerBearerCapabilityRoute(mux, http.MethodPost, prefix+"/ton-gift/export/challenge", h.tonGiftProofChallenge)
	registerBearerCapabilityRoute(mux, http.MethodPost, prefix+"/ton-gift/export/proof", h.tonGiftProof)
	mux.HandleFunc(http.MethodGet+" "+prefix+"/ton-gift/nft/{metadataFile}", h.tonGiftMetadata)
	mux.HandleFunc(http.MethodGet+" "+prefix+"/ton-gift/nft/{uniqueGiftID}/media", h.tonGiftMedia)
	mux.Handle(http.MethodGet+" "+prefix+"/ton-gift/assets/"+tonConnectUIAssetName, tonConnectSecurityHeaders(http.HandlerFunc(h.tonConnectUIAsset)))
	mux.HandleFunc(http.MethodGet+" "+prefix+"/ton-gift/assets/icon-180.ico", h.tonGiftIcon)
	mux.HandleFunc(http.MethodGet+" "+prefix+"/tonconnect-manifest.json", h.tonConnectManifest)
}

type tonGiftExportPage struct {
	AppName      string
	IconURL      string
	StateURL     string
	ChallengeURL string
	ProofURL     string
	ManifestURL  string
	SDKURL       string
}

var tonGiftExportTemplate = template.Must(template.New("ton-gift-export").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="referrer" content="no-referrer"><meta name="robots" content="noindex,nofollow">
<title>TON collectible export · {{.AppName}}</title><style>
:root{color-scheme:light;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#18212b;background:#f3f6f8}
*{box-sizing:border-box}body{margin:0;min-height:100svh}.topbar{height:64px;display:flex;align-items:center;padding:0 max(20px,calc((100vw - 1060px)/2));border-bottom:1px solid #dce3e8;background:#fff;font-weight:750}.topbar img{width:32px;height:32px;margin-right:10px}
main{width:min(100%,1060px);margin:0 auto;display:grid;grid-template-columns:minmax(280px,420px) minmax(320px,1fr);gap:56px;padding:52px 28px 64px}.visual{aspect-ratio:1;display:grid;place-items:center;overflow:hidden;border:1px solid #d7e0e6;border-radius:8px;background:#dcebf7}.visual img{width:100%;height:100%;object-fit:contain}.visual-fallback{font-size:72px;font-weight:800;color:#1677c8}.details{align-self:center;min-width:0}.eyebrow{margin:0 0 8px;color:#536574;font-size:13px;font-weight:700;text-transform:uppercase}.title{margin:0;font-size:36px;line-height:1.15;letter-spacing:0;overflow-wrap:anywhere}.slug{margin:8px 0 28px;color:#627584}.facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));border-top:1px solid #d7e0e6}.fact{min-width:0;padding:14px 12px 14px 0;border-bottom:1px solid #d7e0e6}.fact:nth-child(even){padding-left:12px}.label{display:block;color:#6d7e8c;font-size:12px}.value{display:block;margin-top:4px;font-weight:650;overflow-wrap:anywhere}.claim{min-height:104px;margin-top:28px;padding-top:2px}.status{min-height:24px;margin:0 0 14px;color:#2e4251;font-weight:650}.status[data-tone="ok"]{color:#167546}.status[data-tone="error"]{color:#b42318}#ton-connect{min-height:48px}.address{margin:12px 0 0;color:#647582;font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere}
@media(max-width:760px){.topbar{height:58px;padding:0 18px}main{grid-template-columns:1fr;gap:28px;padding:24px 18px 44px}.visual{width:min(100%,390px);margin:0 auto}.title{font-size:30px}.details{align-self:start}}
</style></head><body><header class="topbar"><img src="{{.IconURL}}" alt=""><span>{{.AppName}}</span></header>
<main id="export" data-state-url="{{.StateURL}}" data-challenge-url="{{.ChallengeURL}}" data-proof-url="{{.ProofURL}}" data-manifest-url="{{.ManifestURL}}">
<section class="visual"><img id="gift-media" alt="" hidden onerror="this.hidden=true;this.nextElementSibling.hidden=false"><span id="gift-number" class="visual-fallback">TON</span></section>
<section class="details"><p class="eyebrow">TON collectible export</p><h1 id="gift-title" class="title">Collectible gift</h1><p id="gift-slug" class="slug"></p>
<div class="facts"><div class="fact"><span class="label">Model</span><span id="gift-model" class="value">-</span></div><div class="fact"><span class="label">Pattern</span><span id="gift-pattern" class="value">-</span></div><div class="fact"><span class="label">Backdrop</span><span id="gift-backdrop" class="value">-</span></div><div class="fact"><span class="label">Network</span><span id="gift-network" class="value">-</span></div></div>
<div class="claim"><p id="status" class="status">Opening export</p><div id="ton-connect"></div><p id="address" class="address"></p></div></section></main>
<script src="{{.SDKURL}}"></script><script>
(() => {
  const root = document.getElementById('export');
  const statusNode = document.getElementById('status');
  const addressNode = document.getElementById('address');
  const token = location.hash.slice(1);
  const labels = {resolving_item:'Preparing export',awaiting_wallet:'Wallet proof required',proof_verified:'Wallet verified',mint_queued:'Mint queued',prepared:'Mint prepared',submitted:'Mint submitted',confirmed:'Confirming ownership',finalized:'Export complete',failed:'Export failed',quarantined:'Manual review required'};
  let connectUI = null, challengePayload = '', proofSent = false, stopped = false;
  const setStatus = (text, tone='') => { statusNode.textContent = text; statusNode.dataset.tone = tone; };
  async function requestJSON(url, options) {
    options = options || {};
    options.headers = Object.assign({}, options.headers || {}, {authorization:'Bearer '+token});
    const response = await fetch(url, options);
    let body = null;
    try { body = await response.json(); } catch (_) {}
    if (!response.ok) { const e = new Error('request failed'); e.status = response.status; throw e; }
    return body;
  }
  async function initializeProof(network) {
    if (connectUI || stopped) return;
    const challenge = await requestJSON(root.dataset.challengeUrl, {method:'POST',headers:{'content-type':'application/json'},body:'{}'});
    challengePayload = challenge.payload;
    connectUI = new TON_CONNECT_UI.TonConnectUI({manifestUrl:root.dataset.manifestUrl,buttonRootId:'ton-connect',restoreConnection:false,analytics:{mode:'off'}});
    if (typeof connectUI.setConnectionNetwork === 'function') connectUI.setConnectionNetwork(network === 'testnet' ? '-3' : '-239');
    connectUI.setConnectRequestParameters({state:'ready',value:{tonProof:challengePayload}});
    connectUI.onStatusChange(async walletInfo => {
      const item = walletInfo && walletInfo.connectItems && walletInfo.connectItems.tonProof;
      if (!walletInfo || !item || !item.proof || proofSent) return;
      if (item.proof.payload !== challengePayload) { setStatus('Wallet proof expired','error'); return; }
      proofSent = true; setStatus('Verifying wallet');
      try {
        await requestJSON(root.dataset.proofUrl, {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({account:walletInfo.account,proof:item.proof})});
        setStatus('Mint queued','ok');
      } catch (_) { proofSent = false; setStatus('Wallet proof rejected','error'); }
    });
  }
  async function refreshState() {
    if (stopped) return;
    try {
      const state = await requestJSON(root.dataset.stateUrl);
      document.getElementById('gift-title').textContent = state.title;
      document.getElementById('gift-slug').textContent = state.slug;
      document.getElementById('gift-number').textContent = '#'+state.number;
      document.getElementById('gift-model').textContent = state.model;
      document.getElementById('gift-pattern').textContent = state.pattern;
      document.getElementById('gift-backdrop').textContent = state.backdrop;
      document.getElementById('gift-network').textContent = state.network;
      const media = document.getElementById('gift-media'); if (!media.src) { media.src = state.media_url; media.hidden = false; }
      setStatus(labels[state.status] || state.status, state.status === 'finalized' ? 'ok' : (state.status === 'failed' || state.status === 'quarantined' ? 'error' : ''));
      addressNode.textContent = state.owner_address || state.wallet_address || state.item_address || '';
      if (state.status === 'awaiting_wallet') await initializeProof(state.network);
      if (state.terminal) { stopped = true; const button = document.getElementById('ton-connect'); if (button) button.hidden = true; }
    } catch (error) { if (error.status !== 409) setStatus('Export state unavailable','error'); }
  }
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(token)) { stopped = true; setStatus('Invalid export link','error'); }
  else { refreshState(); window.setInterval(refreshState, 2500); }
})();
</script></body></html>`))

func (h *handler) tonGiftExportPage(w http.ResponseWriter, r *http.Request) {
	if h.tonGiftExports == nil {
		http.NotFound(w, r)
		return
	}
	base := strings.TrimRight(h.publicBaseURL, "/")
	exportURL := base + "/ton-gift/export"
	page := tonGiftExportPage{
		AppName: h.appName, IconURL: base + "/ton-gift/assets/icon-180.ico",
		StateURL: exportURL + "/state", ChallengeURL: exportURL + "/challenge", ProofURL: exportURL + "/proof",
		ManifestURL: base + "/tonconnect-manifest.json", SDKURL: base + "/ton-gift/assets/" + tonConnectUIAssetName,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", tonConnectCSP)
	if err := tonGiftExportTemplate.Execute(w, page); err != nil {
		h.logger.Error("Render TON gift export page failed", zap.Error(err))
	}
}

type tonGiftStateResponse struct {
	Title           string `json:"title"`
	Slug            string `json:"slug"`
	Number          int    `json:"number"`
	Model           string `json:"model"`
	Pattern         string `json:"pattern"`
	Backdrop        string `json:"backdrop"`
	MediaURL        string `json:"media_url"`
	Status          string `json:"status"`
	Network         string `json:"network"`
	Collection      string `json:"collection_address"`
	ItemIndex       string `json:"item_index"`
	ItemAddress     string `json:"item_address,omitempty"`
	WalletAddress   string `json:"wallet_address,omitempty"`
	OwnerAddress    string `json:"owner_address,omitempty"`
	MetadataURI     string `json:"metadata_uri"`
	ExpiresAt       int    `json:"expires_at"`
	ProofVerifiedAt int    `json:"proof_verified_at,omitempty"`
	SubmittedAt     int    `json:"submitted_at,omitempty"`
	ConfirmedAt     int    `json:"confirmed_at,omitempty"`
	FinalizedAt     int    `json:"finalized_at,omitempty"`
	Terminal        bool   `json:"terminal"`
}

func (h *handler) tonGiftExportState(w http.ResponseWriter, r *http.Request) {
	view, ok := h.resolveTONGiftCapability(w, r)
	if !ok {
		return
	}
	status := view.Export.Status
	writeTONJSON(w, http.StatusOK, tonGiftStateResponse{
		Title: view.Gift.Title + " #" + strconv.Itoa(view.Gift.Num), Slug: view.Gift.Slug, Number: view.Gift.Num,
		Model: view.Gift.Model.Name, Pattern: view.Gift.Pattern.Name, Backdrop: view.Gift.Backdrop.Name,
		MediaURL: strings.TrimRight(h.publicBaseURL, "/") + "/ton-gift/nft/" + strconv.FormatInt(view.Gift.ID, 10) + "/media",
		Status:   string(status), Network: string(view.Export.Network), Collection: view.Export.CollectionAddress,
		ItemIndex: view.Export.ItemIndex, ItemAddress: view.Export.ExpectedItemAddress, WalletAddress: view.Export.WalletAddress,
		OwnerAddress: view.Gift.OwnerAddress, MetadataURI: view.Export.MetadataURI, ExpiresAt: view.Export.ExpiresAt,
		ProofVerifiedAt: view.Export.ProofVerifiedAt, SubmittedAt: view.Export.SubmittedAt,
		ConfirmedAt: view.Export.ConfirmedAt, FinalizedAt: view.Export.FinalizedAt,
		Terminal: status == domain.StarGiftTONExportFinalized || status == domain.StarGiftTONExportFailed || status == domain.StarGiftTONExportQuarantined,
	})
}

func (h *handler) tonGiftProofChallenge(w http.ResponseWriter, r *http.Request) {
	token := tonBearerCapability(r)
	if token == "" || h.tonGiftExports == nil {
		http.NotFound(w, r)
		return
	}
	if !decodeTONJSON(w, r, &struct{}{}) {
		return
	}
	challenge, err := h.tonGiftExports.CreateProofChallenge(r.Context(), token, int(time.Now().Unix()))
	if err != nil {
		writeTONError(w, err)
		return
	}
	writeTONJSON(w, http.StatusCreated, map[string]any{
		"payload": challenge.Payload, "domain": challenge.Domain, "network": challenge.Export.Network,
		"expires_at": challenge.ExpiresAt,
	})
}

type tonProofHTTPAccount struct {
	Address         string `json:"address"`
	Chain           string `json:"chain"`
	WalletStateInit string `json:"walletStateInit"`
}

type tonProofHTTPProof struct {
	Timestamp json.RawMessage `json:"timestamp"`
	Domain    struct {
		LengthBytes uint32 `json:"lengthBytes"`
		Value       string `json:"value"`
	} `json:"domain"`
	Signature string `json:"signature"`
	Payload   string `json:"payload"`
}

type tonProofHTTPRequest struct {
	Account tonProofHTTPAccount `json:"account"`
	Proof   tonProofHTTPProof   `json:"proof"`
}

func (h *handler) tonGiftProof(w http.ResponseWriter, r *http.Request) {
	token := tonBearerCapability(r)
	var input tonProofHTTPRequest
	if token == "" || h.tonGiftExports == nil {
		http.NotFound(w, r)
		return
	}
	if !decodeTONJSON(w, r, &input) {
		return
	}
	stateInit, err := decodeTONBase64(input.Account.WalletStateInit, 16384)
	if err != nil {
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid proof"})
		return
	}
	signature, err := decodeTONBase64(input.Proof.Signature, 64)
	if err != nil || len(signature) != 64 {
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid proof"})
		return
	}
	timestamp, err := parseTONProofTimestamp(input.Proof.Timestamp)
	if err != nil {
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid proof"})
		return
	}
	proof := wallet.TonConnectProof{Timestamp: timestamp, Signature: signature, Payload: input.Proof.Payload}
	proof.Domain.LengthBytes = input.Proof.Domain.LengthBytes
	proof.Domain.Value = input.Proof.Domain.Value
	export, err := h.tonGiftExports.VerifyProof(r.Context(), token, stargifts.TONProofSubmission{
		Payload: input.Proof.Payload, Chain: input.Account.Chain, Address: input.Account.Address,
		StateInit: stateInit, Proof: proof,
	}, int(time.Now().Unix()))
	if err != nil {
		writeTONError(w, err)
		return
	}
	writeTONJSON(w, http.StatusOK, map[string]any{"status": export.Status, "export_id": export.ID})
}

func (h *handler) resolveTONGiftCapability(w http.ResponseWriter, r *http.Request) (stargifts.TONExportView, bool) {
	token := tonBearerCapability(r)
	if token == "" || h.tonGiftExports == nil {
		http.NotFound(w, r)
		return stargifts.TONExportView{}, false
	}
	view, found, err := h.tonGiftExports.Resolve(r.Context(), token)
	if err != nil {
		http.Error(w, "export lookup failed", http.StatusInternalServerError)
		return stargifts.TONExportView{}, false
	}
	if !found {
		http.NotFound(w, r)
		return stargifts.TONExportView{}, false
	}
	return view, true
}

func (h *handler) tonGiftMetadata(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PathValue("metadataFile"))
	if !strings.HasSuffix(name, ".json") || h.tonGiftExports == nil {
		http.NotFound(w, r)
		return
	}
	uniqueGiftID, err := strconv.ParseInt(strings.TrimSuffix(name, ".json"), 10, 64)
	if err != nil || uniqueGiftID <= 0 {
		http.NotFound(w, r)
		return
	}
	metadata, found, err := h.tonGiftExports.Metadata(r.Context(), uniqueGiftID)
	if err != nil {
		http.Error(w, "metadata lookup failed", http.StatusInternalServerError)
		return
	}
	if !found || !json.Valid(metadata) {
		http.NotFound(w, r)
		return
	}
	digest := sha256.Sum256(metadata)
	serveTONImmutable(w, r, "application/json", `"sha256-`+hex.EncodeToString(digest[:])+`"`, metadata)
}

func (h *handler) tonGiftMedia(w http.ResponseWriter, r *http.Request) {
	uniqueGiftID, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("uniqueGiftID")), 10, 64)
	if err != nil || uniqueGiftID <= 0 || h.tonGiftExports == nil || h.tonGiftFiles == nil {
		http.NotFound(w, r)
		return
	}
	document, found, err := h.tonGiftExports.Media(r.Context(), uniqueGiftID)
	if err != nil {
		http.Error(w, "media lookup failed", http.StatusInternalServerError)
		return
	}
	if !found || document.ID <= 0 || document.Size <= 0 || document.Size > maxTONGiftMediaBytes {
		http.NotFound(w, r)
		return
	}
	chunk, found, err := h.tonGiftFiles.GetFile(r.Context(), domain.FileDownloadRequest{
		LocationKey: fmt.Sprintf("doc:%d", document.ID), Limit: maxTONGiftMediaBytes + 1,
	})
	if err != nil {
		http.Error(w, "media read failed", http.StatusInternalServerError)
		return
	}
	if !found || chunk.Total != document.Size || int64(len(chunk.Bytes)) != chunk.Total {
		http.NotFound(w, r)
		return
	}
	mimeType := safeTONGiftMediaType(document.MimeType, chunk.MimeType, chunk.Bytes)
	if mimeType == "" {
		http.NotFound(w, r)
		return
	}
	etag := fmt.Sprintf(`"ton-gift-media-%d-%d-%d"`, document.ID, document.AccessHash, document.Size)
	if document.Date > 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(document.Date), 0).UTC().Format(http.TimeFormat))
	}
	w.Header().Set("Content-Disposition", "inline")
	serveTONImmutable(w, r, mimeType, etag, chunk.Bytes)
}

func (h *handler) tonConnectManifest(w http.ResponseWriter, _ *http.Request) {
	base := strings.TrimRight(h.publicBaseURL, "/")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=3600, must-revalidate")
	writeTONJSON(w, http.StatusOK, map[string]string{
		"url": base, "name": h.appName + " Gifts", "iconUrl": base + "/ton-gift/assets/icon-180.ico",
	})
}

func (h *handler) tonConnectUIAsset(w http.ResponseWriter, r *http.Request) {
	data, err := tonGiftStatic.ReadFile("static/" + tonConnectUIAssetName)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	serveTONImmutable(w, r, "text/javascript; charset=utf-8", `"tonconnect-ui-3.0.2"`, data)
}

func (h *handler) tonGiftIcon(w http.ResponseWriter, r *http.Request) {
	data, err := tonGiftStatic.ReadFile("static/ton-gift-icon.ico")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	serveTONImmutable(w, r, "image/x-icon", `"ton-gift-icon-v1"`, data)
}

func serveTONImmutable(w http.ResponseWriter, r *http.Request, contentType, etag string, data []byte) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func decodeTONJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "application/json") {
		writeTONJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "application/json required"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTONProofBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return false
	}
	return true
}

func decodeTONBase64(value string, maxBytes int) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBytes*2 {
		return nil, fmt.Errorf("invalid base64")
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(value)
		if err == nil && len(decoded) > 0 && len(decoded) <= maxBytes {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid base64")
}

func parseTONProofTimestamp(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing timestamp")
	}
	value := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	timestamp, err := strconv.ParseInt(value, 10, 64)
	if err != nil || timestamp <= 0 {
		return 0, fmt.Errorf("invalid timestamp")
	}
	return timestamp, nil
}

func validTONCapabilityToken(token string) string {
	if token == "" || token != strings.TrimSpace(token) || len(token) > 128 {
		return ""
	}
	for _, ch := range token {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			continue
		}
		return ""
	}
	return token
}

func tonBearerCapability(r *http.Request) string {
	if r == nil {
		return ""
	}
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") || strings.Contains(value[7:], " ") {
		return ""
	}
	return validTONCapabilityToken(value[7:])
}

func safeTONGiftMediaType(documentType, chunkType string, data []byte) string {
	detected := http.DetectContentType(data)
	for _, candidate := range []string{strings.ToLower(strings.TrimSpace(documentType)), strings.ToLower(strings.TrimSpace(chunkType)), detected} {
		switch candidate {
		case "image/jpeg", "image/png", "image/gif", "image/webp", "video/mp4", "video/webm", "application/x-tgsticker":
			return candidate
		}
	}
	return ""
}

func writeTONError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrStarGiftTONExportInvalid):
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid proof"})
	case errors.Is(err, domain.ErrStarGiftTONExportStateConflict), errors.Is(err, domain.ErrStarGiftExternalizationPending):
		writeTONJSON(w, http.StatusConflict, map[string]string{"error": "export state conflict"})
	case errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable):
		writeTONJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "TON export is temporarily unavailable"})
	default:
		writeTONJSON(w, http.StatusInternalServerError, map[string]string{"error": "export operation failed"})
	}
}

func writeTONJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

const tonConnectCSP = "default-src 'none'; img-src 'self' data: https:; style-src 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src https: wss: 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

func tonConnectSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", tonConnectCSP)
		next.ServeHTTP(w, r)
	})
}
