package web

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/ton/wallet"

	"telesrv/internal/app/stargifts"
	"telesrv/internal/domain"
)

type TONGiftClaimResolver interface {
	Resolve(ctx context.Context, ref string) (stargifts.TONExportView, domain.StarGiftTONAsset, bool, error)
	CreateChallenge(ctx context.Context, initData, ref string, now int) (stargifts.TONClaimChallenge, error)
	Verify(ctx context.Context, submission stargifts.TONClaimSubmission, now int) (domain.StarGiftTONFinalizationResult, error)
}

func registerTONGiftClaimRoutes(mux *http.ServeMux, prefix string, h *handler) {
	mux.Handle(http.MethodGet+" "+prefix+"/ton-gift/claim", bearerCapabilitySecurityHeaders(http.HandlerFunc(h.tonGiftClaimPage)))
	mux.Handle(http.MethodPost+" "+prefix+"/ton-gift/claim/challenge", bearerCapabilitySecurityHeaders(http.HandlerFunc(h.tonGiftClaimChallenge)))
	mux.Handle(http.MethodPost+" "+prefix+"/ton-gift/claim/proof", bearerCapabilitySecurityHeaders(http.HandlerFunc(h.tonGiftClaimProof)))
}

var tonGiftClaimTemplate = template.Must(template.New("ton-gift-claim").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="referrer" content="no-referrer"><meta name="robots" content="noindex,nofollow">
<title>Claim TON collectible · {{.AppName}}</title><style>
:root{color-scheme:light;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#18212b;background:#f4f7f8}*{box-sizing:border-box}body{margin:0;min-height:100svh}.bar{height:60px;display:flex;align-items:center;padding:0 20px;border-bottom:1px solid #dce3e8;background:#fff;font-weight:750}.bar img{width:30px;height:30px;margin-right:10px}main{width:min(100%,680px);margin:0 auto;padding:44px 22px}.eyebrow{font-size:13px;font-weight:700;text-transform:uppercase;color:#647582}.title{margin:8px 0 10px;font-size:34px;line-height:1.15;letter-spacing:0}.lead{margin:0 0 28px;color:#536574}label{display:block;margin:0 0 7px;font-size:13px;font-weight:700}input{width:100%;height:48px;padding:0 13px;border:1px solid #aebcc6;border-radius:6px;background:#fff;font:15px ui-monospace,SFMono-Regular,Consolas,monospace}.actions{display:flex;align-items:center;gap:12px;min-height:50px;margin-top:18px}button{height:44px;padding:0 18px;border:0;border-radius:6px;background:#1677c8;color:#fff;font-weight:750;cursor:pointer}button:disabled{opacity:.5;cursor:default}#ton-connect{min-height:44px}.status{min-height:24px;margin:18px 0 0;font-weight:650;color:#334b5c}.status[data-tone=ok]{color:#167546}.status[data-tone=error]{color:#b42318}.facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));margin-top:24px;border-top:1px solid #d7e0e6}.fact{padding:13px 10px 13px 0;border-bottom:1px solid #d7e0e6;min-width:0}.fact span{display:block;font-size:12px;color:#70818d}.fact strong{display:block;margin-top:4px;overflow-wrap:anywhere}@media(max-width:520px){main{padding-top:28px}.title{font-size:29px}.facts{grid-template-columns:1fr}.actions{align-items:flex-start;flex-direction:column}}
</style></head><body><header class="bar"><img src="{{.IconURL}}" alt=""><span>{{.AppName}}</span></header><main id="claim" data-challenge="{{.ChallengeURL}}" data-proof="{{.ProofURL}}" data-manifest="{{.ManifestURL}}"><p class="eyebrow">TON ownership proof</p><h1 class="title">Claim a collectible</h1><p class="lead">Attach a TON gift NFT you currently own to your profile.</p><label for="gift">Gift slug or NFT address</label><input id="gift" autocomplete="off" maxlength="255" placeholder="gift-slug or EQ..."><div class="actions"><button id="claim-button" type="button">Verify ownership</button><div id="ton-connect"></div></div><p id="status" class="status">Open this Mini App from your account.</p><div class="facts" hidden id="facts"><div class="fact"><span>Gift</span><strong id="gift-name"></strong></div><div class="fact"><span>NFT address</span><strong id="nft-address"></strong></div><div class="fact"><span>Wallet</span><strong id="wallet-address"></strong></div><div class="fact"><span>Profile host</span><strong id="profile-host"></strong></div></div></main><script src="https://telegram.org/js/telegram-web-app.js"></script><script src="{{.SDKURL}}"></script><script>
(() => { const root=document.getElementById('claim'),button=document.getElementById('claim-button'),status=document.getElementById('status'),input=document.getElementById('gift'); const tg=window.Telegram&&Telegram.WebApp; const initData=tg&&tg.initData||''; let ui=null,payload='',busy=false;
const say=(text,tone='')=>{status.textContent=text;status.dataset.tone=tone}; const api=async(path,body)=>{const r=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});let data={};try{data=await r.json()}catch(_){}if(!r.ok){const e=new Error(data.error||'request failed');e.status=r.status;throw e}return data};
const show=data=>{document.getElementById('facts').hidden=false;document.getElementById('gift-name').textContent=data.title||data.slug||'';document.getElementById('nft-address').textContent=data.nft_address||'';document.getElementById('wallet-address').textContent=data.wallet_address||'';document.getElementById('profile-host').textContent=data.host_user_id?'user:'+data.host_user_id:'not hosted'};
async function submit(walletInfo){const item=walletInfo&&walletInfo.connectItems&&walletInfo.connectItems.tonProof;if(!busy||!item||!item.proof||item.proof.payload!==payload)return;for(let i=0;i<8;i++){try{const data=await api(root.dataset.proof,{init_data:initData,account:walletInfo.account,proof:item.proof});show(data);say('Collectible attached to your profile','ok');busy=false;button.disabled=false;return}catch(e){if(e.status!==409)throw e;say('Checking the latest on-chain owner');await new Promise(r=>setTimeout(r,1500))}}throw new Error('Ownership observation is not fresh yet')}
button.addEventListener('click',async()=>{if(busy)return;const gift=input.value.trim();if(!initData){say('Mini App authentication is unavailable','error');return}if(!gift){say('Enter a gift slug or NFT address','error');return}busy=true;button.disabled=true;try{const challenge=await api(root.dataset.challenge,{init_data:initData,gift});payload=challenge.payload;show(challenge);if(!ui){ui=new TON_CONNECT_UI.TonConnectUI({manifestUrl:root.dataset.manifest,buttonRootId:'ton-connect',restoreConnection:false,analytics:{mode:'off'}});ui.onStatusChange(info=>submit(info).catch(e=>{busy=false;button.disabled=false;say(e.message||'Claim failed','error')}))}ui.setConnectRequestParameters({state:'ready',value:{tonProof:payload}});if(ui.connected)await ui.disconnect();say('Connect the current owner wallet');await ui.openModal()}catch(e){busy=false;button.disabled=false;say(e.message||'Claim failed','error')}});if(tg){tg.ready();tg.expand()} })();
</script></body></html>`))

type tonGiftClaimPageData struct {
	AppName, IconURL, ChallengeURL, ProofURL, ManifestURL, SDKURL string
}

func (h *handler) tonGiftClaimPage(w http.ResponseWriter, r *http.Request) {
	if h.tonGiftClaims == nil {
		http.NotFound(w, r)
		return
	}
	base := strings.TrimRight(h.publicBaseURL, "/")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", strings.Replace(tonConnectCSP, "script-src 'self'", "script-src 'self' https://telegram.org", 1))
	_ = tonGiftClaimTemplate.Execute(w, tonGiftClaimPageData{
		AppName: h.appName, IconURL: base + "/ton-gift/assets/icon-180.ico",
		ChallengeURL: base + "/ton-gift/claim/challenge", ProofURL: base + "/ton-gift/claim/proof",
		ManifestURL: base + "/tonconnect-manifest.json", SDKURL: base + "/ton-gift/assets/" + tonConnectUIAssetName,
	})
}

type tonGiftClaimChallengeRequest struct {
	InitData string `json:"init_data"`
	Gift     string `json:"gift"`
}

func (h *handler) tonGiftClaimChallenge(w http.ResponseWriter, r *http.Request) {
	var input tonGiftClaimChallengeRequest
	if h.tonGiftClaims == nil || !decodeTONJSON(w, r, &input) {
		return
	}
	challenge, err := h.tonGiftClaims.CreateChallenge(r.Context(), input.InitData, input.Gift, int(time.Now().Unix()))
	if err != nil {
		writeTONClaimError(w, err)
		return
	}
	writeTONJSON(w, http.StatusCreated, map[string]any{
		"payload": challenge.Payload, "expires_at": challenge.ExpiresAt,
		"title": challenge.Gift.Title + " #" + strconv.Itoa(challenge.Gift.Num), "slug": challenge.Gift.Slug,
		"wallet_address": challenge.WalletAddress, "nft_address": challenge.NFTAddress,
	})
}

type tonGiftClaimProofRequest struct {
	InitData string              `json:"init_data"`
	Account  tonProofHTTPAccount `json:"account"`
	Proof    tonProofHTTPProof   `json:"proof"`
}

func (h *handler) tonGiftClaimProof(w http.ResponseWriter, r *http.Request) {
	var input tonGiftClaimProofRequest
	if h.tonGiftClaims == nil || !decodeTONJSON(w, r, &input) {
		return
	}
	stateInit, err := decodeTONBase64(input.Account.WalletStateInit, 16384)
	if err != nil {
		writeTONClaimError(w, stargifts.ErrTONClaimInvalid)
		return
	}
	signature, err := decodeTONBase64(input.Proof.Signature, 64)
	if err != nil || len(signature) != 64 {
		writeTONClaimError(w, stargifts.ErrTONClaimInvalid)
		return
	}
	timestamp, err := parseTONProofTimestamp(input.Proof.Timestamp)
	if err != nil {
		writeTONClaimError(w, stargifts.ErrTONClaimInvalid)
		return
	}
	proof := wallet.TonConnectProof{Timestamp: timestamp, Signature: signature, Payload: input.Proof.Payload}
	proof.Domain.LengthBytes, proof.Domain.Value = input.Proof.Domain.LengthBytes, input.Proof.Domain.Value
	result, err := h.tonGiftClaims.Verify(r.Context(), stargifts.TONClaimSubmission{
		InitData: input.InitData, Payload: input.Proof.Payload, Chain: input.Account.Chain,
		Address: input.Account.Address, StateInit: stateInit, Proof: proof,
	}, int(time.Now().Unix()))
	if err != nil {
		writeTONClaimError(w, err)
		return
	}
	writeTONJSON(w, http.StatusOK, map[string]any{
		"status": "claimed", "slug": result.Gift.Slug, "title": result.Gift.Title,
		"nft_address": result.Asset.ItemAddress, "wallet_address": result.Asset.OwnerAddress,
		"host_user_id": result.Gift.Host.ID,
	})
}

func writeTONClaimError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, stargifts.ErrTONClaimUnauthorized):
		writeTONJSON(w, http.StatusUnauthorized, map[string]string{"error": "Mini App authentication failed"})
	case errors.Is(err, stargifts.ErrTONClaimNotOwner):
		writeTONJSON(w, http.StatusConflict, map[string]string{"error": "fresh on-chain ownership proof required"})
	case errors.Is(err, stargifts.ErrTONClaimInvalid):
		writeTONJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid claim"})
	default:
		writeTONJSON(w, http.StatusInternalServerError, map[string]string{"error": "claim failed"})
	}
}
