package domain

import (
	"crypto/sha256"
	"testing"
)

func TestStarGiftTONExportRequestValidation(t *testing.T) {
	metadataJSON := []byte(`{}`)
	metadataHash := sha256.Sum256(metadataJSON)
	req := StarGiftTONExportRequest{
		UserID:      7,
		Ref:         SavedStarGiftRef{Owner: Peer{Type: PeerTypeUser, ID: 7}, MsgID: 11},
		TokenDigest: make([]byte, 32), Network: TONNetworkMainnet,
		CollectionAddress: "collection", CollectionCodeHash: make([]byte, 32), InitialItemIndex: "17",
		MintABI:     StarGiftTONMintABIBasicCollectionV1,
		MetadataURI: "https://example.test/gift.json", MetadataHash: metadataHash[:], MetadataJSON: metadataJSON,
		Date: 100, ExpiresAt: 200,
	}
	if !req.Valid() {
		t.Fatal("valid TON export request rejected")
	}
	req.Ref.Owner.ID++
	if req.Valid() {
		t.Fatal("cross-owner TON export request accepted")
	}
}

func TestStarGiftTONStateAndChainFactValidation(t *testing.T) {
	for _, status := range []StarGiftTONExportStatus{
		StarGiftTONExportResolvingItem, StarGiftTONExportAwaitingWallet, StarGiftTONExportProofVerified, StarGiftTONExportMintQueued,
		StarGiftTONExportPrepared,
		StarGiftTONExportSubmitted, StarGiftTONExportConfirmed, StarGiftTONExportFinalized,
		StarGiftTONExportFailed, StarGiftTONExportQuarantined,
	} {
		if !status.Valid() {
			t.Fatalf("status %q rejected", status)
		}
	}
	if StarGiftTONExportMintQueued.PossiblySubmitted() || !StarGiftTONExportQuarantined.PossiblySubmitted() {
		t.Fatal("possibly-submitted boundary is incorrect")
	}

	preparation := StarGiftTONMintPreparation{JobID: 1, WorkerID: "worker-a", FencingToken: 2,
		SubmissionHash: make([]byte, 32), SubmissionBOC: []byte{1}, PreparedAt: 3, ValidUntil: 30}
	if !preparation.Valid() {
		t.Fatal("valid TON mint preparation rejected")
	}
	submission := StarGiftTONMintSubmission{JobID: 1, WorkerID: "worker-a", FencingToken: 2,
		TxHash: make([]byte, 32), TxLT: "18446744073709551615", BlockSeqno: 3, SubmittedAt: 4}
	if !submission.Valid() {
		t.Fatal("valid uint64 TON logical time rejected")
	}
	submission.TxLT = "184467440737095516150"
	if submission.Valid() {
		t.Fatal("oversized TON logical time accepted")
	}
}

func TestStarGiftTONRecoveryRequiresPinnedAdmissionAndAudit(t *testing.T) {
	req := StarGiftTONRecoveryRequest{
		ExportID: 7, Network: TONNetworkMainnet, CollectionAddress: "0:collection",
		CollectionCodeHash: make([]byte, 32), MintABI: StarGiftTONMintABIBasicCollectionV1,
		InitialItemIndex: "17", Actor: "operator-a", Reason: "INC-7", RecoveredAt: 100,
	}
	if !req.Valid() {
		t.Fatal("valid TON recovery rejected")
	}
	req.MintABI = "custom-v1"
	if req.Valid() {
		t.Fatal("unsupported TON mint ABI accepted")
	}
}
