package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
)

func TestStarGiftTONAdmissionAndConcurrentProofLanePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	store := NewStarGiftTONStore(pool)
	collectionAddress := "mainnet:lane:" + suffix
	codeHash := sha256.Sum256([]byte("code:" + collectionAddress))

	ownerA, grantedA := grantTONExportTestGift(t, ctx, pool, suffix+"a", now)
	metadataA := []byte(`{"name":"lane-a"}`)
	metadataHashA := sha256.Sum256(metadataA)
	tokenA := sha256.Sum256([]byte("lane-token-a-" + suffix))
	reqA := domain.StarGiftTONExportRequest{
		UserID: ownerA.ID, Ref: domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: ownerA.ID}, MsgID: grantedA.Saved.MsgID},
		TokenDigest: tokenA[:], Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress,
		CollectionCodeHash: codeHash[:], MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		InitialItemIndex: "41", MetadataURI: "https://example.test/lane-a.json",
		MetadataHash: metadataHashA[:], MetadataJSON: metadataA, Date: now + 1, ExpiresAt: now + 301,
	}
	if _, err := store.CreateTONExport(ctx, reqA); !errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable) {
		t.Fatalf("create without admission err=%v, want admission unavailable", err)
	}
	uniqueA, found, err := NewStarGiftStore(pool).UniqueByID(ctx, grantedA.Unique.ID)
	if err != nil || !found || uniqueA.ExternalizationPending {
		t.Fatalf("failed admission mutated gift = %+v found=%v err=%v", uniqueA, found, err)
	}
	if err := store.RegisterTONAdmission(ctx, domain.StarGiftTONAdmissionRequest{
		StarGiftTONChainState: domain.StarGiftTONChainState{
			Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress,
			CollectionCodeHash: codeHash[:], ObservedNextItemIndex: "41",
		},
		InitialItemIndex: "41", MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		WorkerInstanceID: "lane-worker", MintEnabled: false,
		ObservedAt: now, LeaseExpiresAt: now + 3600,
	}); err != nil {
		t.Fatalf("register lane admission: %v", err)
	}
	if _, err := store.CreateTONExport(ctx, reqA); !errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable) {
		t.Fatalf("create with mint-disabled admission err=%v, want admission unavailable", err)
	}
	if err := store.RegisterTONAdmission(ctx, domain.StarGiftTONAdmissionRequest{
		StarGiftTONChainState: domain.StarGiftTONChainState{
			Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress,
			CollectionCodeHash: codeHash[:], ObservedNextItemIndex: "41",
		},
		InitialItemIndex: "41", MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		WorkerInstanceID: "lane-worker", MintEnabled: true,
		ObservedAt: now + 1, LeaseExpiresAt: now + 3601,
	}); err != nil {
		t.Fatalf("enable lane admission: %v", err)
	}
	exportA, err := store.CreateTONExport(ctx, reqA)
	if err != nil || exportA.ItemIndex != "" || exportA.Status != domain.StarGiftTONExportAwaitingWallet {
		t.Fatalf("create lane export A = %+v err=%v", exportA, err)
	}

	ownerB, grantedB := grantTONExportTestGift(t, ctx, pool, suffix+"b", now)
	metadataB := []byte(`{"name":"lane-b"}`)
	metadataHashB := sha256.Sum256(metadataB)
	tokenB := sha256.Sum256([]byte("lane-token-b-" + suffix))
	exportB, err := store.CreateTONExport(ctx, domain.StarGiftTONExportRequest{
		UserID: ownerB.ID, Ref: domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: ownerB.ID}, MsgID: grantedB.Saved.MsgID},
		TokenDigest: tokenB[:], Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress,
		CollectionCodeHash: codeHash[:], MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		InitialItemIndex: "41", MetadataURI: "https://example.test/lane-b.json",
		MetadataHash: metadataHashB[:], MetadataJSON: metadataB, Date: now + 1, ExpiresAt: now + 301,
	})
	if err != nil || exportB.ItemIndex != "" {
		t.Fatalf("create lane export B = %+v err=%v", exportB, err)
	}

	proofs := make([]domain.StarGiftTONProofResult, 2)
	for i, candidate := range []struct {
		export domain.StarGiftTONExport
		owner  domain.User
		token  []byte
	}{
		{export: exportA, owner: ownerA, token: tokenA[:]},
		{export: exportB, owner: ownerB, token: tokenB[:]},
	} {
		challengeDigest := sha256.Sum256([]byte("lane-challenge-" + suffix + string(rune('a'+i))))
		if _, err := store.CreateTONProofChallenge(ctx, domain.StarGiftTONChallengeRequest{
			ChallengeDigest: challengeDigest[:], Purpose: domain.StarGiftTONChallengeExport,
			ExportID: candidate.export.ID, UserID: candidate.owner.ID, AuthContextHash: candidate.token,
			Network: domain.TONNetworkMainnet, ProofDomain: "gifts.example.test", PayloadHash: challengeDigest[:],
			Date: now + 2, ExpiresAt: now + 102,
		}); err != nil {
			t.Fatalf("create lane challenge %d: %v", i, err)
		}
		proofHash := sha256.Sum256([]byte("lane-proof-" + suffix + string(rune('a'+i))))
		stateHash := sha256.Sum256([]byte("lane-state-" + suffix + string(rune('a'+i))))
		publicKey := sha256.Sum256([]byte("lane-key-" + suffix + string(rune('a'+i))))
		proofs[i] = domain.StarGiftTONProofResult{
			ExportID: candidate.export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: "lane-wallet-" + suffix,
			WalletStateInitHash: stateHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:], VerifiedAt: now + 3,
		}
	}

	start := make(chan struct{})
	errs := make([]error, len(proofs))
	var wg sync.WaitGroup
	for i := range proofs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = store.VerifyTONExportProof(ctx, proofs[i])
		}(i)
	}
	close(start)
	wg.Wait()
	succeeded, blocked := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable):
			blocked++
		default:
			t.Fatalf("concurrent proof err=%v", err)
		}
	}
	if succeeded != 1 || blocked != 1 {
		t.Fatalf("concurrent proof outcomes success=%d blocked=%d errors=%v", succeeded, blocked, errs)
	}
	var allocated int
	var nextItemIndex string
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE item_index='41'),
	 (SELECT next_item_index::text FROM star_gift_ton_collection_cursors WHERE network=$1 AND collection_address=$2)
	 FROM star_gift_ton_exports WHERE network=$1 AND collection_address=$2`,
		string(domain.TONNetworkMainnet), collectionAddress).Scan(&allocated, &nextItemIndex); err != nil || allocated != 1 || nextItemIndex != "42" {
		t.Fatalf("lane allocation count=%d cursor=%q err=%v", allocated, nextItemIndex, err)
	}
}

func TestStarGiftTONExportLeaseFencingPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	owner, granted := grantTONExportTestGift(t, ctx, pool, suffix, now)
	tonStore := NewStarGiftTONStore(pool)
	token := sha256.Sum256([]byte("export-token-" + suffix))
	metadataJSON := []byte(`{"name":"` + suffix + `"}`)
	metadata := sha256.Sum256(metadataJSON)
	collectionAddress := "mainnet:collection:" + suffix
	collectionCodeHash := registerTONTestAdmission(t, ctx, tonStore, collectionAddress, "17", "17", now)

	export, err := tonStore.CreateTONExport(ctx, domain.StarGiftTONExportRequest{
		UserID:      owner.ID,
		Ref:         domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, MsgID: granted.Saved.MsgID},
		TokenDigest: token[:], Network: domain.TONNetworkMainnet,
		CollectionAddress: collectionAddress, CollectionCodeHash: collectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "17",
		MetadataURI: "https://example.test/nft/" + suffix + ".json", MetadataHash: metadata[:], MetadataJSON: metadataJSON,
		Date: now + 1, ExpiresAt: now + 301,
	})
	if err != nil || export.Status != domain.StarGiftTONExportAwaitingWallet || export.ItemIndex != "" || export.UniqueGiftID != granted.Unique.ID {
		t.Fatalf("create TON export = %+v err=%v", export, err)
	}
	replay, err := tonStore.CreateTONExport(ctx, domain.StarGiftTONExportRequest{
		UserID:      owner.ID,
		Ref:         domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, MsgID: granted.Saved.MsgID},
		TokenDigest: token[:], Network: domain.TONNetworkMainnet,
		CollectionAddress: export.CollectionAddress, CollectionCodeHash: collectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "17",
		MetadataURI: export.MetadataURI, MetadataHash: metadata[:], MetadataJSON: metadataJSON, Date: now + 1, ExpiresAt: now + 301,
	})
	if err != nil || replay.ID != export.ID {
		t.Fatalf("replay TON export = %+v err=%v", replay, err)
	}
	unique, found, err := NewStarGiftStore(pool).UniqueByID(ctx, granted.Unique.ID)
	if err != nil || !found || !unique.ExternalizationPending {
		t.Fatalf("externalization lock = %+v found=%v err=%v", unique, found, err)
	}
	otherToken := sha256.Sum256([]byte("other-token-" + suffix))
	conflictReq := domain.StarGiftTONExportRequest{
		UserID:      owner.ID,
		Ref:         domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, MsgID: granted.Saved.MsgID},
		TokenDigest: otherToken[:], Network: domain.TONNetworkMainnet,
		CollectionAddress: export.CollectionAddress, CollectionCodeHash: collectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "17",
		MetadataURI: export.MetadataURI, MetadataHash: metadata[:], MetadataJSON: metadataJSON, Date: now + 2, ExpiresAt: now + 302,
	}
	if _, err := tonStore.CreateTONExport(ctx, conflictReq); !errors.Is(err, domain.ErrStarGiftExternalizationPending) {
		t.Fatalf("second export err=%v, want externalization pending", err)
	}

	lifecycle := NewStarGiftLifecycleStore(pool, newTestMessageStore(pool), 0)
	if _, err := lifecycle.SetStarGiftListing(ctx, domain.StarGiftListingRequest{
		ActorUserID: owner.ID, Ref: conflictReq.Ref, Date: now + 2,
		Amount: &domain.StarGiftAmount{Currency: domain.StarGiftCurrencyStars, Amount: 50},
	}); !errors.Is(err, domain.ErrStarGiftResaleUnavailable) {
		t.Fatalf("listing while externalizing err=%v", err)
	}

	proofHash := sha256.Sum256([]byte("proof-" + suffix))
	challengeDigest := sha256.Sum256([]byte("challenge-" + suffix))
	payloadHash := sha256.Sum256([]byte("payload-" + suffix))
	challenge, err := tonStore.CreateTONProofChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: challengeDigest[:], Purpose: domain.StarGiftTONChallengeExport,
		ExportID: export.ID, UserID: owner.ID, AuthContextHash: token[:], Network: domain.TONNetworkMainnet,
		ProofDomain: "gifts.example.test", PayloadHash: payloadHash[:], Date: now + 3, ExpiresAt: now + 103,
	})
	if err != nil || challenge.Status != "pending" || challenge.ExportID != export.ID {
		t.Fatalf("create TON proof challenge = %+v err=%v", challenge, err)
	}
	stateInitHash := sha256.Sum256([]byte("state-init-" + suffix))
	publicKey := sha256.Sum256([]byte("public-key-" + suffix))
	export, err = tonStore.VerifyTONExportProof(ctx, domain.StarGiftTONProofResult{
		ExportID: export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: "mainnet:wallet:" + suffix,
		WalletStateInitHash: stateInitHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:],
		VerifiedAt: now + 4,
	})
	if err != nil || export.Status != domain.StarGiftTONExportProofVerified || export.ItemIndex != "17" || export.ExpectedItemAddress != "" {
		t.Fatalf("verify TON proof = %+v err=%v", export, err)
	}
	chainKinds := []domain.StarGiftTONJobKind{domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobMint, domain.StarGiftTONJobConfirm, domain.StarGiftTONJobReconcileOwner}
	resolveJobs, err := tonStore.ClaimTONJobs(ctx, "worker-a", chainKinds, now+4, 10, 100)
	resolveJob := requireTONTestJob(t, resolveJobs, err, export.ID, domain.StarGiftTONJobResolveItem, "claim item resolution")
	export, err = tonStore.RecordTONItemResolved(ctx, domain.StarGiftTONItemResolution{
		JobID: resolveJob.ID, WorkerID: "worker-a", FencingToken: resolveJob.FencingToken,
		ItemAddress: "mainnet:item:" + suffix, ResolvedAt: now + 4,
	})
	if err != nil || export.Status != domain.StarGiftTONExportMintQueued || export.ExpectedItemAddress == "" {
		t.Fatalf("record item resolution = %+v err=%v", export, err)
	}
	if _, err := tonStore.VerifyTONExportProof(ctx, domain.StarGiftTONProofResult{
		ExportID: export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: export.WalletAddress,
		WalletStateInitHash: stateInitHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:], VerifiedAt: now + 4,
	}); err != nil {
		t.Fatalf("idempotent proof replay: %v", err)
	}
	if _, err := tonStore.CancelTONExport(ctx, export.ID, owner.ID, now+5, "too late"); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("cancel queued export err=%v", err)
	}

	jobsA, err := tonStore.ClaimTONJobs(ctx, "worker-a", chainKinds, now+5, 10, 100)
	jobA := requireTONTestJob(t, jobsA, err, export.ID, domain.StarGiftTONJobMint, "worker-a claim")
	jobsEarly, err := tonStore.ClaimTONJobs(ctx, "worker-b", chainKinds, now+14, 10, 100)
	if err != nil || hasTONTestJob(jobsEarly, export.ID, domain.StarGiftTONJobMint) {
		t.Fatalf("early worker-b claim = %+v err=%v", jobsEarly, err)
	}
	jobsB, err := tonStore.ClaimTONJobs(ctx, "worker-b", chainKinds, now+15, 10, 100)
	jobB := requireTONTestJob(t, jobsB, err, export.ID, domain.StarGiftTONJobMint, "worker-b takeover")
	if jobB.FencingToken <= jobA.FencingToken {
		t.Fatalf("worker-b fencing token=%d, want > %d", jobB.FencingToken, jobA.FencingToken)
	}
	txHash := sha256.Sum256([]byte("mint-tx-" + suffix))
	submissionHash := sha256.Sum256([]byte("submission-" + suffix))
	preparation := domain.StarGiftTONMintPreparation{JobID: jobA.ID, WorkerID: "worker-a",
		FencingToken: jobA.FencingToken, SubmissionHash: submissionHash[:], SubmissionBOC: []byte{1, 2, 3},
		PreparedAt: now + 16, ValidUntil: now + 180}
	if _, err := tonStore.RecordTONMintPrepared(ctx, preparation); !errors.Is(err, domain.ErrStarGiftTONLeaseLost) {
		t.Fatalf("stale worker preparation err=%v", err)
	}
	preparation.WorkerID = "worker-b"
	preparation.FencingToken = jobB.FencingToken
	asset, err := tonStore.RecordTONMintPrepared(ctx, preparation)
	if err != nil || asset.PreparedAt != now+16 || asset.SubmittedAt != 0 {
		t.Fatalf("record mint preparation = %+v err=%v", asset, err)
	}
	submission := domain.StarGiftTONMintSubmission{JobID: jobB.ID, WorkerID: "worker-b",
		FencingToken: jobB.FencingToken, TxHash: txHash[:], TxLT: "100", BlockSeqno: 0, SubmittedAt: now + 17}
	asset, err = tonStore.RecordTONMintSubmitted(ctx, submission)
	if err != nil || asset.ItemAddress != export.ExpectedItemAddress || asset.ConfirmedAt != 0 || asset.MintBlockSeqno != 0 {
		t.Fatalf("record mint submission = %+v err=%v", asset, err)
	}

	confirmJobs, err := tonStore.ClaimTONJobs(ctx, "worker-b", chainKinds, now+18, 10, 100)
	confirmJob := requireTONTestJob(t, confirmJobs, err, export.ID, domain.StarGiftTONJobConfirm, "claim confirm")
	observation := domain.StarGiftTONMintObservation{
		JobID: confirmJob.ID, WorkerID: "worker-b", FencingToken: confirmJob.FencingToken,
		TxHash: txHash[:], TxLT: "100", BlockSeqno: 7, ObservedAt: now + 18,
	}
	asset, err = tonStore.RecordTONMintObserved(ctx, observation)
	if err != nil || asset.MintBlockSeqno != 7 || !bytes.Equal(asset.MintTxHash, txHash[:]) || asset.MintTxLT != "100" {
		t.Fatalf("record mint finality anchor = %+v err=%v", asset, err)
	}
	observation.BlockSeqno = 8
	if _, err := tonStore.RecordTONMintObserved(ctx, observation); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("move mint finality anchor err=%v, want state conflict", err)
	}
	if _, err := tonStore.RecordTONMintConfirmed(ctx, domain.StarGiftTONMintConfirmation{
		JobID: confirmJob.ID, WorkerID: "worker-b", FencingToken: confirmJob.FencingToken,
		OwnerAddress: export.WalletAddress, TxHash: txHash[:], TxLT: "100", BlockSeqno: 8,
		FinalityDepth: 12, ConfirmedAt: now + 19,
	}); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("confirm with moved finality anchor err=%v, want state conflict", err)
	}
	asset, err = tonStore.RecordTONMintConfirmed(ctx, domain.StarGiftTONMintConfirmation{
		JobID: confirmJob.ID, WorkerID: "worker-b", FencingToken: confirmJob.FencingToken,
		OwnerAddress: export.WalletAddress, TxHash: txHash[:], TxLT: "100", BlockSeqno: 7,
		FinalityDepth: 12, ConfirmedAt: now + 19,
	})
	if err != nil || asset.OwnerAddress != export.WalletAddress || asset.FinalityDepth != 12 {
		t.Fatalf("record mint confirmation = %+v err=%v", asset, err)
	}
	export, found, err = tonStore.TONExportByID(ctx, export.ID)
	if err != nil || !found || export.Status != domain.StarGiftTONExportConfirmed {
		t.Fatalf("confirmed export = %+v found=%v err=%v", export, found, err)
	}
	unique, found, err = NewStarGiftStore(pool).UniqueByID(ctx, granted.Unique.ID)
	if err != nil || !found || !unique.ExternalizationPending || unique.OwnerAddress != "" {
		t.Fatalf("pre-finalize gift lock = %+v found=%v err=%v", unique, found, err)
	}

	gifts := NewStarGiftStore(pool)
	ownerPeer := domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}
	if err := gifts.SetPinned(ctx, ownerPeer, []int64{granted.Saved.ID}); err != nil {
		t.Fatalf("pin gift before finalization: %v", err)
	}
	collection, err := gifts.CreateCollection(ctx, ownerPeer, "TON "+suffix, []int64{granted.Saved.ID})
	if err != nil {
		t.Fatalf("create pre-finalize collection: %v", err)
	}

	finalizerStore := NewStarGiftTONFinalizerStore(pool, lifecycle)
	coreA, err := finalizerStore.ClaimTONCoreJobs(ctx, "core-a", now+20, 10, 100)
	coreJobA := requireTONTestJob(t, coreA, err, export.ID, domain.StarGiftTONJobFinalize, "core-a claim")
	coreEarly, err := finalizerStore.ClaimTONCoreJobs(ctx, "core-b", now+29, 10, 100)
	if err != nil || hasTONTestJob(coreEarly, export.ID, domain.StarGiftTONJobFinalize) {
		t.Fatalf("early core-b claim = %+v err=%v", coreEarly, err)
	}
	coreB, err := finalizerStore.ClaimTONCoreJobs(ctx, "core-b", now+30, 10, 100)
	coreJobB := requireTONTestJob(t, coreB, err, export.ID, domain.StarGiftTONJobFinalize, "core-b takeover")
	if coreJobB.FencingToken <= coreJobA.FencingToken {
		t.Fatalf("core-b fencing token=%d, want > %d", coreJobB.FencingToken, coreJobA.FencingToken)
	}
	if _, err := finalizerStore.FinalizeTONExport(ctx, domain.StarGiftTONFinalization{
		JobID: coreJobA.ID, WorkerID: "core-a", FencingToken: coreJobA.FencingToken, FinalizedAt: now + 31,
	}); !errors.Is(err, domain.ErrStarGiftTONLeaseLost) {
		t.Fatalf("stale Core finalization err=%v", err)
	}
	finalized, err := finalizerStore.FinalizeTONExport(ctx, domain.StarGiftTONFinalization{
		JobID: coreJobB.ID, WorkerID: "core-b", FencingToken: coreJobB.FencingToken, FinalizedAt: now + 31,
	})
	if err != nil || finalized.Export.Status != domain.StarGiftTONExportFinalized || finalized.Gift.OwnerAddress != export.WalletAddress ||
		finalized.Gift.GiftAddress != export.ExpectedItemAddress || finalized.Gift.Host != ownerPeer || finalized.Gift.ExternalizationPending {
		t.Fatalf("finalized export = %+v err=%v", finalized, err)
	}
	hosted, err := gifts.ListByOwnerFiltered(ctx, domain.SavedStarGiftFilter{Owner: ownerPeer, Limit: 10})
	if err != nil || hosted.Count != 1 || len(hosted.Gifts) != 1 || hosted.Gifts[0].Owner != ownerPeer ||
		hosted.Gifts[0].PinnedOrder != 1 || hosted.Gifts[0].LifecycleStatus != domain.StarGiftLifecycleExported {
		t.Fatalf("hosted profile page = %+v err=%v", hosted, err)
	}
	excluded, err := gifts.ListByOwnerFiltered(ctx, domain.SavedStarGiftFilter{Owner: ownerPeer, ExcludeHosted: true, Limit: 10})
	if err != nil || excluded.Count != 0 || len(excluded.Gifts) != 0 {
		t.Fatalf("exclude hosted page = %+v err=%v", excluded, err)
	}
	bySlug, found, err := gifts.GetByRef(ctx, domain.SavedStarGiftRef{Owner: ownerPeer, Slug: granted.Unique.Slug})
	if err != nil || !found || bySlug.Owner != ownerPeer || bySlug.PinnedOrder != 1 {
		t.Fatalf("hosted slug projection = %+v found=%v err=%v", bySlug, found, err)
	}
	if _, err := gifts.ResolveSavedIDs(ctx, ownerPeer, []domain.SavedStarGiftRef{{Owner: ownerPeer, MsgID: granted.Saved.MsgID}}); !errors.Is(err, domain.ErrStarGiftNotFound) {
		t.Fatalf("historical message ref after export err=%v, want not found", err)
	}
	assertTONFinalizationMessageEvidence(t, ctx, pool, owner.ID, granted.Saved.MsgID)

	reconcile, err := tonStore.ClaimTONJobs(ctx, "worker-c", chainKinds, now+32, 10, 100)
	reconcileJob := requireTONTestJob(t, reconcile, err, export.ID, domain.StarGiftTONJobReconcileOwner, "claim owner reconciliation")
	newOwnerAddress := "mainnet:wallet:new:" + suffix
	asset, err = tonStore.RecordTONOwnerObserved(ctx, domain.StarGiftTONOwnerObservation{
		JobID: reconcileJob.ID, WorkerID: "worker-c", FencingToken: reconcileJob.FencingToken,
		OwnerAddress: newOwnerAddress, ObservedLT: "101", ObservedAt: now + 33, NextCheckAt: now + 93,
	})
	if err != nil || asset.OwnerAddress != newOwnerAddress {
		t.Fatalf("observe transferred owner = %+v err=%v", asset, err)
	}
	syncJobs, err := finalizerStore.ClaimTONCoreJobs(ctx, "core-c", now+34, 10, 100)
	syncJob := requireTONTestJob(t, syncJobs, err, export.ID, domain.StarGiftTONJobSyncOwner, "claim owner sync")
	synced, err := finalizerStore.SyncTONOwner(ctx, domain.StarGiftTONFinalization{
		JobID: syncJob.ID, WorkerID: "core-c", FencingToken: syncJob.FencingToken, FinalizedAt: now + 35,
	})
	if err != nil || synced.Gift.OwnerAddress != newOwnerAddress || synced.Gift.Host.ID != 0 || synced.PreviousHost != ownerPeer {
		t.Fatalf("sync transferred owner = %+v err=%v", synced, err)
	}
	collections, err := gifts.ListCollections(ctx, ownerPeer)
	if err != nil || len(collections) != 1 || collections[0].CollectionID != collection.CollectionID || len(collections[0].GiftIDs) != 0 {
		t.Fatalf("collection after owner sync = %+v err=%v", collections, err)
	}
	hosted, err = gifts.ListByOwnerFiltered(ctx, domain.SavedStarGiftFilter{Owner: ownerPeer, Limit: 10})
	if err != nil || hosted.Count != 0 {
		t.Fatalf("old hosted profile after owner sync = %+v err=%v", hosted, err)
	}

	claimant := createTestUser(t, ctx, NewUserStore(pool), "+1779"+suffix+"72", "TONClaimant", "")
	claimDigest := sha256.Sum256([]byte("claim-challenge-" + suffix))
	claimAuth := sha256.Sum256([]byte("claim-auth-" + suffix))
	claimPayload := sha256.Sum256([]byte("claim-payload-" + suffix))
	claimChallenge, err := finalizerStore.CreateTONClaimChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: claimDigest[:], Purpose: domain.StarGiftTONChallengeClaim, ExportID: export.ID,
		UserID: claimant.ID, AuthContextHash: claimAuth[:], Network: domain.TONNetworkMainnet,
		ProofDomain: "gifts.example.test", PayloadHash: claimPayload[:], Date: now + 36, ExpiresAt: now + 136,
	})
	if err != nil || claimChallenge.ExportID != export.ID {
		t.Fatalf("create claim challenge = %+v err=%v", claimChallenge, err)
	}
	claimCommit := domain.StarGiftTONClaimCommit{
		ChallengeDigest: claimDigest[:], UserID: claimant.ID, AuthContextHash: claimAuth[:], WalletAddress: newOwnerAddress,
		StateInitHash: stateInitHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:], ClaimedAt: now + 37,
	}
	if _, err := finalizerStore.CommitTONClaim(ctx, claimCommit); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("claim before fresh observation err=%v", err)
	}
	if err := finalizerStore.ScheduleTONOwnerCheck(ctx, export.ID, now+36); err != nil {
		t.Fatalf("schedule fresh owner check: %v", err)
	}
	reconcile, err = tonStore.ClaimTONJobs(ctx, "worker-d", chainKinds, now+36, 10, 100)
	reconcileJob = requireTONTestJob(t, reconcile, err, export.ID, domain.StarGiftTONJobReconcileOwner, "claim boundary owner reconciliation")
	asset, err = tonStore.RecordTONOwnerObserved(ctx, domain.StarGiftTONOwnerObservation{
		JobID: reconcileJob.ID, WorkerID: "worker-d", FencingToken: reconcileJob.FencingToken,
		OwnerAddress: newOwnerAddress, ObservedLT: "102", ObservedAt: now + 36, NextCheckAt: now + 96,
	})
	if err != nil || asset.OwnerObservedAt != now+36 {
		t.Fatalf("record challenge-boundary owner = %+v err=%v", asset, err)
	}
	var ownerCheckAt int
	if err := pool.QueryRow(ctx, `SELECT available_at FROM star_gift_ton_jobs
	 WHERE export_id=$1 AND kind='reconcile_owner'`, export.ID).Scan(&ownerCheckAt); err != nil || ownerCheckAt != now+37 {
		t.Fatalf("owner check after non-fresh observation available_at=%d err=%v, want %d", ownerCheckAt, err, now+37)
	}
	reconcile, err = tonStore.ClaimTONJobs(ctx, "worker-e", chainKinds, now+37, 10, 100)
	reconcileJob = requireTONTestJob(t, reconcile, err, export.ID, domain.StarGiftTONJobReconcileOwner, "claim fresh owner reconciliation")
	asset, err = tonStore.RecordTONOwnerObserved(ctx, domain.StarGiftTONOwnerObservation{
		JobID: reconcileJob.ID, WorkerID: "worker-e", FencingToken: reconcileJob.FencingToken,
		OwnerAddress: newOwnerAddress, ObservedLT: "103", ObservedAt: now + 38, NextCheckAt: now + 98,
	})
	if err != nil || asset.OwnerObservedAt != now+38 {
		t.Fatalf("record fresh owner = %+v err=%v", asset, err)
	}
	if err := pool.QueryRow(ctx, `SELECT available_at FROM star_gift_ton_jobs
	 WHERE export_id=$1 AND kind='reconcile_owner'`, export.ID).Scan(&ownerCheckAt); err != nil || ownerCheckAt != now+98 {
		t.Fatalf("owner check after fresh observation available_at=%d err=%v, want %d", ownerCheckAt, err, now+98)
	}
	claimCommit.ClaimedAt = now + 39
	claimed, err := finalizerStore.CommitTONClaim(ctx, claimCommit)
	claimantPeer := domain.Peer{Type: domain.PeerTypeUser, ID: claimant.ID}
	if err != nil || claimed.Gift.Host != claimantPeer || claimed.Gift.OwnerAddress != newOwnerAddress {
		t.Fatalf("commit TON claim = %+v err=%v", claimed, err)
	}
	claimPage, err := gifts.ListByOwnerFiltered(ctx, domain.SavedStarGiftFilter{Owner: claimantPeer, Limit: 10})
	if err != nil || claimPage.Count != 1 || len(claimPage.Gifts) != 1 || claimPage.Gifts[0].Owner != claimantPeer {
		t.Fatalf("claimant hosted profile = %+v err=%v", claimPage, err)
	}
	loadedClaim, found, err := tonStore.TONProofChallengeByDigest(ctx, claimDigest[:])
	if err != nil || !found || loadedClaim.Status != "consumed" || loadedClaim.ConsumedAt != now+39 {
		t.Fatalf("consumed claim = %+v found=%v err=%v", loadedClaim, found, err)
	}
}

func TestStarGiftTONPreProofExpiryDoesNotConsumeIndexAndPostProofFailureQuarantinesPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	owner, granted := grantTONExportTestGift(t, ctx, pool, suffix, now)
	store := NewStarGiftTONStore(pool)
	token := sha256.Sum256([]byte("failure-token-" + suffix))
	metadataJSON := []byte(`{"name":"failure-` + suffix + `"}`)
	metadataHash := sha256.Sum256(metadataJSON)
	collectionAddress := "mainnet:failure:" + suffix
	collectionCodeHash := registerTONTestAdmission(t, ctx, store, collectionAddress, "23", "23", now)
	req := domain.StarGiftTONExportRequest{
		UserID: owner.ID, Ref: domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, MsgID: granted.Saved.MsgID},
		TokenDigest: token[:], Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress, CollectionCodeHash: collectionCodeHash,
		MintABI:          domain.StarGiftTONMintABIBasicCollectionV1,
		InitialItemIndex: "23", MetadataURI: "https://example.test/nft/failure-" + suffix + ".json",
		MetadataHash: metadataHash[:], MetadataJSON: metadataJSON, Date: now + 1, ExpiresAt: now + 301,
	}
	export, err := store.CreateTONExport(ctx, req)
	if err != nil || export.Status != domain.StarGiftTONExportAwaitingWallet || export.ItemIndex != "" {
		t.Fatalf("create pre-proof export = %+v err=%v", export, err)
	}
	kinds := []domain.StarGiftTONJobKind{domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobMint, domain.StarGiftTONJobConfirm}
	jobs, err := store.ClaimTONJobs(ctx, "failure-worker", kinds, now+2, 30, 100)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("pre-proof jobs = %+v err=%v, want none", jobs, err)
	}
	var cursorExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM star_gift_ton_collection_cursors
	 WHERE network=$1 AND collection_address=$2)`, string(req.Network), req.CollectionAddress).Scan(&cursorExists); err != nil || cursorExists {
		t.Fatalf("pre-proof cursor exists=%v err=%v", cursorExists, err)
	}
	if _, err := store.CancelTONExport(ctx, export.ID, owner.ID, now+302, "wallet proof expired"); err != nil {
		t.Fatalf("expire pre-proof export: %v", err)
	}
	assertTONExportFailureLock(t, ctx, pool, store, export.ID, granted.Unique.ID, domain.StarGiftTONExportFailed, false)
	req.Date, req.ExpiresAt = now+303, now+603
	if _, err := store.CreateTONExport(ctx, req); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("rearm with consumed capability err=%v, want state conflict", err)
	}

	retryToken := sha256.Sum256([]byte("failure-retry-token-" + suffix))
	req.TokenDigest = retryToken[:]
	rearmed, err := store.CreateTONExport(ctx, req)
	if err != nil || rearmed.ID != export.ID || rearmed.ItemIndex != "" || rearmed.Status != domain.StarGiftTONExportAwaitingWallet {
		t.Fatalf("rearm pre-proof export = %+v err=%v", rearmed, err)
	}
	if _, found, err := store.TONExportByTokenDigest(ctx, token[:]); err != nil || found {
		t.Fatalf("expired capability after rearm found=%v err=%v", found, err)
	}
	challengeDigest := sha256.Sum256([]byte("failure-challenge-" + suffix))
	proofHash := sha256.Sum256([]byte("failure-proof-" + suffix))
	stateInitHash := sha256.Sum256([]byte("failure-state-init-" + suffix))
	publicKey := sha256.Sum256([]byte("failure-public-key-" + suffix))
	if _, err := store.CreateTONProofChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: challengeDigest[:], Purpose: domain.StarGiftTONChallengeExport, ExportID: export.ID,
		UserID: owner.ID, AuthContextHash: req.TokenDigest, Network: domain.TONNetworkMainnet, ProofDomain: "gifts.example.test",
		PayloadHash: challengeDigest[:], Date: now + 304, ExpiresAt: now + 404,
	}); err != nil {
		t.Fatalf("create export challenge: %v", err)
	}
	rearmed, err = store.VerifyTONExportProof(ctx, domain.StarGiftTONProofResult{
		ExportID: export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: "mainnet:failure-wallet:" + suffix,
		WalletStateInitHash: stateInitHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:], VerifiedAt: now + 305,
	})
	if err != nil || rearmed.Status != domain.StarGiftTONExportProofVerified || rearmed.ItemIndex != "23" {
		t.Fatalf("verify and reserve export index = %+v err=%v", rearmed, err)
	}
	var nextItemIndex string
	if err := pool.QueryRow(ctx, `SELECT next_item_index::text FROM star_gift_ton_collection_cursors
	 WHERE network=$1 AND collection_address=$2`, string(req.Network), req.CollectionAddress).Scan(&nextItemIndex); err != nil || nextItemIndex != "24" {
		t.Fatalf("reserved cursor=%q err=%v, want 24", nextItemIndex, err)
	}
	jobs, err = store.ClaimTONJobs(ctx, "failure-worker", kinds, now+306, 30, 100)
	job := requireTONTestJob(t, jobs, err, export.ID, domain.StarGiftTONJobResolveItem, "claim failing post-proof resolve")
	if err := store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: job.ID, WorkerID: "failure-worker", FencingToken: job.FencingToken,
		Error: "permanent resolver failure", FailedAt: now + 307, RetryAt: now + 308, Terminal: true,
	}); err != nil {
		t.Fatalf("fail post-proof resolver: %v", err)
	}
	assertTONExportFailureLock(t, ctx, pool, store, export.ID, granted.Unique.ID, domain.StarGiftTONExportQuarantined, true)
	wrongCodeHash := append([]byte(nil), collectionCodeHash...)
	wrongCodeHash[0] ^= 0xff
	if _, err := store.ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: export.ID, Network: req.Network, CollectionAddress: req.CollectionAddress,
		CollectionCodeHash: wrongCodeHash, MintABI: req.MintABI, InitialItemIndex: req.InitialItemIndex,
		Actor: "operator-a", Reason: "INC-41 corrected resolver", RecoveredAt: now + 309,
	}); !errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable) {
		t.Fatalf("resume with mismatched admission err=%v, want admission unavailable", err)
	}
	resumed, err := store.ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: export.ID, Network: req.Network, CollectionAddress: req.CollectionAddress,
		CollectionCodeHash: collectionCodeHash, MintABI: req.MintABI, InitialItemIndex: req.InitialItemIndex,
		Actor: "operator-a", Reason: "INC-41 corrected resolver", RecoveredAt: now + 309,
	})
	if err != nil || resumed.RecoveryID <= 0 || resumed.Export.Status != domain.StarGiftTONExportProofVerified ||
		resumed.Export.ItemIndex != "23" || resumed.Job.Kind != domain.StarGiftTONJobResolveItem || resumed.Job.Status != domain.StarGiftTONJobPending {
		t.Fatalf("resume quarantined resolver = %+v err=%v", resumed, err)
	}
	if _, err := store.ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: export.ID, Network: req.Network, CollectionAddress: req.CollectionAddress,
		CollectionCodeHash: collectionCodeHash, MintABI: req.MintABI, InitialItemIndex: req.InitialItemIndex,
		Actor: "operator-a", Reason: "duplicate command", RecoveredAt: now + 310,
	}); !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		t.Fatalf("duplicate resolver recovery err=%v, want state conflict", err)
	}
	jobs, err = store.ClaimTONJobs(ctx, "failure-worker", kinds, now+310, 30, 100)
	job = requireTONTestJob(t, jobs, err, export.ID, domain.StarGiftTONJobResolveItem, "claim recovered resolver")
	rearmed, err = store.RecordTONItemResolved(ctx, domain.StarGiftTONItemResolution{
		JobID: job.ID, WorkerID: "failure-worker", FencingToken: job.FencingToken,
		ItemAddress: "mainnet:failure-item:" + suffix, ResolvedAt: now + 311,
	})
	if err != nil || rearmed.Status != domain.StarGiftTONExportMintQueued || rearmed.ItemIndex != "23" {
		t.Fatalf("complete recovered resolver = %+v err=%v", rearmed, err)
	}
	jobs, err = store.ClaimTONJobs(ctx, "failure-worker", kinds, now+312, 30, 100)
	job = requireTONTestJob(t, jobs, err, export.ID, domain.StarGiftTONJobMint, "claim unprepared mint")
	if err := store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: job.ID, WorkerID: "failure-worker", FencingToken: job.FencingToken,
		Error: "permanent mint builder failure", FailedAt: now + 313, RetryAt: now + 314, Terminal: true,
	}); err != nil {
		t.Fatalf("quarantine unprepared mint: %v", err)
	}
	resumed, err = store.ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: export.ID, Network: req.Network, CollectionAddress: req.CollectionAddress,
		CollectionCodeHash: collectionCodeHash, MintABI: req.MintABI, InitialItemIndex: req.InitialItemIndex,
		Actor: "operator-b", Reason: "INC-42 corrected mint builder", RecoveredAt: now + 314,
	})
	if err != nil || resumed.Export.Status != domain.StarGiftTONExportMintQueued ||
		resumed.Export.ItemIndex != "23" || resumed.Export.ExpectedItemAddress != "mainnet:failure-item:"+suffix ||
		resumed.Job.Kind != domain.StarGiftTONJobMint || resumed.Job.Status != domain.StarGiftTONJobPending {
		t.Fatalf("resume unprepared mint = %+v err=%v", resumed, err)
	}
	var recoveryCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM star_gift_ton_recoveries WHERE export_id=$1`, export.ID).Scan(&recoveryCount); err != nil || recoveryCount != 2 {
		t.Fatalf("recovery audit count=%d err=%v, want 2", recoveryCount, err)
	}

	secondRetryToken := sha256.Sum256([]byte("failure-second-retry-token-" + suffix))
	req.TokenDigest = secondRetryToken[:]
	req.Date, req.ExpiresAt = now+315, now+615
	if _, err := store.CreateTONExport(ctx, req); !errors.Is(err, domain.ErrStarGiftExternalizationPending) {
		t.Fatalf("rearm allocated quarantined export err=%v, want externalization pending", err)
	}
}

func TestStarGiftTONPreparedFailureQuarantinesAndHandsOffConfirmPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	owner, granted := grantTONExportTestGift(t, ctx, pool, suffix, now)
	store := NewStarGiftTONStore(pool)
	token := sha256.Sum256([]byte("prepared-token-" + suffix))
	metadataJSON := []byte(`{"name":"prepared-` + suffix + `"}`)
	metadataHash := sha256.Sum256(metadataJSON)
	collectionAddress := "mainnet:prepared:" + suffix
	collectionCodeHash := registerTONTestAdmission(t, ctx, store, collectionAddress, "31", "31", now)
	export, err := store.CreateTONExport(ctx, domain.StarGiftTONExportRequest{
		UserID: owner.ID, Ref: domain.SavedStarGiftRef{Owner: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, MsgID: granted.Saved.MsgID},
		TokenDigest: token[:], Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress, CollectionCodeHash: collectionCodeHash,
		MintABI:          domain.StarGiftTONMintABIBasicCollectionV1,
		InitialItemIndex: "31", MetadataURI: "https://example.test/nft/prepared-" + suffix + ".json",
		MetadataHash: metadataHash[:], MetadataJSON: metadataJSON, Date: now + 1, ExpiresAt: now + 301,
	})
	if err != nil {
		t.Fatalf("create prepared export: %v", err)
	}
	challengeDigest := sha256.Sum256([]byte("prepared-challenge-" + suffix))
	proofHash := sha256.Sum256([]byte("prepared-proof-" + suffix))
	stateInitHash := sha256.Sum256([]byte("prepared-state-init-" + suffix))
	publicKey := sha256.Sum256([]byte("prepared-public-key-" + suffix))
	if _, err := store.CreateTONProofChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: challengeDigest[:], Purpose: domain.StarGiftTONChallengeExport, ExportID: export.ID,
		UserID: owner.ID, AuthContextHash: token[:], Network: domain.TONNetworkMainnet, ProofDomain: "gifts.example.test",
		PayloadHash: challengeDigest[:], Date: now + 4, ExpiresAt: now + 104,
	}); err != nil {
		t.Fatalf("create prepared challenge: %v", err)
	}
	export, err = store.VerifyTONExportProof(ctx, domain.StarGiftTONProofResult{
		ExportID: export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: "mainnet:prepared-wallet:" + suffix,
		WalletStateInitHash: stateInitHash[:], WalletPublicKey: publicKey[:], ProofHash: proofHash[:], VerifiedAt: now + 5,
	})
	if err != nil {
		t.Fatalf("verify prepared proof: %v", err)
	}
	kinds := []domain.StarGiftTONJobKind{domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobMint, domain.StarGiftTONJobConfirm}
	jobs, err := store.ClaimTONJobs(ctx, "prepared-worker", kinds, now+6, 30, 100)
	job := requireTONTestJob(t, jobs, err, export.ID, domain.StarGiftTONJobResolveItem, "claim prepared resolve")
	export, err = store.RecordTONItemResolved(ctx, domain.StarGiftTONItemResolution{
		JobID: job.ID, WorkerID: "prepared-worker", FencingToken: job.FencingToken,
		ItemAddress: "mainnet:prepared-item:" + suffix, ResolvedAt: now + 6,
	})
	if err != nil || export.Status != domain.StarGiftTONExportMintQueued {
		t.Fatalf("resolve prepared item = %+v err=%v", export, err)
	}
	jobs, err = store.ClaimTONJobs(ctx, "prepared-worker", kinds, now+7, 30, 100)
	job = requireTONTestJob(t, jobs, err, export.ID, domain.StarGiftTONJobMint, "claim prepared mint")
	submissionHash := sha256.Sum256([]byte("prepared-boc-" + suffix))
	asset, err := store.RecordTONMintPrepared(ctx, domain.StarGiftTONMintPreparation{
		JobID: job.ID, WorkerID: "prepared-worker", FencingToken: job.FencingToken,
		SubmissionHash: submissionHash[:], SubmissionBOC: []byte{9, 8, 7}, PreparedAt: now + 8, ValidUntil: now + 180,
	})
	if err != nil {
		t.Fatalf("record prepared BOC: %v", err)
	}
	if err := store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: job.ID, WorkerID: "prepared-worker", FencingToken: job.FencingToken,
		Error: "submission outcome uncertain", FailedAt: now + 9, RetryAt: now + 10, Terminal: true,
	}); err != nil {
		t.Fatalf("quarantine prepared mint: %v", err)
	}
	assertTONExportFailureLock(t, ctx, pool, store, export.ID, granted.Unique.ID, domain.StarGiftTONExportQuarantined, true)
	export, _, err = store.TONExportByID(ctx, export.ID)
	if err != nil || export.SubmittedAt != 0 {
		t.Fatalf("quarantined submission boundary = %+v err=%v", export, err)
	}
	confirmJobs, err := store.ClaimTONJobs(ctx, "confirm-worker", kinds, now+10, 30, 100)
	confirmJob := requireTONTestJob(t, confirmJobs, err, export.ID, domain.StarGiftTONJobConfirm, "prepared confirm handoff")
	if confirmJob.Asset == nil || string(confirmJob.Asset.SubmissionBOC) != string(asset.SubmissionBOC) {
		t.Fatalf("prepared confirm asset = %+v", confirmJob.Asset)
	}
	if _, err := pool.Exec(ctx, `UPDATE star_gift_ton_jobs SET attempts=max_attempts WHERE id=$1`, confirmJob.ID); err != nil {
		t.Fatalf("exhaust confirm attempts: %v", err)
	}
	if err := store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: confirmJob.ID, WorkerID: "confirm-worker", FencingToken: confirmJob.FencingToken,
		Error: "temporary lite endpoint outage", FailedAt: now + 11, RetryAt: now + 12,
	}); err != nil {
		t.Fatalf("continue quarantined confirmation: %v", err)
	}
	var status string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT status,attempts FROM star_gift_ton_jobs WHERE id=$1`, confirmJob.ID).Scan(&status, &attempts); err != nil {
		t.Fatalf("load continued confirm job: %v", err)
	}
	if status != "pending" || attempts != 0 {
		t.Fatalf("continued confirm job status=%q attempts=%d", status, attempts)
	}
	confirmJobs, err = store.ClaimTONJobs(ctx, "confirm-worker", kinds, now+12, 30, 100)
	confirmJob = requireTONTestJob(t, confirmJobs, err, export.ID, domain.StarGiftTONJobConfirm, "claim confirm for operator recovery")
	if err := store.FailTONJob(ctx, domain.StarGiftTONJobFailure{
		JobID: confirmJob.ID, WorkerID: "confirm-worker", FencingToken: confirmJob.FencingToken,
		Error: "confirmed manual endpoint repair required", FailedAt: now + 13, RetryAt: now + 14, Terminal: true,
	}); err != nil {
		t.Fatalf("terminal confirm quarantine: %v", err)
	}
	resumed, err := store.ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: export.ID, Network: export.Network, CollectionAddress: export.CollectionAddress,
		CollectionCodeHash: collectionCodeHash, MintABI: export.MintABI, InitialItemIndex: "31",
		Actor: "operator-c", Reason: "INC-43 repaired confirmation endpoint", RecoveredAt: now + 14,
	})
	if err != nil || resumed.Export.Status != domain.StarGiftTONExportQuarantined ||
		resumed.Job.Kind != domain.StarGiftTONJobConfirm || resumed.Job.Status != domain.StarGiftTONJobPending ||
		resumed.Job.Asset == nil || !bytes.Equal(resumed.Job.Asset.SubmissionBOC, asset.SubmissionBOC) {
		t.Fatalf("resume quarantined confirmation = %+v err=%v", resumed, err)
	}
}

func requireTONTestJob(t *testing.T, jobs []domain.StarGiftTONJob, err error, exportID int64,
	kind domain.StarGiftTONJobKind, operation string,
) domain.StarGiftTONJob {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
	for _, job := range jobs {
		if job.ExportID == exportID && job.Kind == kind {
			return job
		}
	}
	t.Fatalf("%s: export=%d kind=%s jobs=%+v", operation, exportID, kind, jobs)
	return domain.StarGiftTONJob{}
}

func hasTONTestJob(jobs []domain.StarGiftTONJob, exportID int64, kind domain.StarGiftTONJobKind) bool {
	for _, job := range jobs {
		if job.ExportID == exportID && job.Kind == kind {
			return true
		}
	}
	return false
}

func registerTONTestAdmission(t *testing.T, ctx context.Context, store *StarGiftTONStore, collectionAddress,
	initialItemIndex, observedNextItemIndex string, now int,
) []byte {
	t.Helper()
	codeHash := sha256.Sum256([]byte("code:" + collectionAddress))
	err := store.RegisterTONAdmission(ctx, domain.StarGiftTONAdmissionRequest{
		StarGiftTONChainState: domain.StarGiftTONChainState{
			Network: domain.TONNetworkMainnet, CollectionAddress: collectionAddress,
			CollectionCodeHash: codeHash[:], ObservedNextItemIndex: observedNextItemIndex,
		},
		InitialItemIndex: initialItemIndex, MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		WorkerInstanceID: "ton-integration-" + collectionAddress,
		MintEnabled:      true, ObservedAt: now, LeaseExpiresAt: now + 3600,
	})
	if err != nil {
		t.Fatalf("register TON test admission: %v", err)
	}
	return codeHash[:]
}

func assertTONExportFailureLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, store *StarGiftTONStore,
	exportID, uniqueGiftID int64, wantStatus domain.StarGiftTONExportStatus, wantLocked bool,
) {
	t.Helper()
	export, found, err := store.TONExportByID(ctx, exportID)
	if err != nil || !found || export.Status != wantStatus {
		t.Fatalf("export failure state = %+v found=%v err=%v want=%s", export, found, err, wantStatus)
	}
	unique, found, err := NewStarGiftStore(pool).UniqueByID(ctx, uniqueGiftID)
	if err != nil || !found || unique.ExternalizationPending != wantLocked {
		t.Fatalf("export failure lock = %+v found=%v err=%v want=%v", unique, found, err, wantLocked)
	}
}

func assertTONFinalizationMessageEvidence(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ownerUserID int64, ownerMessageID int) {
	t.Helper()
	var senderID, privateMessageID int64
	if err := pool.QueryRow(ctx, `SELECT message_sender_id,private_message_id FROM message_boxes
WHERE owner_user_id=$1 AND box_id=$2 AND NOT deleted`, ownerUserID, ownerMessageID).Scan(&senderID, &privateMessageID); err != nil {
		t.Fatalf("load finalized gift message root: %v", err)
	}
	rows, err := pool.Query(ctx, `SELECT owner_user_id,box_id,pts FROM message_boxes
WHERE message_sender_id=$1 AND private_message_id=$2 AND NOT deleted`, senderID, privateMessageID)
	if err != nil {
		t.Fatalf("list finalized gift message boxes: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var userID int64
		var boxID, pts int
		if err := rows.Scan(&userID, &boxID, &pts); err != nil {
			t.Fatalf("scan finalized gift message box: %v", err)
		}
		var eventCount, outboxCount int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_update_events
WHERE user_id=$1 AND pts=$2 AND pts_count=1 AND event_type='edit_message' AND message_box_id=$3`, userID, pts, boxID).Scan(&eventCount); err != nil {
			t.Fatalf("load finalized gift update event: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dispatch_outbox
WHERE target_user_id=$1 AND pts=$2 AND event_type='edit_message'`, userID, pts).Scan(&outboxCount); err != nil {
			t.Fatalf("load finalized gift dispatch outbox: %v", err)
		}
		if eventCount != 1 || outboxCount != 1 {
			t.Fatalf("finalized gift event/outbox for %d/%d pts=%d: %d/%d", userID, boxID, pts, eventCount, outboxCount)
		}
		count++
	}
	if err := rows.Err(); err != nil || count == 0 {
		t.Fatalf("iterate finalized gift messages: count=%d err=%v", count, err)
	}
}

func grantTONExportTestGift(t *testing.T, ctx context.Context, pool *pgxpool.Pool, suffix string, now int) (domain.User, domain.AdminStarGiftGrantResult) {
	t.Helper()
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1779"+suffix+"71", "TONGiftOwner", "")
	gifts := NewStarGiftStore(pool)
	base := time.Now().UnixNano() & 0x7ffffffffffff000
	entry, err := gifts.CreateCatalogRevision(ctx, domain.StarGiftCatalogWrite{
		Title: "TON Export " + suffix, Stars: 50, ConvertStars: 25, Enabled: true,
		Document: collectibleTestDocument(base, "ton-export.tgs"), Blob: collectibleTestBlob(base, "ton-export"),
		Animation: collectibleTestAnimation("ton-export.tgs"), Actor: "integration", CommandID: "ton-export-catalog-" + suffix,
	})
	if err != nil {
		t.Fatalf("create TON export catalog: %v", err)
	}
	revision, err := gifts.PublishCollectibleRevision(ctx, domain.StarGiftCollectibleWrite{
		GiftID: entry.Gift.ID, UpgradeStars: 100, SupplyTotal: 2, SlugPrefix: "ton-" + suffix,
		Models: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectibleModel, Name: "One", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500,
				Document: collectibleTestDocumentPtr(base+1, "ton-model-one.tgs"), Blob: collectibleTestBlobPtr(base+1, "ton-model-one"), Animation: collectibleTestAnimationPtr("ton-model-one.tgs")},
			{Kind: domain.StarGiftCollectibleModel, Name: "Two", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500,
				Document: collectibleTestDocumentPtr(base+2, "ton-model-two.tgs"), Blob: collectibleTestBlobPtr(base+2, "ton-model-two"), Animation: collectibleTestAnimationPtr("ton-model-two.tgs")},
		},
		Patterns: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectiblePattern, Name: "One", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500,
				Document: collectibleTestPatternDocumentPtr(base+3, "ton-pattern-one.tgs"), Blob: collectibleTestBlobPtr(base+3, "ton-pattern-one"), Animation: collectibleTestAnimationPtr("ton-pattern-one.tgs")},
			{Kind: domain.StarGiftCollectiblePattern, Name: "Two", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500,
				Document: collectibleTestPatternDocumentPtr(base+4, "ton-pattern-two.tgs"), Blob: collectibleTestBlobPtr(base+4, "ton-pattern-two"), Animation: collectibleTestAnimationPtr("ton-pattern-two.tgs")},
		},
		Backdrops: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectibleBackdrop, Name: "One", BackdropID: 1, CenterColor: 1, EdgeColor: 2, PatternColor: 3, TextColor: 4, RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500},
			{Kind: domain.StarGiftCollectibleBackdrop, Name: "Two", BackdropID: 2, CenterColor: 5, EdgeColor: 6, PatternColor: 7, TextColor: 8, RarityKind: domain.StarGiftRarityPermille, RarityPermille: 500},
		},
		Actor: "integration", CommandID: "ton-export-pool-" + suffix,
	})
	if err != nil {
		t.Fatalf("publish TON export collectible: %v", err)
	}
	granted, err := NewStarGiftUpgradeStore(pool, newTestMessageStore(pool)).GrantUniqueStarGift(ctx, domain.AdminStarGiftGrant{
		SenderID: domain.OfficialSystemUserID, Recipient: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		GiftID: entry.Gift.ID, Upgrade: true, CommandKey: "ton-export-grant-" + suffix, Date: now,
		ModelAttributeID: revision.Models[0].ID, PatternAttributeID: revision.Patterns[0].ID, BackdropAttributeID: revision.Backdrops[0].ID,
	})
	if err != nil {
		t.Fatalf("grant TON export gift: %v", err)
	}
	return owner, granted
}
