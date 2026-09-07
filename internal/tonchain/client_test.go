package tonchain

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/nft"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"telesrv/internal/domain"
)

func TestDeterministicMintBodyUsesStableQueryAndTEP64URI(t *testing.T) {
	owner := address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111")
	export := domain.StarGiftTONExport{
		ID: 7, UniqueGiftID: 9, Network: domain.TONNetworkMainnet,
		CollectionAddress: "0:2222222222222222222222222222222222222222222222222222222222222222",
		MintABI:           domain.StarGiftTONMintABIBasicCollectionV1,
		ItemIndex:         "17", ExpectedItemAddress: "0:3333333333333333333333333333333333333333333333333333333333333333",
		MetadataURI: "https://gifts.example/nft/9.json", MetadataHash: bytes.Repeat([]byte{4}, 32),
	}
	forward := tlb.MustFromTON("0.01")
	first, err := deterministicMintBody(export, big.NewInt(17), owner, forward)
	if err != nil {
		t.Fatalf("deterministicMintBody: %v", err)
	}
	second, err := deterministicMintBody(export, big.NewInt(17), owner, forward)
	if err != nil || !bytes.Equal(first.Hash(), second.Hash()) {
		t.Fatalf("body is not deterministic: err=%v", err)
	}
	if got, want := hex.EncodeToString(first.Hash()), "4125d2ecb9e439316589c2a7cff48f4683b4bdd4e06a558da63ef18190053325"; got != want {
		t.Fatalf("mint body golden hash = %s, want %s", got, want)
	}
	var payload nft.ItemMintPayload
	if err := tlb.LoadFromCell(&payload, first.MustBeginParse()); err != nil {
		t.Fatalf("decode mint payload: %v", err)
	}
	if payload.QueryID != stableMintQueryID(export) || payload.Index.Cmp(big.NewInt(17)) != 0 {
		t.Fatalf("payload query/index = %d/%s", payload.QueryID, payload.Index)
	}
	content := payload.Content.MustBeginParse()
	gotOwner := content.MustLoadAddr()
	uri := content.MustLoadRef().MustLoadStringSnake()
	if !gotOwner.Equals(owner) || uri != export.MetadataURI {
		t.Fatalf("payload owner/uri = %s/%q", gotOwner, uri)
	}

	export.MetadataHash[0]++
	changed, err := deterministicMintBody(export, big.NewInt(17), owner, forward)
	if err != nil || bytes.Equal(first.Hash(), changed.Hash()) {
		t.Fatalf("metadata evidence did not change signed payload: err=%v", err)
	}
}

func TestValidatePreparedMintMessageUsesPersistedBOCIntent(t *testing.T) {
	owner := address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111")
	collection := address.MustParseRawAddr("0:2222222222222222222222222222222222222222222222222222222222222222")
	export := domain.StarGiftTONExport{
		ID: 7, UniqueGiftID: 9, Network: domain.TONNetworkMainnet,
		CollectionAddress: collection.StringRaw(), MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		ItemIndex: "17", ExpectedItemAddress: "0:3333333333333333333333333333333333333333333333333333333333333333",
		MetadataURI: "https://gifts.example/nft/9.json", MetadataHash: bytes.Repeat([]byte{4}, 32),
	}
	mintBody, err := deterministicMintBody(export, big.NewInt(17), owner, tlb.MustFromTON("0.007"))
	if err != nil {
		t.Fatalf("build mint body: %v", err)
	}
	prepared := preparedMintMessageFixture(t, collection, mintBody, false)
	got, err := validatePreparedMintMessage(prepared, export, big.NewInt(17), owner, collection)
	if err != nil || !bytes.Equal(got.Hash(), mintBody.Hash()) {
		t.Fatalf("validate prepared mint = %x, %v", got.Hash(), err)
	}

	drifted := export
	drifted.MetadataHash = append([]byte(nil), export.MetadataHash...)
	drifted.MetadataHash[0]++
	if _, err := validatePreparedMintMessage(prepared, drifted, big.NewInt(17), owner, collection); err == nil {
		t.Fatal("validate prepared mint succeeded after export identity drift")
	}
	withExtraTransfer := preparedMintMessageFixture(t, collection, mintBody, true)
	if _, err := validatePreparedMintMessage(withExtraTransfer, export, big.NewInt(17), owner, collection); err == nil {
		t.Fatal("validate prepared mint accepted more than one wallet transfer")
	}
}

func TestDecodeHashAcceptsHexAndBase64(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, 32)
	for _, encoded := range []string{hex.EncodeToString(want), base64.StdEncoding.EncodeToString(want), base64.RawURLEncoding.EncodeToString(want)} {
		got, err := decodeHash(encoded)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("decodeHash(%q) = %x, %v", encoded, got, err)
		}
	}
}

func TestValidateSubmissionTransactionRequiresSuccessfulExactMint(t *testing.T) {
	collection := address.MustParseRawAddr("0:2222222222222222222222222222222222222222222222222222222222222222")
	mintBody := cell.BeginCell().MustStoreUInt(0x10203040, 32).EndCell()

	t.Run("valid", func(t *testing.T) {
		tx, submissionHash := submissionTransactionFixture(t, collection, mintBody)
		if err := validateSubmissionTransaction(tx, submissionHash, collection, mintBody); err != nil {
			t.Fatalf("validate submission transaction: %v", err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*tlb.Transaction, []byte) []byte
	}{
		{name: "non ordinary", mutate: func(tx *tlb.Transaction, hash []byte) []byte {
			tx.Description = tlb.TransactionDescriptionStorage{}
			return hash
		}},
		{name: "aborted", mutate: func(tx *tlb.Transaction, hash []byte) []byte {
			description := tx.Description.(tlb.TransactionDescriptionOrdinary)
			description.Aborted = true
			tx.Description = description
			return hash
		}},
		{name: "compute failed", mutate: func(tx *tlb.Transaction, hash []byte) []byte {
			description := tx.Description.(tlb.TransactionDescriptionOrdinary)
			description.ComputePhase = tlb.ComputePhase{Phase: tlb.ComputePhaseVM{Success: false}}
			tx.Description = description
			return hash
		}},
		{name: "action failed", mutate: func(tx *tlb.Transaction, hash []byte) []byte {
			description := tx.Description.(tlb.TransactionDescriptionOrdinary)
			description.ActionPhase.Success = false
			description.ActionPhase.ResultCode = 37
			tx.Description = description
			return hash
		}},
		{name: "wrong input hash", mutate: func(_ *tlb.Transaction, hash []byte) []byte {
			return bytes.Repeat([]byte{hash[0] ^ 0xff}, 32)
		}},
		{name: "missing output", mutate: func(tx *tlb.Transaction, hash []byte) []byte {
			tx.IO.Out = nil
			return hash
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, submissionHash := submissionTransactionFixture(t, collection, mintBody)
			submissionHash = test.mutate(tx, submissionHash)
			if err := validateSubmissionTransaction(tx, submissionHash, collection, mintBody); err == nil {
				t.Fatal("validate submission transaction succeeded, want failure")
			}
		})
	}

	t.Run("wrong collection", func(t *testing.T) {
		other := address.MustParseRawAddr("0:3333333333333333333333333333333333333333333333333333333333333333")
		tx, submissionHash := submissionTransactionFixture(t, other, mintBody)
		if err := validateSubmissionTransaction(tx, submissionHash, collection, mintBody); err == nil {
			t.Fatal("validate submission transaction succeeded, want collection mismatch")
		}
	})

	t.Run("wrong mint body", func(t *testing.T) {
		other := cell.BeginCell().MustStoreUInt(0x50607080, 32).EndCell()
		tx, submissionHash := submissionTransactionFixture(t, collection, other)
		if err := validateSubmissionTransaction(tx, submissionHash, collection, mintBody); err == nil {
			t.Fatal("validate submission transaction succeeded, want body mismatch")
		}
	})
}

func TestMintFinalityProgressUsesOneFixedAnchor(t *testing.T) {
	txHash := bytes.Repeat([]byte{0x5a}, 32)
	observation, final := mintFinalityProgress(txHash, "123", 0, 100, 12)
	if final || !observation.ObservationValid() || observation.BlockSeqno != 100 || observation.FinalityDepth != 0 {
		t.Fatalf("first observation = %+v final=%v", observation, final)
	}
	waiting, final := mintFinalityProgress(txHash, "123", observation.BlockSeqno, 111, 12)
	if final || waiting.HasObservation() {
		t.Fatalf("waiting progress = %+v final=%v", waiting, final)
	}
	confirmed, final := mintFinalityProgress(txHash, "123", observation.BlockSeqno, 112, 12)
	if !final || confirmed.BlockSeqno != 100 || confirmed.FinalityDepth != 12 || !confirmed.ObservationValid() {
		t.Fatalf("confirmed progress = %+v final=%v", confirmed, final)
	}
}

func submissionTransactionFixture(t *testing.T, destination *address.Address, body *cell.Cell) (*tlb.Transaction, []byte) {
	t.Helper()
	relayer := address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111")
	external := &tlb.ExternalMessage{
		SrcAddr: address.NewAddressNone(), DstAddr: relayer, ImportFee: tlb.FromNanoTONU(0),
		Body: cell.BeginCell().MustStoreUInt(0xaabbccdd, 32).EndCell(),
	}
	externalCell, err := tlb.ToCell(external)
	if err != nil {
		t.Fatalf("encode external message: %v", err)
	}
	outbound := &tlb.InternalMessage{
		IHRDisabled: true, Bounce: true, SrcAddr: relayer, DstAddr: destination,
		Amount: tlb.MustFromTON("0.1"), IHRFee: tlb.FromNanoTONU(0), FwdFee: tlb.FromNanoTONU(0), Body: body,
	}
	outboundCell, err := tlb.ToCell(outbound)
	if err != nil {
		t.Fatalf("encode outbound message: %v", err)
	}
	dictionary := cell.NewDict(15)
	key := cell.BeginCell().MustStoreUInt(0, 15).EndCell()
	value := cell.BeginCell().MustStoreRef(outboundCell).EndCell()
	if err := dictionary.Set(key, value); err != nil {
		t.Fatalf("store outbound message: %v", err)
	}
	tx := &tlb.Transaction{
		LT: 100,
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseVM{Success: true}},
			ActionPhase:  &tlb.ActionPhase{Success: true, Valid: true, ResultCode: 0, TotalActions: 1, MessagesCreated: 1},
		},
	}
	tx.IO.In = &tlb.Message{MsgType: tlb.MsgTypeExternalIn, Msg: external}
	tx.IO.Out = &tlb.MessagesList{List: dictionary}
	return tx, externalCell.Hash()
}

func preparedMintMessageFixture(t *testing.T, collection *address.Address, body *cell.Cell, extraTransfer bool) *tlb.ExternalMessage {
	t.Helper()
	internal := &tlb.InternalMessage{
		IHRDisabled: true, Bounce: true, DstAddr: collection, Amount: tlb.MustFromTON("0.05"), Body: body,
	}
	internalCell, err := tlb.ToCell(internal)
	if err != nil {
		t.Fatalf("encode prepared internal message: %v", err)
	}
	walletBody := cell.BeginCell().MustStoreSlice(make([]byte, 64), 512).
		MustStoreUInt(wallet.DefaultSubwallet, 32).MustStoreUInt(2_000_000_000, 32).
		MustStoreUInt(11, 32).MustStoreUInt(0, 8).
		MustStoreUInt(uint64(wallet.PayGasSeparately+wallet.IgnoreErrors), 8).MustStoreRef(internalCell)
	if extraTransfer {
		walletBody.MustStoreUInt(uint64(wallet.PayGasSeparately+wallet.IgnoreErrors), 8).MustStoreRef(internalCell)
	}
	return &tlb.ExternalMessage{
		DstAddr: address.MustParseRawAddr("0:4444444444444444444444444444444444444444444444444444444444444444"),
		Body:    walletBody.EndCell(),
	}
}
