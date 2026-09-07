// Package tonchain contains the isolated TON chain adapter used by the durable
// Star Gift worker.
package tonchain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/nft"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"telesrv/internal/domain"
	"telesrv/internal/tonaddr"
	"telesrv/internal/tonworker"
)

type LiteServer struct {
	Address   string
	PublicKey string
}

type Config struct {
	Network             domain.TONNetwork
	LiteServers         []LiteServer
	TrustedBlock        TrustedBlock
	CollectionAddress   string
	CollectionCodeHash  string
	MintABI             string
	RelayerMnemonicFile string
	RelayerMinBalance   string
	RelayerMessageTTL   time.Duration
	MintSendAmount      string
	MintForwardAmount   string
	FinalityDepth       int
}

type TrustedBlock struct {
	Workchain int32
	Shard     int64
	Seqno     uint32
	RootHash  string
	FileHash  string
}

type Client struct {
	pool               *liteclient.ConnectionPool
	api                *ton.APIClient
	collection         *nft.CollectionClient
	collectionAddress  *address.Address
	collectionCodeHash []byte
	mintABI            string
	relayer            *wallet.Wallet
	minBalance         tlb.Coins
	sendAmount         tlb.Coins
	forwardAmount      tlb.Coins
	messageTTL         time.Duration
	finalityDepth      int
	network            domain.TONNetwork
	mintMu             sync.Mutex
}

var _ tonworker.Chain = (*Client)(nil)

func New(ctx context.Context, cfg Config) (*Client, error) {
	if !cfg.Network.Valid() || len(cfg.LiteServers) == 0 || cfg.FinalityDepth < 1 ||
		cfg.RelayerMessageTTL < time.Minute || cfg.RelayerMessageTTL > time.Hour ||
		!domain.ValidStarGiftTONMintABI(strings.TrimSpace(cfg.MintABI)) {
		return nil, fmt.Errorf("invalid TON chain config")
	}
	collectionAddress, err := tonaddr.Parse(cfg.CollectionAddress, cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("parse TON collection address: %w", err)
	}
	codeHash, err := decodeHash(cfg.CollectionCodeHash)
	if err != nil {
		return nil, fmt.Errorf("decode TON collection code hash: %w", err)
	}
	rootHash, err := decodeHash(cfg.TrustedBlock.RootHash)
	if err != nil {
		return nil, fmt.Errorf("decode TON trusted block root hash: %w", err)
	}
	fileHash, err := decodeHash(cfg.TrustedBlock.FileHash)
	if err != nil {
		return nil, fmt.Errorf("decode TON trusted block file hash: %w", err)
	}
	if cfg.TrustedBlock.Workchain != -1 || cfg.TrustedBlock.Shard == 0 || cfg.TrustedBlock.Seqno == 0 {
		return nil, fmt.Errorf("invalid TON trusted masterchain checkpoint")
	}
	minBalance, err := tlb.FromTON(cfg.RelayerMinBalance)
	if err != nil || !minBalance.IsPositive() {
		return nil, fmt.Errorf("parse TON relayer minimum balance")
	}
	sendAmount, err := tlb.FromTON(cfg.MintSendAmount)
	if err != nil || !sendAmount.IsPositive() {
		return nil, fmt.Errorf("parse TON mint send amount")
	}
	forwardAmount, err := tlb.FromTON(cfg.MintForwardAmount)
	if err != nil || forwardAmount.IsNegative() {
		return nil, fmt.Errorf("parse TON mint forward amount")
	}

	pool := liteclient.NewConnectionPool()
	pool.SetOnDisconnect(pool.DefaultReconnect(2*time.Second, -1))
	connected := 0
	var connectionErrors []error
	for _, server := range cfg.LiteServers {
		if err := pool.AddConnection(ctx, strings.TrimSpace(server.Address), strings.TrimSpace(server.PublicKey)); err != nil {
			connectionErrors = append(connectionErrors, err)
			continue
		}
		connected++
	}
	if connected == 0 {
		pool.Stop()
		return nil, fmt.Errorf("connect TON lite servers: %w", errors.Join(connectionErrors...))
	}
	api := ton.NewAPIClient(pool, ton.ProofCheckPolicySecure)
	api.SetTrustedBlock(&ton.BlockIDExt{
		Workchain: cfg.TrustedBlock.Workchain, Shard: cfg.TrustedBlock.Shard, SeqNo: cfg.TrustedBlock.Seqno,
		RootHash: rootHash, FileHash: fileHash,
	})
	client := &Client{
		pool: pool, api: api, collection: nft.NewCollectionClient(api, collectionAddress),
		collectionAddress: collectionAddress, collectionCodeHash: codeHash,
		mintABI:    strings.TrimSpace(cfg.MintABI),
		minBalance: minBalance, sendAmount: sendAmount, forwardAmount: forwardAmount,
		messageTTL: cfg.RelayerMessageTTL, finalityDepth: cfg.FinalityDepth, network: cfg.Network,
	}
	if strings.TrimSpace(cfg.RelayerMnemonicFile) != "" {
		seedData, err := os.ReadFile(strings.TrimSpace(cfg.RelayerMnemonicFile))
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("read TON relayer mnemonic file: %w", err)
		}
		seed := strings.Fields(string(seedData))
		if len(seed) != 24 {
			client.Close()
			return nil, fmt.Errorf("TON relayer mnemonic file must contain exactly 24 words")
		}
		client.relayer, err = wallet.FromSeed(api, seed, wallet.V4R2)
		for i := range seedData {
			seedData[i] = 0
		}
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("initialize TON relayer wallet: %w", err)
		}
		spec, ok := client.relayer.GetSpec().(*wallet.SpecV4R2)
		if !ok {
			client.Close()
			return nil, fmt.Errorf("TON relayer is not wallet v4r2")
		}
		spec.SetMessagesTTL(uint32(cfg.RelayerMessageTTL.Seconds()))
	}
	return client, nil
}

func (c *Client) Close() {
	if c != nil && c.pool != nil {
		c.pool.Stop()
	}
}

func (c *Client) RelayerAddress() string {
	if c == nil || c.relayer == nil {
		return ""
	}
	return c.relayer.Address().StringRaw()
}

func (c *Client) Preflight(ctx context.Context, mintEnabled bool) (domain.StarGiftTONChainState, error) {
	if c == nil || c.api == nil || c.collection == nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("TON client is not initialized")
	}
	block, err := c.api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("read TON masterchain: %w", err)
	}
	account, err := c.api.WaitForBlock(block.SeqNo).GetAccount(ctx, block, c.collectionAddress)
	if err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("read TON collection account: %w", err)
	}
	if !account.IsActive || account.Code == nil || !bytes.Equal(account.Code.Hash(), c.collectionCodeHash) {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON collection is inactive or code hash does not match admission config"))
	}
	collectionData, err := c.collection.GetCollectionDataAtBlock(ctx, block)
	if err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("read TON collection data: %w", err)
	}
	if collectionData.OwnerAddress == nil || collectionData.OwnerAddress.IsAddrNone() {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON collection has no mint owner"))
	}
	if collectionData.NextItemIndex == nil || collectionData.NextItemIndex.Sign() < 0 || collectionData.NextItemIndex.BitLen() > 64 {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON collection returned an invalid next item index"))
	}
	state := domain.StarGiftTONChainState{
		Network: c.network, CollectionAddress: c.collectionAddress.StringRaw(),
		CollectionCodeHash:    append([]byte(nil), account.Code.Hash()...),
		ObservedNextItemIndex: collectionData.NextItemIndex.String(),
	}
	if !mintEnabled {
		return state, nil
	}
	if c.relayer == nil {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON mint is enabled without a relayer wallet"))
	}
	if !collectionData.OwnerAddress.Equals(c.relayer.Address()) {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON collection owner does not match relayer wallet"))
	}
	balance, err := c.relayer.GetBalance(ctx, block)
	if err != nil {
		return domain.StarGiftTONChainState{}, fmt.Errorf("read TON relayer balance: %w", err)
	}
	if balance.LessThan(c.minBalance) {
		return domain.StarGiftTONChainState{}, tonworker.Permanent(fmt.Errorf("TON relayer balance %s is below configured reserve %s", balance.TON(), c.minBalance.TON()))
	}
	return state, nil
}

func (c *Client) ResolveItemAddress(ctx context.Context, export domain.StarGiftTONExport) (string, error) {
	if export.Network != c.network {
		return "", tonworker.Permanent(fmt.Errorf("TON export network does not match worker"))
	}
	collection, err := tonaddr.Parse(export.CollectionAddress, export.Network)
	if err != nil || !collection.Equals(c.collectionAddress) {
		return "", tonworker.Permanent(fmt.Errorf("TON export collection does not match worker admission config"))
	}
	index, ok := new(big.Int).SetString(export.ItemIndex, 10)
	if !ok || index.Sign() < 0 || index.BitLen() > 64 {
		return "", tonworker.Permanent(fmt.Errorf("invalid TON NFT item index"))
	}
	itemAddress, err := c.collection.GetNFTAddressByIndex(ctx, index)
	if err != nil {
		return "", err
	}
	if itemAddress == nil || itemAddress.IsAddrNone() {
		return "", tonworker.Permanent(fmt.Errorf("TON collection returned an empty NFT item address"))
	}
	return itemAddress.StringRaw(), nil
}

func (c *Client) BuildMint(ctx context.Context, export domain.StarGiftTONExport) (tonworker.MintPreparation, error) {
	c.mintMu.Lock()
	defer c.mintMu.Unlock()
	if c.relayer == nil {
		return tonworker.MintPreparation{}, tonworker.Permanent(fmt.Errorf("TON relayer wallet is unavailable"))
	}
	index, owner, err := c.validateExport(ctx, export, true)
	if err != nil {
		return tonworker.MintPreparation{}, err
	}
	body, err := deterministicMintBody(export, index, owner, c.forwardAmount)
	if err != nil {
		return tonworker.MintPreparation{}, tonworker.Permanent(err)
	}
	message, err := c.relayer.BuildTransfer(c.collectionAddress, c.sendAmount, true, "")
	if err != nil {
		return tonworker.MintPreparation{}, err
	}
	message.InternalMessage.Body = body
	external, err := c.relayer.BuildExternalMessage(ctx, message)
	if err != nil {
		return tonworker.MintPreparation{}, err
	}
	validUntil, err := walletV4ValidUntil(external)
	if err != nil {
		return tonworker.MintPreparation{}, tonworker.Permanent(err)
	}
	externalCell, err := tlb.ToCell(external)
	if err != nil {
		return tonworker.MintPreparation{}, tonworker.Permanent(fmt.Errorf("encode signed TON mint: %w", err))
	}
	return tonworker.MintPreparation{
		SubmissionHash: externalCell.Hash(), SubmissionBOC: externalCell.ToBOC(), ValidUntil: validUntil,
	}, nil
}

func (c *Client) SendMint(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (tonworker.MintDispatch, error) {
	c.mintMu.Lock()
	defer c.mintMu.Unlock()
	external, err := decodePreparedMessage(asset)
	if err != nil {
		return tonworker.MintDispatch{}, tonworker.Permanent(err)
	}
	if c.relayer == nil || !external.DstAddr.Equals(c.relayer.Address()) {
		return tonworker.MintDispatch{}, tonworker.Permanent(fmt.Errorf("prepared TON mint relayer does not match configured wallet"))
	}
	index, owner, err := c.validateExport(ctx, export, false)
	if err != nil {
		return tonworker.MintDispatch{}, err
	}
	mintBody, err := validatePreparedMintMessage(external, export, index, owner, c.collectionAddress)
	if err != nil {
		return tonworker.MintDispatch{}, tonworker.Permanent(err)
	}
	if tx, err := c.findSubmissionTransaction(ctx, external.DstAddr, asset); err == nil {
		if err := validateSubmissionTransaction(tx, asset.SubmissionHash, c.collectionAddress, mintBody); err != nil {
			return tonworker.MintDispatch{}, tonworker.Permanent(err)
		}
		return dispatchFromTransaction(tx, true), nil
	} else if !errors.Is(err, ton.ErrTxWasNotFound) {
		return tonworker.MintDispatch{}, err
	}
	if int(time.Now().Unix()) >= asset.ValidUntil {
		return tonworker.MintDispatch{}, tonworker.Permanent(fmt.Errorf("prepared TON mint expired without an observed relayer transaction"))
	}
	tx, block, _, sendErr := c.api.SendExternalMessageWaitTransaction(ctx, external)
	if tx == nil || block == nil {
		return tonworker.MintDispatch{Dispatched: true}, sendErr
	}
	if err := validateSubmissionTransaction(tx, asset.SubmissionHash, c.collectionAddress, mintBody); err != nil {
		return tonworker.MintDispatch{}, tonworker.Permanent(err)
	}
	result := dispatchFromTransaction(tx, true)
	result.BlockSeqno = int64(block.SeqNo)
	return result, sendErr
}

func (c *Client) ConfirmMint(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (tonworker.MintConfirmation, bool, error) {
	external, err := decodePreparedMessage(asset)
	if err != nil {
		return tonworker.MintConfirmation{}, false, tonworker.Permanent(err)
	}
	index, owner, err := c.validateExport(ctx, export, false)
	if err != nil {
		return tonworker.MintConfirmation{}, false, err
	}
	mintBody, err := validatePreparedMintMessage(external, export, index, owner, c.collectionAddress)
	if err != nil {
		return tonworker.MintConfirmation{}, false, tonworker.Permanent(err)
	}
	tx, err := c.findSubmissionTransaction(ctx, external.DstAddr, asset)
	if errors.Is(err, ton.ErrTxWasNotFound) {
		if int(time.Now().Unix()) >= asset.ValidUntil {
			return tonworker.MintConfirmation{}, false, tonworker.Permanent(fmt.Errorf("TON mint message expired and no relayer transaction was found"))
		}
		if err := c.api.SendExternalMessage(ctx, external); err != nil {
			return tonworker.MintConfirmation{}, false, err
		}
		return tonworker.MintConfirmation{}, false, nil
	}
	if err != nil {
		return tonworker.MintConfirmation{}, false, err
	}
	if err := validateSubmissionTransaction(tx, asset.SubmissionHash, c.collectionAddress, mintBody); err != nil {
		return tonworker.MintConfirmation{}, false, tonworker.Permanent(err)
	}
	txHash, err := transactionHash(tx)
	if err != nil {
		return tonworker.MintConfirmation{}, false, tonworker.Permanent(err)
	}
	txLT := strconv.FormatUint(tx.LT, 10)
	if (len(asset.MintTxHash) != 0 && !bytes.Equal(asset.MintTxHash, txHash)) ||
		(asset.MintTxLT != "" && asset.MintTxLT != txLT) {
		return tonworker.MintConfirmation{}, false, tonworker.Permanent(fmt.Errorf("TON mint transaction evidence does not match stored submission"))
	}
	block, err := c.api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return tonworker.MintConfirmation{}, false, err
	}
	baseSeqno := asset.MintBlockSeqno
	progress, final := mintFinalityProgress(txHash, txLT, baseSeqno, int64(block.SeqNo), c.finalityDepth)
	if !final {
		return progress, false, nil
	}
	if err := c.verifyMintedItemAtBlock(ctx, block, export, true); err != nil {
		return tonworker.MintConfirmation{}, false, err
	}
	progress.OwnerAddress = export.WalletAddress
	return progress, true, nil
}

func mintFinalityProgress(txHash []byte, txLT string, baseSeqno, currentSeqno int64, requiredDepth int) (tonworker.MintConfirmation, bool) {
	if baseSeqno == 0 {
		return tonworker.MintConfirmation{
			TxHash: append([]byte(nil), txHash...), TxLT: txLT, BlockSeqno: currentSeqno,
		}, false
	}
	if currentSeqno < baseSeqno+int64(requiredDepth) {
		return tonworker.MintConfirmation{}, false
	}
	return tonworker.MintConfirmation{
		TxHash: append([]byte(nil), txHash...), TxLT: txLT, BlockSeqno: baseSeqno,
		FinalityDepth: int(currentSeqno - baseSeqno),
	}, true
}

func (c *Client) ObserveOwner(ctx context.Context, export domain.StarGiftTONExport, asset domain.StarGiftTONAsset) (tonworker.OwnerObservation, error) {
	itemAddress, err := tonaddr.Parse(export.ExpectedItemAddress, export.Network)
	if err != nil {
		return tonworker.OwnerObservation{}, tonworker.Permanent(fmt.Errorf("parse TON item address: %w", err))
	}
	block, err := c.api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return tonworker.OwnerObservation{}, err
	}
	data, err := nft.NewItemClient(c.api, itemAddress).GetNFTDataAtBlock(ctx, block)
	if err != nil {
		return tonworker.OwnerObservation{}, err
	}
	if err := verifyItemData(export, c.collectionAddress, data, false); err != nil {
		return tonworker.OwnerObservation{}, err
	}
	account, err := c.api.WaitForBlock(block.SeqNo).GetAccount(ctx, block, itemAddress)
	if err != nil {
		return tonworker.OwnerObservation{}, err
	}
	if !account.IsActive || account.LastTxLT == 0 {
		return tonworker.OwnerObservation{}, tonworker.Permanent(fmt.Errorf("TON NFT item account is inactive"))
	}
	return tonworker.OwnerObservation{
		OwnerAddress: data.OwnerAddress.StringRaw(), ObservedLT: strconv.FormatUint(account.LastTxLT, 10),
	}, nil
}

func (c *Client) validateExport(ctx context.Context, export domain.StarGiftTONExport, requireNextIndex bool) (*big.Int, *address.Address, error) {
	if export.Network != c.network || export.MintABI != c.mintABI || len(export.MetadataHash) != 32 {
		return nil, nil, tonworker.Permanent(fmt.Errorf("TON export network, mint ABI, or metadata hash does not match worker"))
	}
	collection, err := tonaddr.Parse(export.CollectionAddress, export.Network)
	if err != nil || !collection.Equals(c.collectionAddress) {
		return nil, nil, tonworker.Permanent(fmt.Errorf("TON export collection does not match worker admission config"))
	}
	index, ok := new(big.Int).SetString(export.ItemIndex, 10)
	if !ok || index.Sign() < 0 || index.BitLen() > 64 {
		return nil, nil, tonworker.Permanent(fmt.Errorf("invalid TON NFT item index"))
	}
	owner, err := tonaddr.Parse(export.WalletAddress, export.Network)
	if err != nil {
		return nil, nil, tonworker.Permanent(fmt.Errorf("parse TON export wallet: %w", err))
	}
	if err := validateAddressNetwork(owner, c.network); err != nil {
		return nil, nil, tonworker.Permanent(err)
	}
	expected, err := c.collection.GetNFTAddressByIndex(ctx, index)
	if err != nil {
		return nil, nil, err
	}
	storedExpected, err := tonaddr.Parse(export.ExpectedItemAddress, export.Network)
	if err != nil || !storedExpected.Equals(expected) {
		return nil, nil, tonworker.Permanent(fmt.Errorf("TON expected NFT item address does not match collection getter"))
	}
	metadataURL, err := url.Parse(export.MetadataURI)
	if err != nil || metadataURL.Scheme != "https" || metadataURL.Host == "" {
		return nil, nil, tonworker.Permanent(fmt.Errorf("TON NFT metadata URI must be absolute HTTPS"))
	}
	if requireNextIndex {
		data, err := c.collection.GetCollectionData(ctx)
		if err != nil {
			return nil, nil, err
		}
		if data.NextItemIndex.Cmp(index) != 0 {
			return nil, nil, fmt.Errorf("TON collection next item index is %s, export reserved %s", data.NextItemIndex, index)
		}
	}
	return index, owner, nil
}

func (c *Client) verifyMintedItemAtBlock(ctx context.Context, block *ton.BlockIDExt, export domain.StarGiftTONExport, requireExpectedOwner bool) error {
	itemAddress, err := tonaddr.Parse(export.ExpectedItemAddress, export.Network)
	if err != nil {
		return tonworker.Permanent(err)
	}
	data, err := nft.NewItemClient(c.api, itemAddress).GetNFTDataAtBlock(ctx, block)
	if err != nil {
		return err
	}
	return verifyItemData(export, c.collectionAddress, data, requireExpectedOwner)
}

func verifyItemData(export domain.StarGiftTONExport, collection *address.Address, data *nft.ItemData, requireExpectedOwner bool) error {
	index, _ := new(big.Int).SetString(export.ItemIndex, 10)
	if data == nil || !data.Initialized || data.Index == nil || data.Index.Cmp(index) != 0 ||
		data.CollectionAddress == nil || !data.CollectionAddress.Equals(collection) {
		return tonworker.Permanent(fmt.Errorf("TON NFT item collection/index evidence does not match export"))
	}
	offchain, ok := data.Content.(*nft.ContentOffchain)
	if !ok || offchain.URI != export.MetadataURI {
		return tonworker.Permanent(fmt.Errorf("TON NFT item metadata URI does not match export"))
	}
	if data.OwnerAddress == nil || data.OwnerAddress.IsAddrNone() {
		return tonworker.Permanent(fmt.Errorf("TON NFT item has no owner"))
	}
	if requireExpectedOwner {
		owner, err := tonaddr.Parse(export.WalletAddress, export.Network)
		if err != nil || !data.OwnerAddress.Equals(owner) {
			return tonworker.Permanent(fmt.Errorf("TON NFT item owner does not match export wallet"))
		}
	}
	return nil
}

func deterministicMintBody(export domain.StarGiftTONExport, index *big.Int, owner *address.Address, forward tlb.Coins) (*cell.Cell, error) {
	individualContent := cell.BeginCell().MustStoreStringSnake(export.MetadataURI).EndCell()
	content := cell.BeginCell().MustStoreAddr(owner).MustStoreRef(individualContent).EndCell()
	return tlb.ToCell(nft.ItemMintPayload{
		QueryID: stableMintQueryID(export), Index: index, TonAmount: forward, Content: content,
	})
}

func stableMintQueryID(export domain.StarGiftTONExport) uint64 {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "telesrv-star-gift-ton-v1\x00%d\x00%d\x00%s\x00%s\x00%s\x00%s\x00",
		export.ID, export.UniqueGiftID, export.Network, export.CollectionAddress, export.ItemIndex, export.ExpectedItemAddress)
	h.Write(export.MetadataHash)
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

func walletV4ValidUntil(message *tlb.ExternalMessage) (int, error) {
	if message == nil || message.Body == nil {
		return 0, fmt.Errorf("prepared TON wallet message has no body")
	}
	slice := message.Body.MustBeginParse()
	if _, err := slice.LoadSlice(512); err != nil {
		return 0, fmt.Errorf("read TON wallet signature: %w", err)
	}
	if _, err := slice.LoadUInt(32); err != nil {
		return 0, fmt.Errorf("read TON wallet subwallet: %w", err)
	}
	validUntil, err := slice.LoadUInt(32)
	if err != nil {
		return 0, fmt.Errorf("read TON wallet valid_until: %w", err)
	}
	return int(validUntil), nil
}

func validatePreparedMintMessage(message *tlb.ExternalMessage, export domain.StarGiftTONExport, index *big.Int,
	owner, collection *address.Address) (*cell.Cell, error) {
	if message == nil || message.Body == nil || index == nil || owner == nil || collection == nil {
		return nil, fmt.Errorf("prepared TON mint message evidence is incomplete")
	}
	slice := message.Body.MustBeginParse()
	if _, err := slice.LoadSlice(512); err != nil {
		return nil, fmt.Errorf("read TON wallet signature: %w", err)
	}
	for _, field := range []string{"subwallet", "valid_until", "seqno"} {
		if _, err := slice.LoadUInt(32); err != nil {
			return nil, fmt.Errorf("read TON wallet %s: %w", field, err)
		}
	}
	op, err := slice.LoadUInt(8)
	if err != nil || op != 0 {
		return nil, fmt.Errorf("prepared TON wallet message has invalid operation")
	}
	mode, err := slice.LoadUInt(8)
	if err != nil || mode != uint64(wallet.PayGasSeparately+wallet.IgnoreErrors) {
		return nil, fmt.Errorf("prepared TON wallet message has invalid send mode")
	}
	internalSlice, err := slice.LoadRef()
	if err != nil {
		return nil, fmt.Errorf("read prepared TON mint transfer: %w", err)
	}
	if slice.BitsLeft() != 0 || slice.RefsNum() != 0 {
		return nil, fmt.Errorf("prepared TON wallet message must contain exactly one transfer")
	}
	var internal tlb.InternalMessage
	if err := tlb.LoadFromCell(&internal, internalSlice); err != nil {
		return nil, fmt.Errorf("decode prepared TON mint transfer: %w", err)
	}
	if internal.DstAddr == nil || !internal.DstAddr.Equals(collection) || !internal.Amount.IsPositive() || internal.Body == nil {
		return nil, fmt.Errorf("prepared TON mint transfer does not target the configured collection")
	}
	var payload nft.ItemMintPayload
	if err := tlb.LoadFromCell(&payload, internal.Body.MustBeginParse()); err != nil {
		return nil, fmt.Errorf("decode prepared TON mint payload: %w", err)
	}
	if payload.QueryID != stableMintQueryID(export) || payload.Index == nil || payload.Index.Cmp(index) != 0 ||
		payload.TonAmount.IsNegative() || payload.Content == nil {
		return nil, fmt.Errorf("prepared TON mint payload does not match export identity")
	}
	content := payload.Content.MustBeginParse()
	payloadOwner, err := content.LoadAddr()
	if err != nil || payloadOwner == nil || !payloadOwner.Equals(owner) {
		return nil, fmt.Errorf("prepared TON mint payload owner does not match export wallet")
	}
	individualContent, err := content.LoadRef()
	if err != nil {
		return nil, fmt.Errorf("read prepared TON mint metadata: %w", err)
	}
	metadataURI, err := individualContent.LoadStringSnake()
	if err != nil || metadataURI != export.MetadataURI {
		return nil, fmt.Errorf("prepared TON mint metadata does not match export")
	}
	return internal.Body, nil
}

func decodePreparedMessage(asset domain.StarGiftTONAsset) (*tlb.ExternalMessage, error) {
	if len(asset.SubmissionHash) != 32 || len(asset.SubmissionBOC) == 0 || asset.PreparedAt <= 0 || asset.ValidUntil <= asset.PreparedAt {
		return nil, fmt.Errorf("stored TON mint preparation is incomplete")
	}
	root, err := cell.FromBOC(asset.SubmissionBOC)
	if err != nil {
		return nil, fmt.Errorf("decode prepared TON mint BOC: %w", err)
	}
	if !bytes.Equal(root.Hash(), asset.SubmissionHash) {
		return nil, fmt.Errorf("prepared TON mint BOC hash mismatch")
	}
	var external tlb.ExternalMessage
	if err := tlb.LoadFromCell(&external, root.MustBeginParse()); err != nil {
		return nil, fmt.Errorf("decode prepared TON external message: %w", err)
	}
	if external.DstAddr == nil || external.DstAddr.IsAddrNone() {
		return nil, fmt.Errorf("prepared TON mint has no relayer destination")
	}
	validUntil, err := walletV4ValidUntil(&external)
	if err != nil || validUntil != asset.ValidUntil {
		return nil, fmt.Errorf("prepared TON mint valid_until mismatch")
	}
	return &external, nil
}

func (c *Client) findSubmissionTransaction(ctx context.Context, relayer *address.Address, asset domain.StarGiftTONAsset) (*tlb.Transaction, error) {
	return c.api.FindLastTransactionByInMsgHashAfterTime(ctx, relayer, asset.SubmissionHash, time.Unix(int64(asset.PreparedAt-5), 0))
}

func validateSubmissionTransaction(tx *tlb.Transaction, submissionHash []byte, collection *address.Address, mintBody *cell.Cell) error {
	if tx == nil || len(submissionHash) != 32 || collection == nil || collection.IsAddrNone() || mintBody == nil {
		return fmt.Errorf("TON mint transaction evidence is incomplete")
	}
	description, ok := tx.Description.(tlb.TransactionDescriptionOrdinary)
	if !ok {
		return fmt.Errorf("TON mint relayer transaction is not ordinary")
	}
	if description.Aborted || description.Destroyed {
		return fmt.Errorf("TON mint relayer transaction was aborted or destroyed")
	}
	compute, ok := description.ComputePhase.Phase.(tlb.ComputePhaseVM)
	if !ok || !compute.Success {
		return fmt.Errorf("TON mint relayer compute phase did not succeed")
	}
	action := description.ActionPhase
	if action == nil || !action.Success || !action.Valid || action.NoFunds || action.ResultCode != 0 {
		return fmt.Errorf("TON mint relayer action phase did not succeed")
	}
	if tx.IO.In == nil || tx.IO.In.MsgType != tlb.MsgTypeExternalIn || tx.IO.In.Msg == nil {
		return fmt.Errorf("TON mint relayer transaction has no external input")
	}
	incoming, err := tlb.ToCell(tx.IO.In.Msg)
	if err != nil {
		return fmt.Errorf("encode TON mint relayer input: %w", err)
	}
	if !bytes.Equal(incoming.Hash(), submissionHash) {
		return fmt.Errorf("TON mint relayer input hash does not match prepared submission")
	}
	if tx.IO.Out == nil {
		return fmt.Errorf("TON mint relayer transaction has no outgoing messages")
	}
	outgoing, err := tx.IO.Out.ToSlice()
	if err != nil {
		return fmt.Errorf("decode TON mint relayer outgoing messages: %w", err)
	}
	for i := range outgoing {
		if outgoing[i].MsgType != tlb.MsgTypeInternal || outgoing[i].Msg == nil {
			continue
		}
		message := outgoing[i].AsInternal()
		if message.DstAddr != nil && message.DstAddr.Equals(collection) && message.Body != nil &&
			bytes.Equal(message.Body.Hash(), mintBody.Hash()) {
			return nil
		}
	}
	return fmt.Errorf("TON mint relayer transaction has no matching collection mint message")
}

func dispatchFromTransaction(tx *tlb.Transaction, dispatched bool) tonworker.MintDispatch {
	if tx == nil {
		return tonworker.MintDispatch{Dispatched: dispatched}
	}
	hash, _ := transactionHash(tx)
	return tonworker.MintDispatch{
		Dispatched: dispatched, TxHash: hash, TxLT: strconv.FormatUint(tx.LT, 10),
	}
}

func transactionHash(tx *tlb.Transaction) ([]byte, error) {
	if tx == nil {
		return nil, fmt.Errorf("TON transaction is nil")
	}
	if len(tx.Hash) == 32 {
		return append([]byte(nil), tx.Hash...), nil
	}
	transactionCell, err := tx.ToCell()
	if err != nil {
		return nil, fmt.Errorf("encode TON transaction hash: %w", err)
	}
	return transactionCell.Hash(), nil
}

func decodeHash(value string) ([]byte, error) {
	return domain.DecodeTONHash(value)
}

func validateAddressNetwork(addr *address.Address, network domain.TONNetwork) error {
	if addr == nil || addr.IsAddrNone() {
		return fmt.Errorf("TON address is empty")
	}
	if network == domain.TONNetworkMainnet && addr.IsTestnetOnly() {
		return fmt.Errorf("testnet-only TON address is forbidden on mainnet")
	}
	return nil
}
