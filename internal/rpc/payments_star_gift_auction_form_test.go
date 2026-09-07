package rpc

import (
	"testing"

	"telesrv/internal/domain"
)

// The bid form id is the only thing standing between two bids of the same amount by
// the same bidder for the same recipient. Once the first bid's round is settled its
// payment receipt stays in star_gift_auction_bid_payments forever, so a repeated id
// makes BidStarGiftAuction answer the next bid from that receipt — reported as paid,
// never active. Binding the bidder's own bid generation is what keeps the ids apart.
func TestStarGiftAuctionBidFormIDSeparatesBidGenerations(t *testing.T) {
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 7001}
	base := starGiftAuctionBidFormID(42, 900, peer, 200, 1)
	if base <= 0 {
		t.Fatalf("form id = %d, want a positive id (0 is what sendStarGiftAuctionBidForm rejects)", base)
	}
	if again := starGiftAuctionBidFormID(42, 900, peer, 200, 1); again != base {
		t.Fatalf("form id unstable within one generation: %d then %d", base, again)
	}
	for _, tc := range []struct {
		name string
		id   int64
	}{
		{"next generation", starGiftAuctionBidFormID(42, 900, peer, 200, 2)},
		{"generation after a refund and a win", starGiftAuctionBidFormID(42, 900, peer, 200, 3)},
		{"another bidder", starGiftAuctionBidFormID(43, 900, peer, 200, 1)},
		{"another gift", starGiftAuctionBidFormID(42, 901, peer, 200, 1)},
		{"another recipient", starGiftAuctionBidFormID(42, 900, domain.Peer{Type: domain.PeerTypeUser, ID: 7002}, 200, 1)},
		{"a channel recipient with the same id", starGiftAuctionBidFormID(42, 900, domain.Peer{Type: domain.PeerTypeChannel, ID: 7001}, 200, 1)},
		{"another amount", starGiftAuctionBidFormID(42, 900, peer, 201, 1)},
	} {
		if tc.id == base {
			t.Fatalf("form id for %s repeats the first bid's id (%d)", tc.name, base)
		}
	}
}
