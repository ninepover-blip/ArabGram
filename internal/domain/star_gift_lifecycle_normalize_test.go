package domain

import (
	"errors"
	"testing"
)

// TestNormalizeLifecycleAuthoringSatisfiesCatalogConstraints locks the derived
// fields that star_gift_catalog_revisions' CHECK constraints require. An operator
// states supply, slug, per-round and (optionally) a start time; "limited", the
// seeded availability_remains and a positive auction_start_date are consequences.
// Without them the revision INSERT is rejected by the database, which is exactly
// the failure this test exists to prevent regressing.
func TestNormalizeLifecycleAuthoringSatisfiesCatalogConstraints(t *testing.T) {
	const now = 1_000_000

	t.Run("auction with immediate start", func(t *testing.T) {
		w := StarGiftCatalogWrite{
			Auction: true, AuctionSlug: "spring", GiftsPerRound: 2, AvailabilityTotal: 10,
		}
		if err := w.ValidateLifecycleAuthoring(now); err != nil {
			t.Fatalf("validate: %v", err)
		}
		w.NormalizeLifecycleAuthoring(now)
		if !w.Limited {
			t.Fatalf("auction must be limited so the supply and auction checks hold")
		}
		if w.AuctionStartDate != now {
			t.Fatalf("start date = %d, want %d (a zero start means start now)", w.AuctionStartDate, now)
		}
		if w.AvailabilityRemains != 10 {
			t.Fatalf("availability remains = %d, want the full supply 10", w.AvailabilityRemains)
		}
	})

	t.Run("auction with explicit future start is preserved", func(t *testing.T) {
		w := StarGiftCatalogWrite{
			Auction: true, AuctionSlug: "summer", GiftsPerRound: 5, AvailabilityTotal: 20,
			AuctionStartDate: now + 3600,
		}
		w.NormalizeLifecycleAuthoring(now)
		if w.AuctionStartDate != now+3600 {
			t.Fatalf("start date = %d, want the operator's %d", w.AuctionStartDate, now+3600)
		}
		if w.AvailabilityRemains != 20 {
			t.Fatalf("availability remains = %d, want 20", w.AvailabilityRemains)
		}
	})

	// A scheduled drop is an ordinary unlimited gift behind a countdown. Marking it
	// limited would violate "NOT limited AND availability_total = 0".
	t.Run("scheduled drop stays unlimited", func(t *testing.T) {
		w := StarGiftCatalogWrite{LockedUntilDate: now + 120}
		if err := w.ValidateLifecycleAuthoring(now); err != nil {
			t.Fatalf("validate: %v", err)
		}
		w.NormalizeLifecycleAuthoring(now)
		if w.Limited || w.AvailabilityTotal != 0 || w.AvailabilityRemains != 0 {
			t.Fatalf("drop was altered: limited=%v total=%d remains=%d",
				w.Limited, w.AvailabilityTotal, w.AvailabilityRemains)
		}
		if w.AuctionStartDate != 0 {
			t.Fatalf("start date = %d, want 0 on a non-auction gift", w.AuctionStartDate)
		}
	})

	t.Run("regular gift is untouched", func(t *testing.T) {
		w := StarGiftCatalogWrite{}
		w.NormalizeLifecycleAuthoring(now)
		if w.Limited || w.AuctionStartDate != 0 || w.AvailabilityRemains != 0 {
			t.Fatalf("regular gift was altered: limited=%v start=%d remains=%d",
				w.Limited, w.AuctionStartDate, w.AvailabilityRemains)
		}
	})
}

// TestValidateLifecycleAuthoringBoundsOpeningBid guards the ceiling shared with
// MaxStarGiftAuctionBidStars. ensureStarGiftAuction seeds min_bid_amount from the
// gift price, so an opening bid above the cap would produce an auction in which
// every bid is rejected by starGiftAuctionBidTarget — and, once a top bid climbs
// into that range, one that crashes official clients outright.
func TestValidateLifecycleAuthoringBoundsOpeningBid(t *testing.T) {
	const now = 1_000_000

	base := func(stars int64) StarGiftCatalogWrite {
		return StarGiftCatalogWrite{
			Auction: true, AuctionSlug: "capped", GiftsPerRound: 1, AvailabilityTotal: 3,
			Stars: stars,
		}
	}

	w := base(MaxStarGiftAuctionBidStars)
	if err := w.ValidateLifecycleAuthoring(now); err != nil {
		t.Fatalf("opening bid exactly at the cap must be allowed: %v", err)
	}

	w = base(MaxStarGiftAuctionBidStars + 1)
	if err := w.ValidateLifecycleAuthoring(now); err == nil {
		t.Fatalf("opening bid of %d stars was accepted, want rejection above %d",
			MaxStarGiftAuctionBidStars+1, MaxStarGiftAuctionBidStars)
	} else if !errors.Is(err, ErrStarGiftLifecycleInvalid) {
		t.Fatalf("error = %v, want it to wrap ErrStarGiftLifecycleInvalid", err)
	}

	// The cap is an auction rule. A plain gift priced above it is a market matter,
	// governed by the resale limits, and must not be rejected here.
	plain := StarGiftCatalogWrite{Stars: MaxStarGiftAuctionBidStars + 1}
	if err := plain.ValidateLifecycleAuthoring(now); err != nil {
		t.Fatalf("non-auction gift priced above the bid cap: %v", err)
	}
}
