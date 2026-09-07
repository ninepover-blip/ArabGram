package domain

import "testing"

func TestPaidReactionFingerprintBindsClientIntent(t *testing.T) {
	base := SendChannelPaidReactionRequest{
		UserID: 10, ChannelID: 20, MessageID: 30, Stars: 40,
		RandomID: 50, Date: 60,
		Privacy: PaidReactionPrivacy{Kind: PaidReactionPrivacyDefault},
	}
	want := base.Fingerprint()
	derivedChanged := base
	derivedChanged.Date++
	derivedChanged.Anonymous = true
	if got := derivedChanged.Fingerprint(); got != want {
		t.Fatalf("server-derived date/default resolution changed fingerprint")
	}
	for name, mutate := range map[string]func(*SendChannelPaidReactionRequest){
		"payer":   func(r *SendChannelPaidReactionRequest) { r.UserID++ },
		"channel": func(r *SendChannelPaidReactionRequest) { r.ChannelID++ },
		"message": func(r *SendChannelPaidReactionRequest) { r.MessageID++ },
		"stars":   func(r *SendChannelPaidReactionRequest) { r.Stars++ },
		"privacy": func(r *SendChannelPaidReactionRequest) { r.Privacy.Kind = PaidReactionPrivacyAnonymous },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if got := changed.Fingerprint(); got == want {
				t.Fatalf("changed %s retained fingerprint", name)
			}
		})
	}
	// random_id is the command key, not part of its payload fingerprint.
	changedKey := base
	changedKey.RandomID++
	if got := changedKey.Fingerprint(); got != want {
		t.Fatalf("random_id unexpectedly included in payload fingerprint")
	}
}

func TestPaidReactionFingerprintDistinguishesAccountDefaultFromExplicitSelf(t *testing.T) {
	base := SendChannelPaidReactionRequest{
		UserID: 1, ChannelID: 2, MessageID: 3, Stars: 4,
		Privacy: PaidReactionPrivacy{Kind: PaidReactionPrivacyAccountDefault},
	}
	explicitSelf := base
	explicitSelf.Privacy.Kind = PaidReactionPrivacyDefault
	if base.Fingerprint() == explicitSelf.Fingerprint() {
		t.Fatal("missing private flag and explicit paidReactionPrivacyDefault share a fingerprint")
	}

	resolved := base
	resolved.Anonymous = true
	resolved.DisplayPeer = Peer{Type: PeerTypeChannel, ID: 99}
	if base.Fingerprint() != resolved.Fingerprint() {
		t.Fatal("mutable account-default resolution changed fingerprint")
	}
}

func TestPaidReactionRandomIDExpiry(t *testing.T) {
	const now = 2_000_000_000
	encode := func(timestamp int64) int64 { return int64(uint64(uint32(timestamp)) << 32) }
	for name, test := range map[string]struct {
		randomID int64
		expired  bool
	}{
		"current":         {encode(now), false},
		"old boundary":    {encode(now - PaidReactionRandomIDMaxAgeSeconds), false},
		"too old":         {encode(now - PaidReactionRandomIDMaxAgeSeconds - 1), true},
		"future boundary": {encode(now + PaidReactionRandomIDMaxFutureSeconds), false},
		"too far future":  {encode(now + PaidReactionRandomIDMaxFutureSeconds + 1), true},
		"malformed":       {1, true},
		"empty":           {0, true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := PaidReactionRandomIDExpired(test.randomID, now); got != test.expired {
				t.Fatalf("expired=%v, want %v", got, test.expired)
			}
		})
	}
}

func TestPaidReactionFingerprintBindsPeerPrivacy(t *testing.T) {
	a := SendChannelPaidReactionRequest{
		UserID: 1, ChannelID: 2, MessageID: 3, Stars: 4,
		Privacy: PaidReactionPrivacy{Kind: PaidReactionPrivacyPeer, Peer: &Peer{Type: PeerTypeChannel, ID: 5}},
	}
	b := a
	b.Privacy.Peer = &Peer{Type: PeerTypeChannel, ID: 6}
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatal("changed privacy peer retained fingerprint")
	}
}
