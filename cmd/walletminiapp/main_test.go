package main

import (
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestParseWebAppSessionValidatesHash(t *testing.T) {
	token := "1900000001:test-secret"
	values := url.Values{}
	values.Set("query_id", "web-query-1")
	values.Set("auth_date", strconv.FormatInt(time.Now().Unix(), 10))
	values.Set("start_param", "wallet")
	values.Set("user", `{"id":1780243210,"first_name":"Alice","username":"alice"}`)
	values.Set("hash", webAppInitDataHash(values, token))

	session, err := parseWebAppSession(values.Encode(), token, time.Hour)
	if err != nil {
		t.Fatalf("parseWebAppSession: %v", err)
	}
	if !session.Verified || session.QueryID != "web-query-1" || session.User.ID != 1780243210 || session.User.Username != "alice" {
		t.Fatalf("session = %+v", session)
	}

	values.Set("user", `{"id":1780243211,"first_name":"Bob"}`)
	if _, err := parseWebAppSession(values.Encode(), token, time.Hour); err == nil {
		t.Fatal("tampered init data accepted")
	}
}

func TestWalletTransferBoundsAndSnapshot(t *testing.T) {
	store := newWalletStore()
	tx, snapshot, err := store.transfer(1001, "@bob", 1250, "coffee")
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if tx.ID == 0 || tx.To != "@bob" || tx.AmountCents != 1250 {
		t.Fatalf("tx = %+v", tx)
	}
	if snapshot.BalanceCents != defaultBalanceCents-1250 || len(snapshot.Recent) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if _, _, err := store.transfer(1001, "", 1250, "coffee"); err == nil {
		t.Fatal("empty recipient accepted")
	}
	if _, _, err := store.transfer(1001, "@bob", maxTransferCents+1, "coffee"); err == nil {
		t.Fatal("oversized transfer accepted")
	}
}
