package sfu

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPGroupCallTouchReporterRoundTrip(t *testing.T) {
	var gotCallID, gotUserID int64
	srv := httptest.NewServer(NewGroupCallControlHTTPHandler(func(_ context.Context, callID, userID int64) error {
		gotCallID = callID
		gotUserID = userID
		return nil
	}, "secret"))
	defer srv.Close()

	reporter, err := NewHTTPGroupCallTouchReporter(srv.Client(), srv.URL, "secret")
	if err != nil {
		t.Fatalf("reporter: %v", err)
	}
	if err := reporter.Touch(context.Background(), 1001, 44); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if gotCallID != 1001 || gotUserID != 44 {
		t.Fatalf("touch target = %d/%d, want 1001/44", gotCallID, gotUserID)
	}
}

func TestGroupCallControlRequiresToken(t *testing.T) {
	srv := httptest.NewServer(NewGroupCallControlHTTPHandler(func(context.Context, int64, int64) error {
		return nil
	}, "secret"))
	defer srv.Close()

	if _, err := NewHTTPGroupCallTouchReporter(srv.Client(), srv.URL, ""); !errors.Is(err, ErrGroupCallControlTokenMissing) {
		t.Fatalf("reporter without token err = %v, want ErrGroupCallControlTokenMissing", err)
	}
}

func TestGroupCallControlRejectsTouchWhenTokenUnconfigured(t *testing.T) {
	called := false
	req := httptest.NewRequest(http.MethodPost, "/v1/groupcalls/touch", strings.NewReader(`{"call_id":1002,"user_id":45}`))
	rec := httptest.NewRecorder()
	NewGroupCallControlHTTPHandler(func(context.Context, int64, int64) error {
		called = true
		return nil
	}, "").ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("touch without configured token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("unauthorized touch reached callback")
	}
}

func TestGroupCallControlHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	NewGroupCallControlHTTPHandler(func(context.Context, int64, int64) error { return nil }, "").ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("health status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
