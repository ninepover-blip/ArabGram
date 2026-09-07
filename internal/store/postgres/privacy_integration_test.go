package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	"telesrv/internal/domain"
)

// TestPrivacyRulesDoNotAllocateAccountPts protects the protocol boundary:
// account privacy is authoritative absolute state, not a message-box event.
func TestPrivacyRulesDoNotAllocateAccountPts(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	user, err := users.Create(ctx, domain.User{
		AccessHash: 9201,
		Phone:      "+1665" + suffix + "01",
		FirstName:  "PrivacyPts",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", user.ID)
	})
	var privacyPayloadTableAbsent bool
	if err := pool.QueryRow(ctx, `
SELECT to_regclass('public.user_update_privacy_payloads') IS NULL`).Scan(&privacyPayloadTableAbsent); err != nil {
		t.Fatalf("inspect privacy payload schema: %v", err)
	}
	if !privacyPayloadTableAbsent {
		t.Fatal("development-only user_update_privacy_payloads table still exists")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
VALUES ($1, 1, 1, 1700000000, 'privacy')`, user.ID); err == nil {
		t.Fatal("development-only privacy update event type is still accepted")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.CheckViolation {
			t.Fatalf("insert privacy update event error=%v, want check violation", err)
		}
	}

	type updateFootprint struct {
		eventCount      int
		maxPts          int
		outboxCount     int
		edgeOutboxCount int
		watermarkRow    int
		watermarkPts    int
	}
	readFootprint := func() updateFootprint {
		t.Helper()
		var got updateFootprint
		if err := pool.QueryRow(ctx, `
SELECT count(*), COALESCE(max(pts), 0)
FROM user_update_events
WHERE user_id = $1`, user.ID).Scan(&got.eventCount, &got.maxPts); err != nil {
			t.Fatalf("read update events footprint: %v", err)
		}
		if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM dispatch_outbox
WHERE target_user_id = $1`, user.ID).Scan(&got.outboxCount); err != nil {
			t.Fatalf("read outbox footprint: %v", err)
		}
		if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM edge_delivery_outbox
WHERE target_user_id = $1`, user.ID).Scan(&got.edgeOutboxCount); err != nil {
			t.Fatalf("read edge delivery outbox footprint: %v", err)
		}
		if err := pool.QueryRow(ctx, `
SELECT count(*), COALESCE(max(contiguous_pts), 0)
FROM user_update_watermarks
WHERE user_id = $1`, user.ID).Scan(&got.watermarkRow, &got.watermarkPts); err != nil {
			t.Fatalf("read update watermark footprint: %v", err)
		}
		return got
	}

	before := readFootprint()
	store := NewPrivacyStore(pool)
	want := domain.PrivacyRules{
		OwnerUserID: user.ID,
		Key:         domain.PrivacyKeyPhoneNumber,
		Rules:       []domain.PrivacyRule{{Kind: domain.PrivacyRuleDisallowAll}},
	}
	if err := store.SetPrivacyRules(ctx, want); err != nil {
		t.Fatalf("set privacy rules: %v", err)
	}
	got, found, err := store.GetPrivacyRules(ctx, user.ID, want.Key)
	if err != nil || !found {
		t.Fatalf("get privacy rules: found=%v err=%v", found, err)
	}
	if len(got.Rules) != 1 || got.Rules[0].Kind != domain.PrivacyRuleDisallowAll {
		t.Fatalf("stored privacy rules=%+v, want disallow_all", got)
	}
	after := readFootprint()
	if after != before {
		t.Fatalf("privacy write changed PTS footprint: before=%+v after=%+v", before, after)
	}

	rollbackErr := errors.New("build privacy payload")
	withDelivery := domain.PrivacyRules{
		OwnerUserID: user.ID,
		Key:         domain.PrivacyKeyPhoneNumber,
		Rules:       []domain.PrivacyRule{{Kind: domain.PrivacyRuleAllowAll}},
	}
	if err := store.SetPrivacyRulesWithDelivery(ctx, withDelivery, func(domain.PrivacyRules) ([]byte, error) {
		return nil, rollbackErr
	}, [8]byte{8, 8}, 88); !errors.Is(err, rollbackErr) {
		t.Fatalf("set privacy with failing delivery err=%v, want %v", err, rollbackErr)
	}
	got, found, err = store.GetPrivacyRules(ctx, user.ID, want.Key)
	if err != nil || !found {
		t.Fatalf("get privacy rules after rollback: found=%v err=%v", found, err)
	}
	if len(got.Rules) != 1 || got.Rules[0].Kind != domain.PrivacyRuleDisallowAll {
		t.Fatalf("privacy rules after rollback=%+v, want original disallow_all", got)
	}
	if rolledBack := readFootprint(); rolledBack != before {
		t.Fatalf("failing privacy delivery changed footprint: before=%+v after=%+v", before, rolledBack)
	}

	if err := store.SetPrivacyRulesWithDelivery(ctx, withDelivery, func(rules domain.PrivacyRules) ([]byte, error) {
		if rules.OwnerUserID != user.ID || rules.Key != domain.PrivacyKeyPhoneNumber {
			t.Fatalf("delivery builder rules=%+v, want user %d phone_number", rules, user.ID)
		}
		return []byte{0x01}, nil
	}, [8]byte{8, 9}, 89); err != nil {
		t.Fatalf("set privacy with delivery: %v", err)
	}
	got, found, err = store.GetPrivacyRules(ctx, user.ID, want.Key)
	if err != nil || !found {
		t.Fatalf("get privacy rules after delivery: found=%v err=%v", found, err)
	}
	if len(got.Rules) != 1 || got.Rules[0].Kind != domain.PrivacyRuleAllowAll {
		t.Fatalf("privacy rules after delivery=%+v, want allow_all", got)
	}
	afterDelivery := readFootprint()
	if afterDelivery.eventCount != before.eventCount ||
		afterDelivery.maxPts != before.maxPts ||
		afterDelivery.outboxCount != before.outboxCount ||
		afterDelivery.watermarkRow != before.watermarkRow ||
		afterDelivery.watermarkPts != before.watermarkPts ||
		afterDelivery.edgeOutboxCount != before.edgeOutboxCount+1 {
		t.Fatalf("privacy delivery footprint=%+v, want PTS unchanged and one edge delivery over %+v", afterDelivery, before)
	}
}
