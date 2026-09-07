package postgres

import (
	"context"
	"fmt"
	"testing"

	"telesrv/internal/domain"
)

func TestChannelRecommendationsUsesBoundedCountAndMemberIndex(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 911,
		Phone:      "+1889" + suffix + "01",
		FirstName:  "RecommendOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	creator, err := users.Create(ctx, domain.User{
		AccessHash: 912,
		Phone:      "+1889" + suffix + "02",
		FirstName:  "RecommendCreator",
	})
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}

	var baseID int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) + 1000 FROM channels`).Scan(&baseID); err != nil {
		t.Fatalf("allocate channel id range: %v", err)
	}
	const total = domain.MaxChannelRecommendationsLimit + 5
	sourceID := baseID
	joinedID := baseID + 1
	firstCandidateID := baseID + 2
	lastChannelID := firstCandidateID + total - 1
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM user_channel_member_index WHERE user_id = $1 AND channel_id BETWEEN $2 AND $3", owner.ID, sourceID, lastChannelID)
		_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id BETWEEN $1 AND $2", sourceID, lastChannelID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, creator.ID})
	})

	insertChannel := func(id int64, username string, participants, date int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
INSERT INTO channels (
  id, access_hash, creator_user_id, title, username, broadcast, megagroup,
  participants_count, admins_count, top_message_id, pts, date
) VALUES ($1, $2, $3, $4, $5, true, false, $6, 1, 1, 1, $7)`,
			id, 90_000+id, creator.ID, "Recommendation "+username, username, participants, date); err != nil {
			t.Fatalf("insert channel %d: %v", id, err)
		}
	}

	insertChannel(sourceID, "source_"+suffix, 10_000, 1700000000)
	insertChannel(joinedID, "joined_"+suffix, 9_999, 1700000001)
	for i := 0; i < total; i++ {
		id := firstCandidateID + int64(i)
		insertChannel(id, fmt.Sprintf("candidate_%s_%03d", suffix, i), total-i, 1700000100+i)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO user_channel_member_index (user_id, channel_id, status, megagroup, broadcast, deleted)
VALUES
  ($1, $2, 'active', false, true, false),
  ($1, $3, 'active', false, true, false)`,
		owner.ID, sourceID, joinedID); err != nil {
		t.Fatalf("insert membership index: %v", err)
	}

	store := newTestChannelStore(pool)
	source, err := store.ListChannelRecommendations(ctx, domain.ChannelRecommendationsRequest{
		UserID:          owner.ID,
		SourceChannelID: sourceID,
		Limit:           domain.DefaultChannelRecommendationsLimit,
	})
	if err != nil {
		t.Fatalf("source recommendations: %v", err)
	}
	if source.Count != domain.MaxChannelRecommendationsLimit || len(source.Channels) != domain.DefaultChannelRecommendationsLimit {
		t.Fatalf("source recommendations count=%d len=%d, want bounded count %d len %d",
			source.Count, len(source.Channels), domain.MaxChannelRecommendationsLimit, domain.DefaultChannelRecommendationsLimit)
	}
	for _, ch := range source.Channels {
		if ch.ID == sourceID || !ch.Broadcast || ch.Megagroup || ch.Username == "" {
			t.Fatalf("source recommendation = %+v, want public broadcast excluding source", ch)
		}
	}

	global, err := store.ListChannelRecommendations(ctx, domain.ChannelRecommendationsRequest{
		UserID: owner.ID,
		Limit:  domain.DefaultChannelRecommendationsLimit,
	})
	if err != nil {
		t.Fatalf("global recommendations: %v", err)
	}
	for _, ch := range global.Channels {
		if ch.ID == sourceID || ch.ID == joinedID {
			t.Fatalf("global recommendation included joined channel %d", ch.ID)
		}
	}
	if global.Count != domain.MaxChannelRecommendationsLimit || len(global.Channels) != domain.DefaultChannelRecommendationsLimit {
		t.Fatalf("global recommendations count=%d len=%d, want bounded count %d len %d",
			global.Count, len(global.Channels), domain.MaxChannelRecommendationsLimit, domain.DefaultChannelRecommendationsLimit)
	}
}
