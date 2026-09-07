package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPrivatePollMutationCommitsAbsoluteDeliveryInSameTransactionPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	owner, err := users.Create(ctx, domain.User{AccessHash: 81, Phone: "+1894" + suffix + "01", FirstName: "PollOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	voter, err := users.Create(ctx, domain.User{AccessHash: 82, Phone: "+1894" + suffix + "02", FirstName: "PollVoter"})
	if err != nil {
		t.Fatalf("create voter: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, voter.ID})
	})

	pollID := int64(9_400_000_000) + owner.ID%1_000_000
	optionA, optionB := []byte{1}, []byte{2}
	if err := NewPollStore(pool).CreatePoll(ctx, domain.PollDefinition{
		ID: pollID, CreatorUserID: owner.ID, Options: [][]byte{optionA, optionB}, PublicVoters: true,
	}); err != nil {
		t.Fatalf("create poll: %v", err)
	}
	messages := newTestMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: owner.ID, RecipientUserID: voter.ID, RandomID: pollID, Date: 1_700_002_001,
		Media: &domain.MessageMedia{Kind: domain.MessageMediaKindPoll, Poll: &domain.MessagePoll{
			ID: pollID, Question: "Q?", PublicVoters: true,
			Answers: []domain.MessagePollAnswer{{Text: "A", Option: optionA}, {Text: "B", Option: optionB}},
		}},
	})
	if err != nil {
		t.Fatalf("send private poll: %v", err)
	}
	var beforeOwnerPTS, beforeVoterPTS int
	if err := pool.QueryRow(ctx, `SELECT
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $1), 0),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0)`, owner.ID, voter.ID).Scan(&beforeOwnerPTS, &beforeVoterPTS); err != nil {
		t.Fatalf("load pts before vote: %v", err)
	}
	var originAuthKeyID [8]byte
	originAuthKeyID[0] = 9
	res, err := messages.VoteMessagePoll(ctx, domain.VotePrivateMessagePollRequest{
		UserID: voter.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		MessageID: sent.RecipientMessage.ID, Options: [][]byte{optionA}, Date: 1_700_002_002,
	}, func(result domain.PrivateMessagePollResult) ([]store.DeliveryEffect, error) {
		effects := make([]store.DeliveryEffect, 0, len(result.Messages))
		for _, msg := range result.Messages {
			authKeyID, sessionID := [8]byte{}, int64(0)
			if msg.OwnerUserID == voter.ID {
				authKeyID, sessionID = originAuthKeyID, 77
			}
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: msg.OwnerUserID, ExcludeAuthKeyID: authKeyID, ExcludeSessionID: sessionID,
				Payload: []byte{byte(msg.ID)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
		return effects, nil
	})
	if err != nil {
		t.Fatalf("vote private poll: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("poll owner projections = %d, want 2", len(res.Messages))
	}

	var deliveryRows, eventRows, dispatchRows, afterOwnerPTS, afterVoterPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
  (SELECT count(*) FROM user_update_events WHERE user_id = ANY($1::bigint[]) AND event_type = 'message_poll'),
  (SELECT count(*) FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[]) AND event_type = 'message_poll'),
	  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0),
	  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $3), 0)`, []int64{owner.ID, voter.ID}, owner.ID, voter.ID).
		Scan(&deliveryRows, &eventRows, &dispatchRows, &afterOwnerPTS, &afterVoterPTS); err != nil {
		t.Fatalf("count poll durable rows: %v", err)
	}
	if deliveryRows != 2 || eventRows != 0 || dispatchRows != 0 {
		t.Fatalf("poll durable rows = delivery %d events %d dispatch %d, want 2/0/0", deliveryRows, eventRows, dispatchRows)
	}
	if afterOwnerPTS != beforeOwnerPTS || afterVoterPTS != beforeVoterPTS {
		t.Fatalf("poll changed account pts owner %d->%d voter %d->%d", beforeOwnerPTS, afterOwnerPTS, beforeVoterPTS, afterVoterPTS)
	}
	var excludedAuthKeyID []byte
	var excludedSessionID int64
	if err := pool.QueryRow(ctx, `SELECT exclude_auth_key_id, exclude_session_id
FROM edge_delivery_outbox WHERE target_user_id = $1 ORDER BY id DESC LIMIT 1`, voter.ID).
		Scan(&excludedAuthKeyID, &excludedSessionID); err != nil {
		t.Fatalf("load voter poll delivery: %v", err)
	}
	storedAuthKeyID, authErr := outboxAuthKeyID(excludedAuthKeyID)
	if authErr != nil || storedAuthKeyID != originAuthKeyID || excludedSessionID != 77 {
		t.Fatalf("voter poll exclusion = auth %x session %d err=%v", excludedAuthKeyID, excludedSessionID, authErr)
	}

	var beforeOptions string
	if err := pool.QueryRow(ctx, `SELECT options::text FROM poll_votes WHERE poll_id = $1 AND user_id = $2`, pollID, voter.ID).Scan(&beforeOptions); err != nil {
		t.Fatalf("load vote before rollback probe: %v", err)
	}
	_, err = messages.VoteMessagePoll(ctx, domain.VotePrivateMessagePollRequest{
		UserID: voter.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		MessageID: sent.RecipientMessage.ID, Options: [][]byte{optionB}, Date: 1_700_002_003,
	}, func(domain.PrivateMessagePollResult) ([]store.DeliveryEffect, error) {
		return nil, errors.New("projection failed")
	})
	if err == nil {
		t.Fatal("vote with failing delivery builder succeeded")
	}
	var afterOptions string
	if err := pool.QueryRow(ctx, `SELECT options::text FROM poll_votes WHERE poll_id = $1 AND user_id = $2`, pollID, voter.ID).Scan(&afterOptions); err != nil {
		t.Fatalf("load vote after rollback probe: %v", err)
	}
	if afterOptions != beforeOptions {
		t.Fatalf("poll vote survived failed delivery builder: before %s after %s", beforeOptions, afterOptions)
	}
}
