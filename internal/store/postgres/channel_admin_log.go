package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
	"telesrv/internal/domain"
)

func (s *ChannelStore) ListAdminLog(ctx context.Context, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MaxID < 0 || req.MinID < 0 {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelAdminLogResult{}, err
	}
	if !isChannelAdmin(member) {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelAdminRequired
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelAdminLogLimit {
		limit = domain.MaxChannelAdminLogLimit
	}
	where := []string{"channel_id = $1"}
	args := []any{req.ChannelID}
	nextArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if req.MaxID > 0 {
		where = append(where, "id < "+nextArg(req.MaxID))
	}
	if req.MinID > 0 {
		where = append(where, "id > "+nextArg(req.MinID))
	}
	if len(req.AdminUserIDs) > 0 {
		where = append(where, "actor_user_id = ANY("+nextArg(int64s(req.AdminUserIDs))+"::bigint[])")
	}
	if types := adminLogEventTypesForFilter(req.Filter); len(types) > 0 {
		where = append(where, "event_type = ANY("+nextArg(types)+"::text[])")
	} else if !req.Filter.Empty() {
		return domain.ChannelAdminLogResult{Channel: channel}, nil
	}
	query := strings.ToLower(strings.TrimSpace(req.Query))
	if query != "" {
		like := adminLogLikePattern(query)
		where = append(where, `(lower(prev_string) LIKE `+nextArg(like)+` ESCAPE '\' OR lower(new_string) LIKE `+nextArg(like)+` ESCAPE '\' OR lower(query) LIKE `+nextArg(like)+` ESCAPE '\')`)
	}
	args = append(args, limit)
	rows, err := s.db.Query(ctx, `
SELECT channel_id, id, actor_user_id, event_date, event_type, prev_string, new_string, prev_bool, new_bool, prev_int, new_int,
       prev_participant::text, new_participant::text, participant::text, message::text, prev_message::text, new_message::text, query
FROM channel_admin_log_events
WHERE `+strings.Join(where, " AND ")+`
ORDER BY id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelAdminLogResult{}, fmt.Errorf("list channel admin log: %w", err)
	}
	defer rows.Close()
	events := make([]domain.ChannelAdminLogEvent, 0, limit)
	for rows.Next() {
		event, err := scanChannelAdminLogEvent(rows)
		if err != nil {
			return domain.ChannelAdminLogResult{}, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelAdminLogResult{}, err
	}
	return domain.ChannelAdminLogResult{Channel: channel, Events: events}, nil
}

func (s *ChannelStore) insertChannelAdminLogTx(ctx context.Context, tx pgx.Tx, event domain.ChannelAdminLogEvent) error {
	if event.ChannelID == 0 || event.UserID == 0 || event.Type == "" {
		return nil
	}
	if event.Date == 0 {
		event.Date = nowUnix()
	}
	id, err := nextChannelAdminLogIDTx(ctx, tx, event.ChannelID)
	if err != nil {
		return err
	}
	prevParticipant, err := marshalJSON(event.PrevParticipant, "{}")
	if err != nil {
		return err
	}
	newParticipant, err := marshalJSON(event.NewParticipant, "{}")
	if err != nil {
		return err
	}
	participant, err := marshalJSON(event.Participant, "{}")
	if err != nil {
		return err
	}
	message, err := marshalJSON(event.Message, "{}")
	if err != nil {
		return err
	}
	prevMessage, err := marshalJSON(event.PrevMessage, "{}")
	if err != nil {
		return err
	}
	newMessage, err := marshalJSON(event.NewMessage, "{}")
	if err != nil {
		return err
	}
	query := adminLogSearchText(event)
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_admin_log_events (
    channel_id, id, actor_user_id, event_date, event_type, prev_string, new_string,
    prev_bool, new_bool, prev_int, new_int, prev_participant, new_participant,
    participant, message, prev_message, new_message, query
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		event.ChannelID, id, event.UserID, event.Date, string(event.Type), event.PrevString, event.NewString,
		event.PrevBool, event.NewBool, event.PrevInt, event.NewInt, prevParticipant, newParticipant,
		participant, message, prevMessage, newMessage, query); err != nil {
		return fmt.Errorf("insert channel admin log: %w", err)
	}
	return nil
}

func nextChannelAdminLogIDTx(ctx context.Context, tx pgx.Tx, channelID int64) (int64, error) {
	var id int64
	if err := tx.QueryRow(ctx, `
UPDATE channels
SET admin_log_seq = admin_log_seq + 1, updated_at = now()
WHERE id = $1
RETURNING admin_log_seq`, channelID).Scan(&id); err != nil {
		return 0, fmt.Errorf("allocate channel admin log id: %w", err)
	}
	return id, nil
}

func scanChannelAdminLogEvent(row rowScanner) (domain.ChannelAdminLogEvent, error) {
	var event domain.ChannelAdminLogEvent
	var typ string
	var prevParticipant, newParticipant, participant, message, prevMessage, newMessage string
	if err := row.Scan(
		&event.ChannelID, &event.ID, &event.UserID, &event.Date, &typ,
		&event.PrevString, &event.NewString, &event.PrevBool, &event.NewBool,
		&event.PrevInt, &event.NewInt, &prevParticipant, &newParticipant, &participant,
		&message, &prevMessage, &newMessage, &event.Query,
	); err != nil {
		return domain.ChannelAdminLogEvent{}, err
	}
	event.Type = domain.ChannelAdminLogEventType(typ)
	event.PrevParticipant = decodeJSONPtr[domain.ChannelMember](prevParticipant)
	event.NewParticipant = decodeJSONPtr[domain.ChannelMember](newParticipant)
	event.Participant = decodeJSONPtr[domain.ChannelMember](participant)
	event.Message = decodeJSONPtr[domain.ChannelMessage](message)
	event.PrevMessage = decodeJSONPtr[domain.ChannelMessage](prevMessage)
	event.NewMessage = decodeJSONPtr[domain.ChannelMessage](newMessage)
	return event, nil
}
