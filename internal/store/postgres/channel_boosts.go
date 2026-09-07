package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

func (s *ChannelStore) GetPremiumBoostStatus(ctx context.Context, viewerUserID, channelID int64, now int) (domain.PremiumBoostStatus, error) {
	if viewerUserID == 0 || channelID == 0 {
		return domain.PremiumBoostStatus{}, domain.ErrChannelInvalid
	}
	if _, _, _, err := s.getChannelForViewer(ctx, s.db, viewerUserID, channelID); err != nil {
		return domain.PremiumBoostStatus{}, err
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	total, err := s.countActiveBoostsForPeer(ctx, s.db, peer, now)
	if err != nil {
		return domain.PremiumBoostStatus{}, err
	}
	my, err := listActiveUserBoostsForPeer(ctx, s.db, viewerUserID, peer, now)
	if err != nil {
		return domain.PremiumBoostStatus{}, err
	}
	return domain.PremiumBoostStatusForCount(peer, total, my), nil
}

func (s *ChannelStore) ListPremiumBoosts(ctx context.Context, viewerUserID, channelID int64, gifts bool, offset string, limit, now int) (domain.PremiumBoostList, error) {
	if viewerUserID == 0 || channelID == 0 {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxPremiumBoostsListLimit {
		limit = domain.MaxPremiumBoostsListLimit
	}
	_, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	if member.Role != domain.ChannelRoleCreator && member.Role != domain.ChannelRoleAdmin {
		return domain.PremiumBoostList{}, domain.ErrChannelAdminRequired
	}
	cursor, err := parseBoostCursor(offset)
	if err != nil {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	return listPremiumBoostsPage(ctx, s.db, domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}, gifts, cursor, limit, now)
}

func (s *ChannelStore) GetPremiumMyBoosts(ctx context.Context, userID int64, now, premiumUntil int) (domain.PremiumMyBoosts, error) {
	if userID == 0 {
		return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
	}
	return s.premiumMyBoosts(ctx, s.db, userID, now, premiumUntil)
}

func (s *ChannelStore) ApplyPremiumBoost(ctx context.Context, userID, channelID int64, slots []int, now, premiumUntil int) (domain.PremiumMyBoosts, error) {
	if userID == 0 || channelID == 0 || len(slots) == 0 {
		return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
	}
	if premiumUntil <= now {
		return domain.PremiumMyBoosts{}, domain.ErrPremiumRequired
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.PremiumMyBoosts{}, fmt.Errorf("apply premium boost: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.PremiumMyBoosts{}, fmt.Errorf("begin apply premium boost: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if _, _, err := s.getChannelForMember(ctx, tx, userID, channelID); err != nil {
		return domain.PremiumMyBoosts{}, err
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	invalidatedPeers := make(map[domain.Peer]struct{}, len(slots)+1)
	for _, slotID := range slots {
		if slotID != domain.DefaultPremiumBoostSlotID {
			return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
		}
		current, err := getPremiumBoostSlotForUpdate(ctx, tx, userID, slotID)
		if err != nil {
			return domain.PremiumMyBoosts{}, err
		}
		next, changed, err := domain.ApplyPremiumBoostSlot(
			current,
			userID,
			slotID,
			peer,
			now,
			premiumUntil,
			domain.DefaultPremiumBoostReassignCooldownSeconds,
		)
		if err != nil {
			return domain.PremiumMyBoosts{}, err
		}
		if !changed {
			continue
		}
		if current.Assigned(now) {
			invalidatedPeers[current.Peer] = struct{}{}
		}
		if next.Assigned(now) {
			invalidatedPeers[next.Peer] = struct{}{}
		}
		if err := upsertPremiumBoostSlot(ctx, tx, next); err != nil {
			return domain.PremiumMyBoosts{}, fmt.Errorf("upsert premium boost slot: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.PremiumMyBoosts{}, fmt.Errorf("commit apply premium boost: %w", err)
	}
	committed = true
	s.invalidateBoostCacheForUserAndPeers(userID, invalidatedPeers)
	return s.premiumMyBoosts(ctx, s.db, userID, now, premiumUntil)
}

func getPremiumBoostSlotForUpdate(ctx context.Context, db sqlcgen.DBTX, userID int64, slotID int) (domain.PremiumBoostSlot, error) {
	row := db.QueryRow(ctx, `
SELECT user_id, slot, peer_type, peer_id, assigned_at, expires_at, cooldown_until, multiplier,
       gift, giveaway, unclaimed, giveaway_msg_id, used_gift_slug, stars
FROM channel_boost_slots
WHERE user_id = $1 AND slot = $2
FOR UPDATE`, userID, slotID)
	slot, err := scanPremiumBoostSlot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.PremiumBoostSlot{}, nil
		}
		return domain.PremiumBoostSlot{}, fmt.Errorf("lock premium boost slot: %w", err)
	}
	return slot, nil
}

func upsertPremiumBoostSlot(ctx context.Context, db sqlcgen.DBTX, slot domain.PremiumBoostSlot) error {
	peerType := string(slot.Peer.Type)
	peerID := slot.Peer.ID
	if peerID == 0 {
		peerType = ""
	}
	_, err := db.Exec(ctx, `
INSERT INTO channel_boost_slots (
    user_id, slot, peer_type, peer_id, assigned_at, expires_at, cooldown_until, multiplier,
    gift, giveaway, unclaimed, giveaway_msg_id, used_gift_slug, stars
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (user_id, slot) DO UPDATE SET
    peer_type = EXCLUDED.peer_type,
    peer_id = EXCLUDED.peer_id,
    assigned_at = EXCLUDED.assigned_at,
    expires_at = EXCLUDED.expires_at,
    cooldown_until = EXCLUDED.cooldown_until,
    multiplier = EXCLUDED.multiplier,
    gift = EXCLUDED.gift,
    giveaway = EXCLUDED.giveaway,
    unclaimed = EXCLUDED.unclaimed,
    giveaway_msg_id = EXCLUDED.giveaway_msg_id,
    used_gift_slug = EXCLUDED.used_gift_slug,
    stars = EXCLUDED.stars,
    updated_at = now()`,
		slot.UserID,
		slot.Slot,
		peerType,
		peerID,
		slot.Date,
		slot.Expires,
		slot.CooldownUntil,
		slot.Multiplier,
		slot.Gift,
		slot.Giveaway,
		slot.Unclaimed,
		slot.GiveawayMsgID,
		slot.UsedGiftSlug,
		slot.Stars,
	)
	return err
}

func (s *ChannelStore) GetPremiumUserBoosts(ctx context.Context, viewerUserID, channelID, targetUserID int64, now int) (domain.PremiumBoostList, error) {
	if viewerUserID == 0 || channelID == 0 || targetUserID == 0 {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	_, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	if member.Role != domain.ChannelRoleCreator && member.Role != domain.ChannelRoleAdmin {
		return domain.PremiumBoostList{}, domain.ErrChannelAdminRequired
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	items, err := listActiveUserBoostsForPeer(ctx, s.db, targetUserID, peer, now)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	users, err := listUsersByIDs(ctx, s.db, []int64{targetUserID})
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	return domain.PremiumBoostList{Count: len(items), Boosts: items, Users: users}, nil
}

func (s *ChannelStore) SetBoostsToUnblockRestrictions(ctx context.Context, userID, channelID int64, boosts int) (domain.Channel, error) {
	if userID == 0 || channelID == 0 || boosts < 0 || boosts > domain.MaxChannelBoostsToUnblockRestrictions {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := s.db.Exec(ctx, `UPDATE channels SET boosts_unrestrict = $2, updated_at = now() WHERE id = $1`, channelID, boosts); err != nil {
		return domain.Channel{}, fmt.Errorf("set boosts unrestrict: %w", err)
	}
	channel.BoostsUnrestrict = boosts
	return channel, nil
}

func (s *ChannelStore) premiumMyBoosts(ctx context.Context, db sqlcgen.DBTX, userID int64, now, premiumUntil int) (domain.PremiumMyBoosts, error) {
	out := domain.PremiumMyBoosts{}
	if premiumUntil <= now {
		return out, nil
	}
	rows, err := db.Query(ctx, `
SELECT user_id, slot, peer_type, peer_id, assigned_at, expires_at, cooldown_until, multiplier,
       gift, giveaway, unclaimed, giveaway_msg_id, used_gift_slug, stars
FROM channel_boost_slots
WHERE user_id = $1
  AND (expires_at = 0 OR expires_at > $2)
ORDER BY slot ASC`, userID, now)
	if err != nil {
		return domain.PremiumMyBoosts{}, fmt.Errorf("list my boost slots: %w", err)
	}
	defer rows.Close()
	foundBase := false
	channelRefs := make(map[int64]struct{})
	for rows.Next() {
		slot, err := scanPremiumBoostSlot(rows)
		if err != nil {
			return domain.PremiumMyBoosts{}, err
		}
		if slot.Slot == domain.DefaultPremiumBoostSlotID {
			foundBase = true
			if slot.Expires < premiumUntil {
				slot.Expires = premiumUntil
			}
		}
		out.Slots = append(out.Slots, slot)
		if slot.Peer.Type == domain.PeerTypeChannel && slot.Peer.ID != 0 {
			channelRefs[slot.Peer.ID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return domain.PremiumMyBoosts{}, err
	}
	if !foundBase {
		out.Slots = append(out.Slots, domain.PremiumBoostSlot{
			UserID:     userID,
			Slot:       domain.DefaultPremiumBoostSlotID,
			Multiplier: 1,
		})
	}
	if len(channelRefs) > 0 {
		channels, err := listChannelsByIDsInOrder(ctx, db, mapKeysInt64(channelRefs))
		if err != nil {
			return domain.PremiumMyBoosts{}, err
		}
		out.Channels = channels
	}
	return out, nil
}

func countActiveBoostsForPeer(ctx context.Context, db sqlcgen.DBTX, peer domain.Peer, now int) (int, error) {
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return 0, domain.ErrChannelInvalid
	}
	var total int
	if err := db.QueryRow(ctx, `
SELECT COALESCE(SUM(multiplier), 0)::int
FROM channel_boost_slots
WHERE peer_type = $1
  AND peer_id = $2
  AND (expires_at = 0 OR expires_at > $3)`, string(peer.Type), peer.ID, now).Scan(&total); err != nil {
		return 0, fmt.Errorf("count peer boosts: %w", err)
	}
	return total, nil
}

func (s *ChannelStore) countActiveBoostsForPeer(ctx context.Context, db sqlcgen.DBTX, peer domain.Peer, now int) (int, error) {
	if !s.boostCacheActive(db) {
		return countActiveBoostsForPeer(ctx, db, peer, now)
	}
	return s.boostCache.getPeerTotalOrLoad(ctx, peer, func() (int, error) {
		return countActiveBoostsForPeer(ctx, db, peer, now)
	})
}

func countActiveUserBoostsForPeer(ctx context.Context, db sqlcgen.DBTX, userID int64, peer domain.Peer, now int) (int, error) {
	if userID == 0 || peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return 0, domain.ErrChannelInvalid
	}
	var total int
	if err := db.QueryRow(ctx, `
SELECT COALESCE(SUM(multiplier), 0)::int
FROM channel_boost_slots
WHERE user_id = $1
  AND peer_type = $2
  AND peer_id = $3
  AND (expires_at = 0 OR expires_at > $4)`, userID, string(peer.Type), peer.ID, now).Scan(&total); err != nil {
		return 0, fmt.Errorf("count user peer boosts: %w", err)
	}
	return total, nil
}

func (s *ChannelStore) countActiveUserBoostsForPeer(ctx context.Context, db sqlcgen.DBTX, userID int64, peer domain.Peer, now int) (int, error) {
	if !s.boostCacheActive(db) {
		return countActiveUserBoostsForPeer(ctx, db, userID, peer, now)
	}
	return s.boostCache.getOrLoad(ctx, userID, peer, func() (int, error) {
		return countActiveUserBoostsForPeer(ctx, db, userID, peer, now)
	})
}

func countActiveUserBoostsForChannels(ctx context.Context, db sqlcgen.DBTX, userID int64, channelIDs []int64, now int) (map[int64]int, error) {
	out := make(map[int64]int)
	ids := uniqueNonZeroInt64s(channelIDs...)
	if userID == 0 || len(ids) == 0 {
		return out, nil
	}
	rows, err := db.Query(ctx, `
SELECT peer_id, COALESCE(SUM(multiplier), 0)::int
FROM channel_boost_slots
WHERE user_id = $1
  AND peer_type = 'channel'
  AND peer_id = ANY($2::bigint[])
  AND (expires_at = 0 OR expires_at > $3)
GROUP BY peer_id`, userID, ids, now)
	if err != nil {
		return nil, fmt.Errorf("count user channel boosts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var channelID int64
		var boosts int
		if err := rows.Scan(&channelID, &boosts); err != nil {
			return nil, err
		}
		out[channelID] = boosts
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) countActiveUserBoostsForChannels(ctx context.Context, db sqlcgen.DBTX, userID int64, channelIDs []int64, now int) (map[int64]int, error) {
	if !s.boostCacheActive(db) {
		return countActiveUserBoostsForChannels(ctx, db, userID, channelIDs, now)
	}
	out := make(map[int64]int)
	misses := make([]int64, 0, len(channelIDs))
	for _, channelID := range uniqueNonZeroInt64s(channelIDs...) {
		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
		if boosts, ok := s.boostCache.get(userID, peer); ok {
			out[channelID] = boosts
			continue
		}
		misses = append(misses, channelID)
	}
	if len(misses) == 0 {
		return out, nil
	}
	loaded, err := countActiveUserBoostsForChannels(ctx, db, userID, misses, now)
	if err != nil {
		return nil, err
	}
	for _, channelID := range misses {
		boosts := loaded[channelID]
		out[channelID] = boosts
		s.boostCache.put(userID, domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}, boosts)
	}
	return out, nil
}

func (s *ChannelStore) invalidateBoostCacheForUserAndPeers(userID int64, peers map[domain.Peer]struct{}) {
	if s.boostCache == nil {
		return
	}
	s.boostCache.deleteUser(userID)
	for peer := range peers {
		s.boostCache.deletePeerTotal(peer)
	}
}

func listActiveUserBoostsForPeer(ctx context.Context, db sqlcgen.DBTX, userID int64, peer domain.Peer, now int) ([]domain.PremiumBoostSlot, error) {
	rows, err := db.Query(ctx, `
SELECT user_id, slot, peer_type, peer_id, assigned_at, expires_at, cooldown_until, multiplier,
       gift, giveaway, unclaimed, giveaway_msg_id, used_gift_slug, stars
FROM channel_boost_slots
WHERE user_id = $1
  AND peer_type = $2
  AND peer_id = $3
  AND (expires_at = 0 OR expires_at > $4)
ORDER BY assigned_at DESC, user_id ASC, slot ASC`, userID, string(peer.Type), peer.ID, now)
	if err != nil {
		return nil, fmt.Errorf("list user boosts for peer: %w", err)
	}
	defer rows.Close()
	return scanPremiumBoostSlots(rows)
}

type boostCursor struct {
	assignedAt int
	userID     int64
	slot       int
	set        bool
}

func listPremiumBoostsPage(ctx context.Context, db sqlcgen.DBTX, peer domain.Peer, gifts bool, cursor boostCursor, limit, now int) (domain.PremiumBoostList, error) {
	var count int
	if err := db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_boost_slots
WHERE peer_type = $1
  AND peer_id = $2
  AND (expires_at = 0 OR expires_at > $3)
  AND (NOT $4::boolean OR gift OR giveaway)`, string(peer.Type), peer.ID, now, gifts).Scan(&count); err != nil {
		return domain.PremiumBoostList{}, fmt.Errorf("count premium boosts: %w", err)
	}
	args := []any{string(peer.Type), peer.ID, now, gifts, limit + 1}
	cursorSQL := ""
	if cursor.set {
		args = append(args, cursor.assignedAt, cursor.userID, cursor.slot)
		cursorSQL = fmt.Sprintf(` AND (
    assigned_at < $%d
    OR (assigned_at = $%d AND user_id > $%d)
    OR (assigned_at = $%d AND user_id = $%d AND slot > $%d)
  )`, len(args)-2, len(args)-2, len(args)-1, len(args)-2, len(args)-1, len(args))
	}
	rows, err := db.Query(ctx, `
SELECT user_id, slot, peer_type, peer_id, assigned_at, expires_at, cooldown_until, multiplier,
       gift, giveaway, unclaimed, giveaway_msg_id, used_gift_slug, stars
FROM channel_boost_slots
WHERE peer_type = $1
  AND peer_id = $2
  AND (expires_at = 0 OR expires_at > $3)
  AND (NOT $4::boolean OR gift OR giveaway)`+cursorSQL+`
ORDER BY assigned_at DESC, user_id ASC, slot ASC
LIMIT $5`, args...)
	if err != nil {
		return domain.PremiumBoostList{}, fmt.Errorf("list premium boosts: %w", err)
	}
	defer rows.Close()
	items, err := scanPremiumBoostSlots(rows)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = formatBoostCursor(last)
		items = items[:limit]
	}
	userIDs := make([]int64, 0, len(items))
	for _, item := range items {
		userIDs = append(userIDs, item.UserID)
	}
	users, err := listUsersByIDs(ctx, db, uniqueNonZeroInt64s(userIDs...))
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	return domain.PremiumBoostList{Count: count, Boosts: items, Users: users, NextOffset: next}, nil
}

func scanPremiumBoostSlots(rows pgx.Rows) ([]domain.PremiumBoostSlot, error) {
	items := make([]domain.PremiumBoostSlot, 0)
	for rows.Next() {
		slot, err := scanPremiumBoostSlot(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, slot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func scanPremiumBoostSlot(row rowScanner) (domain.PremiumBoostSlot, error) {
	var slot domain.PremiumBoostSlot
	var peerType string
	if err := row.Scan(
		&slot.UserID, &slot.Slot, &peerType, &slot.Peer.ID, &slot.Date, &slot.Expires, &slot.CooldownUntil, &slot.Multiplier,
		&slot.Gift, &slot.Giveaway, &slot.Unclaimed, &slot.GiveawayMsgID, &slot.UsedGiftSlug, &slot.Stars,
	); err != nil {
		return domain.PremiumBoostSlot{}, err
	}
	slot.Peer.Type = domain.PeerType(peerType)
	if slot.Peer.Type == "" || slot.Peer.ID == 0 {
		slot.Peer = domain.Peer{}
	}
	return slot, nil
}

func parseBoostCursor(raw string) (boostCursor, error) {
	if raw == "" {
		return boostCursor{}, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return boostCursor{}, fmt.Errorf("invalid boost cursor")
	}
	assignedAt, err := strconv.Atoi(parts[0])
	if err != nil || assignedAt < 0 {
		return boostCursor{}, fmt.Errorf("invalid boost cursor")
	}
	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || userID <= 0 {
		return boostCursor{}, fmt.Errorf("invalid boost cursor")
	}
	slot, err := strconv.Atoi(parts[2])
	if err != nil || slot <= 0 {
		return boostCursor{}, fmt.Errorf("invalid boost cursor")
	}
	return boostCursor{assignedAt: assignedAt, userID: userID, slot: slot, set: true}, nil
}

func formatBoostCursor(slot domain.PremiumBoostSlot) string {
	return fmt.Sprintf("%d:%d:%d", slot.Date, slot.UserID, slot.Slot)
}
