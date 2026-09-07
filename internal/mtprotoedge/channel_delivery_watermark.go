package mtprotoedge

import "telesrv/internal/edgecontrol"

type channelDeliveryWatermarkReservation struct {
	conn        *Conn
	delivery    edgecontrol.ChannelDeliveryWatermark
	previous    int
	hadPrevious bool
	active      bool
}

func (c *Conn) claimChannelDeliveryWatermark(delivery edgecontrol.ChannelDeliveryWatermark) bool {
	claim, ok := c.reserveChannelDeliveryWatermark(delivery)
	if ok {
		claim.commit()
	}
	return ok
}

func (c *Conn) reserveChannelDeliveryWatermark(delivery edgecontrol.ChannelDeliveryWatermark) (channelDeliveryWatermarkReservation, bool) {
	if c == nil || !delivery.Present() {
		return channelDeliveryWatermarkReservation{}, true
	}
	if !delivery.Valid() {
		return channelDeliveryWatermarkReservation{}, false
	}
	c.channelWatermarkMu.Lock()
	defer c.channelWatermarkMu.Unlock()

	table := c.channelDeliveryWatermarkTableLocked(delivery.Kind, true)
	if table == nil {
		return channelDeliveryWatermarkReservation{}, false
	}

	current, exists := table[delivery.ChannelID]
	if current >= delivery.MinPts {
		return channelDeliveryWatermarkReservation{}, false
	}
	if !exists && len(table) >= maxChannelDeliveryWatermarksPerSession {
		for channelID := range table {
			delete(table, channelID)
			break
		}
	}
	table[delivery.ChannelID] = delivery.MaxPts
	return channelDeliveryWatermarkReservation{
		conn:        c,
		delivery:    delivery,
		previous:    current,
		hadPrevious: exists,
		active:      true,
	}, true
}

func (r *channelDeliveryWatermarkReservation) commit() {
	if r != nil {
		r.active = false
	}
}

func (r *channelDeliveryWatermarkReservation) rollback() {
	if r == nil || !r.active || r.conn == nil || !r.delivery.Valid() {
		return
	}
	r.conn.channelWatermarkMu.Lock()
	defer r.conn.channelWatermarkMu.Unlock()
	table := r.conn.channelDeliveryWatermarkTableLocked(r.delivery.Kind, false)
	if table == nil {
		r.active = false
		return
	}
	current, exists := table[r.delivery.ChannelID]
	if exists && current == r.delivery.MaxPts {
		if r.hadPrevious {
			table[r.delivery.ChannelID] = r.previous
		} else {
			delete(table, r.delivery.ChannelID)
		}
	}
	r.active = false
}

func (c *Conn) channelDeliveryWatermarkTableLocked(kind edgecontrol.ChannelDeliveryKind, create bool) map[int64]int {
	switch kind {
	case edgecontrol.ChannelDeliveryPayload:
		if c.channelPayloadPts == nil && create {
			c.channelPayloadPts = make(map[int64]int)
		}
		return c.channelPayloadPts
	case edgecontrol.ChannelDeliveryNudge:
		if c.channelNudgePts == nil && create {
			c.channelNudgePts = make(map[int64]int)
		}
		return c.channelNudgePts
	default:
		return nil
	}
}

func (c *Conn) clearChannelDeliveryWatermarks() {
	if c == nil {
		return
	}
	c.channelWatermarkMu.Lock()
	c.channelPayloadPts = nil
	c.channelNudgePts = nil
	c.channelWatermarkMu.Unlock()
}
