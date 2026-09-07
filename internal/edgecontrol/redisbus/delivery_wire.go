package redisbus

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/proto"

	"telesrv/internal/edgecontrol"
)

var deliveryCommandMagic = [4]byte{'E', 'D', 'C', 4}
var deliveryAdmissionMagic = [4]byte{'E', 'D', 'A', 3}

const (
	maxDeliveryWireBytes  = edgecontrol.MaxDeliveryBatchBytes + (edgecontrol.MaxDeliveryBatchItems * 128) + 4096
	maxDeliveryWireString = edgecontrol.MaxDeliveryInstanceIDBytes
)

func encodeDeliveryBatchWire(batch edgecontrol.DeliveryBatch) ([]byte, error) {
	if err := edgecontrol.ValidateDeliveryBatchEnvelope(batch); err != nil {
		return nil, err
	}
	if len(batch.Items) > edgecontrol.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("edge redis bus: delivery item count exceeds %d", edgecontrol.MaxDeliveryBatchItems)
	}
	size := 4 + 16 + 16 + 2 + len(batch.SourceInstanceID) + 2 + len(batch.TargetInstanceID) + 8 + 8 + 8 + 8 + 8 + 2
	for _, item := range batch.Items {
		size += 1 + 8 + 8 + 8 + 4 + 8 + 4 + 1 + 16 + 8 + 1 + 2 + 2 +
			8*(len(item.Channel.AudienceUsers)+len(item.Channel.AffectedUsers)) + 4 + len(item.UpdateBytes)
	}
	if size > maxDeliveryWireBytes {
		return nil, fmt.Errorf("edge redis bus: delivery command exceeds wire limit")
	}
	out := make([]byte, 0, size)
	out = append(out, deliveryCommandMagic[:]...)
	out = append(out, batch.BatchID[:]...)
	out = append(out, batch.CommandID[:]...)
	var err error
	if out, err = appendWireString(out, batch.SourceInstanceID); err != nil {
		return nil, err
	}
	if out, err = appendWireString(out, batch.TargetInstanceID); err != nil {
		return nil, err
	}
	out = appendI64(out, batch.TargetUserID)
	out = append(out, batch.ExcludeAuthKeyID[:]...)
	out = appendI64(out, batch.ExcludeSessionID)
	out = appendI64(out, batch.NotAfter.UnixNano())
	out = appendI64(out, batch.EvidenceDeadline.UnixNano())
	out = appendU16(out, uint16(len(batch.Items)))
	for _, item := range batch.Items {
		out = append(out, byte(item.Ref.Domain.Kind))
		out = appendI64(out, item.Ref.Domain.StreamID)
		out = appendI64(out, item.Ref.OutboxID)
		out = appendI64(out, item.Ref.TargetUserID)
		out = appendU32(out, uint32(item.Ref.PTS))
		out = appendU64(out, item.Ref.LeaseFence)
		out = appendU32(out, item.Ref.Attempt)
		out = append(out, byte(item.MessageType))
		out = append(out, item.PayloadHash[:]...)
		out = appendI64(out, item.Channel.ChannelID)
		out = append(out, byte(item.Channel.Audience))
		out = appendU16(out, uint16(len(item.Channel.AudienceUsers)))
		for _, userID := range item.Channel.AudienceUsers {
			out = appendI64(out, userID)
		}
		out = appendU16(out, uint16(len(item.Channel.AffectedUsers)))
		for _, userID := range item.Channel.AffectedUsers {
			out = appendI64(out, userID)
		}
		out = appendU32(out, uint32(len(item.UpdateBytes)))
		out = append(out, item.UpdateBytes...)
	}
	return out, nil
}

func decodeDeliveryBatchWire(raw string) (edgecontrol.DeliveryBatch, error) {
	var batch edgecontrol.DeliveryBatch
	if len(raw) > maxDeliveryWireBytes {
		return batch, fmt.Errorf("edge redis bus: delivery command exceeds wire limit")
	}
	r := deliveryWireReader{raw: []byte(raw)}
	if got, err := r.fixed(4); err != nil || string(got) != string(deliveryCommandMagic[:]) {
		return batch, fmt.Errorf("edge redis bus: invalid delivery command version")
	}
	copy(batch.BatchID[:], r.mustFixed(16))
	copy(batch.CommandID[:], r.mustFixed(16))
	var err error
	if batch.SourceInstanceID, err = r.str(); err != nil {
		return batch, err
	}
	if batch.TargetInstanceID, err = r.str(); err != nil {
		return batch, err
	}
	if batch.TargetUserID, err = r.i64(); err != nil {
		return batch, err
	}
	key, err := r.fixed(8)
	if err != nil {
		return batch, err
	}
	copy(batch.ExcludeAuthKeyID[:], key)
	if batch.ExcludeSessionID, err = r.i64(); err != nil {
		return batch, err
	}
	notAfter, err := r.i64()
	if err != nil {
		return batch, err
	}
	batch.NotAfter = time.Unix(0, notAfter).UTC()
	evidenceDeadline, err := r.i64()
	if err != nil {
		return batch, err
	}
	batch.EvidenceDeadline = time.Unix(0, evidenceDeadline).UTC()
	count, err := r.u16()
	if err != nil {
		return batch, err
	}
	if count == 0 || count > edgecontrol.MaxDeliveryBatchItems {
		return batch, fmt.Errorf("edge redis bus: invalid delivery item count")
	}
	batch.Items = make([]edgecontrol.DeliveryItem, int(count))
	for i := range batch.Items {
		kind, err := r.u8()
		if err != nil {
			return batch, err
		}
		stream, err := r.i64()
		if err != nil {
			return batch, err
		}
		outbox, err := r.i64()
		if err != nil {
			return batch, err
		}
		user, err := r.i64()
		if err != nil {
			return batch, err
		}
		pts, err := r.u32()
		if err != nil {
			return batch, err
		}
		fence, err := r.u64()
		if err != nil {
			return batch, err
		}
		attempt, err := r.u32()
		if err != nil {
			return batch, err
		}
		messageType, err := r.u8()
		if err != nil {
			return batch, err
		}
		hash, err := r.fixed(16)
		if err != nil {
			return batch, err
		}
		channelID, err := r.i64()
		if err != nil {
			return batch, err
		}
		audience, err := r.u8()
		if err != nil {
			return batch, err
		}
		audienceCount, err := r.u16()
		if err != nil || audienceCount > edgecontrol.MaxChannelDeliveryExplicitUsers {
			return batch, fmt.Errorf("edge redis bus: invalid channel audience user count")
		}
		audienceUsers := make([]int64, int(audienceCount))
		for j := range audienceUsers {
			if audienceUsers[j], err = r.i64(); err != nil {
				return batch, err
			}
		}
		affectedCount, err := r.u16()
		if err != nil || affectedCount > edgecontrol.MaxChannelDeliveryExplicitUsers {
			return batch, fmt.Errorf("edge redis bus: invalid channel affected user count")
		}
		affectedUsers := make([]int64, int(affectedCount))
		for j := range affectedUsers {
			if affectedUsers[j], err = r.i64(); err != nil {
				return batch, err
			}
		}
		length, err := r.u32()
		if err != nil {
			return batch, err
		}
		payload, err := r.fixed(int(length))
		if err != nil {
			return batch, err
		}
		item := &batch.Items[i]
		item.Ref = edgecontrol.DeliveryRef{
			Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueKind(kind), StreamID: stream},
			OutboxID: outbox, TargetUserID: user, PTS: int(int32(pts)), LeaseFence: fence, Attempt: attempt,
		}
		item.MessageType = proto.MessageType(messageType)
		copy(item.PayloadHash[:], hash)
		item.Channel = edgecontrol.ChannelDeliveryRoute{
			ChannelID: channelID, Audience: edgecontrol.ChannelDeliveryAudience(audience),
			AudienceUsers: audienceUsers, AffectedUsers: affectedUsers,
		}
		item.UpdateBytes = payload
	}
	if r.remaining() != 0 {
		return batch, fmt.Errorf("edge redis bus: trailing delivery command bytes")
	}
	if err := edgecontrol.ValidateDeliveryBatchEnvelope(batch); err != nil {
		return batch, err
	}
	return batch, nil
}

func encodeDeliveryAdmissionWire(value edgecontrol.DeliveryAdmission) ([]byte, error) {
	if !value.Valid() {
		return nil, fmt.Errorf("edge redis bus: invalid delivery admission")
	}
	out := make([]byte, 0, 80)
	out = append(out, deliveryAdmissionMagic[:]...)
	out = append(out, value.BatchID[:]...)
	out = append(out, value.CommandID[:]...)
	var err error
	if out, err = appendWireString(out, value.SourceInstanceID); err != nil {
		return nil, err
	}
	if out, err = appendWireString(out, value.TargetInstanceID); err != nil {
		return nil, err
	}
	out = append(out, byte(value.Outcome))
	out = appendU16(out, uint16(value.Detail))
	return out, nil
}

func decodeDeliveryAdmissionWire(raw string) (edgecontrol.DeliveryAdmission, error) {
	var value edgecontrol.DeliveryAdmission
	if len(raw) > 4096 {
		return value, fmt.Errorf("edge redis bus: delivery admission exceeds wire limit")
	}
	r := deliveryWireReader{raw: []byte(raw)}
	magic, err := r.fixed(4)
	if err != nil || string(magic) != string(deliveryAdmissionMagic[:]) {
		return value, fmt.Errorf("edge redis bus: invalid delivery admission version")
	}
	copy(value.BatchID[:], r.mustFixed(16))
	copy(value.CommandID[:], r.mustFixed(16))
	if value.SourceInstanceID, err = r.str(); err != nil {
		return value, err
	}
	if value.TargetInstanceID, err = r.str(); err != nil {
		return value, err
	}
	outcome, err := r.u8()
	if err != nil {
		return value, err
	}
	value.Outcome = edgecontrol.AdmissionOutcome(outcome)
	detail, err := r.u16()
	if err != nil {
		return value, err
	}
	value.Detail = edgecontrol.DetailCode(detail)
	if r.remaining() != 0 {
		return value, fmt.Errorf("edge redis bus: trailing admission bytes")
	}
	if _, err := encodeDeliveryAdmissionWire(value); err != nil {
		return value, err
	}
	return value, nil
}

func appendWireString(dst []byte, value string) ([]byte, error) {
	if len(value) == 0 || len(value) > maxDeliveryWireString {
		return nil, fmt.Errorf("edge redis bus: invalid wire string")
	}
	dst = appendU16(dst, uint16(len(value)))
	return append(dst, value...), nil
}
func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}
func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}
func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}
func appendI64(dst []byte, v int64) []byte { return appendU64(dst, uint64(v)) }

type deliveryWireReader struct {
	raw []byte
	off int
	err error
}

func (r *deliveryWireReader) remaining() int { return len(r.raw) - r.off }
func (r *deliveryWireReader) fixed(n int) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	if n < 0 || n > r.remaining() {
		r.err = fmt.Errorf("edge redis bus: truncated delivery wire")
		return nil, r.err
	}
	v := r.raw[r.off : r.off+n]
	r.off += n
	return v, nil
}
func (r *deliveryWireReader) mustFixed(n int) []byte {
	v, err := r.fixed(n)
	if err != nil {
		return make([]byte, n)
	}
	return v
}
func (r *deliveryWireReader) u8() (uint8, error) {
	v, e := r.fixed(1)
	if e != nil {
		return 0, e
	}
	return v[0], nil
}
func (r *deliveryWireReader) u16() (uint16, error) {
	v, e := r.fixed(2)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint16(v), nil
}
func (r *deliveryWireReader) u32() (uint32, error) {
	v, e := r.fixed(4)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(v), nil
}
func (r *deliveryWireReader) u64() (uint64, error) {
	v, e := r.fixed(8)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(v), nil
}
func (r *deliveryWireReader) i64() (int64, error) { v, e := r.u64(); return int64(v), e }
func (r *deliveryWireReader) str() (string, error) {
	n, e := r.u16()
	if e != nil {
		return "", e
	}
	if n == 0 || n > maxDeliveryWireString {
		return "", fmt.Errorf("edge redis bus: invalid wire string")
	}
	v, e := r.fixed(int(n))
	return string(v), e
}
