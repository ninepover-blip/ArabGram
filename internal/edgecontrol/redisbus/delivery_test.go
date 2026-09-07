package redisbus

import (
	"context"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"

	"telesrv/internal/edgecontrol"
)

func testDeliveryBatch() edgecontrol.DeliveryBatch {
	payload := []byte{1, 2, 3, 4}
	notAfter := time.Now().Add(time.Minute).UTC()
	return edgecontrol.DeliveryBatch{BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", TargetUserID: 42, NotAfter: notAfter, EvidenceDeadline: notAfter.Add(time.Second), Items: []edgecontrol.DeliveryItem{{Ref: edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 9, TargetUserID: 42, PTS: 3, LeaseFence: 7, Attempt: 2}, MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload}}}
}

func TestDeliveryV4WireRoundTripCarriesBothDeadlinesAndRejectsOlderVersions(t *testing.T) {
	want := testDeliveryBatch()
	raw, err := encodeDeliveryBatchWire(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeDeliveryBatchWire(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.BatchID != want.BatchID || got.CommandID != want.CommandID ||
		!got.NotAfter.Equal(want.NotAfter) || !got.EvidenceDeadline.Equal(want.EvidenceDeadline) ||
		got.Items[0].Ref != want.Items[0].Ref || got.Items[0].MessageType != proto.MessageFromServer {
		t.Fatalf("roundtrip=%+v", got)
	}
	for _, legacy := range []string{"EDC\x03payload", "OBX\x02payload", "{\"CommandID\":\"old\"}"} {
		if _, err := decodeDeliveryBatchWire(legacy); err == nil {
			t.Fatalf("legacy wire accepted: %q", legacy)
		}
	}
}

func TestDeliveryAdmissionV3WireUsesTypedDetailOnly(t *testing.T) {
	want := edgecontrol.DeliveryAdmission{BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", Outcome: edgecontrol.AdmissionOverloaded, Detail: edgecontrol.DetailCapacity}
	raw, err := encodeDeliveryAdmissionWire(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeDeliveryAdmissionWire(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("admission=%+v want %+v", got, want)
	}
	if _, err := decodeDeliveryAdmissionWire("OBA\x02legacy-error-string"); err == nil {
		t.Fatal("legacy admission accepted")
	}
}

func TestDeliveryBusRequiresClient(t *testing.T) {
	bus := New(nil)
	if _, err := bus.SendDeliveryBatch(context.Background(), "edge-b", testDeliveryBatch()); err != ErrNilClient {
		t.Fatalf("SendDeliveryBatch err=%v", err)
	}
}
