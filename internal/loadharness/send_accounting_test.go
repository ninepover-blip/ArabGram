package loadharness

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

func observeTestDelivery(tracker *deliveryTracker, marker string, device int, source deliverySource) {
	e := deliveryExpectation{}
	tracker.mu.Lock()
	if key, ok := tracker.key(marker); ok {
		if record, found := tracker.load(key); found {
			e = record.expectation
		}
	}
	tracker.mu.Unlock()
	viewer := tracker.devices[device].UserID
	message := deliveryMessage{marker: marker, id: 1, peerUserID: e.senderUserID, fromUserID: e.senderUserID}
	if viewer == e.senderUserID {
		message.out = true
		message.peerUserID = e.targetUserID
	}
	tracker.observe(message, device, source, 1)
}

func testSendConfirmation(request *tg.MessagesSendMessageRequest, id int) tg.UpdatesClass {
	peer := request.Peer.(*tg.InputPeerUser)
	return &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateMessageID{RandomID: request.RandomID, ID: id},
		&tg.UpdateNewMessage{Message: &tg.Message{ID: id, Message: request.Message, Out: true, PeerID: &tg.PeerUser{UserID: peer.UserID}}, Pts: 1, PtsCount: 1},
	}}
}

func TestSendRetriesPreserveIntentAndExposeInitialUncertainty(t *testing.T) {
	tracker := testDeliveryTracker(t, "uncertain")
	worker := &loadWorker{record: SessionRecord{Index: 0, UserID: 100}, target: SessionRecord{UserID: 200, AccessHash: 22},
		delivery: tracker, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
	var first *tg.MessagesSendMessageRequest
	calls := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request := input.(*tg.MessagesSendMessageRequest)
		calls++
		if calls == 1 {
			first = request
			return context.DeadlineExceeded
		}
		if request == first || request.RandomID != first.RandomID || request.Message != first.Message || *request.Peer.(*tg.InputPeerUser) != *first.Peer.(*tg.InputPeerUser) {
			t.Fatal("retry changed immutable intent or reused mutable TL object")
		}
		output.(*tg.UpdatesBox).Updates = testSendConfirmation(request, 7)
		tracker.observe(deliveryMessage{marker: request.Message, id: 9, peerUserID: 100}, 1, deliveryLive, 1)
		return nil
	}))
	now := time.Now()
	worker.sendMessage(context.Background(), raw, messageArrival{plannedAt: now.Add(-time.Second), enqueuedAt: now})
	report := tracker.report()
	metrics := worker.metrics.report()
	if calls != 2 || report.AttemptedMessages != 1 || report.InitialUncertain != 1 || report.RetryConfirmed != 1 || report.CommittedAfterUncertainty != 1 || report.UnresolvedMessages != 0 || report.LiveDelivered != 1 || report.PlannedArrivalP99MS < 1000 {
		t.Fatalf("uncertain retry accounting: %+v", report)
	}
	if metrics["messages.sendMessage"].Timeouts != 1 || metrics["messages.sendMessage.retry"].Count != 1 || worker.counters.messageCompleted.Load() != 1 {
		t.Fatalf("retry hid first error: %+v", metrics)
	}
}

func TestSendUnresolvedIsNotAssumedRolledBack(t *testing.T) {
	tracker := testDeliveryTracker(t, "uncertain")
	worker := &loadWorker{record: SessionRecord{Index: 0, UserID: 100}, target: SessionRecord{UserID: 200},
		delivery: tracker, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
	raw := tg.NewClient(arrivalInvoker(func(context.Context, bin.Encoder, bin.Decoder) error { return context.DeadlineExceeded }))
	worker.sendMessage(context.Background(), raw, messageArrival{})
	report := tracker.report()
	if report.AttemptedMessages != 1 || report.InitialUncertain != 1 || report.RetryUncertain != 1 || report.UnresolvedMessages != 1 || report.NotCommittedMessages != 0 {
		t.Fatalf("timeout inferred rollback: %+v", report)
	}
	marker := tracker.marker(0, 1)
	tracker.observe(deliveryMessage{marker: marker, id: 7, out: true, peerUserID: 200}, 0, deliveryDifference, 1)
	report = tracker.report()
	if report.CommittedMessages != 1 || report.CommittedByObservationOnly != 1 || report.UnresolvedMessages != 0 || report.UnmatchedMarkers != 0 || report.Missing != 1 {
		t.Fatalf("difference commit not retained: %+v", report)
	}
}

func TestRejectionCannotEraseEarlierUncertainty(t *testing.T) {
	for _, ambiguousFirst := range []bool{false, true} {
		tracker := testDeliveryTracker(t, "run")
		marker := beginTestDelivery(t, tracker, 0, 200, 1)
		if ambiguousFirst {
			tracker.finish(marker, sendUncertain, false, 0)
			tracker.finish(marker, sendRejected, true, 0)
		} else {
			tracker.finish(marker, sendRejected, false, 0)
		}
		report := tracker.report()
		if ambiguousFirst && (report.UnresolvedMessages != 1 || report.NotCommittedMessages != 0) {
			t.Fatalf("prior uncertainty erased: %+v", report)
		}
		if !ambiguousFirst && (report.NotCommittedMessages != 1 || report.UnresolvedMessages != 0) {
			t.Fatalf("definite rejection lost: %+v", report)
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, errors.New("transport closed"), tgerr.New(500, "INTERNAL"), tgerr.New(400, "UNREVIEWED_ERROR"), tgerr.New(500, "MESSAGE_TOO_LONG"), tgerr.New(400, "FLOOD_WAIT_1"), tgerr.New(420, "FLOOD_PREMIUM_WAIT_1")} {
		if classifySendAttempt(err) != sendUncertain {
			t.Fatalf("unsafe rejection classification: %v", err)
		}
	}
	for _, err := range []error{tgerr.New(400, "MESSAGE_TOO_LONG"), tgerr.New(420, "FLOOD_WAIT_1")} {
		if classifySendAttempt(err) != sendRejected {
			t.Fatalf("audited pre-mutation rejection not classified: %v", err)
		}
	}
}

func TestSendConfirmationRequiresExactRandomIDMessageAndPeer(t *testing.T) {
	req := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerUser{UserID: 200}, Message: "intent", RandomID: 99}
	for _, fault := range []string{"none", "random", "id", "peer", "direction", "body", "nil"} {
		var response tg.UpdatesClass = testSendConfirmation(req, 7)
		if fault == "nil" {
			response = nil
		} else {
			updates := response.(*tg.Updates).Updates
			ack := updates[0].(*tg.UpdateMessageID)
			message := updates[1].(*tg.UpdateNewMessage).Message.(*tg.Message)
			switch fault {
			case "random":
				ack.RandomID++
			case "id":
				message.ID++
			case "peer":
				message.PeerID = &tg.PeerUser{UserID: 300}
			case "direction":
				message.Out = false
			case "body":
				message.Message = "another intent"
			}
		}
		_, err := validateSendConfirmation(response, req.Message, req.RandomID, 100, 200)
		if (err == nil) != (fault == "none") {
			t.Fatalf("fault=%s err=%v", fault, err)
		}
	}
}

func TestObservationRejectsWrongEnvelopeAndConflictingOwnerIDs(t *testing.T) {
	tracker := testDeliveryTracker(t, "run", SessionRecord{Index: 3, AccountIndex: 1, DeviceIndex: 1, UserID: 200})
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(marker, sendUncertain, false, 0)
	tracker.observe(deliveryMessage{marker: marker, id: 9, peerUserID: 300}, 1, deliveryLive, 1)
	if r := tracker.report(); r.CommittedMessages != 0 || r.InvalidMessageObserved != 1 {
		t.Fatalf("wrong peer became commit proof: %+v", r)
	}
	tracker.observe(deliveryMessage{marker: marker, id: 9, peerUserID: 100}, 1, deliveryLive, 1)
	tracker.observe(deliveryMessage{marker: marker, id: 10, peerUserID: 100}, 3, deliveryLive, 1)
	if r := tracker.report(); r.CommittedMessages != 1 || r.MessageIDConflicts != 1 || r.LiveDelivered != 1 || r.Missing != 1 {
		t.Fatalf("conflicting device IDs hidden: %+v", r)
	}
}

func TestRejectedIntentCannotHideObservedCommitInEitherOrder(t *testing.T) {
	for _, observedFirst := range []bool{false, true} {
		tracker := testDeliveryTracker(t, "run")
		marker := beginTestDelivery(t, tracker, 0, 200, 1)
		if observedFirst {
			observeTestDelivery(tracker, marker, 1, deliveryLive)
		}
		tracker.finish(marker, sendRejected, false, 0)
		if !observedFirst {
			observeTestDelivery(tracker, marker, 1, deliveryLive)
		}
		if r := tracker.report(); r.CommittedMessages != 1 || r.NotCommittedMessages != 0 || r.CommitContradictions != 1 {
			t.Fatalf("observedFirst=%v contradiction erased: %+v", observedFirst, r)
		}
	}
}

func TestUnresolvedIntentSurvivesEmptyDifference(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(marker, sendUncertain, false, 0)
	worker := &loadWorker{record: SessionRecord{Index: 0, UserID: 100}, delivery: tracker, metrics: newMetricSet(), operationTimeout: time.Second}
	worker.deliveryState.store(tg.UpdatesState{Pts: 1, Qts: 1, Date: 1})
	calls := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		if _, ok := in.(*tg.UpdatesGetDifferenceRequest); !ok {
			t.Fatalf("unexpected RPC %T", in)
		}
		calls++
		out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: 2}
		return nil
	}))
	worker.catchUpDelivery(context.Background(), raw)
	if r := tracker.report(); calls != 1 || r.UnresolvedMessages != 1 || r.NotCommittedMessages != 0 || r.CommittedMessages != 0 {
		t.Fatalf("empty difference inferred rollback: calls=%d report=%+v", calls, r)
	}
}
