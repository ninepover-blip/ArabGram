package loadharness

import (
	"errors"

	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

type sendAttemptOutcome uint8

const (
	sendPending sendAttemptOutcome = iota
	sendAccepted
	sendRejected
	sendUncertain
)

func classifySendAttempt(err error) sendAttemptOutcome {
	if err == nil {
		return sendAccepted
	}
	// Audited before any mutation in onMessagesSendMessage. Do not infer
	// rollback from arbitrary RPC errors, cancellation, timeout or absence.
	if rpcErr, ok := tgerr.As(err); ok && rpcErr.Code == 400 && tgerr.Is(err, "RANDOM_ID_EMPTY", "MESSAGE_TOO_LONG", "ENTITIES_TOO_LONG") {
		return sendRejected
	}
	// Plain-text replay lookup precedes the send-rate gate, and the gate
	// precedes every mutation. Do not amplify FLOOD_WAIT with an immediate retry.
	if rpcErr, ok := tgerr.As(err); ok && rpcErr.Code == 420 && tgerr.Is(err, tgerr.ErrFloodWait) {
		return sendRejected
	}
	return sendUncertain
}

type deliveryMessage struct {
	marker     string
	id         int
	peerUserID int64
	fromUserID int64 // Zero means the optional from_id was omitted.
	out        bool
}

func privateDeliveryMessage(m *tg.Message) deliveryMessage {
	message := deliveryMessage{marker: m.Message, id: m.ID, out: m.Out}
	if peer, ok := m.PeerID.(*tg.PeerUser); ok {
		message.peerUserID = peer.UserID
	}
	if m.FromID != nil {
		message.fromUserID = -1 // A channel sender is invalid for this private-text workload.
		if from, ok := m.FromID.(*tg.PeerUser); ok {
			message.fromUserID = from.UserID
		}
	}
	return message
}

func validDeliveryMessage(message deliveryMessage, viewer, sender, recipient int64) bool {
	if message.id <= 0 || (message.fromUserID != 0 && message.fromUserID != sender) {
		return false
	}
	if viewer == sender {
		return message.out && message.peerUserID == recipient
	}
	return viewer == recipient && !message.out && message.peerUserID == sender
}

// Correlated rpc_result alone is insufficient: the current server contract
// returns both random-id acknowledgement and its matching sender message.
func validateSendConfirmation(result tg.UpdatesClass, marker string, randomID, sender, recipient int64) (int, error) {
	var updates []tg.UpdateClass
	switch value := result.(type) {
	case *tg.Updates:
		updates = value.Updates
	case *tg.UpdatesCombined:
		updates = value.Updates
	default:
		return 0, errors.New("send confirmation omitted expected updates envelope")
	}
	var id, acknowledgements, messages int
	var message deliveryMessage
	for _, update := range updates {
		switch value := update.(type) {
		case *tg.UpdateMessageID:
			if value.RandomID == randomID {
				id = value.ID
				acknowledgements++
			}
		case *tg.UpdateNewMessage:
			if m, ok := value.Message.(*tg.Message); ok && m.Message == marker {
				message = privateDeliveryMessage(m)
				messages++
			}
		}
	}
	if acknowledgements != 1 || messages != 1 || id != message.id || !validDeliveryMessage(message, sender, sender, recipient) {
		return 0, errors.New("send confirmation does not match the immutable private-message intent")
	}
	return id, nil
}
