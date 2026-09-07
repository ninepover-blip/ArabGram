package mtprotoedge

import (
	"fmt"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

// decodeDurableDeliveryUpdate is the receiver-owned TL boundary. Delivery
// Fabric and Egress carry only immutable bytes and their integrity hash.
func decodeDurableDeliveryUpdate(raw []byte) (tg.UpdatesClass, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty durable delivery update")
	}
	return tg.DecodeUpdates(&bin.Buffer{Buf: raw})
}
