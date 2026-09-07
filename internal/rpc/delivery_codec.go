package rpc

import (
	"fmt"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

// encodeDeliveryUpdate is the Core/RPC-owned TL boundary for immutable durable
// delivery payloads. Store, app and Egress only handle the returned opaque bytes.
func encodeDeliveryUpdate(update tg.UpdatesClass) ([]byte, error) {
	if update == nil {
		return nil, fmt.Errorf("nil delivery update")
	}
	var b bin.Buffer
	if err := update.Encode(&b); err != nil {
		return nil, err
	}
	return b.Raw(), nil
}

func decodeDeliveryUpdate(raw []byte) (tg.UpdatesClass, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty delivery update")
	}
	return tg.DecodeUpdates(&bin.Buffer{Buf: raw})
}
