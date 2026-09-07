// Package tonaddr canonicalizes TON friendly and raw account addresses at
// configuration and chain boundaries.
package tonaddr

import (
	"fmt"
	"strings"

	"github.com/xssnick/tonutils-go/address"

	"telesrv/internal/domain"
)

func Parse(value string, network domain.TONNetwork) (*address.Address, error) {
	value = strings.TrimSpace(value)
	var (
		addr *address.Address
		err  error
	)
	if strings.Contains(value, ":") {
		addr, err = address.ParseRawAddr(value)
	} else {
		addr, err = address.ParseAddr(value)
	}
	if err != nil || addr == nil || addr.IsAddrNone() {
		return nil, fmt.Errorf("invalid TON address")
	}
	if network == domain.TONNetworkMainnet && addr.IsTestnetOnly() {
		return nil, fmt.Errorf("testnet-only TON address is forbidden on mainnet")
	}
	return addr, nil
}
