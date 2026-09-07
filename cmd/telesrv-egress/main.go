// Command telesrv-egress starts the dedicated Durable Egress outbox service.
package main

import (
	"telesrv/internal/node/common"
	egressnode "telesrv/internal/node/egress"
)

func main() {
	common.MainWithMetadata("telesrv-egress", currentBuildMetadata(), egressnode.Run)
}
