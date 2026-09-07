// Command telesrv-edge starts the production MTProto Edge role.
package main

import (
	"telesrv/internal/node/common"
	edgenode "telesrv/internal/node/edge"
)

func main() {
	common.MainWithMetadata("telesrv-edge", currentBuildMetadata(), edgenode.Run)
}
