// Command telesrv-core starts the dedicated business/CoreExec role.
package main

import (
	"telesrv/internal/node/common"
	corenode "telesrv/internal/node/core"
)

func main() {
	common.MainWithMetadata("telesrv-core", currentBuildMetadata(), corenode.Run)
}
