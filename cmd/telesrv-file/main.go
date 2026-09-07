// Command telesrv-file starts the standalone FileData role.
package main

import (
	"telesrv/internal/node/common"
	filenode "telesrv/internal/node/file"
)

func main() {
	common.MainWithMetadata("telesrv-file", currentBuildMetadata(), filenode.Run)
}
