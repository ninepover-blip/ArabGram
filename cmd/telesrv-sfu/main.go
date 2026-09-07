// Command telesrv-sfu starts the dedicated standalone SFU media role.
package main

import (
	"go.uber.org/zap"

	"telesrv/internal/node/common"
	sfunode "telesrv/internal/node/sfu"
)

func main() {
	common.MainWithMetadata("telesrv-sfu", currentBuildMetadata(), func(logger *zap.Logger, _ common.BuildMetadata) error {
		return sfunode.Run(logger)
	})
}
