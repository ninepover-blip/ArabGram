// Command telesrv-ton starts the isolated TON Star Gift chain worker.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"

	"telesrv/internal/node/common"
	tonnode "telesrv/internal/node/ton"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "resume-export" {
		common.MainWithMetadata("telesrv-ton resume-export", currentBuildMetadata(), func(logger *zap.Logger, meta common.BuildMetadata) error {
			flags := flag.NewFlagSet("resume-export", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			exportID := flags.Int64("export-id", 0, "quarantined export ID")
			actor := flags.String("actor", "", "operator identity")
			reason := flags.String("reason", "", "ticket or recovery reason")
			_ = flags.String("config", "", "TON role YAML path")
			if err := flags.Parse(os.Args[2:]); err != nil {
				return fmt.Errorf("parse resume-export arguments: %w", err)
			}
			if len(flags.Args()) != 0 {
				return fmt.Errorf("resume-export does not accept positional arguments")
			}
			return tonnode.ResumeExport(logger, meta, tonnode.ResumeExportOptions{
				ExportID: *exportID, Actor: *actor, Reason: *reason,
			})
		})
		return
	}
	common.MainWithMetadata("telesrv-ton", currentBuildMetadata(), tonnode.Run)
}
