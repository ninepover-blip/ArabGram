package main

import "telesrv/internal/node/common"

var (
	gitCommit    = ""
	gitBranch    = ""
	gitTreeState = ""
	buildTime    = ""
)

func currentBuildMetadata() common.BuildMetadata {
	return common.BuildMetadataFromValues(gitCommit, gitBranch, gitTreeState, buildTime)
}
