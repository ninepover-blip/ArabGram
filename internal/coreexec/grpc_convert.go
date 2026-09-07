package coreexec

import (
	"fmt"

	"github.com/iamxvbaba/td/tlprofile"

	"telesrv/internal/coreexec/coreexecpb"
)

func limitsToPB(in tlprofile.Limits) *coreexecpb.Limits {
	return &coreexecpb.Limits{
		MaxWireBytes:         int64(in.MaxWireBytes),
		MaxVectorElements:    int64(in.MaxVectorElements),
		MaxAggregateElements: int64(in.MaxAggregateElements),
		MaxDepth:             int64(in.MaxDepth),
	}
}

func limitsFromPB(in *coreexecpb.Limits) tlprofile.Limits {
	if in == nil {
		return tlprofile.Limits{}
	}
	return tlprofile.Limits{
		MaxWireBytes:         boundedInt(in.GetMaxWireBytes()),
		MaxVectorElements:    boundedInt(in.GetMaxVectorElements()),
		MaxAggregateElements: boundedInt(in.GetMaxAggregateElements()),
		MaxDepth:             boundedInt(in.GetMaxDepth()),
	}
}

func boundedInt(v int64) int {
	const maxInt = int(^uint(0) >> 1)
	const minInt = -maxInt - 1
	if v > int64(maxInt) {
		return maxInt
	}
	if v < int64(minInt) {
		return minInt
	}
	return int(v)
}

func admissionModeToPB(mode AdmissionMode) coreexecpb.AdmissionMode {
	switch mode {
	case AdmissionModeLayer:
		return coreexecpb.AdmissionMode_ADMISSION_MODE_LAYER
	case AdmissionModeDefault:
		return coreexecpb.AdmissionMode_ADMISSION_MODE_DEFAULT
	case AdmissionModeUnprofiled:
		return coreexecpb.AdmissionMode_ADMISSION_MODE_UNPROFILED
	default:
		return coreexecpb.AdmissionMode_ADMISSION_MODE_UNSPECIFIED
	}
}

func admissionModeFromPB(mode coreexecpb.AdmissionMode) AdmissionMode {
	switch mode {
	case coreexecpb.AdmissionMode_ADMISSION_MODE_LAYER:
		return AdmissionModeLayer
	case coreexecpb.AdmissionMode_ADMISSION_MODE_DEFAULT:
		return AdmissionModeDefault
	case coreexecpb.AdmissionMode_ADMISSION_MODE_UNPROFILED:
		return AdmissionModeUnprofiled
	default:
		return AdmissionMode("")
	}
}

func proofToPB(in wireProof) *coreexecpb.WireProof {
	return &coreexecpb.WireProof{
		Profile:       int32(in.Profile),
		Method:        in.Method,
		WireId:        in.WireID,
		WireSize:      int64(in.WireSize),
		WireDigest:    in.WireDigest,
		WireInvariant: in.WireInvariant,
		EffectiveProfile: &coreexecpb.ProfileMark{
			Profile: int32(in.EffectiveProfile.Profile),
			Present: in.EffectiveProfile.Present,
		},
		ProfileEvidence: &coreexecpb.ProfileMark{
			Profile: int32(in.ProfileEvidence.Profile),
			Present: in.ProfileEvidence.Present,
		},
	}
}

func proofFromPB(in *coreexecpb.WireProof) wireProof {
	if in == nil {
		return wireProof{}
	}
	return wireProof{
		Profile:       int(in.GetProfile()),
		Method:        in.GetMethod(),
		WireID:        in.GetWireId(),
		WireSize:      boundedInt(in.GetWireSize()),
		WireDigest:    in.GetWireDigest(),
		WireInvariant: in.GetWireInvariant(),
		EffectiveProfile: profileMark{
			Profile: int(in.GetEffectiveProfile().GetProfile()),
			Present: in.GetEffectiveProfile().GetPresent(),
		},
		ProfileEvidence: profileMark{
			Profile: int(in.GetProfileEvidence().GetProfile()),
			Present: in.GetProfileEvidence().GetPresent(),
		},
	}
}

func errorFromResponse(message string) error {
	if message == "" {
		return nil
	}
	return remoteControlError(message)
}

func validateCoreExecTarget(target string) error {
	if target == "" {
		return fmt.Errorf("empty coreexec target")
	}
	if err := validateHostPort(target); err != nil {
		return fmt.Errorf("coreexec grpc target must be host:port %q: %w", target, err)
	}
	return nil
}
