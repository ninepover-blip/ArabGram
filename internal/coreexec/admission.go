package coreexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tlprofile"
)

const (
	defaultPendingAdmissionTTL  = 2 * time.Minute
	defaultMaxPendingAdmissions = 8192
	defaultReplayHookTTL        = 2 * time.Minute
	defaultMaxReplayHooks       = 8192
)

var (
	ErrRemoteAdmissionLost         = errors.New("coreexec: admitted wire is not registered")
	ErrWireProofMismatch           = errors.New("coreexec: admitted wire proof mismatch")
	ErrRemoteRPCUnavailable        = errors.New("coreexec: remote rpc unavailable")
	ErrRemoteRequestTimeout        = errors.New("coreexec: remote request timeout")
	ErrRemoteInvalidRequest        = errors.New("coreexec: remote invalid request")
	ErrRemoteUnauthenticated       = errors.New("coreexec: remote unauthenticated")
	ErrRemoteAdmissionCapacity     = errors.New("coreexec: pending admission capacity exceeded")
	ErrGRPCProtocolVersionMismatch = errors.New("coreexec grpc: protocol version mismatch")
)

type AdmissionMode string

const (
	AdmissionModeLayer      AdmissionMode = "layer"
	AdmissionModeDefault    AdmissionMode = "default"
	AdmissionModeUnprofiled AdmissionMode = "unprofiled"
)

type capturedAdmission struct {
	Mode    AdmissionMode
	Profile tlprofile.Profile
	Limits  tlprofile.Limits
	// Wire is the one owned, immutable copy detached from the reusable Edge
	// inbound frame. Ownership moves to the protobuf request when the admission
	// lease is consumed; no later CoreExec stage may clone or mutate it.
	Wire      []byte
	Proof     wireProof
	CreatedAt time.Time
}

type profileMark struct {
	Profile int
	Present bool
}

type wireProof struct {
	Profile          int
	Method           uint64
	WireID           uint32
	WireSize         int
	WireDigest       string
	WireInvariant    bool
	EffectiveProfile profileMark
	ProfileEvidence  profileMark
}

type profileEvidenceFreshKey struct{}

type replayHookEntry struct {
	after     func() error
	expiresAt time.Time
}

type encodedResult struct {
	prepared      tlprofile.PreparedCall
	wireInvariant bool
	// body owns the immutable protobuf response bytes returned by grpc-go. The
	// response message transfers this slice to encodedResult before becoming
	// unreachable; Encode only appends it to the caller's outbound buffer.
	body []byte
}

func (r *encodedResult) Prepared() tlprofile.PreparedCall { return r.prepared }
func (r *encodedResult) WireInvariant() bool              { return r != nil && r.wireInvariant }
func (r *encodedResult) CanonicalValue() any              { return nil }

func (r *encodedResult) Encode(out *bin.Buffer) error {
	if r == nil {
		return errors.New("coreexec: encode nil result")
	}
	out.Put(r.body)
	return nil
}

func copyBufferRaw(b *bin.Buffer) []byte {
	if b == nil {
		return nil
	}
	// This is the sole mandatory request-wire copy: Edge admission consumes a
	// view into a reusable decrypted MTProto frame whose backing array may be
	// overwritten as soon as the inbound request leaves the connection actor.
	return append([]byte(nil), b.Raw()...)
}

func admitCaptured(handler Handler, mode AdmissionMode, profile int, limits tlprofile.Limits, wire []byte) (tlprofile.Admission, error) {
	if handler == nil {
		return tlprofile.Admission{}, ErrNilHandler
	}
	// request_wire belongs to the inbound protobuf request for the complete
	// unary-handler lifetime. Generated admission advances only this local slice
	// header; TL decoders do not mutate the backing wire and materialized bytes
	// fields own their data. The admitted request is dispatched before the gRPC
	// handler returns, so borrowing here is both bounded and race-free.
	body := bin.Buffer{Buf: wire}
	options := tlprofile.AdmissionOptions{
		Limits:     limits,
		ExpandGZIP: coreExecGZIPExpander,
	}
	switch mode {
	case AdmissionModeLayer:
		return handler.AdmitLayerWithOptions(tlprofile.Profile(profile), &body, options)
	case AdmissionModeDefault:
		return handler.AdmitDefaultLayerWithOptions(tlprofile.Profile(profile), &body, options)
	case AdmissionModeUnprofiled:
		return handler.AdmitUnprofiledWithOptions(&body, options)
	default:
		return tlprofile.Admission{}, fmt.Errorf("coreexec: invalid admission mode %q", mode)
	}
}

// takeBufferRaw transfers the buffer's owned backing array to its caller and
// clears the source header so an accidental later Reset/Put cannot mutate the
// transferred protobuf payload.
func takeBufferRaw(b *bin.Buffer) []byte {
	if b == nil {
		return nil
	}
	raw := b.Raw()
	b.ResetTo(nil)
	return raw
}

func proofFromAdmission(admitted tlprofile.Admission) wireProof {
	prepared := admitted.Prepared()
	call := admitted.Call()
	effective, effectiveOK := admitted.EffectiveProfile()
	evidence, evidenceOK := admitted.ProfileEvidence()
	digest := prepared.WireDigest()
	return wireProof{
		Profile:       int(call.Profile()),
		Method:        uint64(call.Method()),
		WireID:        call.WireID(),
		WireSize:      prepared.WireSize(),
		WireDigest:    hex.EncodeToString(digest[:]),
		WireInvariant: call.WireInvariant(),
		EffectiveProfile: profileMark{
			Profile: int(effective),
			Present: effectiveOK,
		},
		ProfileEvidence: profileMark{
			Profile: int(evidence),
			Present: evidenceOK,
		},
	}
}

func profileEvidenceFresh(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	fresh, specified := ctx.Value(profileEvidenceFreshKey{}).(bool)
	return !specified || fresh
}

func remoteControlError(message string) error {
	if message == context.Canceled.Error() {
		return context.Canceled
	}
	if message == context.DeadlineExceeded.Error() {
		return context.DeadlineExceeded
	}
	return fmt.Errorf("%w: %s", ErrRemoteRPCUnavailable, message)
}

func newReplayToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("coreexec grpc: replay token random: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
