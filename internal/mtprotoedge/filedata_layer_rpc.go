package mtprotoedge

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/postresponse"
	"telesrv/internal/rpcresult"
)

const fileDataMaxUploadGetFileChunkLimit = 1 << 20

var errNotFileDataTerminal = errors.New("mtprotoedge: terminal RPC is not FileData")

// FileDataDataPlane is the Edge-visible File service contract. It deliberately
// carries only upload/download data-plane calls; message ownership, permissions
// and durable update facts remain Core-owned.
type FileDataDataPlane interface {
	SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error)
	SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error)
	GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error)
	GetFileHashes(ctx context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error)
}

type immutableFileDataRangeReader interface {
	ReadImmutableFileRange(context.Context, domain.ImmutableFileRange) ([]byte, error)
}

type immutableFileDataValueSource struct {
	reader immutableFileDataRangeReader
	range_ domain.ImmutableFileRange
}

func (s *immutableFileDataValueSource) Value(ctx context.Context) (any, error) {
	if s == nil || s.reader == nil {
		return nil, fmt.Errorf("immutable filedata source is unavailable")
	}
	data, err := s.reader.ReadImmutableFileRange(ctx, s.range_)
	if err != nil {
		return nil, err
	}
	if len(data) != s.range_.Length || sha256.Sum256(data) != s.range_.RangeSHA256 {
		return nil, fmt.Errorf("immutable filedata source changed")
	}
	return &tg.UploadFile{
		Type:  fileDataStorageFileType(s.range_.MimeType, data),
		Mtime: 0,
		Bytes: data,
	}, nil
}

func (s *immutableFileDataValueSource) RetainedBytes() int {
	if s == nil {
		return 0
	}
	return 384 + len(s.range_.ObjectKey) + len(s.range_.MimeType)
}

type immutableFileDataReplaySource struct {
	call   tlprofile.Call
	source rpcresult.ValueSource
}

func (s *immutableFileDataReplaySource) EncodeInner(ctx context.Context, out *bin.Buffer) error {
	if s == nil || s.source == nil {
		return fmt.Errorf("immutable filedata replay source is unavailable")
	}
	value, err := s.source.Value(ctx)
	if err != nil {
		return err
	}
	return s.call.EncodeResult(value, out)
}

func (s *immutableFileDataReplaySource) RetainedBytes() int {
	if s == nil || s.source == nil {
		return 0
	}
	return s.source.RetainedBytes() + 128
}

type immutableFileDataResult struct {
	tlprofile.Result
	source rpcresult.ReplaySource
}

func (r *immutableFileDataResult) ExactReplaySource() rpcresult.ReplaySource {
	if r == nil {
		return nil
	}
	return r.source
}

// FileDataLayerRPCBase owns only non-FileData RPCs and shared session/profile
// state. Every FileData terminal, including invoke/gzip wrapping, is admitted
// by the Edge-local generated dispatcher and can never be sent to CoreExec.
type FileDataLayerRPCBase interface {
	LayerRPCHandler
	LayerRPCOptionsAdmitter
	LayerRPCDefaultProfileOptionsAdmitter
	LayerRPCFlatBytesPayloadSizer
	LayerRPCSessionProfileRegistry
	LayerRPCOrderedSessionProfileRegistry
	LayerRPCDurableSessionProfileResolver
	LayerRPCInheritedAuthKeyProfileResolver
	LayerRPCDurableSessionProfileAdvancer
	LayerRPCDurableSessionProfileDeleter
	LayerRPCReplayPreparer
	LayerRPCProfileEvidenceContext
	LayerRPCIdentityHintContext
	LayerRPCAdmissionProfilePublisher
	RPCInitConnectionObserver
	postresponse.ActionExecutor
	DiscardAdmitted(tlprofile.PreparedIdentity) bool
}

type fileDataIdentityHintKey struct{}

// FileDataLayerRPC intercepts the high-volume upload/download data plane and
// delegates every business/control capability to the wrapped CoreExec handler.
type FileDataLayerRPC struct {
	base       FileDataLayerRPCBase
	dispatcher *tlprofile.Dispatcher
}

func NewFileDataLayerRPC(base FileDataLayerRPCBase, files FileDataDataPlane, _ *zap.Logger) *FileDataLayerRPC {
	return &FileDataLayerRPC{
		base:       base,
		dispatcher: newFileDataDispatcher(files),
	}
}

func newFileDataDispatcher(files FileDataDataPlane) *tlprofile.Dispatcher {
	d := tlprofile.NewDispatcher()
	registerFileDataAdmissionPreflight(d)
	d.OnWrappers(func(ctx context.Context, _ tlprofile.Admission, next tlprofile.Next) error {
		return next(ctx)
	})
	mustRegisterFileDataRPC[*tg.UploadSaveFilePartRequest](d, tlprofile.SemanticMethodUploadSaveFilePart, func(ctx context.Context, req *tg.UploadSaveFilePartRequest) (any, error) {
		if files == nil {
			return false, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		userID, ok := fileDataCurrentUserID(ctx)
		if !ok {
			return false, tgerr.New(400, "FILE_ID_INVALID")
		}
		if req.FilePart < 0 {
			return false, tgerr.New(400, "FILE_PART_INVALID")
		}
		saved, err := files.SaveFilePart(ctx, userID, req.FileID, req.FilePart, req.Bytes)
		if err != nil {
			return false, fileDataSaveErr(err)
		}
		return saved, nil
	})
	mustRegisterFileDataRPC[*tg.UploadSaveBigFilePartRequest](d, tlprofile.SemanticMethodUploadSaveBigFilePart, func(ctx context.Context, req *tg.UploadSaveBigFilePartRequest) (any, error) {
		if files == nil {
			return false, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		userID, ok := fileDataCurrentUserID(ctx)
		if !ok {
			return false, tgerr.New(400, "FILE_ID_INVALID")
		}
		if req.FilePart < 0 {
			return false, tgerr.New(400, "FILE_PART_INVALID")
		}
		saved, err := files.SaveBigFilePart(ctx, userID, req.FileID, req.FilePart, req.FileTotalParts, req.Bytes)
		if err != nil {
			return false, fileDataSaveErr(err)
		}
		return saved, nil
	})
	mustRegisterFileDataRPC[*tg.UploadGetFileRequest](d, tlprofile.SemanticMethodUploadGetFile, func(ctx context.Context, req *tg.UploadGetFileRequest) (any, error) {
		if files == nil {
			return nil, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		if req.Offset < 0 || req.Limit <= 0 || req.Limit > fileDataMaxUploadGetFileChunkLimit {
			return nil, tgerr.New(400, "LIMIT_INVALID")
		}
		key, ok := fileDataLocationKey(req.Location)
		if !ok {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		chunk, found, err := files.GetFile(ctx, domain.FileDownloadRequest{
			LocationKey: key,
			Offset:      req.Offset,
			Limit:       req.Limit,
		})
		if err != nil {
			return nil, tgerr.New(500, "INTERNAL_SERVER_ERROR")
		}
		if !found {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		result := &tg.UploadFile{
			Type:  fileDataStorageFileType(chunk.MimeType, chunk.Bytes),
			Mtime: 0,
			Bytes: chunk.Bytes,
		}
		if chunk.ImmutableRange != nil {
			if reader, ok := files.(immutableFileDataRangeReader); ok {
				rpcresult.Publish(ctx, &immutableFileDataValueSource{reader: reader, range_: *chunk.ImmutableRange})
			}
		}
		return result, nil
	})
	mustRegisterFileDataRPC[*tg.UploadGetFileHashesRequest](d, tlprofile.SemanticMethodUploadGetFileHashes, func(ctx context.Context, req *tg.UploadGetFileHashesRequest) (any, error) {
		if files == nil {
			return nil, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		if req.Offset < 0 {
			return nil, tgerr.New(400, "OFFSET_INVALID")
		}
		key, ok := fileDataLocationKey(req.Location)
		if !ok {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		hashes, found, err := files.GetFileHashes(ctx, domain.FileHashRequest{
			LocationKey: key,
			Offset:      req.Offset,
		})
		if err != nil {
			return nil, tgerr.New(500, "INTERNAL_SERVER_ERROR")
		}
		if !found {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		return fileDataTGFileHashes(hashes), nil
	})
	return d
}

func registerFileDataAdmissionPreflight(d *tlprofile.Dispatcher) {
	if d == nil {
		panic("mtprotoedge filedata: nil dispatcher")
	}
	d.OnAdmissionPreflight(func(view tlprofile.AdmissionView) error {
		if !fileDataFastPathMethod(view.Semantic()) {
			return errNotFileDataTerminal
		}
		return nil
	})
	for _, fieldID := range []tlprofile.FieldID{
		tlprofile.FieldUploadSaveFilePartBytes,
		tlprofile.FieldUploadSaveBigFilePartBytes,
	} {
		fieldID := fieldID
		if err := d.OnFieldPreflight(fieldID, func(view tlprofile.FieldView) error {
			length, ok := view.BytesLength()
			if !ok {
				return tgerr.New(400, "INPUT_REQUEST_INVALID")
			}
			if length > filesapp.MaxUploadPartBytes {
				return tgerr.New(400, "FILE_PART_TOO_BIG")
			}
			return nil
		}); err != nil {
			panic(fmt.Sprintf("mtprotoedge filedata: register upload bytes field %#016x: %v", uint64(fieldID), err))
		}
	}
	if err := d.OnFieldPreflight(tlprofile.FieldUploadSaveBigFilePartFileTotalParts, func(view tlprofile.FieldView) error {
		totalParts, ok := view.Int32()
		if !ok {
			return tgerr.New(400, "INPUT_REQUEST_INVALID")
		}
		if totalParts <= 0 || totalParts > int32(filesapp.MaxUploadParts) {
			return tgerr.New(400, "FILE_PART_INVALID")
		}
		return nil
	}); err != nil {
		panic(fmt.Sprintf("mtprotoedge filedata: register upload total-parts field: %v", err))
	}
}

func mustRegisterFileDataRPC[T bin.Object](d *tlprofile.Dispatcher, method tlprofile.SemanticID, handler func(context.Context, T) (any, error)) {
	if err := d.Register(method, func(ctx context.Context, object bin.Object) (any, error) {
		request, ok := object.(T)
		if !ok {
			return nil, fmt.Errorf("mtprotoedge filedata: semantic %#016x decoded unexpected canonical request %T", uint64(method), object)
		}
		return handler(ctx, request)
	}); err != nil {
		panic(fmt.Sprintf("mtprotoedge filedata: register canonical RPC %#016x: %v", uint64(method), err))
	}
}

func (r *FileDataLayerRPC) AdmitLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitLayerWithOptions(profile, b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if r.directFileDataMethod(profile, b) {
		return r.dispatcher.AdmitWithOptions(profile, b, options)
	}
	if fileDataEnvelopeMayWrap(profile, b) {
		if admitted, handled, err := r.tryLocalFileDataAdmission(b, func(probe *bin.Buffer) (tlprofile.Admission, error) {
			return r.dispatcher.AdmitWithOptions(profile, probe, options)
		}); handled {
			return admitted, err
		}
	}
	admitted, err := r.base.AdmitLayerWithOptions(profile, b, options)
	return r.rejectCoreFileDataAdmission(admitted, err)
}

func (r *FileDataLayerRPC) AdmitDefaultLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitDefaultLayerWithOptions(profile, b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitDefaultLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if r.directFileDataMethod(profile, b) {
		return r.dispatcher.AdmitDefaultWithOptions(profile, b, options)
	}
	if fileDataEnvelopeMayWrap(profile, b) {
		if admitted, handled, err := r.tryLocalFileDataAdmission(b, func(probe *bin.Buffer) (tlprofile.Admission, error) {
			return r.dispatcher.AdmitDefaultWithOptions(profile, probe, options)
		}); handled {
			return admitted, err
		}
	}
	admitted, err := r.base.AdmitDefaultLayerWithOptions(profile, b, options)
	return r.rejectCoreFileDataAdmission(admitted, err)
}

func (r *FileDataLayerRPC) AdmitUnprofiled(b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitUnprofiledWithOptions(b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitUnprofiledWithOptions(b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if admitted, handled, err := r.tryLocalFileDataAdmission(b, func(probe *bin.Buffer) (tlprofile.Admission, error) {
		return r.dispatcher.AdmitUnprofiledWithOptions(probe, options)
	}); handled {
		return admitted, err
	}
	admitted, err := r.base.AdmitUnprofiledWithOptions(b, options)
	return r.rejectCoreFileDataAdmission(admitted, err)
}

// directFileDataMethod performs only a generated constructor-to-semantic lookup.
// It never scans or materializes the terminal request. Wrapped and gzip-packed
// requests are admitted once by the base codec shell; finishBaseAdmission then
// discards the captured wire and diverts the admission before any CoreExec call.
func (r *FileDataLayerRPC) directFileDataMethod(profile tlprofile.Profile, b *bin.Buffer) bool {
	if r == nil || r.dispatcher == nil || b == nil || len(b.Raw()) < bin.Word {
		return false
	}
	wireID, err := b.PeekID()
	if err != nil {
		return false
	}
	method, ok := tlprofile.SemanticForWireID(profile, wireID)
	return ok && fileDataFastPathMethod(method)
}

func fileDataEnvelopeMayWrap(profile tlprofile.Profile, b *bin.Buffer) bool {
	if b == nil || len(b.Raw()) < bin.Word {
		return false
	}
	wireID, err := b.PeekID()
	if err != nil {
		return false
	}
	if wireID == proto.GZIPTypeID {
		return true
	}
	method, ok := tlprofile.SemanticForWireID(profile, wireID)
	return ok && fileDataWrapperMethod(method)
}

func fileDataWrapperMethod(method tlprofile.SemanticID) bool {
	switch method {
	case tlprofile.SemanticMethodInitConnection,
		tlprofile.SemanticMethodInvokeAfterMsg,
		tlprofile.SemanticMethodInvokeAfterMsgs,
		tlprofile.SemanticMethodInvokeWithApnsSecret,
		tlprofile.SemanticMethodInvokeWithBusinessConnection,
		tlprofile.SemanticMethodInvokeWithGooglePlayIntegrity,
		tlprofile.SemanticMethodInvokeWithLayer,
		tlprofile.SemanticMethodInvokeWithMessagesRange,
		tlprofile.SemanticMethodInvokeWithReCaptcha,
		tlprofile.SemanticMethodInvokeWithTakeout,
		tlprofile.SemanticMethodInvokeWithoutUpdates:
		return true
	default:
		return false
	}
}

func (r *FileDataLayerRPC) tryLocalFileDataAdmission(
	body *bin.Buffer,
	admit func(*bin.Buffer) (tlprofile.Admission, error),
) (tlprofile.Admission, bool, error) {
	if r == nil || r.dispatcher == nil || body == nil || admit == nil {
		return tlprofile.Admission{}, true, fmt.Errorf("mtprotoedge filedata: local admission dependency missing")
	}
	probe := &bin.Buffer{Buf: body.Raw()}
	admitted, err := admit(probe)
	if errors.Is(err, errNotFileDataTerminal) || errors.Is(err, tlprofile.ErrUnknownRPCMethod) {
		return tlprofile.Admission{}, false, nil
	}
	if err != nil {
		return tlprofile.Admission{}, true, err
	}
	if !fileDataFastPathMethod(admitted.Call().Method()) {
		return tlprofile.Admission{}, true, fmt.Errorf("mtprotoedge filedata: local dispatcher admitted non-FileData terminal")
	}
	body.Skip(body.Len())
	return admitted, true, nil
}

// rejectCoreFileDataAdmission is a hard-cut invariant guard. A CoreExec codec
// must never produce a FileData admission; if it does, release the capture and
// fail the request instead of treating it as a compatibility route.
func (r *FileDataLayerRPC) rejectCoreFileDataAdmission(admitted tlprofile.Admission, err error) (tlprofile.Admission, error) {
	if err != nil || !fileDataFastPathMethod(admitted.Call().Method()) {
		return admitted, err
	}
	_ = r.base.DiscardAdmitted(admitted.Prepared().Identity())
	return tlprofile.Admission{}, fmt.Errorf("mtprotoedge filedata: CoreExec returned forbidden FileData admission")
}

func (r *FileDataLayerRPC) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	switch request.Call().Method() {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFile,
		tlprofile.SemanticMethodUploadGetFileHashes:
		dispatchCtx, capture := rpcresult.WithCapture(ctx)
		result, err := r.dispatcher.Dispatch(dispatchCtx, request)
		if err == nil && result != nil {
			if valueSource := capture.Take(); valueSource != nil {
				result = &immutableFileDataResult{
					Result: result,
					source: &immutableFileDataReplaySource{call: request.Call(), source: valueSource},
				}
			}
		}
		return result, fileDataMethodName(request.Call().Method()), err
	}
	return r.base.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func fileDataFastPathMethod(method tlprofile.SemanticID) bool {
	switch method {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFile,
		tlprofile.SemanticMethodUploadGetFileHashes:
		return true
	default:
		return false
	}
}

func fileDataMethodName(method tlprofile.SemanticID) string {
	_, name, ok := tlprofile.SemanticName(method)
	if !ok || name == "" {
		return "unknown"
	}
	return name
}

func fileDataCurrentUserID(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	hint, ok := ctx.Value(fileDataIdentityHintKey{}).(LayerRPCIdentityHint)
	if !ok || !hint.UserIDResolved || hint.UserID == 0 {
		return 0, false
	}
	return hint.UserID, true
}

func (r *FileDataLayerRPC) WithLayerRPCIdentityHint(ctx context.Context, hint LayerRPCIdentityHint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = r.base.WithLayerRPCIdentityHint(ctx, hint)
	return context.WithValue(ctx, fileDataIdentityHintKey{}, hint)
}

func (r *FileDataLayerRPC) WithLayerRPCProfileEvidenceFresh(ctx context.Context, fresh bool) context.Context {
	return r.base.WithLayerRPCProfileEvidenceFresh(ctx, fresh)
}

func (r *FileDataLayerRPC) LayerRPCFlatBytesPayloadSize(wire []byte) (int, bool) {
	return r.base.LayerRPCFlatBytesPayloadSize(wire)
}

func (r *FileDataLayerRPC) NegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) (int, bool) {
	return r.base.NegotiatedSessionLayer(authKeyID, sessionID)
}

func (r *FileDataLayerRPC) NegotiatedSessionLayerEvidence(authKeyID [8]byte, sessionID int64) (int, int64, bool) {
	return r.base.NegotiatedSessionLayerEvidence(authKeyID, sessionID)
}

func (r *FileDataLayerRPC) FreezeNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64, layer int) error {
	return r.base.FreezeNegotiatedSessionLayer(authKeyID, sessionID, layer)
}

func (r *FileDataLayerRPC) FreezeNegotiatedSessionLayerAt(authKeyID [8]byte, sessionID int64, layer int, msgID int64) (bool, error) {
	return r.base.FreezeNegotiatedSessionLayerAt(authKeyID, sessionID, layer, msgID)
}

func (r *FileDataLayerRPC) ForgetNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) {
	r.base.ForgetNegotiatedSessionLayer(authKeyID, sessionID)
}

func (r *FileDataLayerRPC) ForgetNegotiatedAuthKey(authKeyID [8]byte) {
	r.base.ForgetNegotiatedAuthKey(authKeyID)
}

func (r *FileDataLayerRPC) ResolveNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (int, int64, bool, error) {
	return r.base.ResolveNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
}

func (r *FileDataLayerRPC) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (int, bool, error) {
	return r.base.ResolveInheritedAuthKeyLayer(ctx, rawAuthKeyID)
}

func (r *FileDataLayerRPC) AdvanceNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, layer int, msgID int64) (int, int64, bool, error) {
	return r.base.AdvanceNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID, layer, msgID)
}

func (r *FileDataLayerRPC) DeleteNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (bool, error) {
	return r.base.DeleteNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
}

func (r *FileDataLayerRPC) PublishAdmittedLayerProfileEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, safeFloor uint64, layer int) error {
	return r.base.PublishAdmittedLayerProfileEvidence(ctx, rawAuthKeyID, sessionID, msgID, admissionSeq, safeFloor, layer)
}

func (r *FileDataLayerRPC) PrepareAdmittedReplay(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (func() error, error) {
	switch request.Call().Method() {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFile,
		tlprofile.SemanticMethodUploadGetFileHashes:
		return nil, nil
	}
	return r.base.PrepareAdmittedReplay(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (r *FileDataLayerRPC) ObserveInitConnection(ctx context.Context, authKeyID [8]byte, sessionID int64, layer, apiID int, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode string) error {
	return r.base.ObserveInitConnection(ctx, authKeyID, sessionID, layer, apiID, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode)
}

func (r *FileDataLayerRPC) RunPostResponseActions(ctx context.Context, actions []postresponse.Action) error {
	if len(actions) == 0 {
		return nil
	}
	return r.base.RunPostResponseActions(ctx, actions)
}

func fileDataSaveErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrFilePartInvalid):
		return tgerr.New(400, "FILE_PART_INVALID")
	case errors.Is(err, domain.ErrFilePartsInvalid):
		return tgerr.New(400, "FILE_PARTS_INVALID")
	case errors.Is(err, domain.ErrFilePartTooBig):
		return tgerr.New(400, "FILE_PART_TOO_BIG")
	case errors.Is(err, domain.ErrUploadQuotaExceeded):
		return tgerr.New(420, "FLOOD_WAIT_60")
	case errors.Is(err, domain.ErrStorageFull):
		return tgerr.New(400, "STORAGE_FULL")
	default:
		return tgerr.New(500, "INTERNAL_SERVER_ERROR")
	}
}

func fileDataLocationKey(location tg.InputFileLocationClass) (string, bool) {
	switch loc := location.(type) {
	case *tg.InputFileLocation:
		return fileDataLegacyVolumeLocationKey(loc.VolumeID, loc.LocalID)
	case *tg.InputDocumentFileLocation:
		if loc.ID == 0 {
			return "", false
		}
		if loc.ThumbSize == "" {
			return fmt.Sprintf("doc:%d", loc.ID), true
		}
		return fmt.Sprintf("doc:%d:%s", loc.ID, loc.ThumbSize), true
	case *tg.InputPhotoFileLocation:
		if loc.ID == 0 || loc.ThumbSize == "" {
			return "", false
		}
		return fmt.Sprintf("photo:%d:%s", loc.ID, loc.ThumbSize), true
	case *tg.InputPeerPhotoFileLocation:
		if loc.PhotoID == 0 {
			return "", false
		}
		size := "a"
		if loc.Big {
			size = "c"
		}
		return fmt.Sprintf("photo:%d:%s", loc.PhotoID, size), true
	case *tg.InputPhotoLegacyFileLocation:
		photoID := loc.ID
		if photoID == 0 && loc.VolumeID < 0 {
			photoID = -loc.VolumeID
		}
		if photoID == 0 {
			return "", false
		}
		size, ok := fileDataLegacyPhotoSizeType(loc.LocalID)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("photo:%d:%s", photoID, size), true
	case *tg.InputPeerPhotoFileLocationLegacy:
		photoID := int64(0)
		if loc.VolumeID < 0 {
			photoID = -loc.VolumeID
		}
		if photoID == 0 {
			return "", false
		}
		size := "a"
		if loc.Big {
			size = "c"
		}
		return fmt.Sprintf("photo:%d:%s", photoID, size), true
	default:
		return "", false
	}
}

func fileDataLegacyVolumeLocationKey(volumeID int64, localID int) (string, bool) {
	if volumeID >= 0 || localID <= 0 {
		return "", false
	}
	id := -volumeID
	if size, ok := fileDataLegacyPhotoSizeType(localID); ok {
		return fmt.Sprintf("photo:%d:%s", id, size), true
	}
	if size, ok := fileDataLegacyDocumentThumbSizeType(localID); ok {
		return fmt.Sprintf("doc:%d:%s", id, size), true
	}
	return "", false
}

func fileDataLegacyPhotoSizeType(localID int) (string, bool) {
	if localID < 1 || localID > 127 {
		return "", false
	}
	ch := byte(localID)
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return string(ch), true
	}
	return "", false
}

func fileDataLegacyDocumentThumbSizeType(localID int) (string, bool) {
	localID -= 1000
	if localID < 1 || localID > 127 {
		return "", false
	}
	ch := byte(localID)
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return string(ch), true
	}
	return "", false
}

func fileDataStorageFileType(mime string, data []byte) tg.StorageFileTypeClass {
	switch fileDataSniffImageType(data) {
	case "jpeg":
		return &tg.StorageFileJpeg{}
	case "png":
		return &tg.StorageFilePng{}
	case "gif":
		return &tg.StorageFileGif{}
	case "webp":
		return &tg.StorageFileWebp{}
	}
	switch {
	case strings.Contains(mime, "webp"):
		return &tg.StorageFileWebp{}
	case strings.Contains(mime, "jpeg"), strings.Contains(mime, "jpg"):
		return &tg.StorageFileJpeg{}
	case strings.Contains(mime, "png"):
		return &tg.StorageFilePng{}
	case strings.Contains(mime, "gif"):
		return &tg.StorageFileGif{}
	case strings.Contains(mime, "mp4"), strings.Contains(mime, "quicktime"), strings.Contains(mime, "video"):
		return &tg.StorageFileMov{}
	}
	return &tg.StorageFileUnknown{}
}

func fileDataSniffImageType(data []byte) string {
	if len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return "jpeg"
	}
	if len(data) >= 8 && data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G' {
		return "png"
	}
	if len(data) >= 6 && data[0] == 'G' && data[1] == 'I' && data[2] == 'F' {
		return "gif"
	}
	if len(data) >= 12 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' &&
		data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P' {
		return "webp"
	}
	return ""
}

func fileDataTGFileHashes(hashes []domain.FileHash) []tg.FileHash {
	out := make([]tg.FileHash, 0, len(hashes))
	for _, hash := range hashes {
		out = append(out, tg.FileHash{
			Offset: hash.Offset,
			Limit:  hash.Limit,
			Hash:   hash.Hash,
		})
	}
	return out
}

var _ LayerRPCHandler = (*FileDataLayerRPC)(nil)
var _ LayerRPCOptionsAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCDefaultProfileAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCDefaultProfileOptionsAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCFlatBytesPayloadSizer = (*FileDataLayerRPC)(nil)
var _ LayerRPCOrderedSessionProfileRegistry = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileResolver = (*FileDataLayerRPC)(nil)
var _ LayerRPCInheritedAuthKeyProfileResolver = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileAdvancer = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileDeleter = (*FileDataLayerRPC)(nil)
var _ LayerRPCReplayPreparer = (*FileDataLayerRPC)(nil)
var _ LayerRPCProfileEvidenceContext = (*FileDataLayerRPC)(nil)
var _ LayerRPCIdentityHintContext = (*FileDataLayerRPC)(nil)
var _ LayerRPCAdmissionProfilePublisher = (*FileDataLayerRPC)(nil)
var _ RPCInitConnectionObserver = (*FileDataLayerRPC)(nil)
var _ postresponse.ActionExecutor = (*FileDataLayerRPC)(nil)
