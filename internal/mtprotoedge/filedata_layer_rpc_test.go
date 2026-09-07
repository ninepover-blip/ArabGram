package mtprotoedge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"github.com/iamxvbaba/td/tlprofile"

	"telesrv/internal/domain"
	"telesrv/internal/postresponse"
	"telesrv/internal/rpcresult"
)

func TestFileDataLayerRPCDispatchesUploadPartAtEdge(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadSaveFilePartRequest{
		FileID:   77,
		FilePart: 1,
		Bytes:    []byte("part-one"),
	})
	ctx := layer.WithLayerRPCIdentityHint(context.Background(), LayerRPCIdentityHint{
		UserID:         42,
		UserIDResolved: true,
	})
	result, method, err := layer.DispatchAdmitted(ctx, [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.saveFilePart" {
		t.Fatalf("method = %q, want upload.saveFilePart", method)
	}
	if got, ok := result.CanonicalValue().(bool); !ok || !got {
		t.Fatalf("result = %#v, want true", result.CanonicalValue())
	}
	if base.dispatches != 0 {
		t.Fatalf("base dispatches = %d, want 0", base.dispatches)
	}
	if files.saveParts != 1 || files.lastUserID != 42 || files.lastFileID != 77 || files.lastPart != 1 {
		t.Fatalf("filedata save = parts:%d user:%d file:%d part:%d",
			files.saveParts, files.lastUserID, files.lastFileID, files.lastPart)
	}
}

func TestFileDataLayerRPCRejectsGroupCallGetFileWithoutCoreExecFallback(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
		Location: &tg.InputGroupCallStream{
			Call:   &tg.InputGroupCall{ID: 10, AccessHash: 20},
			TimeMs: 1000,
			Scale:  0,
		},
		Offset: 0,
		Limit:  1024,
	})
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if method != "upload.getFile" {
		t.Fatalf("method = %q, want upload.getFile", method)
	}
	if !tgerr.Is(err, "LOCATION_INVALID") {
		t.Fatalf("DispatchAdmitted error = %v, want LOCATION_INVALID", err)
	}
	if result != nil {
		t.Fatalf("result = %#v, want nil", result)
	}
	if base.admissions != 0 || base.dispatches != 0 {
		t.Fatalf("base admissions/dispatches = %d/%d, want 0/0", base.admissions, base.dispatches)
	}
	if files.getFiles != 0 {
		t.Fatalf("filedata getFiles = %d, want 0", files.getFiles)
	}
}

func TestFileDataLayerRPCDispatchesStaticGetFileAtEdge(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   128,
		Limit:    1024,
	})
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFile" {
		t.Fatalf("method = %q, want upload.getFile", method)
	}
	if base.admissions != 0 || base.dispatches != 0 || base.discards != 0 {
		t.Fatalf("base admissions/dispatches/discards = %d/%d/%d, want 0/0/0", base.admissions, base.dispatches, base.discards)
	}
	if files.getFiles != 1 {
		t.Fatalf("filedata getFiles = %d, want 1", files.getFiles)
	}
	file, ok := result.CanonicalValue().(*tg.UploadFile)
	if !ok || string(file.Bytes) != "file" {
		t.Fatalf("result = %#v, want filedata upload file", result.CanonicalValue())
	}
}

func TestFileDataLayerRPCPublishesImmutableReplaySource(t *testing.T) {
	payload := []byte("immutable-file-range")
	source := &domain.ImmutableFileRange{
		Backend:     domain.MediaBackendLocalFS,
		ObjectKey:   "sha256/object",
		Offset:      128,
		Length:      len(payload),
		Total:       4096,
		MimeType:    "application/octet-stream",
		RangeSHA256: sha256.Sum256(payload),
	}
	files := &fakeFileDataPlane{
		fileBytes:   append([]byte(nil), payload...),
		immutable:   source,
		replayBytes: append([]byte(nil), payload...),
	}
	layer := NewFileDataLayerRPC(newFileDataTestBase(), files, nil)
	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   source.Offset,
		Limit:    source.Length,
	})
	result, _, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatal(err)
	}
	carrier, ok := result.(rpcresult.Carrier)
	if !ok || carrier.ExactReplaySource() == nil {
		t.Fatalf("result %T has no immutable replay source", result)
	}
	var fresh, replayed bin.Buffer
	if err := result.Encode(&fresh); err != nil {
		t.Fatal(err)
	}
	if err := carrier.ExactReplaySource().EncodeInner(context.Background(), &replayed); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fresh.Raw(), replayed.Raw()) {
		t.Fatal("immutable replay did not reproduce the exact admitted-layer result")
	}
	if files.immutableReads != 1 {
		t.Fatalf("immutable reads = %d, want 1", files.immutableReads)
	}
}

func TestFileDataLayerRPCSharesReadOnlyGetFilePayloadWithTG(t *testing.T) {
	for _, size := range []int{64 << 10, 1 << 20} {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i*31 + 7)
			}
			files := &fakeFileDataPlane{fileBytes: payload}
			layer := NewFileDataLayerRPC(newFileDataTestBase(), files, nil)
			admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
				Location: &tg.InputDocumentFileLocation{ID: 77},
				Limit:    size,
			})
			result, _, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
			if err != nil {
				t.Fatal(err)
			}
			file, ok := result.CanonicalValue().(*tg.UploadFile)
			if !ok || len(file.Bytes) != size || &file.Bytes[0] != &payload[0] {
				t.Fatal("upload.getFile copied the FileData read-only payload before tg.UploadFile")
			}
		})
	}
}

func TestFileDataLayerRPCWrappedGetFileAdmitsOnceAndNeverDispatchesCore(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataWrappedTestRPC(t, layer, &tg.InvokeWithLayerRequest{
		Layer: 228,
		Query: &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: 77},
			Offset:   0,
			Limit:    1024,
		},
	})
	if base.admissions != 0 || base.discards != 0 {
		t.Fatalf("base admissions/discards = %d/%d, want 0/0", base.admissions, base.discards)
	}
	if _, err := layer.PrepareAdmittedReplay(context.Background(), [8]byte{1}, 2, 3, 4, admitted); err != nil {
		t.Fatalf("PrepareAdmittedReplay: %v", err)
	}
	if base.replayPrepares != 0 {
		t.Fatalf("base replay prepares = %d, want 0", base.replayPrepares)
	}
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFile" || base.dispatches != 0 || files.getFiles != 1 {
		t.Fatalf("method=%q base dispatches=%d file calls=%d", method, base.dispatches, files.getFiles)
	}
	file, ok := result.CanonicalValue().(*tg.UploadFile)
	if !ok || string(file.Bytes) != "file" {
		t.Fatalf("result = %#v, want filedata upload file", result.CanonicalValue())
	}
}

func TestFileDataLayerRPCGZIPGetFileAdmitsOnceAndNeverDispatchesCore(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	var expanded bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Limit:    1024,
	}, &expanded); err != nil {
		t.Fatalf("encode expanded getFile: %v", err)
	}
	var body bin.Buffer
	if err := (proto.GZIP{Data: expanded.Raw()}).Encode(&body); err != nil {
		t.Fatalf("encode gzip getFile: %v", err)
	}
	expand := func(wire []byte, maxExpandedBytes int) ([]byte, func(), error) {
		cursor := &bin.Buffer{Buf: append([]byte(nil), wire...)}
		var packed proto.GZIP
		if err := packed.Decode(cursor); err != nil {
			return nil, nil, err
		}
		if cursor.Len() != 0 || len(packed.Data) > maxExpandedBytes {
			return nil, nil, fmt.Errorf("invalid gzip expansion size %d trailing %d", len(packed.Data), cursor.Len())
		}
		return packed.Data, func() {}, nil
	}
	admitted, err := layer.AdmitLayerWithOptions(tlprofile.Profile228, &body, tlprofile.AdmissionOptions{
		ExpandGZIP: expand,
	})
	if err != nil {
		t.Fatalf("AdmitLayerWithOptions: %v", err)
	}
	if base.admissions != 0 || base.discards != 0 || base.dispatches != 0 {
		t.Fatalf("base admissions/discards/dispatches = %d/%d/%d, want 0/0/0", base.admissions, base.discards, base.dispatches)
	}
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFile" || base.dispatches != 0 || files.getFiles != 1 {
		t.Fatalf("method=%q base dispatches=%d file calls=%d", method, base.dispatches, files.getFiles)
	}
	if _, ok := result.CanonicalValue().(*tg.UploadFile); !ok {
		t.Fatalf("result = %#v, want *tg.UploadFile", result.CanonicalValue())
	}
}

func TestFileDataLayerRPCWrappedGetFileNeverDependsOnCoreAdmissionRelease(t *testing.T) {
	base := newFileDataTestBase()
	base.discardFails = true
	layer := NewFileDataLayerRPC(base, &fakeFileDataPlane{}, nil)
	request := &tg.InvokeWithLayerRequest{
		Layer: 228,
		Query: &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: 77},
			Offset:   0,
			Limit:    1024,
		},
	}
	var body bin.Buffer
	if err := request.Encode(&body); err != nil {
		t.Fatalf("encode wrapped rpc: %v", err)
	}
	if _, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{}); err != nil {
		t.Fatalf("wrapped upload.getFile local admission: %v", err)
	}
	if base.admissions != 0 || base.discards != 0 || base.dispatches != 0 {
		t.Fatalf("base admissions/discards/dispatches = %d/%d/%d, want 0/0/0", base.admissions, base.discards, base.dispatches)
	}
}

func TestFileDataLayerRPCUnprofiledGetFileAdmitsOnceAndNeverDispatchesCore(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)
	request := &tg.InvokeWithLayerRequest{
		Layer: 228,
		Query: &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: 77},
			Offset:   0,
			Limit:    1024,
		},
	}
	var body bin.Buffer
	if err := request.Encode(&body); err != nil {
		t.Fatalf("encode wrapped rpc: %v", err)
	}
	admitted, err := layer.AdmitUnprofiled(&body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("AdmitUnprofiled: %v", err)
	}
	if base.admissions != 0 || base.discards != 0 {
		t.Fatalf("base admissions/discards = %d/%d, want 0/0", base.admissions, base.discards)
	}
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFile" || base.dispatches != 0 || files.getFiles != 1 {
		t.Fatalf("method=%q base dispatches=%d file calls=%d", method, base.dispatches, files.getFiles)
	}
	if _, ok := result.CanonicalValue().(*tg.UploadFile); !ok {
		t.Fatalf("result = %#v, want *tg.UploadFile", result.CanonicalValue())
	}
}

func admitFileDataWrappedTestRPC(t *testing.T, layer *FileDataLayerRPC, req bin.Object) tlprofile.Admission {
	t.Helper()
	var body bin.Buffer
	if err := req.Encode(&body); err != nil {
		t.Fatalf("encode wrapped rpc: %v", err)
	}
	admitted, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("AdmitLayer: %v", err)
	}
	if body.Len() != 0 {
		t.Fatalf("admission left %d bytes", body.Len())
	}
	return admitted
}

func TestFileDataLayerRPCMissingFileDataFailsWithoutCoreExecFallback(t *testing.T) {
	base := newFileDataTestBase()
	layer := NewFileDataLayerRPC(base, nil, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   0,
		Limit:    1024,
	})
	result, _, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if !tgerr.Is(err, "FILE_SERVICE_UNAVAILABLE") {
		t.Fatalf("DispatchAdmitted error = %v, want FILE_SERVICE_UNAVAILABLE", err)
	}
	if result != nil || base.dispatches != 0 {
		t.Fatalf("result=%#v base dispatches=%d, want nil/0", result, base.dispatches)
	}
}

func TestFileDataLayerRPCOrdinaryRPCUsesOnlyBaseAdmission(t *testing.T) {
	base := newFileDataTestBase()
	layer := NewFileDataLayerRPC(base, &fakeFileDataPlane{}, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.HelpGetNearestDCRequest{})
	if base.admissions != 1 || base.discards != 0 {
		t.Fatalf("base admissions/discards = %d/%d, want 1/0", base.admissions, base.discards)
	}
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "help.getNearestDc" || base.dispatches != 1 {
		t.Fatalf("method=%q base dispatches=%d, want help.getNearestDc/1", method, base.dispatches)
	}
	if _, ok := result.CanonicalValue().(*tg.NearestDC); !ok {
		t.Fatalf("result = %#v, want *tg.NearestDC", result.CanonicalValue())
	}
}

func TestFileDataLayerRPCDispatchesGetFileHashesAtEdge(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{
		hashes: []domain.FileHash{{
			Offset: 128 << 10,
			Limit:  4,
			Hash:   []byte("hash"),
		}},
	}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileHashesRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   128 << 10,
	})
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFileHashes" {
		t.Fatalf("method = %q, want upload.getFileHashes", method)
	}
	if base.dispatches != 0 {
		t.Fatalf("base dispatches = %d, want 0", base.dispatches)
	}
	if files.getHashes != 1 || files.lastHashRequest.LocationKey != "doc:77" || files.lastHashRequest.Offset != 128<<10 {
		t.Fatalf("filedata hashes calls=%d request=%+v", files.getHashes, files.lastHashRequest)
	}
	hashes, ok := result.CanonicalValue().([]tg.FileHash)
	if !ok || len(hashes) != 1 || hashes[0].Limit != 4 || string(hashes[0].Hash) != "hash" {
		t.Fatalf("result = %#v, want one file hash", result.CanonicalValue())
	}
	if &hashes[0].Hash[0] != &files.hashes[0].Hash[0] {
		t.Fatal("file hash bytes were copied instead of transferred")
	}
}

func BenchmarkFileDataLayerRPCAdmitOrdinaryRPC(b *testing.B) {
	layer := NewFileDataLayerRPC(newFileDataTestBase(), &fakeFileDataPlane{}, nil)
	wire := encodeFileDataBenchmarkRPC(b, &tg.HelpGetNearestDCRequest{})
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := bin.Buffer{Buf: wire}
		if _, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileDataBaseAdmitOrdinaryRPC(b *testing.B) {
	base := newFileDataTestBase()
	wire := encodeFileDataBenchmarkRPC(b, &tg.HelpGetNearestDCRequest{})
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := bin.Buffer{Buf: wire}
		if _, err := base.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileDataLayerRPCAdmitGetFile(b *testing.B) {
	layer := NewFileDataLayerRPC(newFileDataTestBase(), &fakeFileDataPlane{}, nil)
	wire := encodeFileDataBenchmarkRPC(b, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   0,
		Limit:    128 << 10,
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := bin.Buffer{Buf: wire}
		if _, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

func encodeFileDataBenchmarkRPC(b *testing.B, req bin.Object) []byte {
	b.Helper()
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, req, &body); err != nil {
		b.Fatal(err)
	}
	return append([]byte(nil), body.Raw()...)
}

func admitFileDataTestRPC(t *testing.T, layer *FileDataLayerRPC, req bin.Object) tlprofile.Admission {
	t.Helper()
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, req, &body); err != nil {
		t.Fatalf("encode rpc: %v", err)
	}
	admitted, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("AdmitLayer: %v", err)
	}
	if body.Len() != 0 {
		t.Fatalf("admission left %d bytes", body.Len())
	}
	return admitted
}

type fileDataTestBase struct {
	dispatcher     *tlprofile.Dispatcher
	admissions     int
	dispatches     int
	discards       int
	replayPrepares int
	discardFails   bool
}

func newFileDataTestBase() *fileDataTestBase {
	d := tlprofile.NewDispatcher()
	d.OnWrappers(func(ctx context.Context, _ tlprofile.Admission, next tlprofile.Next) error {
		return next(ctx)
	})
	if err := d.Register(tlprofile.SemanticMethodUploadGetFile, func(context.Context, bin.Object) (any, error) {
		return &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: []byte("core")}, nil
	}); err != nil {
		panic(err)
	}
	if err := d.Register(tlprofile.SemanticMethodHelpGetNearestDC, func(context.Context, bin.Object) (any, error) {
		return &tg.NearestDC{Country: "ZZ", ThisDC: 1, NearestDC: 1}, nil
	}); err != nil {
		panic(err)
	}
	return &fileDataTestBase{dispatcher: d}
}

func (b *fileDataTestBase) AdmitLayer(profile tlprofile.Profile, body *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	b.admissions++
	return b.dispatcher.Admit(profile, body, limits)
}

func (b *fileDataTestBase) AdmitLayerWithOptions(profile tlprofile.Profile, body *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	b.admissions++
	return b.dispatcher.AdmitWithOptions(profile, body, options)
}

func (b *fileDataTestBase) AdmitDefaultLayerWithOptions(profile tlprofile.Profile, body *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	b.admissions++
	return b.dispatcher.AdmitDefaultWithOptions(profile, body, options)
}

func (b *fileDataTestBase) AdmitUnprofiled(body *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	b.admissions++
	return b.dispatcher.AdmitUnprofiled(body, limits)
}

func (b *fileDataTestBase) AdmitUnprofiledWithOptions(body *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	b.admissions++
	return b.dispatcher.AdmitUnprofiledWithOptions(body, options)
}

func (b *fileDataTestBase) DiscardAdmitted(tlprofile.PreparedIdentity) bool {
	b.discards++
	return !b.discardFails
}

func (b *fileDataTestBase) PrepareAdmittedReplay(context.Context, [8]byte, int64, int64, uint64, tlprofile.Admission) (func() error, error) {
	b.replayPrepares++
	return nil, nil
}

func (*fileDataTestBase) LayerRPCFlatBytesPayloadSize([]byte) (int, bool)   { return 0, false }
func (*fileDataTestBase) NegotiatedSessionLayer([8]byte, int64) (int, bool) { return 0, false }
func (*fileDataTestBase) NegotiatedSessionLayerEvidence([8]byte, int64) (int, int64, bool) {
	return 0, 0, false
}
func (*fileDataTestBase) FreezeNegotiatedSessionLayer([8]byte, int64, int) error { return nil }
func (*fileDataTestBase) FreezeNegotiatedSessionLayerAt([8]byte, int64, int, int64) (bool, error) {
	return false, nil
}
func (*fileDataTestBase) ForgetNegotiatedSessionLayer([8]byte, int64) {}
func (*fileDataTestBase) ForgetNegotiatedAuthKey([8]byte)             {}
func (*fileDataTestBase) ResolveNegotiatedSessionLayerEvidence(context.Context, [8]byte, int64) (int, int64, bool, error) {
	return 0, 0, false, nil
}
func (*fileDataTestBase) ResolveInheritedAuthKeyLayer(context.Context, [8]byte) (int, bool, error) {
	return 0, false, nil
}
func (*fileDataTestBase) AdvanceNegotiatedSessionLayerEvidence(context.Context, [8]byte, int64, int, int64) (int, int64, bool, error) {
	return 0, 0, false, nil
}
func (*fileDataTestBase) DeleteNegotiatedSessionLayerEvidence(context.Context, [8]byte, int64) (bool, error) {
	return false, nil
}
func (*fileDataTestBase) WithLayerRPCProfileEvidenceFresh(ctx context.Context, _ bool) context.Context {
	return ctx
}
func (*fileDataTestBase) WithLayerRPCIdentityHint(ctx context.Context, _ LayerRPCIdentityHint) context.Context {
	return ctx
}
func (*fileDataTestBase) PublishAdmittedLayerProfileEvidence(context.Context, [8]byte, int64, int64, uint64, uint64, int) error {
	return nil
}
func (*fileDataTestBase) ObserveInitConnection(context.Context, [8]byte, int64, int, int, string, string, string, string, string, string) error {
	return nil
}
func (*fileDataTestBase) RunPostResponseActions(context.Context, []postresponse.Action) error {
	return nil
}

func (b *fileDataTestBase) DispatchAdmitted(ctx context.Context, _ [8]byte, _ int64, _ int64, _ uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	b.dispatches++
	result, err := b.dispatcher.Dispatch(ctx, request)
	return result, fileDataMethodName(request.Call().Method()), err
}

type fakeFileDataPlane struct {
	saveParts       int
	getFiles        int
	getHashes       int
	lastUserID      int64
	lastFileID      int64
	lastPart        int
	lastHashRequest domain.FileHashRequest
	hashes          []domain.FileHash
	fileBytes       []byte
	immutable       *domain.ImmutableFileRange
	replayBytes     []byte
	immutableReads  int
}

func (f *fakeFileDataPlane) SaveFilePart(_ context.Context, ownerUserID, fileID int64, part int, _ []byte) (bool, error) {
	f.saveParts++
	f.lastUserID = ownerUserID
	f.lastFileID = fileID
	f.lastPart = part
	return true, nil
}

func (f *fakeFileDataPlane) SaveBigFilePart(context.Context, int64, int64, int, int, []byte) (bool, error) {
	return false, fmt.Errorf("unexpected SaveBigFilePart")
}

func (f *fakeFileDataPlane) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	f.getFiles++
	if f.fileBytes != nil {
		return domain.FileChunk{Bytes: f.fileBytes, MimeType: "application/octet-stream", ImmutableRange: f.immutable}, true, nil
	}
	return domain.FileChunk{Bytes: []byte("file")}, true, nil
}

func (f *fakeFileDataPlane) ReadImmutableFileRange(context.Context, domain.ImmutableFileRange) ([]byte, error) {
	f.immutableReads++
	return append([]byte(nil), f.replayBytes...), nil
}

func (f *fakeFileDataPlane) GetFileHashes(_ context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	f.getHashes++
	f.lastHashRequest = req
	return append([]domain.FileHash(nil), f.hashes...), true, nil
}
