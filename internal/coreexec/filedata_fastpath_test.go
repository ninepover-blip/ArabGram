package coreexec

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/mtprotoedge"
	"telesrv/internal/rpc"
)

func TestFileDataLayerRPCWithGRPCRemoteNeverCapturesOrDispatchesDirectGetFile(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote := NewGRPCRemote(edgeRouter, nil, nil)
	files := &coreExecFileDataProbe{}
	layer := mtprotoedge.NewFileDataLayerRPC(remote, files, zaptest.NewLogger(t))

	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   0,
		Limit:    1024,
	}, &body); err != nil {
		t.Fatalf("encode upload.getFile: %v", err)
	}
	admitted, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("admit upload.getFile: %v", err)
	}
	if remote.pendingCount != 0 {
		t.Fatalf("CoreExec captured direct FileData wire: pending=%d", remote.pendingCount)
	}
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("dispatch upload.getFile: %v", err)
	}
	file, ok := result.CanonicalValue().(*tg.UploadFile)
	if method != "upload.getFile" || !ok || string(file.Bytes) != "filedata" || files.getFileCalls != 1 {
		t.Fatalf("method=%q result=%#v getFileCalls=%d", method, result.CanonicalValue(), files.getFileCalls)
	}

	wrapped := &tg.InvokeWithLayerRequest{
		Layer: 228,
		Query: &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: 77},
			Offset:   1024,
			Limit:    1024,
		},
	}
	body.Reset()
	if err := wrapped.Encode(&body); err != nil {
		t.Fatalf("encode wrapped upload.getFile: %v", err)
	}
	admitted, err = layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("admit wrapped upload.getFile: %v", err)
	}
	if remote.pendingCount != 0 {
		t.Fatalf("CoreExec retained wrapped FileData wire: pending=%d", remote.pendingCount)
	}
	if _, _, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 5, 6, admitted); err != nil {
		t.Fatalf("dispatch wrapped upload.getFile: %v", err)
	}
	if files.getFileCalls != 2 {
		t.Fatalf("wrapped getFile calls = %d, want 2 total", files.getFileCalls)
	}
}

type coreExecFileDataProbe struct {
	getFileCalls int
}

func (*coreExecFileDataProbe) SaveFilePart(context.Context, int64, int64, int, []byte) (bool, error) {
	return true, nil
}

func (*coreExecFileDataProbe) SaveBigFilePart(context.Context, int64, int64, int, int, []byte) (bool, error) {
	return true, nil
}

func (p *coreExecFileDataProbe) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	p.getFileCalls++
	return domain.FileChunk{Bytes: []byte("filedata")}, true, nil
}

func (*coreExecFileDataProbe) GetFileHashes(context.Context, domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	return nil, true, nil
}
