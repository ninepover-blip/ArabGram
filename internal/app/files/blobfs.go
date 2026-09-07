package files

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"telesrv/internal/domain"
)

// BlobBackend 是 blob 字节内容的存储后端。第一阶段只有本地磁盘实现。
// 内容寻址：Put 返回的 objectKey 是内容 sha256，相同内容自动去重。
type BlobBackend interface {
	Name() string
	// Put borrows data as read-only until the call returns and must not retain it.
	Put(ctx context.Context, data []byte) (objectKey string, err error)
	// PutReader consumes r before returning and transfers ownership of sha256.
	PutReader(ctx context.Context, r io.Reader) (objectKey string, size int64, sha256 []byte, err error)
	// Get transfers ownership of the returned bytes to the caller.
	Get(ctx context.Context, objectKey string) ([]byte, error)
	// GetRange 只读 [offset, offset+limit) 段并返回该段字节与文件总大小（limit<=0 读到末尾），
	// 避免大文件每个 chunk 都整文件读入内存（getFile 按 chunk 多次请求 ⇒ 否则 O(N²) 放大）。
	// 返回的 data 会将所有权移交给调用者，后端不得继续引用或修改。
	GetRange(ctx context.Context, objectKey string, offset, limit int64) (data []byte, total int64, err error)
}

// UploadPartBackend 保存 upload.saveFilePart/saveBigFilePart 的临时分片字节。
// 与正式 blob 不同，上传分片 key 唯一且可删除，成功组装/覆盖重传/GC 后必须清理。
type UploadPartBackend interface {
	// PutUploadPart borrows data as read-only until the call returns and must not retain it.
	PutUploadPart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (UploadPartObject, error)
	// GetUploadPart transfers ownership of the returned bytes to the caller.
	GetUploadPart(ctx context.Context, objectKey string) ([]byte, error)
	OpenUploadPart(ctx context.Context, objectKey string) (io.ReadCloser, error)
	DeleteUploadPart(ctx context.Context, objectKey string) error
	DeleteExpiredUploadParts(ctx context.Context, before time.Time, limit int) (int64, error)
}

type UploadPartObject struct {
	Backend   domain.MediaBackend
	ObjectKey string
	Size      int64
	// SHA256 is exclusively owned by this value and may move into a wire response.
	SHA256 []byte
}

// LocalFS 把 blob 字节存到本地磁盘根目录下，路径按内容 hash 两级 fanout。
type LocalFS struct {
	root string

	mu            sync.Mutex
	openBlobFiles map[string]*sharedBlobFile
}

type sharedBlobFile struct {
	key  string
	file *os.File
	refs int
}

// NewLocalFS 创建本地磁盘 blob backend，确保根目录存在。
func NewLocalFS(root string) (*LocalFS, error) {
	if root == "" {
		return nil, fmt.Errorf("blob root dir is empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create blob root %q: %w", root, err)
	}
	return &LocalFS{root: root, openBlobFiles: make(map[string]*sharedBlobFile)}, nil
}

// Name 返回后端标识，与 file_blobs.backend 一致。
func (l *LocalFS) Name() string { return "localfs" }

func (l *LocalFS) pathFor(objectKey string) string {
	if len(objectKey) < 4 {
		return filepath.Join(l.root, "_", objectKey)
	}
	return filepath.Join(l.root, objectKey[:2], objectKey[2:4], objectKey)
}

// Put 写入内容并返回 sha256 hex 作为 objectKey；同内容已存在则跳过写入（去重）。
func (l *LocalFS) Put(ctx context.Context, data []byte) (string, error) {
	key, _, _, err := l.PutReader(ctx, bytes.NewReader(data))
	return key, err
}

// PutReader 流式写入内容，边复制边计算 sha256，避免上层为大视频先拼出完整 []byte。
func (l *LocalFS) PutReader(ctx context.Context, r io.Reader) (string, int64, []byte, error) {
	tmpDir := filepath.Join(l.root, "_tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", 0, nil, fmt.Errorf("create blob tmp dir: %w", err)
	}
	tmp, err := os.CreateTemp(tmpDir, "blob-*.tmp")
	if err != nil {
		return "", 0, nil, fmt.Errorf("create blob tmp file: %w", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	h := sha256.New()
	size, err := copyWithContext(ctx, io.MultiWriter(tmp, h), r)
	closeErr := tmp.Close()
	if err != nil {
		return "", 0, nil, fmt.Errorf("write blob stream: %w", err)
	}
	if closeErr != nil {
		return "", 0, nil, fmt.Errorf("close blob stream: %w", closeErr)
	}
	sum := h.Sum(nil)
	key := hex.EncodeToString(sum)
	path := l.pathFor(key)
	if _, err := os.Stat(path); err == nil {
		committed = true
		_ = os.Remove(tmpPath)
		return key, size, sum, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", 0, nil, fmt.Errorf("stat blob: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", 0, nil, fmt.Errorf("create blob dir: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			committed = true
			_ = os.Remove(tmpPath)
			return key, size, sum, nil
		}
		return "", 0, nil, fmt.Errorf("commit blob: %w", err)
	}
	committed = true
	return key, size, sum, nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 256<<10)
	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			w, writeErr := dst.Write(buf[:n])
			written += int64(w)
			if writeErr != nil {
				return written, writeErr
			}
			if w != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

// Get 读取 objectKey 对应的全部字节。
func (l *LocalFS) Get(_ context.Context, objectKey string) ([]byte, error) {
	return os.ReadFile(l.pathFor(objectKey))
}

// GetRange 用 ReadAt 只读 [offset, offset+limit) 段，total 取自文件大小；
// n 受 total 约束，故即便客户端传超大 limit 也只分配文件实际大小，不会按客户端巨值分配。
func (l *LocalFS) GetRange(_ context.Context, objectKey string, offset, limit int64) ([]byte, int64, error) {
	blobFile, err := l.openBlobFile(objectKey)
	if err != nil {
		return nil, 0, err
	}
	defer l.releaseBlobFile(blobFile)
	f := blobFile.file
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	total := info.Size()
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []byte{}, total, nil
	}
	n := total - offset
	if limit > 0 && limit < n {
		n = limit
	}
	buf := make([]byte, n)
	read, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, 0, err
	}
	return buf[:read], total, nil
}

func (l *LocalFS) openBlobFile(objectKey string) (*sharedBlobFile, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f, ok := l.openBlobFiles[objectKey]; ok {
		f.refs++
		return f, nil
	}
	f, err := os.Open(l.pathFor(objectKey))
	if err != nil {
		return nil, err
	}
	blobFile := &sharedBlobFile{
		key:  objectKey,
		file: f,
		refs: 1,
	}
	l.openBlobFiles[objectKey] = blobFile
	return blobFile, nil
}

func (l *LocalFS) releaseBlobFile(blobFile *sharedBlobFile) {
	if blobFile == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if blobFile.refs > 0 {
		blobFile.refs--
	}
	if blobFile.refs != 0 {
		return
	}
	if current := l.openBlobFiles[blobFile.key]; current == blobFile {
		delete(l.openBlobFiles, blobFile.key)
	}
	_ = blobFile.file.Close()
}

func (l *LocalFS) PutUploadPart(_ context.Context, ownerUserID, fileID int64, part int, data []byte) (UploadPartObject, error) {
	sum := sha256.Sum256(data)
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return UploadPartObject{}, fmt.Errorf("generate upload part key: %w", err)
	}
	key := filepath.ToSlash(filepath.Join(
		"upload_parts",
		fmt.Sprintf("%d", ownerUserID),
		fmt.Sprintf("%d", fileID),
		fmt.Sprintf("%06d-%s.part", part, hex.EncodeToString(nonce[:])),
	))
	path, err := l.uploadPartPath(key)
	if err != nil {
		return UploadPartObject{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return UploadPartObject{}, fmt.Errorf("create upload part dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return UploadPartObject{}, fmt.Errorf("write upload part: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return UploadPartObject{}, fmt.Errorf("commit upload part: %w", err)
	}
	return UploadPartObject{
		Backend:   domain.MediaBackend(l.Name()),
		ObjectKey: key,
		Size:      int64(len(data)),
		SHA256:    sum[:],
	}, nil
}

func (l *LocalFS) GetUploadPart(_ context.Context, objectKey string) ([]byte, error) {
	path, err := l.uploadPartPath(objectKey)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func (l *LocalFS) OpenUploadPart(_ context.Context, objectKey string) (io.ReadCloser, error) {
	path, err := l.uploadPartPath(objectKey)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (l *LocalFS) DeleteUploadPart(_ context.Context, objectKey string) error {
	path, err := l.uploadPartPath(objectKey)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete upload part: %w", err)
	}
	return nil
}

func (l *LocalFS) DeleteExpiredUploadParts(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	root := filepath.Join(l.root, "upload_parts")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("stat upload parts root: %w", err)
	}
	var deleted int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if deleted >= int64(limit) {
			return filepath.SkipAll
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Before(before) {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		deleted++
		return nil
	})
	if err != nil {
		return deleted, fmt.Errorf("delete expired upload part objects: %w", err)
	}
	return deleted, nil
}

func (l *LocalFS) uploadPartPath(objectKey string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(objectKey))
	prefix := "upload_parts" + string(os.PathSeparator)
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || !strings.HasPrefix(clean, prefix) {
		return "", fmt.Errorf("invalid upload part object key %q", objectKey)
	}
	return filepath.Join(l.root, clean), nil
}
