package files

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	videoThumbnailTimeout       = 5 * time.Second
	videoThumbnailMaxInputBytes = 200 << 20 // 200MB；更大文件本阶段跳过 fallback，避免阻塞发送。
	videoThumbnailMaxConcurrent = 2
)

// VideoThumbnailer 从视频字节中抽取静态缩略图。实现必须可失败降级，不影响原发送流程。
type VideoThumbnailer interface {
	Extract(ctx context.Context, data []byte, mimeType string) ([]byte, error)
}

// FFmpegVideoThumbnailer 使用本机 ffmpeg 抽取第一帧 JPEG。
type FFmpegVideoThumbnailer struct {
	path    string
	timeout time.Duration
	slots   chan struct{}
}

// NewFFmpegVideoThumbnailer 返回基于 PATH 中 ffmpeg 的抽帧器。
func NewFFmpegVideoThumbnailer() (*FFmpegVideoThumbnailer, error) {
	path, err := exec.LookPath("ffmpeg")
	if err != nil {
		return nil, err
	}
	return &FFmpegVideoThumbnailer{
		path:    path,
		timeout: videoThumbnailTimeout,
		slots:   make(chan struct{}, videoThumbnailMaxConcurrent),
	}, nil
}

// Extract 抽取第一帧并输出 JPEG bytes。
func (t *FFmpegVideoThumbnailer) Extract(ctx context.Context, data []byte, mimeType string) ([]byte, error) {
	if t == nil || t.path == "" {
		return nil, fmt.Errorf("ffmpeg thumbnailer unavailable")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty video data")
	}
	if len(data) > videoThumbnailMaxInputBytes {
		return nil, fmt.Errorf("video too large for thumbnail fallback: %d bytes", len(data))
	}
	select {
	case t.slots <- struct{}{}:
		defer func() { <-t.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	runCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	input, err := os.CreateTemp("", "telesrv-video-*"+videoTempExt(mimeType))
	if err != nil {
		return nil, fmt.Errorf("create temp video: %w", err)
	}
	inputPath := input.Name()
	defer os.Remove(inputPath)
	if _, err := input.Write(data); err != nil {
		input.Close()
		return nil, fmt.Errorf("write temp video: %w", err)
	}
	if err := input.Close(); err != nil {
		return nil, fmt.Errorf("close temp video: %w", err)
	}

	output, err := os.CreateTemp("", "telesrv-video-thumb-*.jpg")
	if err != nil {
		return nil, fmt.Errorf("create temp thumbnail: %w", err)
	}
	outputPath := output.Name()
	output.Close()
	defer os.Remove(outputPath)

	cmd := exec.CommandContext(
		runCtx,
		t.path,
		"-hide_banner",
		"-loglevel", "error",
		"-y",
		"-i", inputPath,
		"-map", "0:v:0",
		"-frames:v", "1",
		"-an",
		"-vf", "scale=320:320:force_original_aspect_ratio=decrease",
		"-q:v", "3",
		outputPath,
	)
	stderr, err := cmd.CombinedOutput()
	if runCtx.Err() != nil {
		return nil, runCtx.Err()
	}
	if err != nil {
		msg := strings.TrimSpace(string(stderr))
		if msg != "" {
			return nil, fmt.Errorf("ffmpeg extract thumbnail: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("ffmpeg extract thumbnail: %w", err)
	}
	thumb, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, fmt.Errorf("read thumbnail: %w", err)
	}
	if len(thumb) == 0 {
		return nil, fmt.Errorf("ffmpeg produced empty thumbnail")
	}
	return thumb, nil
}

func videoTempExt(mimeType string) string {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "video/webm":
		return ".webm"
	default:
		return ".bin"
	}
}
