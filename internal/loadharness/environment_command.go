package loadharness

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type cappedCommandOutput struct {
	buffer bytes.Buffer
	limit  int
}

func (w *cappedCommandOutput) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.buffer.Len() {
		return 0, errors.New("command_output_limit")
	}
	return w.buffer.Write(p)
}
func environmentCommand(ctx context.Context, program string, env []string, limit int, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, program, args...)
	c.WaitDelay = time.Second
	if env != nil {
		c.Env = env
	}
	out := &cappedCommandOutput{limit: limit}
	c.Stdout = out
	c.Stderr = io.Discard
	if err := c.Run(); err != nil {
		return nil, errors.New("command_failed")
	}
	return out.buffer.Bytes(), nil
}
func localDockerEnvironment(socket string) []string {
	v := make([]string, 0, len(os.Environ())+1)
	for _, s := range os.Environ() {
		if !strings.HasPrefix(s, "DOCKER_") {
			v = append(v, s)
		}
	}
	return append(v, "DOCKER_HOST=unix://"+socket)
}
