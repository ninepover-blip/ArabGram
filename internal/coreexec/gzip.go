package coreexec

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/iamxvbaba/td/proto"
)

const maxCoreExecGZIPExpandedBytes = 10 << 20

func coreExecGZIPExpander(wire []byte, maxExpandedBytes int) ([]byte, func(), error) {
	noop := func() {}
	if maxExpandedBytes <= 0 {
		return nil, noop, fmt.Errorf("coreexec: invalid gzip expansion limit %d", maxExpandedBytes)
	}
	limit := maxExpandedBytes
	if limit > maxCoreExecGZIPExpandedBytes {
		limit = maxCoreExecGZIPExpandedBytes
	}
	compressed, err := coreExecGZIPPackedBytesView(wire)
	if err != nil {
		return nil, noop, err
	}
	r, err := coreExecGZIPPackedReader(compressed)
	if err != nil {
		return nil, noop, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	closeErr := r.Close()
	if readErr != nil {
		return nil, noop, readErr
	}
	if closeErr != nil {
		return nil, noop, closeErr
	}
	if len(data) > limit {
		return nil, noop, fmt.Errorf("coreexec: gzip expansion %d exceeds %d", len(data), limit)
	}
	return data, noop, nil
}

func coreExecGZIPPackedReader(compressed []byte) (io.ReadCloser, error) {
	source := bytes.NewReader(compressed)
	if len(compressed) >= 2 && compressed[0] == 0x1f && compressed[1] == 0x8b {
		return gzip.NewReader(source)
	}
	return zlib.NewReader(source)
}

func coreExecGZIPPackedBytesView(wire []byte) ([]byte, error) {
	if len(wire) < 5 {
		return nil, io.ErrUnexpectedEOF
	}
	if binary.LittleEndian.Uint32(wire[:4]) != proto.GZIPTypeID {
		return nil, fmt.Errorf("coreexec: unexpected gzip constructor %#x", binary.LittleEndian.Uint32(wire[:4]))
	}
	payload, consumed, err := coreExecTLBytesView(wire[4:])
	if err != nil {
		return nil, err
	}
	if 4+consumed != len(wire) {
		return nil, fmt.Errorf("coreexec: gzip_packed has %d trailing bytes", len(wire)-(4+consumed))
	}
	return payload, nil
}

func coreExecTLBytesView(raw []byte) ([]byte, int, error) {
	if len(raw) == 0 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	var header, length int
	if raw[0] < 254 {
		header = 1
		length = int(raw[0])
	} else if raw[0] == 254 {
		if len(raw) < 4 {
			return nil, 0, io.ErrUnexpectedEOF
		}
		header = 4
		length = int(raw[1]) | int(raw[2])<<8 | int(raw[3])<<16
	} else {
		return nil, 0, fmt.Errorf("coreexec: invalid TL bytes prefix %d", raw[0])
	}
	end := header + length
	if end < header || end > len(raw) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	padding := (4 - (end % 4)) % 4
	consumed := end + padding
	if consumed < end || consumed > len(raw) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return raw[header:end], consumed, nil
}
