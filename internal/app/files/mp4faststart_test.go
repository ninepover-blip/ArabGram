package files

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// box 构造一个 MP4 box：[size(4)][type(4)][payload]。
func box(typ string, payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(8+len(payload)))
	copy(b[4:8], typ)
	copy(b[8:], payload)
	return b
}

// stcoBox 构造一个含单个 chunk 偏移的 stco：[ver+flags(4)][count(4)=1][offset(4)]。
func stcoBox(offset uint32) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint32(p[4:8], 1) // entry_count
	binary.BigEndian.PutUint32(p[8:12], offset)
	return box("stco", p)
}

// buildMoovEndMP4 构造一个 moov 在末尾、stco 指向 mdat 内某偏移的最小合法 MP4。
// 返回 (mp4, mdatPayloadAbsOffset)。
func buildMoovEndMP4(mdatPayload []byte) ([]byte, uint32) {
	ftyp := box("ftyp", []byte("isom\x00\x00\x02\x00"))
	mdat := box("mdat", mdatPayload)
	mdatAbs := uint32(len(ftyp) + 8) // mdat payload 紧跟 mdat 头(8 字节)
	// moov→trak→mdia→minf→stbl→stco(offset=mdatAbs)
	stbl := box("stbl", stcoBox(mdatAbs))
	minf := box("minf", stbl)
	mdia := box("mdia", minf)
	trak := box("trak", mdia)
	moov := box("moov", trak)
	out := append([]byte(nil), ftyp...)
	out = append(out, mdat...)
	out = append(out, moov...)
	return out, mdatAbs
}

func TestFaststartMP4MovesMoovAndFixesOffsets(t *testing.T) {
	marker := []byte("THE-REAL-CHUNK-DATA-HERE")
	// mdat payload：前面填充 + marker，stco 指向 marker 的绝对偏移。
	pad := bytes.Repeat([]byte{0xAB}, 40)
	mdatPayload := append(append([]byte(nil), pad...), marker...)
	_, mdatAbs := buildMoovEndMP4(mdatPayload)
	markerAbs := mdatAbs + uint32(len(pad)) // marker 在原文件里的绝对偏移

	// 原 stco 指向 mdatAbs（mdat payload 头）。这里把 stco 改成指向 marker 以便断言。
	in2, _ := buildMoovEndMP4Marker(mdatPayload, markerAbs)
	// 校验前置：原文件里 markerAbs 处确实是 marker。
	if !bytes.Equal(in2[markerAbs:markerAbs+uint32(len(marker))], marker) {
		t.Fatalf("setup: 原文件 markerAbs 处非 marker")
	}

	out, changed := faststartMP4(in2)
	if !changed {
		t.Fatalf("changed = false, want true（moov 在末尾应被搬动）")
	}
	if len(out) != len(in2) {
		t.Fatalf("size 不守恒: in=%d out=%d", len(in2), len(out))
	}
	// 输出顺序：ftyp, moov, mdat。
	boxes, ok := parseTopLevelBoxes(out)
	if !ok || len(boxes) != 3 || boxes[0].typ != "ftyp" || boxes[1].typ != "moov" || boxes[2].typ != "mdat" {
		t.Fatalf("输出顶层顺序错: %+v ok=%v", boxes, ok)
	}
	// 取出输出 moov 里的 stco 偏移，应指向输出文件里仍是 marker 的位置。
	newOff := readSingleStcoOffset(t, out[boxes[1].start:boxes[1].end])
	if int(newOff)+len(marker) > len(out) {
		t.Fatalf("新偏移越界: %d", newOff)
	}
	got := out[newOff : int(newOff)+len(marker)]
	if !bytes.Equal(got, marker) {
		t.Fatalf("新 stco 偏移 %d 指向 %q, want %q（偏移修正错误）", newOff, got, marker)
	}
	// 新偏移 = 原偏移 + moovSize。
	moovSize := boxes[1].end - boxes[1].start
	if int(newOff) != int(markerAbs)+moovSize {
		t.Fatalf("新偏移 = %d, want 原 %d + moovSize %d", newOff, markerAbs, moovSize)
	}
}

func TestFaststartMP4NoopWhenAlreadyFaststart(t *testing.T) {
	// moov 在 mdat 前 → 已 faststart，应原样返回。
	ftyp := box("ftyp", []byte("isom\x00\x00\x02\x00"))
	stbl := box("stbl", stcoBox(100))
	moov := box("moov", box("trak", box("mdia", box("minf", stbl))))
	mdat := box("mdat", bytes.Repeat([]byte{1}, 64))
	in := append(append(append([]byte(nil), ftyp...), moov...), mdat...)
	out, changed := faststartMP4(in)
	if changed {
		t.Fatalf("已 faststart 不应改动")
	}
	if !bytes.Equal(out, in) {
		t.Fatalf("应原样返回")
	}
}

func TestInspectMP4LayoutDetectsMoovAtEnd(t *testing.T) {
	in, _ := buildMoovEndMP4(bytes.Repeat([]byte{0xCD}, 100))
	readAt := func(off, n int64) ([]byte, error) {
		if off+n > int64(len(in)) {
			n = int64(len(in)) - off
		}
		return in[off : off+n], nil
	}
	l, ok := inspectMP4Layout(int64(len(in)), readAt)
	if !ok {
		t.Fatalf("inspect 失败")
	}
	if !l.needsFaststart {
		t.Fatalf("moov 在末尾应 needsFaststart=true")
	}
	if !l.moovIsLast {
		t.Fatalf("moov 是最后一个 box 应 moovIsLast=true")
	}
	if l.ftypStart != 0 || l.moovEnd != int64(len(in)) {
		t.Fatalf("range 不对: %+v (len=%d)", l, len(in))
	}
}

func TestInspectMP4LayoutNoFaststartWhenMoovFirst(t *testing.T) {
	ftyp := box("ftyp", []byte("isom\x00\x00\x02\x00"))
	moov := box("moov", box("trak", box("mdia", box("minf", box("stbl", stcoBox(100))))))
	mdat := box("mdat", bytes.Repeat([]byte{1}, 100))
	in := append(append(append([]byte(nil), ftyp...), moov...), mdat...)
	readAt := func(off, n int64) ([]byte, error) {
		if off+n > int64(len(in)) {
			n = int64(len(in)) - off
		}
		return in[off : off+n], nil
	}
	l, ok := inspectMP4Layout(int64(len(in)), readAt)
	if !ok || l.needsFaststart {
		t.Fatalf("moov 在前应 needsFaststart=false, got ok=%v %+v", ok, l)
	}
}

// 流式拼接(ftyp + patched moov + 中段)必须与全量 faststartMP4 输出逐字节一致。
func TestStreamingAssemblyMatchesFullRewrite(t *testing.T) {
	in, _ := buildMoovEndMP4(bytes.Repeat([]byte{0xEE}, 256))
	readAt := func(off, n int64) ([]byte, error) { return in[off : off+n], nil }
	l, ok := inspectMP4Layout(int64(len(in)), readAt)
	if !ok || !l.needsFaststart || !l.moovIsLast {
		t.Fatalf("setup: %+v ok=%v", l, ok)
	}
	// 流式版本：ftyp + patched moov + in[ftypEnd:moovStart]
	ftyp := append([]byte(nil), in[l.ftypStart:l.ftypEnd]...)
	moov := append([]byte(nil), in[l.moovStart:l.moovEnd]...)
	if !patchChunkOffsets(moov, l.moovEnd-l.moovStart) {
		t.Fatalf("patch 失败")
	}
	var streaming []byte
	streaming = append(streaming, ftyp...)
	streaming = append(streaming, moov...)
	streaming = append(streaming, in[l.ftypEnd:l.moovStart]...)

	full, changed := faststartMP4(in)
	if !changed {
		t.Fatalf("full 应 changed")
	}
	if !bytes.Equal(streaming, full) {
		t.Fatalf("流式拼接与全量重写不一致: streaming=%d full=%d", len(streaming), len(full))
	}
}

func TestFaststartMP4NoopOnNonMP4(t *testing.T) {
	for _, data := range [][]byte{
		nil,
		[]byte("not an mp4 at all"),
		{0, 0, 0, 4}, // size 太小
	} {
		if out, changed := faststartMP4(data); changed || !bytes.Equal(out, data) {
			t.Fatalf("非 MP4 应原样不动: changed=%v", changed)
		}
	}
}

// buildMoovEndMP4Marker 同 buildMoovEndMP4，但 stco 指向给定绝对偏移。
func buildMoovEndMP4Marker(mdatPayload []byte, stcoOffset uint32) ([]byte, uint32) {
	ftyp := box("ftyp", []byte("isom\x00\x00\x02\x00"))
	mdat := box("mdat", mdatPayload)
	mdatAbs := uint32(len(ftyp) + 8)
	stbl := box("stbl", stcoBox(stcoOffset))
	moov := box("moov", box("trak", box("mdia", box("minf", stbl))))
	out := append([]byte(nil), ftyp...)
	out = append(out, mdat...)
	out = append(out, moov...)
	return out, mdatAbs
}

func readSingleStcoOffset(t *testing.T, moov []byte) uint32 {
	t.Helper()
	idx := bytes.Index(moov, []byte("stco"))
	if idx < 0 {
		t.Fatalf("moov 里找不到 stco")
	}
	// stco 头后：type(4) 已在 idx；payload 从 idx+4 起 = ver+flags(4)+count(4)+offset(4)
	off := idx + 4 + 4 + 4
	return binary.BigEndian.Uint32(moov[off : off+4])
}
