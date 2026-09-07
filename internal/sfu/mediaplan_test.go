package sfu

import "testing"

// 选层契约：订阅端只为 SIM[0]（最低层）与其 FID RTX 伙伴建解码 sink，
// SFU 只转发这两个视频 ssrc，其余层在 SFU 终结。
func TestBuildMediaPlanSimulcast(t *testing.T) {
	offer := ClientOffer{
		AudioSSRC: 1000,
		SsrcGroups: []SsrcGroup{
			{Semantics: "SIM", Sources: []uint32{1001, 1003, 1005}},
			{Semantics: "FID", Sources: []uint32{1001, 1002}},
			{Semantics: "FID", Sources: []uint32{1003, 1004}},
			{Semantics: "FID", Sources: []uint32{1005, 1006}},
		},
	}
	plan := buildMediaPlan(offer)
	for _, ssrc := range []uint32{1000, 1001, 1002} {
		if !plan.shouldForward(ssrc) {
			t.Fatalf("ssrc %d must be forwarded (audio / SIM[0] / its RTX)", ssrc)
		}
	}
	for _, ssrc := range []uint32{1003, 1004, 1005, 1006} {
		if plan.shouldForward(ssrc) {
			t.Fatalf("ssrc %d must be dropped (higher simulcast layer)", ssrc)
		}
	}
	// 未声明 ssrc（探测/未来扩展）放行，订阅端安全丢弃。
	if !plan.shouldForward(9999) {
		t.Fatalf("undeclared ssrc must pass through")
	}
}

// 单层发布（conference 模式）：无 SIM 组、唯一 FID 组的第一个 ssrc 即主流。
func TestBuildMediaPlanSingleLayer(t *testing.T) {
	plan := buildMediaPlan(ClientOffer{
		AudioSSRC:  2000,
		SsrcGroups: []SsrcGroup{{Semantics: "FID", Sources: []uint32{2001, 2002}}},
	})
	if !plan.shouldForward(2001) || !plan.shouldForward(2002) {
		t.Fatalf("single-layer media+rtx must be forwarded")
	}
}

// 纯音频（无视频组）：一切放行。
func TestBuildMediaPlanAudioOnly(t *testing.T) {
	plan := buildMediaPlan(ClientOffer{AudioSSRC: 3000})
	if !plan.shouldForward(3000) || !plan.shouldForward(3001) {
		t.Fatalf("audio-only plan must forward everything")
	}
}
