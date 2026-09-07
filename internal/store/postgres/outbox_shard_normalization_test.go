package postgres

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"telesrv/internal/store"
)

func frozenOutboxScheduleShards(shardCount int, shardIDs []int) ([]int16, bool, error) {
	if shardCount == 0 && len(shardIDs) == 0 {
		return nil, false, nil
	}
	if shardCount != store.DispatchOutboxLogicalShards {
		return nil, true, fmt.Errorf("shard count %d, want stable %d", shardCount, store.DispatchOutboxLogicalShards)
	}
	seen := make(map[int]struct{}, len(shardIDs))
	ids := make([]int16, 0, len(shardIDs))
	for _, id := range shardIDs {
		if id < 0 || id >= shardCount {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, int16(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, true, nil
}

func TestNormalizeOutboxScheduleShardsMatchesFrozenSemantics(t *testing.T) {
	cases := [][]int{
		nil,
		{},
		{7},
		{0, 4, 8, 12, 252},
		{252, 12, 8, 4, 0},
		{-1, 0, 0, 255, 256},
		{65536, 0},
		{int(^uint(0) >> 1), -int(^uint(0)>>1) - 1, 0, 255},
	}
	rng := rand.New(rand.NewSource(20260901))
	for i := 0; i < 10000; i++ {
		ids := make([]int, rng.Intn(384))
		for i := range ids {
			ids[i] = rng.Intn(384) - 64
		}
		cases = append(cases, ids)
	}
	for i, input := range cases {
		var inputBefore []int
		if input != nil {
			inputBefore = append([]int{}, input...)
		}
		want, wantScoped, wantErr := frozenOutboxScheduleShards(store.DispatchOutboxLogicalShards, input)
		got, gotScoped, gotErr := normalizeOutboxScheduleShards(store.DispatchOutboxLogicalShards, input)
		if (gotErr == nil) != (wantErr == nil) || gotScoped != wantScoped || !reflect.DeepEqual(got, want) || (got == nil) != (want == nil) {
			t.Fatalf("case %d input=%v: got (%v,%t,%v), want (%v,%t,%v)", i, input, got, gotScoped, gotErr, want, wantScoped, wantErr)
		}
		if !reflect.DeepEqual(input, inputBefore) {
			t.Fatalf("case %d modified input: got %v, want %v", i, input, inputBefore)
		}
	}

	for _, count := range []int{-1, 1, store.DispatchOutboxLogicalShards - 1, store.DispatchOutboxLogicalShards + 1} {
		_, gotScoped, gotErr := normalizeOutboxScheduleShards(count, []int{0})
		_, wantScoped, wantErr := frozenOutboxScheduleShards(count, []int{0})
		if gotScoped != wantScoped || gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("count %d: got scoped=%t err=%v, want scoped=%t err=%v", count, gotScoped, gotErr, wantScoped, wantErr)
		}
	}
}

func TestNormalizeOutboxScheduleShardsReturnsIndependentResults(t *testing.T) {
	input := []int{0, 4, 8, 12, 252}
	first, _, err := normalizeOutboxScheduleShards(store.DispatchOutboxLogicalShards, input)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := normalizeOutboxScheduleShards(store.DispatchOutboxLogicalShards, input)
	if err != nil {
		t.Fatal(err)
	}
	first[0] = 99
	input[1] = 5
	if second[0] != 0 || second[1] != 4 {
		t.Fatalf("second result aliases first/input: %v", second)
	}
}
