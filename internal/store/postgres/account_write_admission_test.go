package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNormalizeAccountWriteUserIDs(t *testing.T) {
	got := normalizeAccountWriteUserIDs([]int64{9, 0, 4, 9, -1, 5, 4})
	want := []int64{4, 5, 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalize=%v, want %v", got, want)
	}
}

func TestAccountWriteAdmissionAllowsDisjointSetsAndSerializesCompleteOverlap(t *testing.T) {
	actor := processAccountWriteAdmission
	hold, err := actor.Acquire(context.Background(), 910001, 910002)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	defer hold()

	disjointCtx, cancelDisjoint := context.WithTimeout(context.Background(), time.Second)
	defer cancelDisjoint()
	disjoint, err := actor.Acquire(disjointCtx, 910003, 910004)
	if err != nil {
		t.Fatalf("disjoint acquire blocked: %v", err)
	}
	disjoint()

	type acquireResult struct {
		release func()
		err     error
	}
	overlapResult := make(chan acquireResult, 1)
	go func() {
		release, acquireErr := actor.Acquire(context.Background(), 910002, 910001)
		overlapResult <- acquireResult{release: release, err: acquireErr}
	}()

	select {
	case result := <-overlapResult:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("overlapping set granted before complete holder release: %v", result.err)
	case <-time.After(25 * time.Millisecond):
	}

	hold()
	select {
	case result := <-overlapResult:
		if result.err != nil {
			t.Fatalf("overlap acquire after release: %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("overlapping set was not granted after holder release")
	}
}

func TestAccountWriteAdmissionCancellationCannotStrandGrant(t *testing.T) {
	actor := processAccountWriteAdmission
	hold, err := actor.Acquire(context.Background(), 920001)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	if release, acquireErr := actor.Acquire(waitCtx, 920001); !errors.Is(acquireErr, context.DeadlineExceeded) {
		if release != nil {
			release()
		}
		t.Fatalf("canceled acquire err=%v, want deadline exceeded", acquireErr)
	}
	hold()

	probeCtx, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	probe, err := actor.Acquire(probeCtx, 920001)
	if err != nil {
		t.Fatalf("account lease stranded after cancellation: %v", err)
	}
	probe()
}
