package loadharness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalEvidenceErrorIsNotTransportFailure(t *testing.T) {
	for _, e := range []error{
		&os.PathError{Op: "open", Path: "private-fixture", Err: os.ErrPermission},
		&os.LinkError{Op: "link", Old: "new-location", New: "reserved-location", Err: os.ErrExist},
	} {
		if got := classifyError(fmt.Errorf("evidence: %w", e)); got != "filesystem" {
			t.Fatalf("local evidence error classified as %q", got)
		}
	}
	if got := classifyError(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}); got != "connection" {
		t.Fatalf("network error classified as %q", got)
	}
	if got := classifyError(fmt.Errorf("evidence: %w", context.DeadlineExceeded)); got != "timeout" {
		t.Fatalf("deadline classified as %q", got)
	}
}

func TestFileFixtureMalformedInputsDoNotReplaceEvidence(t *testing.T) {
	endpoint := Endpoint{Address: "127.0.0.1:2398", DC: 2}
	fixture := &downloadFixture{location: &tg.InputDocumentFileLocation{ID: 1, AccessHash: 2, FileReference: []byte{3}}, size: 1024, chunk: 1024}
	for _, mode := range []string{"trailing", "oversize", "permissions", "reference_limit", "overwrite"} {
		t.Run(mode, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "fixture.json")
			if err := writePersistedFileFixture(p, endpoint, fixture); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(p)
			switch mode {
			case "trailing":
				os.WriteFile(p, append(before, []byte("{}")...), 0600)
			case "oversize":
				os.WriteFile(p, make([]byte, 64<<10+1), 0600)
			case "permissions":
				os.Chmod(p, 0644)
			case "reference_limit":
				v := persistedFixture(endpoint, fixture)
				v.FileReference = make([]byte, 4097)
				b, _ := json.Marshal(v)
				os.WriteFile(p, b, 0600)
			case "overwrite":
				if err := writePersistedFileFixture(p, endpoint, fixture); err == nil {
					t.Fatal("overwrote prior fixture")
				}
				after, _ := os.ReadFile(p)
				if string(after) != string(before) {
					t.Fatal("old location changed")
				}
				return
			}
			if _, err := loadPersistedFileFixture(p, endpoint, fixture.size, fixture.chunk); err == nil {
				t.Fatal("unsafe fixture accepted")
			}
		})
	}
}

func TestWorkloadAdmissionAndPerRPCDeadline(t *testing.T) {
	calls := 0
	base := safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
		calls++
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v := workloadInvoker(SessionRecord{}, base, nil)
	if !errors.Is(v.Invoke(ctx, nil, nil), context.Canceled) || calls != 0 {
		t.Fatal("post-cancel RPC admitted")
	}
	start := time.Now()
	err := (deadlineInvoker{next: v, timeout: 20 * time.Millisecond}).Invoke(context.Background(), nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || time.Since(start) > time.Second {
		t.Fatal("operation escaped deadline", err, calls)
	}
}

func TestStartupReplyViolationsAreNotRPCFailures(t *testing.T) {
	for _, mode := range []string{"rpc", "cursor", "dialogs"} {
		t.Run(mode, func(t *testing.T) {
			raw := tg.NewClient(safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
				if mode == "rpc" {
					return context.DeadlineExceeded
				}
				switch v := out.(type) {
				case *tg.UpdatesDifferenceBox:
					v.Difference = &tg.UpdatesDifference{State: tg.UpdatesState{Pts: 1, Qts: 1}}
				case *tg.MessagesPeerDialogs:
				case *tg.MessagesDialogsBox:
					v.Dialogs = &tg.MessagesDialogsNotModified{}
				default:
					t.Fatalf("unexpected %T", out)
				}
				return nil
			}))
			var err error
			if mode == "dialogs" {
				_, _, err = snapshotDialogsObserved(context.Background(), time.Second, raw, dialogPaginationProfile{20, 100}, nil)
			} else {
				_, _, err = startupAccountDifference(context.Background(), time.Second, raw, ClientAccountState{State: ClientUpdateState{Pts: 10, Qts: 2}}, nil, newMetricSet("updates.getDifference"))
			}
			if mode == "rpc" {
				if invariantClass(err) != "" || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("RPC error became invariant", err)
				}
			} else if invariantClass(err) == "" {
				t.Fatal("reply violation not classified", err)
			}
		})
	}
}

func TestStartupPrivateTopUsesOwnerAndBothDirections(t *testing.T) {
	dataset := &Dataset{RunID: "test", Config: DatasetConfig{Accounts: 2}}
	targets := []SessionRecord{{UserID: 100}, {UserID: 200}}
	mutation := &OfflineMutationState{PrivateMessageIDs: []int{10, 99999}}
	out := sha256.Sum256([]byte(offlinePrivateMarker(dataset, 0, 1)))
	in := sha256.Sum256([]byte(offlinePrivateMarker(dataset, 1, 0)))
	for _, tc := range []struct {
		name   string
		id     int
		digest [32]byte
		valid  bool
	}{{"own_last", 10, out, true}, {"peer_later", 11, in, true}, {"peer_stale", 9, in, false}, {"own_id_wrong", 11, out, false}, {"payload_wrong", 11, sha256.Sum256([]byte("wrong")), false}} {
		t.Run(tc.name, func(t *testing.T) {
			d := map[clientPeerKey]ClientDialogState{{typ: "user", id: 200}: {TopMessage: tc.id, topMessageDigest: tc.digest}}
			e := validateOfflinePrivateTops(dataset, mutation, targets, 0, d)
			if (e == nil) != tc.valid {
				t.Fatal(e)
			}
		})
	}
	dataset.Config.Accounts = 3
	targets = append(targets, SessionRecord{UserID: 300})
	mutation.PrivateMessageIDs = append(mutation.PrivateMessageIDs, 900000)
	d := map[clientPeerKey]ClientDialogState{{typ: "user", id: 200}: {TopMessage: 10, topMessageDigest: sha256.Sum256([]byte(offlinePrivateMarker(dataset, 0, 1)))}, {typ: "user", id: 300}: {TopMessage: 2, topMessageDigest: sha256.Sum256([]byte(offlinePrivateMarker(dataset, 2, 0)))}}
	if e := validateOfflinePrivateTops(dataset, mutation, targets, 0, d); e != nil {
		t.Fatal("compared another owner's ID", e)
	}
}
