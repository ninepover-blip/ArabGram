package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestAttachMenuStateDeliveryAtomicityMemory(t *testing.T) {
	ctx := context.Background()
	st := NewBotStore(NewUserStore())
	st.AttachDeliveryDependencies(NewDialogStore(), NewDeliveryOutboxStore())
	const userID, botID int64 = 8101, 8102
	st.attachMenu[botID] = domain.BotAttachMenuBot{BotUserID: botID, ShortName: "atomic"}
	effects := func(domain.BotAttachMenuState) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
	want := domain.BotAttachMenuState{UserID: userID, BotUserID: botID, Enabled: true}
	if got, err := st.SetAttachMenuState(ctx, want, effects); err != nil || got != want {
		t.Fatalf("set state = %+v err:%v, want %+v", got, err, want)
	}
	if items := st.DeliveryOutbox().Snapshot(); len(items) != 1 || items[0].TargetUserID != userID {
		t.Fatalf("outbox after set = %+v, want one user item", items)
	}

	wantErr := errors.New("projection failed")
	failed := want
	failed.Enabled = false
	if _, err := st.SetAttachMenuState(ctx, failed, func(domain.BotAttachMenuState) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed set error = %v, want %v", err, wantErr)
	}
	if got, found, err := st.GetAttachMenuState(ctx, userID, botID); err != nil || !found || got != want {
		t.Fatalf("state after delivery failure = %+v found:%v err:%v, want rollback", got, found, err)
	}
	if items := st.DeliveryOutbox().Snapshot(); len(items) != 1 {
		t.Fatalf("outbox after failed set = %d, want 1", len(items))
	}
}
