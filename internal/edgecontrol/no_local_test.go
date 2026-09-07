package edgecontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
)

func TestNoLocalControllerPushesFailClosed(t *testing.T) {
	c := NewNoLocalController()
	ctx := context.Background()
	update := &tg.UpdatesTooLong{}
	raw := [8]byte{1}
	business := [8]byte{2}

	if err := c.PushToSessionForAuthKey(ctx, raw, 10, proto.MessageFromServer, update); !errors.Is(err, ErrNoLocalSessionController) {
		t.Fatalf("PushToSessionForAuthKey err = %v, want ErrNoLocalSessionController", err)
	}
	if err := c.PushToSessionForAuthKeyImmediate(ctx, raw, 10, proto.MessageFromServer, update); !errors.Is(err, ErrNoLocalSessionController) {
		t.Fatalf("PushToSessionForAuthKeyImmediate err = %v, want ErrNoLocalSessionController", err)
	}
	for name, push := range map[string]func() (int, error){
		"PushToUserExceptAuthKeySession": func() (int, error) {
			return c.PushToUserExceptAuthKeySession(ctx, 100, raw, 10, proto.MessageFromServer, update)
		},
		"PushToUserExceptAuthKeySessionBounded": func() (int, error) {
			return c.PushToUserExceptAuthKeySessionBounded(ctx, 100, raw, 10, proto.MessageFromServer, update, time.Second)
		},
		"PushToUserTransientExceptAuthKeySession": func() (int, error) {
			return c.PushToUserTransientExceptAuthKeySession(ctx, 100, raw, 10, proto.MessageFromServer, update, time.Second)
		},
		"PushToUserAuthKey": func() (int, error) {
			return c.PushToUserAuthKey(ctx, 100, business, proto.MessageFromServer, update)
		},
		"PushToUserAuthKeyTransient": func() (int, error) {
			return c.PushToUserAuthKeyTransient(ctx, 100, business, proto.MessageFromServer, update, time.Second)
		},
		"PushToUserExceptBusinessAuthKey": func() (int, error) {
			return c.PushToUserExceptBusinessAuthKey(ctx, 100, business, proto.MessageFromServer, update, time.Second)
		},
		"PushToUserTransientAtLeastLayer": func() (int, error) {
			return c.PushToUserTransientAtLeastLayer(ctx, 100, 228, proto.MessageFromServer, update, time.Second)
		},
		"PushToUserAuthKeyTransientAtLeastLayer": func() (int, error) {
			return c.PushToUserAuthKeyTransientAtLeastLayer(ctx, 100, business, 228, proto.MessageFromServer, update, time.Second)
		},
	} {
		sent, err := push()
		if sent != 0 || !errors.Is(err, ErrNoLocalSessionController) {
			t.Fatalf("%s = sent %d err %v, want sent 0 ErrNoLocalSessionController", name, sent, err)
		}
	}
}

func TestControlFabricControllerMissingFabricDoesNotFallbackToLocal(t *testing.T) {
	local := &captureFullController{}
	controller, err := NewControlFabricController(local, nil)
	if controller != nil || !errors.Is(err, ErrSessionControlFabricRequired) {
		t.Fatalf("NewControlFabricController missing fabric = controller %T err %v, want ErrSessionControlFabricRequired", controller, err)
	}
	if local.pushKind != "" {
		t.Fatalf("local controller received push without fabric: kind=%s user=%d", local.pushKind, local.pushUserID)
	}
}

func TestControlFabricControllerRequiresExplicitDependencies(t *testing.T) {
	controller, err := NewControlFabricController(nil, &SessionControlFabric{})
	if controller != nil || !errors.Is(err, ErrControlFabricControllerRequired) {
		t.Fatalf("NewControlFabricController nil controller = controller %T err %v, want ErrControlFabricControllerRequired", controller, err)
	}
	controller, err = NewControlFabricController(&captureFullController{}, &SessionControlFabric{})
	if controller != nil || !errors.Is(err, ErrSessionControlFabricDependenciesRequired) {
		t.Fatalf("NewControlFabricController empty fabric = controller %T err %v, want ErrSessionControlFabricDependenciesRequired", controller, err)
	}
}
