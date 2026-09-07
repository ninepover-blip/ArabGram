package postresponse

import (
	"context"
	"sync"
)

type callback func()

type ActionKind string

const (
	ActionUpdatesDelivery              ActionKind = "updates_delivery"
	ActionAccountAuthorizationTeardown ActionKind = "account_authorization_teardown"
)

type UpdateState struct {
	Pts  int
	Qts  int
	Date int
	Seq  int
}

type UpdatesDeliveryAction struct {
	CommitCursor  bool
	CursorAuthKey [8]byte
	CursorUserID  int64
	CursorState   UpdateState
	CursorMode    uint8

	MarkSecretDelivered bool
	SecretDeviceKey     int64
	SecretEventIDs      []int64

	MarkSessionReady bool
	ReadyRawAuthKey  [8]byte
	ReadySessionID   int64
	ReadyUserID      int64
	ActivationToken  uint64

	PublishBootstrap    bool
	BootstrapUserID     int64
	BootstrapAuthKey    [8]byte
	BootstrapRawAuthKey [8]byte
	BootstrapSessionID  int64
	BootstrapProbeToken uint64
}

// AccountAuthorizationTeardownAction carries only the device-scoped runtime
// retirement that must happen after the deletion result is admitted to the
// client transport. Durable authorization and secret-chat mutations have
// already committed before this action is registered.
type AccountAuthorizationTeardownAction struct {
	UserID    int64
	AuthKeyID [8]byte
}

type Action struct {
	Kind                         ActionKind
	UpdatesDelivery              UpdatesDeliveryAction
	AccountAuthorizationTeardown AccountAuthorizationTeardownAction
}

type ActionExecutor interface {
	RunPostResponseActions(ctx context.Context, actions []Action) error
}

type callbacksKey struct{}
type typedActionDeliveryKey struct{}

type callbacks struct {
	mu      sync.Mutex
	list    []callback
	actions []Action
}

func WithCallbacks(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(callbacksKey{}).(*callbacks); ok {
		return ctx
	}
	return context.WithValue(ctx, callbacksKey{}, &callbacks{})
}

func WithTypedActionDelivery(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, typedActionDeliveryKey{}, true)
}

func TypedActionDelivery(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	typed, _ := ctx.Value(typedActionDeliveryKey{}).(bool)
	return typed
}

func Register(ctx context.Context, cb func()) bool {
	if cb == nil {
		return false
	}
	cbs, ok := ctx.Value(callbacksKey{}).(*callbacks)
	if !ok || cbs == nil {
		return false
	}
	cbs.mu.Lock()
	cbs.list = append(cbs.list, cb)
	cbs.mu.Unlock()
	return true
}

func RegisterAction(ctx context.Context, action Action) bool {
	if action.Kind == "" {
		return false
	}
	cbs, ok := ctx.Value(callbacksKey{}).(*callbacks)
	if !ok || cbs == nil {
		return false
	}
	cbs.mu.Lock()
	cbs.actions = append(cbs.actions, cloneAction(action))
	cbs.mu.Unlock()
	return true
}

func Run(ctx context.Context) {
	run := Take(ctx)
	if run != nil {
		run()
	}
}

// Take transfers ownership of every currently registered callback to the caller.
// The returned function is idempotent and may safely outlive the request context.
// MTProto uses this to release a business worker after admitting rpc_result while
// still delaying follow-up updates until the result reaches the reliable stream.
func Take(ctx context.Context) func() {
	cbs, ok := ctx.Value(callbacksKey{}).(*callbacks)
	if !ok || cbs == nil {
		return nil
	}
	cbs.mu.Lock()
	list := append([]callback(nil), cbs.list...)
	cbs.list = nil
	cbs.mu.Unlock()
	if len(list) == 0 {
		return nil
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for _, cb := range list {
				cb()
			}
		})
	}
}

func TakeActions(ctx context.Context) []Action {
	cbs, ok := ctx.Value(callbacksKey{}).(*callbacks)
	if !ok || cbs == nil {
		return nil
	}
	cbs.mu.Lock()
	actions := cloneActions(cbs.actions)
	cbs.actions = nil
	cbs.mu.Unlock()
	return actions
}

func TakeWithExecutor(ctx context.Context, executor ActionExecutor) func() {
	callbacks := Take(ctx)
	actions := TakeActions(ctx)
	if callbacks == nil && len(actions) == 0 {
		return nil
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if callbacks != nil {
				callbacks()
			}
			if len(actions) != 0 && executor != nil {
				_ = executor.RunPostResponseActions(context.Background(), actions)
			}
		})
	}
}

func cloneActions(actions []Action) []Action {
	if len(actions) == 0 {
		return nil
	}
	out := make([]Action, len(actions))
	for i := range actions {
		out[i] = cloneAction(actions[i])
	}
	return out
}

func cloneAction(action Action) Action {
	if len(action.UpdatesDelivery.SecretEventIDs) != 0 {
		action.UpdatesDelivery.SecretEventIDs = append([]int64(nil), action.UpdatesDelivery.SecretEventIDs...)
	}
	return action
}
