package auth

import "telesrv/internal/store/memory"

// newAuthorizationStoreWithDelivery makes the durable queue dependency
// explicit in every test that can complete a human login. The memory store
// never manufactures a private queue as a hidden fallback.
func newAuthorizationStoreWithDelivery() *memory.AuthorizationStore {
	auths := memory.NewAuthorizationStore()
	auths.AttachDeliveryOutbox(memory.NewDeliveryOutboxStore())
	return auths
}
