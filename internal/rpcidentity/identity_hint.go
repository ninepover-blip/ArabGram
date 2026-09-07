package rpcidentity

// LayerRPCIdentityHint carries connection-local identity state across the
// Edge/CoreExec boundary. It is a latency hint, not authorization authority:
// Core must only consume values it can prove from durable metadata or its own
// positive caches.
type LayerRPCIdentityHint struct {
	RawAuthKeyID                [8]byte
	SessionID                   int64
	BusinessAuthKeyID           [8]byte
	BusinessAuthKeyResolved     bool
	UserID                      int64
	UserIDResolved              bool
	RawAuthKeyExpiresAt         int
	RawAuthKeyExpiresAtResolved bool
	// SessionUpdatesReady is Edge-owned connection state, not authorization
	// evidence. Core uses it only to avoid staging duplicate readiness work.
	SessionUpdatesReady bool
}
