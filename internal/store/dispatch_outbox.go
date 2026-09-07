package store

// DispatchOutboxStore persists account PTS realtime delivery attempts. The
// durable business fact remains user_update_events.
type DispatchOutboxStore interface {
	DurableOutboxStateStore
}
