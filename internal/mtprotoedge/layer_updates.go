package mtprotoedge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
)

var (
	ErrOutboundLayerProfileUnknown  = errors.New("outbound exact layer profile is unknown")
	ErrOutboundLayerProfileMismatch = errors.New("outbound exact layer profile mismatch")
	// ErrOutboundLayerBindingRequired means application bytes reached the edge
	// without the request- or session-owned profile proof required by the native
	// generated codec. Never substitute canonical bytes or the retired legacy
	// transcoder on a production connection.
	ErrOutboundLayerBindingRequired = errors.New("outbound application value requires exact layer binding")
	// ErrOutboundLayerProfileStale means a proactive update was prepared for an
	// older connection profile epoch. It is deliberately non-terminal: discard
	// the online accelerator and let durable difference recovery fill the gap.
	ErrOutboundLayerProfileStale = errors.New("outbound exact layer profile epoch is stale")
	errLayerUpdatesFanoutClosed  = errors.New("exact layer updates fanout is closed")
)

type outboundLayerBindingKind uint8

const (
	// The zero value preserves strict connection binding for defensive and
	// legacy constructions which do not declare a stronger ownership model.
	outboundLayerBindingSession outboundLayerBindingKind = iota
	// A request-bound result retains the profile captured by admission. A later
	// invokeWithLayer correction must not invalidate that in-flight/retained result.
	outboundLayerBindingRequest
)

type outboundLayerBinding struct {
	profile       tlprofile.Profile
	wireInvariant bool
	kind          outboundLayerBindingKind
	// epoch is required for proactive updates. Zero is accepted only for older
	// defensive/test bindings which predate mutable profile correction.
	epoch uint32
}

type preparedLayerUpdates struct {
	done          chan struct{}
	encoded       *encodedOutboundMessage
	err           error
	retainedBytes int
}

// layerUpdatesFanout is one immutable canonical Updates snapshot plus a
// request-scoped cache of exact prepared bytes. FreezeObject and
// FrozenObject.Prepare use the same sparse TypeRef execution plans as RPC results
// and differences; this type adds only fan-out singleflight and ownership.
type layerUpdatesFanout struct {
	frozen *tlprofile.FrozenObject
	size   int

	// Durable delivery supplies a process-wide retained-byte budget. The frozen
	// canonical snapshot and every cached exact-profile body remain charged until
	// the cache drops ownership. Ordinary request-scoped fanout leaves these nil
	// because its completed bodies are transferred to the per-connection budget.
	retainBytes   func(int) bool
	releaseBytes  func(int)
	frozenBytes   int
	preparedLimit int

	mu       sync.Mutex
	prepared map[tlprofile.Profile]*preparedLayerUpdates
	closed   bool
}

func newLayerUpdatesFanout(value tg.UpdatesClass) (*layerUpdatesFanout, error) {
	return newRetainedLayerUpdatesFanout(value, nil, nil, 0)
}

func newRetainedLayerUpdatesFanout(
	value tg.UpdatesClass,
	retain func(int) bool,
	release func(int),
	preparedLimit int,
) (*layerUpdatesFanout, error) {
	if (retain == nil) != (release == nil) {
		return nil, errors.New("exact layer updates retained budget is incomplete")
	}
	frozen, err := tlprofile.FreezeObject(value)
	if err != nil {
		return nil, fmt.Errorf("freeze exact layer updates: %w", err)
	}
	size := frozen.CanonicalSize()
	u := &layerUpdatesFanout{
		frozen: frozen, size: size,
		retainBytes: retain, releaseBytes: release, preparedLimit: preparedLimit,
		prepared: make(map[tlprofile.Profile]*preparedLayerUpdates),
	}
	if retain != nil && size > 0 {
		if !retain(size) {
			return nil, fmt.Errorf("%w: frozen exact layer updates bytes=%d", ErrOutboundTrackedBudget, size)
		}
		u.frozenBytes = size
	}
	return u, nil
}

func (u *layerUpdatesFanout) canonicalSize() int {
	if u == nil {
		return 0
	}
	return u.size
}

func (u *layerUpdatesFanout) prepareForConn(ctx context.Context, c *Conn) (*encodedOutboundMessage, error) {
	if u == nil || c == nil {
		return nil, errors.New("nil exact layer update or connection")
	}
	state := c.LayerProfileState()
	if state.Origin == LayerProfileUnknown {
		return nil, ErrOutboundLayerProfileUnknown
	}
	base, err := u.prepare(ctx, state.Profile)
	if err != nil {
		return nil, err
	}
	if base == nil || base.layer == nil {
		return nil, errors.New("prepared exact layer update lost binding")
	}
	// Exact bytes stay shared per profile. Only the small binding is copied per
	// physical target because epoch belongs to that connection.
	encoded := *base
	binding := *base.layer
	binding.kind = outboundLayerBindingSession
	binding.epoch = state.Epoch
	encoded.layer = &binding
	return &encoded, nil
}

func (u *layerUpdatesFanout) prepare(ctx context.Context, profile tlprofile.Profile) (*encodedOutboundMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return nil, errLayerUpdatesFanoutClosed
	}
	entry := u.prepared[profile]
	if entry == nil {
		var released int
		if u.preparedLimit > 0 && len(u.prepared) >= u.preparedLimit {
			released = u.evictCompletedPreparedLocked(profile)
			if len(u.prepared) >= u.preparedLimit {
				u.mu.Unlock()
				u.releaseRetained(released)
				return nil, fmt.Errorf("%w: concurrent exact layer profile cache is full", ErrOutboundTrackedBudget)
			}
		}
		entry = &preparedLayerUpdates{done: make(chan struct{})}
		u.prepared[profile] = entry
		frozen := u.frozen
		retain := u.retainBytes
		u.mu.Unlock()
		u.releaseRetained(released)

		encoded, prepareErr := prepareFrozenLayerUpdatesContext(ctx, profile, frozen, retain)
		retained := 0
		if prepareErr == nil && encoded != nil && retain != nil {
			retained = len(encoded.body)
		}
		u.mu.Lock()
		if u.closed && prepareErr == nil {
			prepareErr = errLayerUpdatesFanoutClosed
			encoded = nil
		}
		entry.encoded, entry.err, entry.retainedBytes = encoded, prepareErr, retained
		if prepareErr != nil && u.prepared[profile] == entry {
			delete(u.prepared, profile)
		}
		if prepareErr != nil {
			entry.retainedBytes = 0
		}
		// Publish completion while holding the same mutex used by close. This
		// prevents close from observing an in-progress entry immediately before
		// completion and leaking its retained-byte reservation.
		close(entry.done)
		u.mu.Unlock()
		if prepareErr != nil && retained > 0 {
			u.releaseRetained(retained)
		}
	} else {
		u.mu.Unlock()
		// A completed profile is immutable and no longer needs caller budget.
		// Prefer it deterministically even when the shared fan-out deadline became
		// ready at the same instant; otherwise Go's select may randomly choose the
		// canceled branch and skip a healthy later connection of the same profile.
		select {
		case <-entry.done:
			return entry.encoded, entry.err
		default:
		}
		select {
		case <-entry.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return entry.encoded, entry.err
}

func (u *layerUpdatesFanout) discardPrepared(profile tlprofile.Profile, encoded *encodedOutboundMessage) {
	if u == nil || encoded == nil {
		return
	}
	u.mu.Lock()
	released := 0
	entry := u.prepared[profile]
	if entry != nil {
		select {
		case <-entry.done:
			if entry.encoded == encoded || (entry.encoded != nil &&
				entry.encoded.layer != nil && encoded.layer != nil &&
				entry.encoded.layer.profile == encoded.layer.profile &&
				sameBacking(entry.encoded.body, encoded.body)) {
				delete(u.prepared, profile)
				released = entry.retainedBytes
				entry.retainedBytes = 0
			}
		default:
		}
	}
	u.mu.Unlock()
	u.releaseRetained(released)
}

func (u *layerUpdatesFanout) evictCompletedPreparedLocked(keep tlprofile.Profile) int {
	released := 0
	for profile, entry := range u.prepared {
		if profile == keep || entry == nil {
			continue
		}
		select {
		case <-entry.done:
			delete(u.prepared, profile)
			released += entry.retainedBytes
			entry.retainedBytes = 0
		default:
		}
	}
	return released
}

func (u *layerUpdatesFanout) releaseRetained(bytes int) {
	if u != nil && bytes > 0 && u.releaseBytes != nil {
		u.releaseBytes(bytes)
	}
}

// close releases only this cache's ownership. Bodies already admitted to Conn
// actors remain independently covered by their per-connection tracked budgets.
func (u *layerUpdatesFanout) close() {
	if u == nil {
		return
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	u.closed = true
	released := u.frozenBytes
	u.frozenBytes = 0
	u.frozen = nil
	for profile, entry := range u.prepared {
		if entry == nil {
			delete(u.prepared, profile)
			continue
		}
		select {
		case <-entry.done:
			delete(u.prepared, profile)
			released += entry.retainedBytes
			entry.retainedBytes = 0
		default:
			// The encoder observes closed before publishing and releases its own
			// reservation. Keeping the entry until then preserves waiter wakeup.
		}
	}
	u.mu.Unlock()
	u.releaseRetained(released)
}

func prepareFrozenLayerUpdatesContext(
	ctx context.Context,
	profile tlprofile.Profile,
	frozen *tlprofile.FrozenObject,
	retain func(int) bool,
) (*encodedOutboundMessage, error) {
	var encoded *encodedOutboundMessage
	err := withOutboundEncodeSlot(ctx, nil, func() error {
		var body bin.Buffer
		if err := frozen.Encode(profile, &body); err != nil {
			return err
		}
		id, err := body.PeekID()
		if err != nil {
			return fmt.Errorf("peek exact updates constructor: %w", err)
		}
		encoded = &encodedOutboundMessage{
			body: body.Copy(), typeID: id,
			layer: &outboundLayerBinding{profile: profile},
		}
		if retain != nil && len(encoded.body) > 0 && !retain(len(encoded.body)) {
			encoded = nil
			return fmt.Errorf("%w: prepared exact layer updates", ErrOutboundTrackedBudget)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("prepare updates for layer %d: %w", profile, err)
	}
	return encoded, nil
}

func validateOutboundLayerBinding(c *Conn, encoded *encodedOutboundMessage) error {
	if encoded == nil || encoded.layer == nil {
		return nil
	}
	if encoded.layer.wireInvariant || encoded.layer.kind == outboundLayerBindingRequest {
		return nil
	}
	state := c.LayerProfileState()
	if state.Origin == LayerProfileUnknown {
		return ErrOutboundLayerProfileUnknown
	}
	if encoded.layer.epoch != 0 && state.Epoch != encoded.layer.epoch {
		return fmt.Errorf("%w: connection=%d encoded=%d", ErrOutboundLayerProfileStale, state.Epoch, encoded.layer.epoch)
	}
	if state.Profile != encoded.layer.profile {
		return fmt.Errorf("%w: connection=%d encoded=%d", ErrOutboundLayerProfileMismatch, state.Profile, encoded.layer.profile)
	}
	return nil
}

func isOutboundStaleLayerEpoch(err error) bool {
	return errors.Is(err, ErrOutboundLayerProfileStale)
}

func isOutboundLayerProfileError(err error) bool {
	return errors.Is(err, ErrOutboundLayerProfileUnknown) ||
		errors.Is(err, ErrOutboundLayerProfileMismatch)
}
