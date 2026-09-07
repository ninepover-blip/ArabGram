package loadharness

import (
	"context"
	"errors"
	"sync"
	"time"
)

const resourcePSDeadline = time.Second

type businessBucket struct {
	epoch         int64
	calls, errors uint64
}
type BusinessGuardReport struct {
	Calls      uint64    `json:"window_calls"`
	Errors     uint64    `json:"window_errors"`
	AboveSince time.Time `json:"above_since,omitempty"`
	Tripped    bool      `json:"tripped"`
}
type businessGuard struct {
	mu      sync.Mutex
	buckets [30]businessBucket
	above   time.Time
	tripped bool
	last    BusinessGuardReport
}

func businessMethod(name string) bool {
	switch name {
	case "auth.status", "help.getConfig", "messages.sendMessage", "messages.sendMessage.retry", "messages.getDialogs", "updates.getState", "updates.getState.delivery", "updates.getDifference", "updates.getDifference.delivery", "updates.getChannelDifference", "messages.getPinnedDialogs", "upload.getFile", "upload.saveFilePart", "messages.uploadMedia":
		return true
	}
	return false
}
func (g *businessGuard) observe(name string, at time.Time, err error) {
	if g == nil || !businessMethod(name) || errors.Is(err, context.Canceled) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	epoch := at.Unix()
	i := epoch % 30
	b := &g.buckets[i]
	if b.epoch != epoch {
		*b = businessBucket{epoch: epoch}
	}
	b.calls++
	if err != nil {
		b.errors++
	}
}
func (g *businessGuard) evaluate(now time.Time) BusinessGuardReport {
	if g == nil {
		return BusinessGuardReport{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	r := BusinessGuardReport{}
	if g.tripped {
		return g.last
	}
	for _, b := range g.buckets {
		if b.epoch > now.Unix()-30 && b.epoch <= now.Unix() {
			r.Calls += b.calls
			r.Errors += b.errors
		}
	}
	if r.Calls > 0 && float64(r.Errors)/float64(r.Calls) > .01 {
		if g.above.IsZero() {
			g.above = now
		}
		if now.Sub(g.above) >= 30*time.Second {
			g.tripped = true
		}
	} else {
		g.above = time.Time{}
	}
	r.AboveSince, r.Tripped = g.above, g.tripped
	g.last = r
	return r
}
func (g *businessGuard) report() BusinessGuardReport { g.mu.Lock(); defer g.mu.Unlock(); return g.last }
