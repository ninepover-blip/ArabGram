package coreexec

import (
	"time"

	"github.com/iamxvbaba/td/tlprofile"
)

// PendingAdmissionStats is a bounded, identity-free snapshot of edge-side
// captured admission state. It is safe to export as low-cardinality metrics.
type PendingAdmissionStats struct {
	Count     int
	Capacity  int
	TTL       time.Duration
	OldestAge time.Duration
}

func pendingAdmissionLimit(limit int) int {
	if limit <= 0 {
		return defaultMaxPendingAdmissions
	}
	return limit
}

func pendingAdmissionTTLValue(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultPendingAdmissionTTL
	}
	return ttl
}

func pendingAdmissionStats(now time.Time, pending map[tlprofile.PreparedIdentity][]capturedAdmission, count, limit int, ttl time.Duration) PendingAdmissionStats {
	stats := PendingAdmissionStats{
		Count:    count,
		Capacity: pendingAdmissionLimit(limit),
		TTL:      pendingAdmissionTTLValue(ttl),
	}
	if stats.Count < 0 {
		stats.Count = 0
	}
	for _, queue := range pending {
		for i := range queue {
			createdAt := queue[i].CreatedAt
			if createdAt.IsZero() {
				continue
			}
			age := now.Sub(createdAt)
			if age < 0 {
				age = 0
			}
			if age > stats.OldestAge {
				stats.OldestAge = age
			}
		}
	}
	return stats
}

func (r *GRPCRemote) PendingAdmissionStats() PendingAdmissionStats {
	if r == nil {
		return PendingAdmissionStats{
			Capacity: defaultMaxPendingAdmissions,
			TTL:      defaultPendingAdmissionTTL,
		}
	}
	return r.pendingAdmissionStats(time.Now())
}

func (r *GRPCRemote) pendingAdmissionStats(now time.Time) PendingAdmissionStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupPendingLocked(now)
	return pendingAdmissionStats(now, r.pending, r.pendingCount, r.maxPendingAdmissions, r.pendingAdmissionTTL)
}
