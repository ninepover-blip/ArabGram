package core

import (
	obsmetrics "telesrv/internal/observability/metrics"
	"telesrv/internal/rpc"
)

type presenceWorkSource interface {
	PresenceWorkSnapshot() rpc.PresenceWorkSnapshot
}

func registerPresenceWorkMetrics(registry *obsmetrics.Registry, source presenceWorkSource) {
	registry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		s := source.PresenceWorkSnapshot()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_presence_work", Labels: []obsmetrics.Label{{Name: "stage", Value: "grace"}}, Value: float64(s.WaitingTimers)},
			{Name: "telesrv_presence_work", Labels: []obsmetrics.Label{{Name: "stage", Value: "callback"}}, Value: float64(s.RunningCallbacks)},
			{Name: "telesrv_presence_work", Labels: []obsmetrics.Label{{Name: "stage", Value: "direct_write"}}, Value: float64(s.DirectWrites)},
			{Name: "telesrv_presence_work", Labels: []obsmetrics.Label{{Name: "stage", Value: "last_seen_batch"}}, Value: float64(s.PendingLastSeen)},
			{Name: "telesrv_presence_work_pending", Value: float64(s.WaitingTimers + s.RunningCallbacks + s.DirectWrites + s.PendingLastSeen)},
		}
	})
}
