package rpc

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
)

type partialSuggestedPostChannels struct {
	ChannelsService
	results []domain.ToggleSuggestedPostApprovalResult
	err     error
}

func (s *partialSuggestedPostChannels) ProcessSuggestedPostLifecycle(context.Context, domain.SuggestedPostLifecycleRequest) ([]domain.ToggleSuggestedPostApprovalResult, error) {
	return s.results, s.err
}

func (s *partialSuggestedPostChannels) ToggleSuggestedPostApproval(ctx context.Context, req domain.ToggleSuggestedPostApprovalRequest) (domain.ToggleSuggestedPostApprovalResult, error) {
	return s.ChannelsService.(suggestedPostApprovalService).ToggleSuggestedPostApproval(ctx, req)
}

func TestSuggestedPostDispatcherPreservesSuccessfulPrefixWhenAnotherAggregateFails(t *testing.T) {
	fixture := newRPCChannelFixture(t)
	fixture.router.deps.Channels = &partialSuggestedPostChannels{
		ChannelsService: fixture.router.deps.Channels,
		results:         []domain.ToggleSuggestedPostApprovalResult{{}},
		err:             errors.New("poisoned lifecycle row"),
	}
	if !NewSuggestedPostDispatcher(fixture.router, nil).DispatchOnce(context.Background()) {
		t.Fatal("DispatchOnce = false, want successful result preserved despite sibling failure")
	}
}
