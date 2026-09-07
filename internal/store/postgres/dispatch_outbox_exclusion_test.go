package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestEnqueueDispatchRejectsHalfExclusionPair(t *testing.T) {
	tests := []struct {
		name      string
		authKeyID [8]byte
		sessionID int64
	}{
		{name: "auth key only", authKeyID: [8]byte{1}},
		{name: "session only", sessionID: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := enqueueDispatch(context.Background(), nil, dispatchEnqueue{
				ExcludeAuthKeyID: test.authKeyID,
				ExcludeSessionID: test.sessionID,
			})
			if !errors.Is(err, errInvalidDispatchOutboxExclusionPair) {
				t.Fatalf("enqueueDispatch error = %v, want %v", err, errInvalidDispatchOutboxExclusionPair)
			}
		})
	}
}
