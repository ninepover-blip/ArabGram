package messages

import (
	"context"
	"strings"

	"telesrv/internal/domain"
)

type EchoBusinessAutomationProvider struct{}

func NewEchoBusinessAutomationProvider() EchoBusinessAutomationProvider {
	return EchoBusinessAutomationProvider{}
}

func (EchoBusinessAutomationProvider) BusinessAutomationReplies(_ context.Context, input BusinessAutomationReplyInput) ([]domain.QuickReplyMessage, error) {
	body := input.TriggerMessage.Body
	if strings.TrimSpace(body) == "" {
		return nil, nil
	}
	return []domain.QuickReplyMessage{{
		ID:       1,
		Date:     input.Now,
		Message:  body,
		Entities: append([]domain.MessageEntity(nil), input.TriggerMessage.Entities...),
	}}, nil
}
