package config

import (
	"fmt"
	"strings"

	"telesrv/internal/deliverycontract"
)

// RequireInstanceID validates the explicitly configured stable process
// identity. Durable fencing and Redis ownership must never use a generated or
// normalized fallback identity.
func RequireInstanceID(configured string) (string, error) {
	if configured == "" {
		return "", fmt.Errorf("instance id is required")
	}
	if strings.TrimSpace(configured) != configured {
		return "", fmt.Errorf("instance id must be canonical")
	}
	if len(configured) > deliverycontract.MaxInstanceIDBytes {
		return "", fmt.Errorf("instance id exceeds %d bytes", deliverycontract.MaxInstanceIDBytes)
	}
	return configured, nil
}
