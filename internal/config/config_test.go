package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadValidation(t *testing.T) {
	t.Setenv("OPENCLAW_WS_URL", "ws://localhost:18789")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "token")
	t.Setenv("WEBHOOK_URL", "http://localhost:8080/hook")
	t.Setenv("AUTO_SEND_ON_CONNECT", "true")
	t.Setenv("AUTO_SEND_MESSAGE", "oi")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !cfg.AutoSendOnConnect {
		t.Fatalf("expected AutoSendOnConnect=true")
	}
}

func TestValidateMissingRequired(t *testing.T) {
	for _, key := range []string{"OPENCLAW_WS_URL", "OPENCLAW_GATEWAY_TOKEN", "WEBHOOK_URL"} {
		_ = os.Unsetenv(key)
	}
	t.Setenv("OPENCLAW_WS_URL", "")
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("WEBHOOK_URL", "")

	_, err := Load()
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "OPENCLAW_GATEWAY_TOKEN") {
		t.Fatalf("expected token validation error, got %v", err)
	}
}
