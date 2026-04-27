package events

import (
	"testing"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

func TestShouldForward(t *testing.T) {
	cfg := config.Config{
		ForwardSessionMessage: true,
		ForwardSessionTool:    false,
	}

	if !ShouldForward(cfg, "session.message") {
		t.Fatalf("expected session.message to be forwarded")
	}
	if ShouldForward(cfg, "session.tool") {
		t.Fatalf("expected session.tool to be filtered")
	}
}

func TestBuildPayload(t *testing.T) {
	in := Incoming{
		RequestID:  "send-1",
		EventType:  "session.message",
		SessionKey: "agent:main:guardian",
		Data: map[string]any{
			"type":  "event",
			"event": "session.message",
			"params": map[string]any{
				"text": "olá",
			},
		},
	}

	payload, err := BuildPayload(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if payload.Normalized["kind"] != "agent_message" {
		t.Fatalf("expected kind agent_message, got %v", payload.Normalized["kind"])
	}
	if payload.Normalized["text"] != "olá" {
		t.Fatalf("expected text olá, got %v", payload.Normalized["text"])
	}
}
