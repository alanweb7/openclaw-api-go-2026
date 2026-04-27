package webhook

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/events"
)

func TestSend(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Token")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := config.Config{
		WebhookURL:        server.URL,
		WebhookAuthHeader: "X-Token",
		WebhookAuthValue:  "abc123",
		WebhookTimeout:    2 * time.Second,
		HTTPRetryCount:    0,
		HTTPRetryDelay:    1 * time.Millisecond,
	}

	client := New(cfg, slog.Default())
	payload := events.WebhookPayload{
		Source:    "openclaw",
		EventType: "session.message",
		Raw:       map[string]any{"type": "event"},
		Normalized: map[string]any{
			"kind": "agent_message",
		},
	}

	if err := client.Send(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotHeader != "abc123" {
		t.Fatalf("expected auth header to be sent")
	}
}
