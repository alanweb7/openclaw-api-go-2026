package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/events"
)

type Client struct {
	cfg    config.Config
	logger *slog.Logger
	http   *http.Client
}

func New(cfg config.Config, logger *slog.Logger) *Client {
	return &Client{
		cfg:    cfg,
		logger: logger.With("component", "webhook"),
		http: &http.Client{
			Timeout: cfg.WebhookTimeout,
		},
	}
}

func (c *Client) Send(ctx context.Context, payload events.WebhookPayload) error {
	return c.sendToURL(ctx, c.cfg.WebhookURL, payload)
}

func (c *Client) SendToURL(ctx context.Context, webhookURL string, payload events.WebhookPayload) error {
	return c.sendToURL(ctx, webhookURL, payload)
}

func (c *Client) sendToURL(ctx context.Context, webhookURL string, payload events.WebhookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	maxAttempts := c.cfg.HTTPRetryCount + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.HTTPRetryDelay * time.Duration(attempt-1)):
			}
		}

		if err := c.sendOnce(ctx, webhookURL, body); err != nil {
			lastErr = err
			c.logger.Warn("webhook delivery failed",
				"event_type", payload.EventType,
				"session_key", payload.SessionKey,
				"webhook_url", webhookURL,
				"attempt", attempt,
				"error", err.Error(),
			)
			continue
		}

		c.logger.Info("webhook delivered",
			"event_type", payload.EventType,
			"session_key", payload.SessionKey,
			"webhook_url", webhookURL,
			"attempt", attempt,
		)
		return nil
	}
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", maxAttempts, lastErr)
}

func (c *Client) sendOnce(ctx context.Context, webhookURL string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.cfg.WebhookAuthHeader != "" && c.cfg.WebhookAuthValue != "" {
		req.Header.Set(c.cfg.WebhookAuthHeader, c.cfg.WebhookAuthValue)
	}

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform webhook request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}
	return nil
}
