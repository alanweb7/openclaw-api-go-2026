package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/events"
	"github.com/alanweb7/openclaw-2026-api-go/internal/store"
	"github.com/alanweb7/openclaw-2026-api-go/internal/webhook"
	"github.com/alanweb7/openclaw-2026-api-go/internal/wsclient"
)

type App struct {
	cfg     config.Config
	logger  *slog.Logger
	ws      *wsclient.Client
	webhook *webhook.Client
	store   *store.Store

	mu       sync.Mutex
	delivery map[string]SessionDeliveryOptions
}

type SessionDeliveryOptions struct {
	CallbackURL string
	Stream      bool
	CreatedAt   time.Time
}

func New(ctx context.Context, cfg config.Config, logger *slog.Logger) *App {
	return &App{
		cfg:     cfg,
		logger:  logger.With("component", "app"),
		ws:      wsclient.New(cfg, logger),
		webhook: webhook.New(cfg, logger),
		store:   store.New(ctx, cfg, logger),
		delivery: map[string]SessionDeliveryOptions{},
	}
}

func (a *App) Run(ctx context.Context) error {
	for {
		err := a.ws.Serve(ctx, a.handleMessage)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		a.logger.Error("websocket lifecycle ended with error", "error", err.Error())
		if !a.cfg.WSReconnect {
			return fmt.Errorf("websocket closed and reconnect disabled: %w", err)
		}

		a.logger.Info("reconnect scheduled", "delay", a.cfg.WSReconnectDelay.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.cfg.WSReconnectDelay):
		}
	}
}

func (a *App) handleMessage(ctx context.Context, msg wsclient.Message) error {
	if msg.Type != "event" {
		a.logger.Debug("ws response frame", "request_id", msg.RequestID)
		return nil
	}

	a.logger.Info("ws event received",
		"event_type", msg.EventType,
		"session_key", msg.SessionKey,
		"request_id", msg.RequestID,
	)

	shouldForward := events.ShouldForward(a.cfg, msg.EventType)
	a.logger.Info("ws forward decision",
		"event_type", msg.EventType,
		"session_key", msg.SessionKey,
		"should_forward", shouldForward,
	)
	if !shouldForward {
		a.logger.Debug("event skipped by filter", "event_type", msg.EventType)
		return nil
	}

	delivery := a.getSessionDelivery(msg.SessionKey)
	if !delivery.Stream && !isFinalEvent(msg) {
		a.logger.Info("event skipped by stream=false",
			"event_type", msg.EventType,
			"session_key", msg.SessionKey,
		)
		return nil
	}

	incoming := events.Incoming{
		RequestID:  msg.RequestID,
		EventType:  msg.EventType,
		SessionKey: msg.SessionKey,
		RawBytes:   msg.Raw,
		Data:       msg.Frame,
	}

	payload, err := events.BuildPayload(incoming)
	if err != nil {
		return fmt.Errorf("build webhook payload: %w", err)
	}

	outboundDedupeKey := a.buildOutboundDedupeKey(msg)
	if outboundDedupeKey != "" {
		isNew, dedupeErr := a.store.RegisterDedupeKey(ctx, "outbound", outboundDedupeKey, a.cfg.DedupeRecordTTL)
		if dedupeErr != nil {
			a.logger.Warn("outbound dedupe check failed", "error", dedupeErr.Error())
		} else if !isNew {
			a.logger.Info("duplicate outbound event skipped",
				"event_type", msg.EventType,
				"session_key", msg.SessionKey,
				"request_id", msg.RequestID,
			)
			return nil
		}
	}

	a.logger.Info("webhook dispatching",
		"event_type", msg.EventType,
		"session_key", msg.SessionKey,
		"request_id", msg.RequestID,
		"webhook_url", firstNonEmpty(strings.TrimSpace(delivery.CallbackURL), a.cfg.WebhookURL),
		"stream", delivery.Stream,
	)

	var sendErr error
	if strings.TrimSpace(delivery.CallbackURL) != "" {
		sendErr = a.webhook.SendToURL(ctx, delivery.CallbackURL, payload)
	} else {
		sendErr = a.webhook.Send(ctx, payload)
	}
	if sendErr != nil {
		return fmt.Errorf("send webhook event %s: %w", msg.EventType, sendErr)
	}
	if !delivery.Stream {
		a.clearSessionDelivery(msg.SessionKey)
	}
	return nil
}

func (a *App) SendSessionMessage(ctx context.Context, sessionKey, message string) (string, error) {
	return a.ws.SendSessionMessage(ctx, sessionKey, message)
}

func (a *App) CreateSession(ctx context.Context, sessionKey string) (wsclient.CreateSessionResult, error) {
	return a.ws.CreateSession(ctx, sessionKey)
}

func (a *App) SetSessionDelivery(sessionKey string, opts SessionDeliveryOptions) {
	key := strings.TrimSpace(sessionKey)
	if key == "" {
		return
	}
	if opts.CreatedAt.IsZero() {
		opts.CreatedAt = time.Now().UTC()
	}
	a.mu.Lock()
	a.delivery[key] = opts
	a.mu.Unlock()

	if err := a.store.UpsertSessionDelivery(context.Background(), key, store.SessionDelivery{
		CallbackURL: opts.CallbackURL,
		Stream:      opts.Stream,
		UpdatedAt:   opts.CreatedAt,
	}); err != nil {
		a.logger.Warn("failed to persist session delivery config", "session_key", key, "error", err.Error())
	}
}

func (a *App) getSessionDelivery(sessionKey string) SessionDeliveryOptions {
	key := strings.TrimSpace(sessionKey)
	a.mu.Lock()
	opts, ok := a.delivery[key]
	a.mu.Unlock()
	if !ok {
		dbOpts, found, err := a.store.GetSessionDelivery(context.Background(), key)
		if err != nil {
			a.logger.Warn("failed to load session delivery config", "session_key", key, "error", err.Error())
			return SessionDeliveryOptions{Stream: true}
		}
		if !found {
			return SessionDeliveryOptions{Stream: true}
		}
		out := SessionDeliveryOptions{
			CallbackURL: dbOpts.CallbackURL,
			Stream:      dbOpts.Stream,
			CreatedAt:   dbOpts.UpdatedAt,
		}
		a.mu.Lock()
		a.delivery[key] = out
		a.mu.Unlock()
			return out
	}
	if time.Since(opts.CreatedAt) > a.cfg.DeliveryTTL {
		a.mu.Lock()
		delete(a.delivery, key)
		a.mu.Unlock()
		if err := a.store.DeleteSessionDelivery(context.Background(), key); err != nil {
			a.logger.Warn("failed to clear expired session delivery config", "session_key", key, "error", err.Error())
		}
		return SessionDeliveryOptions{Stream: true}
	}
	return opts
}

func (a *App) clearSessionDelivery(sessionKey string) {
	key := strings.TrimSpace(sessionKey)
	if key == "" {
		return
	}
	a.mu.Lock()
	delete(a.delivery, key)
	a.mu.Unlock()
	if err := a.store.DeleteSessionDelivery(context.Background(), key); err != nil {
		a.logger.Warn("failed to delete session delivery config", "session_key", key, "error", err.Error())
	}
}

func (a *App) RegisterInboundDedupe(ctx context.Context, key string) (bool, error) {
	return a.store.RegisterDedupeKey(ctx, "inbound", key, a.cfg.DedupeRecordTTL)
}

func (a *App) buildOutboundDedupeKey(msg wsclient.Message) string {
	material := firstNonEmpty(msg.RequestID, msg.EventType)
	if strings.TrimSpace(material) == "" {
		return ""
	}
	h := sha256.New()
	h.Write([]byte(msg.EventType))
	h.Write([]byte("|"))
	h.Write([]byte(msg.SessionKey))
	h.Write([]byte("|"))
	h.Write([]byte(msg.RequestID))
	h.Write([]byte("|"))
	h.Write(msg.Raw)
	return "out:" + hex.EncodeToString(h.Sum(nil))
}

func isFinalEvent(msg wsclient.Message) bool {
	if hasTrue(msg.Frame, "final") || hasTrue(msg.Frame, "done") {
		return true
	}
	params := getMap(msg.Frame, "params")
	payload := getMap(msg.Frame, "payload")
	if hasTrue(params, "final") || hasTrue(params, "done") || hasTrue(payload, "final") || hasTrue(payload, "done") {
		return true
	}
	status := strings.ToLower(firstNonEmpty(
		getString(msg.Frame, "status"),
		getString(params, "status"),
		getString(payload, "status"),
		getString(params, "state"),
		getString(payload, "state"),
	))
	return status == "done" || status == "completed" || status == "final" || status == "finished" || status == "success"
}

func hasTrue(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	raw, ok := m[key]
	if !ok || raw == nil {
		return false
	}
	v, ok := raw.(bool)
	return ok && v
}

func getMap(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	raw, ok := m[key]
	if !ok || raw == nil {
		return nil
	}
	out, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return out
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	raw, ok := m[key]
	if !ok || raw == nil {
		return ""
	}
	s, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
