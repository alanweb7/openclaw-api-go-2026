package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/events"
	"github.com/alanweb7/openclaw-2026-api-go/internal/webhook"
	"github.com/alanweb7/openclaw-2026-api-go/internal/wsclient"
)

type App struct {
	cfg     config.Config
	logger  *slog.Logger
	ws      *wsclient.Client
	webhook *webhook.Client
}

func New(cfg config.Config, logger *slog.Logger) *App {
	return &App{
		cfg:     cfg,
		logger:  logger.With("component", "app"),
		ws:      wsclient.New(cfg, logger),
		webhook: webhook.New(cfg, logger),
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

	if !events.ShouldForward(a.cfg, msg.EventType) {
		a.logger.Debug("event skipped by filter", "event_type", msg.EventType)
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

	if err := a.webhook.Send(ctx, payload); err != nil {
		return fmt.Errorf("send webhook event %s: %w", msg.EventType, err)
	}
	return nil
}
