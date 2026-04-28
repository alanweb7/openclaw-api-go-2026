package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

const handshakeTimeout = 30 * time.Second
const requestTimeout = 30 * time.Second

type Message struct {
	Type       string
	EventType  string
	RequestID  string
	SessionKey string
	Raw        []byte
	Frame      map[string]any
}

type Client struct {
	cfg       config.Config
	logger    *slog.Logger
	dialer    *websocket.Dialer
	requestID uint64
}

func New(cfg config.Config, logger *slog.Logger) *Client {
	return &Client{
		cfg:    cfg,
		logger: logger.With("component", "wsclient"),
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 15 * time.Second,
		},
	}
}

func (c *Client) Serve(ctx context.Context, onMessage func(context.Context, Message) error) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if c.cfg.AutoSendOnConnect {
		if err := c.sendAutoMessage(conn); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read websocket frame: %w", err)
		}

		frame, err := decodeFrame(raw)
		if err != nil {
			c.logger.Warn("invalid websocket frame", "error", err.Error())
			continue
		}

		msg := Message{
			Type:       getString(frame, "type"),
			EventType:  extractEventType(frame),
			RequestID:  extractRequestID(frame),
			SessionKey: extractSessionKey(frame),
			Raw:        raw,
			Frame:      frame,
		}

		if onMessage != nil {
			if err := onMessage(ctx, msg); err != nil {
				c.logger.Warn("message handler failed", "error", err.Error(), "event_type", msg.EventType)
			}
		}
	}
}

func (c *Client) SendSessionMessage(ctx context.Context, sessionKey, message string) (string, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	reqID := "send-" + strconv.FormatUint(c.nextID(), 10)
	payload := map[string]any{
		"type":   "req",
		"id":     reqID,
		"method": "sessions.send",
		"params": map[string]any{
			"key":     sessionKey,
			"message": message,
		},
	}

	if err := conn.WriteJSON(payload); err != nil {
		return "", fmt.Errorf("send sessions.send request: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(requestTimeout)); err != nil {
		return "", fmt.Errorf("set send request deadline: %w", err)
	}
	defer conn.SetReadDeadline(time.Time{})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return "", fmt.Errorf("read sessions.send response: %w", err)
		}

		frame, err := decodeFrame(raw)
		if err != nil {
			continue
		}

		if getString(frame, "type") != "res" {
			continue
		}
		if getString(frame, "id") != reqID {
			continue
		}

		if rawErr, ok := frame["error"]; ok && rawErr != nil {
			return reqID, fmt.Errorf("sessions.send rejected: %v", rawErr)
		}
		return reqID, nil
	}
}

func (c *Client) handshake(_ context.Context, conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return fmt.Errorf("set handshake deadline: %w", err)
	}

	if err := c.waitForChallenge(conn); err != nil {
		return err
	}

	connectReqID := "connect-" + strconv.FormatUint(c.nextID(), 10)
	connectPayload := map[string]any{
		"type":   "req",
		"id":     connectReqID,
		"method": "connect",
		"params": map[string]any{
			"minProtocol": c.cfg.OpenClawMinProtocol,
			"maxProtocol": c.cfg.OpenClawMaxProtocol,
			"client": map[string]any{
				"id":       c.cfg.OpenClawClientID,
				"version":  c.cfg.OpenClawClientVersion,
				"platform": c.cfg.OpenClawClientPlatform,
				"mode":     c.cfg.OpenClawClientMode,
			},
			"role":        c.cfg.OpenClawRole,
			"scopes":      c.cfg.OpenClawScopes,
			"caps":        []string{},
			"commands":    []string{},
			"permissions": map[string]any{},
			"auth": map[string]any{
				"token": c.cfg.OpenClawGatewayToken,
			},
			"locale":    c.cfg.OpenClawLocale,
			"userAgent": c.cfg.OpenClawUserAgent,
		},
	}

	if err := conn.WriteJSON(connectPayload); err != nil {
		return fmt.Errorf("send connect request: %w", err)
	}

	if err := c.waitForConnectResponse(conn, connectReqID); err != nil {
		return err
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("clear handshake deadline: %w", err)
	}

	c.logger.Info("handshake completed")
	return nil
}

func (c *Client) connect(ctx context.Context) (*websocket.Conn, error) {
	conn, _, err := c.dialer.DialContext(ctx, c.cfg.OpenClawWSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	c.logger.Info("websocket connected", "url", c.cfg.OpenClawWSURL)
	if err := c.handshake(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

func (c *Client) waitForChallenge(conn *websocket.Conn) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read challenge frame: %w", err)
		}

		frame, err := decodeFrame(raw)
		if err != nil {
			continue
		}

		msgType := getString(frame, "type")
		eventType := extractEventType(frame)
		if msgType == "event" && eventType == "connect.challenge" {
			c.logger.Info("received connect challenge")
			return nil
		}
	}
}

func (c *Client) waitForConnectResponse(conn *websocket.Conn, reqID string) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read connect response frame: %w", err)
		}

		frame, err := decodeFrame(raw)
		if err != nil {
			continue
		}

		if getString(frame, "type") != "res" {
			continue
		}
		if getString(frame, "id") != reqID {
			continue
		}

		if rawErr, ok := frame["error"]; ok && rawErr != nil {
			return fmt.Errorf("connect rejected: %v", rawErr)
		}

		return nil
	}
}

func (c *Client) sendAutoMessage(conn *websocket.Conn) error {
	reqID := "send-" + strconv.FormatUint(c.nextID(), 10)
	payload := map[string]any{
		"type":   "req",
		"id":     reqID,
		"method": "sessions.send",
		"params": map[string]any{
			"key":     c.cfg.OpenClawSessionKey,
			"message": c.cfg.AutoSendMessage,
		},
	}
	if err := conn.WriteJSON(payload); err != nil {
		return fmt.Errorf("send auto message: %w", err)
	}
	c.logger.Info("auto message sent", "session_key", c.cfg.OpenClawSessionKey, "request_id", reqID)
	return nil
}

func (c *Client) nextID() uint64 {
	return atomic.AddUint64(&c.requestID, 1)
}

func decodeFrame(raw []byte) (map[string]any, error) {
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func extractEventType(frame map[string]any) string {
	return firstNonEmpty(
		getString(frame, "eventType"),
		getString(frame, "event"),
		getString(frame, "method"),
	)
}

func extractRequestID(frame map[string]any) string {
	return firstNonEmpty(
		getString(frame, "requestId"),
		getString(frame, "id"),
		getString(frame, "reqId"),
	)
}

func extractSessionKey(frame map[string]any) string {
	if v := getString(frame, "sessionKey"); v != "" {
		return v
	}
	if v := getString(frame, "key"); v != "" {
		return v
	}
	params, ok := frame["params"].(map[string]any)
	if !ok || params == nil {
		return ""
	}
	if v := getString(params, "sessionKey"); v != "" {
		return v
	}
	return getString(params, "key")
}

func getString(data map[string]any, key string) string {
	if data == nil {
		return ""
	}
	raw, ok := data[key]
	if !ok || raw == nil {
		return ""
	}
	v, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
