package hermes

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

type Client struct {
	cfg    config.Config
	dialer *websocket.Dialer
}

func New(cfg config.Config) *Client {
	return &Client{
		cfg: cfg,
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: cfg.HermesTimeout,
		},
	}
}

func (c *Client) SendCommand(ctx context.Context, command string) (string, error) {
	wsURL, err := normalizeWSURL(c.cfg.HermesWSURL)
	if err != nil {
		return "", err
	}

	headers := http.Header{}
	if c.cfg.HermesBasicUser != "" || c.cfg.HermesBasicPass != "" {
		raw := c.cfg.HermesBasicUser + ":" + c.cfg.HermesBasicPass
		headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(raw)))
	}

	conn, _, err := c.dialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		return "", fmt.Errorf("dial hermes ws: %w", err)
	}
	defer conn.Close()

	// ttyd expects stdin frames prefixed with "0".
	if err := conn.WriteMessage(websocket.TextMessage, []byte("0"+strings.TrimSpace(command)+"\n")); err != nil {
		return "", fmt.Errorf("write hermes command: %w", err)
	}

	deadline := time.Now().Add(c.cfg.HermesTimeout)
	_ = conn.SetReadDeadline(deadline)
	for {
		_, payload, readErr := conn.ReadMessage()
		if readErr != nil {
			return "", fmt.Errorf("read hermes response: %w", readErr)
		}
		if len(payload) < 2 {
			continue
		}
		// ttyd output frames generally start with "0".
		if payload[0] == '0' {
			out := strings.TrimSpace(string(payload[1:]))
			if out != "" {
				return out, nil
			}
		}
	}
}

func normalizeWSURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid HERMES_WS_URL")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}
	switch strings.ToLower(u.Scheme) {
	case "ws", "wss":
		return u.String(), nil
	case "http":
		u.Scheme = "ws"
		return u.String(), nil
	case "https":
		u.Scheme = "wss"
		return u.String(), nil
	default:
		return "", fmt.Errorf("invalid HERMES_WS_URL scheme")
	}
}

