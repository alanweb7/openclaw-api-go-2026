package hermes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
			Subprotocols:     []string{"tty"},
		},
	}
}

func (c *Client) SendCommand(ctx context.Context, command string) (string, error) {
	baseURL, err := normalizeWSURL(c.cfg.HermesWSURL)
	if err != nil {
		return "", err
	}

	token, err := c.fetchToken(ctx, baseURL)
	if err != nil {
		return "", err
	}
	wsURL, err := wsURLWithToken(baseURL, token)
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
		if len(payload) == 0 {
			continue
		}
		out := decodeTTYOutput(payload)
		if out != "" {
			return out, nil
		}
	}
}

func (c *Client) fetchToken(ctx context.Context, wsURL string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", fmt.Errorf("parse HERMES_WS_URL: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return "", fmt.Errorf("invalid HERMES_WS_URL scheme")
	}
	u.Path = "/token"
	u.RawQuery = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("build hermes token request: %w", err)
	}
	if c.cfg.HermesBasicUser != "" || c.cfg.HermesBasicPass != "" {
		req.SetBasicAuth(c.cfg.HermesBasicUser, c.cfg.HermesBasicPass)
	}

	httpClient := &http.Client{Timeout: c.cfg.HermesTimeout}
	res, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request hermes token: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("request hermes token: unexpected status %d", res.StatusCode)
	}

	rawBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("read hermes token body: %w", err)
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return "", fmt.Errorf("parse hermes token body: %w", err)
	}
	if strings.TrimSpace(payload.Token) == "" {
		return "", fmt.Errorf("empty hermes token")
	}
	return strings.TrimSpace(payload.Token), nil
}

func wsURLWithToken(base, token string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse HERMES_WS_URL: %w", err)
	}
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func decodeTTYOutput(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	// ttyd output frames usually prefix data with one-byte opcode like '0'.
	if payload[0] == '0' && len(payload) > 1 {
		return strings.TrimSpace(string(payload[1:]))
	}
	return strings.TrimSpace(string(payload))
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
