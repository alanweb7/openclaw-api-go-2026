package hermes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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

	token, _ := c.fetchToken(ctx, baseURL)
	conn, err := c.dialWithAuthFallback(ctx, baseURL, token)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := sendCommandWithFallback(conn, command); err != nil {
		return "", err
	}

	deadline := time.Now().Add(c.cfg.HermesTimeout)
	_ = conn.SetReadDeadline(deadline)
	var chunks []string
	for {
		_, payload, readErr := conn.ReadMessage()
		if readErr != nil {
			if len(chunks) > 0 && isUnexpectedClose(readErr) {
				return strings.TrimSpace(strings.Join(chunks, "\n")), nil
			}
			return "", fmt.Errorf("read hermes response: %w", readErr)
		}
		if len(payload) == 0 {
			continue
		}
		out := decodeTTYOutput(payload)
		if out != "" {
			chunks = append(chunks, out)
			joined := strings.TrimSpace(strings.Join(chunks, "\n"))
			if joined != "" {
				return joined, nil
			}
		}
	}
}

func (c *Client) dialWithAuthFallback(ctx context.Context, baseURL, token string) (*websocket.Conn, error) {
	basicHeader := http.Header{}
	if c.cfg.HermesBasicUser != "" || c.cfg.HermesBasicPass != "" {
		raw := c.cfg.HermesBasicUser + ":" + c.cfg.HermesBasicPass
		basicHeader.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(raw)))
	}

	type attempt struct {
		name    string
		url     string
		headers http.Header
	}
	attempts := make([]attempt, 0, 3)

	// 1) Basic only
	if len(basicHeader) > 0 {
		attempts = append(attempts, attempt{name: "basic", url: baseURL, headers: basicHeader.Clone()})
	}

	// 2) Token only
	if strings.TrimSpace(token) != "" {
		if tokenURL, err := wsURLWithToken(baseURL, token); err == nil {
			attempts = append(attempts, attempt{name: "token", url: tokenURL, headers: http.Header{}})
			// 3) Token + Basic
			if len(basicHeader) > 0 {
				attempts = append(attempts, attempt{name: "token+basic", url: tokenURL, headers: basicHeader.Clone()})
			}
		}
	}

	// Final fallback: plain no-auth if nothing configured.
	if len(attempts) == 0 {
		attempts = append(attempts, attempt{name: "plain", url: baseURL, headers: http.Header{}})
	}

	var lastErr error
	for _, a := range attempts {
		conn, _, err := c.dialer.DialContext(ctx, a.url, a.headers)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", a.name, err)
			continue
		}
		return conn, nil
	}
	return nil, fmt.Errorf("dial hermes ws: %w", lastErr)
}

func sendCommandWithFallback(conn *websocket.Conn, command string) error {
	clean := strings.TrimSpace(command)
	if clean == "" {
		return fmt.Errorf("write hermes command: empty command")
	}
	candidates := [][]byte{
		[]byte("0" + clean + "\n"), // ttyd stdin frame
		[]byte(clean + "\n"),       // plain text fallback for variant builds
	}
	var lastErr error
	for _, payload := range candidates {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("write hermes command: %w", lastErr)
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

func isUnexpectedClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	return websocket.IsCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway)
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
