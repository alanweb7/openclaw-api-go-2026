package events

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

type Incoming struct {
	RequestID  string
	EventType  string
	SessionKey string
	RawBytes   []byte
	Data       map[string]any
}

type WebhookPayload struct {
	Source     string         `json:"source"`
	ReceivedAt time.Time      `json:"receivedAt"`
	EventType  string         `json:"eventType"`
	SessionKey string         `json:"sessionKey,omitempty"`
	RequestID  string         `json:"requestId,omitempty"`
	Raw        map[string]any `json:"raw"`
	Normalized map[string]any `json:"normalized"`
}

func ShouldForward(cfg config.Config, eventType string) bool {
	switch eventType {
	case "session.message":
		return cfg.ForwardSessionMessage
	case "agent":
		// Newer OpenClaw builds can emit agent responses under "agent".
		return cfg.ForwardSessionMessage
	case "session.tool":
		return cfg.ForwardSessionTool
	case "sessions.changed":
		return cfg.ForwardSessionsChanged
	case "tick":
		return cfg.ForwardTick
	case "health":
		return cfg.ForwardHealth
	default:
		return false
	}
}

func BuildPayload(in Incoming) (WebhookPayload, error) {
	normalized := normalize(in)
	return WebhookPayload{
		Source:     "openclaw",
		ReceivedAt: time.Now().UTC(),
		EventType:  in.EventType,
		SessionKey: in.SessionKey,
		RequestID:  in.RequestID,
		Raw:        in.Data,
		Normalized: normalized,
	}, nil
}

func ExtractEventType(frame map[string]any) string {
	candidates := []string{
		getString(frame, "eventType"),
		getString(frame, "event"),
		getString(frame, "method"),
	}
	for _, c := range candidates {
		if strings.TrimSpace(c) != "" {
			return c
		}
	}
	return ""
}

func ExtractSessionKey(frame map[string]any) string {
	if direct := getString(frame, "sessionKey"); direct != "" {
		return direct
	}
	params := getMap(frame, "params")
	if params != nil {
		if s := getString(params, "sessionKey"); s != "" {
			return s
		}
	}
	payload := getMap(frame, "payload")
	if payload != nil {
		if s := getString(payload, "sessionKey"); s != "" {
			return s
		}
	}
	return ""
}

func ExtractRequestID(frame map[string]any) string {
	for _, key := range []string{"requestId", "id", "reqId"} {
		if value := getString(frame, key); value != "" {
			return value
		}
	}
	return ""
}

func DecodeFrame(raw []byte) (map[string]any, error) {
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func normalize(in Incoming) map[string]any {
	params := getMap(in.Data, "params")
	if params == nil {
		params = map[string]any{}
	}
	payload := getMap(in.Data, "payload")
	if payload == nil {
		payload = map[string]any{}
	}

	switch in.EventType {
	case "session.message":
		return map[string]any{
			"kind":  "agent_message",
			"text":  firstNonEmpty(extractText(params), extractText(payload)),
			"agent": firstNonEmpty(extractAgent(params, in.SessionKey), extractAgent(payload, in.SessionKey)),
		}
	case "agent":
		return map[string]any{
			"kind":  "agent_message",
			"text":  firstNonEmpty(extractText(params), extractText(payload), extractText(in.Data)),
			"agent": firstNonEmpty(extractAgent(params, in.SessionKey), extractAgent(payload, in.SessionKey), extractAgent(in.Data, in.SessionKey)),
		}
	case "session.tool":
		return map[string]any{
			"kind":   "tool_event",
			"tool":   firstNonEmpty(getString(params, "tool"), getString(params, "name")),
			"status": firstNonEmpty(getString(params, "status"), getString(params, "state")),
		}
	case "sessions.changed":
		return map[string]any{
			"kind":  "sessions_changed",
			"count": extractCount(params),
		}
	case "tick":
		return map[string]any{
			"kind": "tick",
			"at":   firstNonEmpty(getString(params, "at"), getString(params, "timestamp")),
		}
	case "health":
		return map[string]any{
			"kind":   "health",
			"status": firstNonEmpty(getString(params, "status"), getString(params, "state")),
		}
	default:
		return map[string]any{"kind": "unknown"}
	}
}

func extractText(params map[string]any) string {
	for _, key := range []string{"text", "message", "content"} {
		if v := getString(params, key); v != "" {
			return v
		}
	}
	msgMap := getMap(params, "message")
	if msgMap != nil {
		for _, key := range []string{"text", "content"} {
			if v := getString(msgMap, key); v != "" {
				return v
			}
		}
	}
	return ""
}

func extractAgent(params map[string]any, sessionKey string) string {
	for _, key := range []string{"agent", "agentName"} {
		if v := getString(params, key); v != "" {
			return v
		}
	}
	if sessionKey != "" {
		parts := strings.Split(sessionKey, ":")
		return parts[len(parts)-1]
	}
	return ""
}

func extractCount(params map[string]any) int {
	if arr, ok := params["sessions"].([]any); ok {
		return len(arr)
	}
	if v, ok := params["count"].(float64); ok {
		return int(v)
	}
	return 0
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	raw, ok := m[key]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

func getMap(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	raw, ok := m[key]
	if !ok || raw == nil {
		return nil
	}
	casted, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return casted
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
