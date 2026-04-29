package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	OpenClawWSURL          string
	OpenClawGatewayToken   string
	OpenClawMinProtocol    int
	OpenClawMaxProtocol    int
	OpenClawClientID       string
	OpenClawClientVersion  string
	OpenClawClientPlatform string
	OpenClawClientMode     string
	OpenClawRole           string
	OpenClawScopes         []string
	OpenClawLocale         string
	OpenClawUserAgent      string
	OpenClawWSOrigin       string
	OpenClawSessionKey     string

	WebhookURL        string
	WebhookAuthHeader string
	WebhookAuthValue  string
	WebhookTimeout    time.Duration
	HTTPRetryCount    int
	HTTPRetryDelay    time.Duration

	ForwardSessionMessage  bool
	ForwardSessionTool     bool
	ForwardSessionsChanged bool
	ForwardTick            bool
	ForwardHealth          bool

	AutoSendOnConnect bool
	AutoSendMessage   string

	WSReconnect      bool
	WSReconnectDelay time.Duration

	LogLevel string

	HTTPListenPort string

	DatabaseURL      string
	DeliveryTTL      time.Duration
	DedupeRecordTTL  time.Duration

	HermesWSURL      string
	HermesBasicUser  string
	HermesBasicPass  string
	HermesTimeout    time.Duration
}

func Load() (Config, error) {
	_ = godotenv.Load()

	cfg := Config{
		OpenClawWSURL:          envOrDefault("OPENCLAW_WS_URL", "ws://127.0.0.1:18789"),
		OpenClawGatewayToken:   os.Getenv("OPENCLAW_GATEWAY_TOKEN"),
		OpenClawMinProtocol:    intOrDefault("OPENCLAW_MIN_PROTOCOL", 3),
		OpenClawMaxProtocol:    intOrDefault("OPENCLAW_MAX_PROTOCOL", 3),
		OpenClawClientID:       envOrDefault("OPENCLAW_CLIENT_ID", "cli"),
		OpenClawClientVersion:  envOrDefault("OPENCLAW_CLIENT_VERSION", "1.0.0"),
		OpenClawClientPlatform: envOrDefault("OPENCLAW_CLIENT_PLATFORM", "linux"),
		OpenClawClientMode:     envOrDefault("OPENCLAW_CLIENT_MODE", "cli"),
		OpenClawRole:           envOrDefault("OPENCLAW_ROLE", "operator"),
		OpenClawScopes:         splitCSV(envOrDefault("OPENCLAW_SCOPES", "operator.read,operator.write")),
		OpenClawLocale:         envOrDefault("OPENCLAW_LOCALE", "pt-BR"),
		OpenClawUserAgent:      envOrDefault("OPENCLAW_USER_AGENT", "openclaw-bridge-go/1.0.0"),
		OpenClawWSOrigin:       strings.TrimSpace(os.Getenv("OPENCLAW_WS_ORIGIN")),
		OpenClawSessionKey:     envOrDefault("OPENCLAW_SESSION_KEY", "agent:main:guardian"),

		WebhookURL:        os.Getenv("WEBHOOK_URL"),
		WebhookAuthHeader: os.Getenv("WEBHOOK_AUTH_HEADER"),
		WebhookAuthValue:  os.Getenv("WEBHOOK_AUTH_VALUE"),
		WebhookTimeout:    time.Duration(intOrDefault("WEBHOOK_TIMEOUT_SECONDS", 15)) * time.Second,
		HTTPRetryCount:    intOrDefault("HTTP_RETRY_COUNT", 3),
		HTTPRetryDelay:    time.Duration(intOrDefault("HTTP_RETRY_DELAY_MS", 1000)) * time.Millisecond,

		ForwardSessionMessage:  boolOrDefault("FORWARD_SESSION_MESSAGE", true),
		ForwardSessionTool:     boolOrDefault("FORWARD_SESSION_TOOL", true),
		ForwardSessionsChanged: boolOrDefault("FORWARD_SESSIONS_CHANGED", true),
		ForwardTick:            boolOrDefault("FORWARD_TICK", false),
		ForwardHealth:          boolOrDefault("FORWARD_HEALTH", false),

		AutoSendOnConnect: boolOrDefault("AUTO_SEND_ON_CONNECT", false),
		AutoSendMessage:   os.Getenv("AUTO_SEND_MESSAGE"),

		WSReconnect:      boolOrDefault("WS_RECONNECT", true),
		WSReconnectDelay: time.Duration(intOrDefault("WS_RECONNECT_DELAY_SECONDS", 5)) * time.Second,

		LogLevel: strings.ToLower(envOrDefault("LOG_LEVEL", "info")),

		HTTPListenPort: envOrDefault("BRIDGE_HTTP_PORT", "8080"),

		DatabaseURL:     strings.TrimSpace(os.Getenv("DATABASE_URL")),
		DeliveryTTL:     time.Duration(intOrDefault("DELIVERY_TTL_SECONDS", 600)) * time.Second,
		DedupeRecordTTL: time.Duration(intOrDefault("DEDUPE_RECORD_TTL_SECONDS", 86400)) * time.Second,

		HermesWSURL:     strings.TrimSpace(os.Getenv("HERMES_WS_URL")),
		HermesBasicUser: strings.TrimSpace(os.Getenv("HERMES_BASIC_USER")),
		HermesBasicPass: strings.TrimSpace(os.Getenv("HERMES_BASIC_PASS")),
		HermesTimeout:   time.Duration(intOrDefault("HERMES_TIMEOUT_SECONDS", 15)) * time.Second,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	if cfg.OpenClawWSOrigin == "" {
		cfg.OpenClawWSOrigin = deriveOriginFromWSURL(cfg.OpenClawWSURL)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []string

	if c.OpenClawWSURL == "" {
		errs = append(errs, "OPENCLAW_WS_URL is required")
	}
	if c.OpenClawGatewayToken == "" {
		errs = append(errs, "OPENCLAW_GATEWAY_TOKEN is required")
	}
	if c.WebhookURL == "" {
		errs = append(errs, "WEBHOOK_URL is required")
	}
	if c.AutoSendOnConnect && strings.TrimSpace(c.AutoSendMessage) == "" {
		errs = append(errs, "AUTO_SEND_MESSAGE is required when AUTO_SEND_ON_CONNECT=true")
	}
	if c.WebhookTimeout <= 0 {
		errs = append(errs, "WEBHOOK_TIMEOUT_SECONDS must be > 0")
	}
	if c.HTTPRetryCount < 0 {
		errs = append(errs, "HTTP_RETRY_COUNT must be >= 0")
	}
	if c.HTTPRetryDelay < 0 {
		errs = append(errs, "HTTP_RETRY_DELAY_MS must be >= 0")
	}
	if c.WSReconnectDelay < 0 {
		errs = append(errs, "WS_RECONNECT_DELAY_SECONDS must be >= 0")
	}
	if c.DeliveryTTL <= 0 {
		errs = append(errs, "DELIVERY_TTL_SECONDS must be > 0")
	}
	if c.DedupeRecordTTL <= 0 {
		errs = append(errs, "DEDUPE_RECORD_TTL_SECONDS must be > 0")
	}
	if c.HermesTimeout <= 0 {
		errs = append(errs, "HERMES_TIMEOUT_SECONDS must be > 0")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func intOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func boolOrDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func (c Config) String() string {
	return fmt.Sprintf("ws=%s webhook=%s reconnect=%t", c.OpenClawWSURL, c.WebhookURL, c.WSReconnect)
}

func deriveOriginFromWSURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return ""
	}
	switch strings.ToLower(u.Scheme) {
	case "wss":
		return "https://" + u.Host
	case "ws":
		return "http://" + u.Host
	default:
		return ""
	}
}
