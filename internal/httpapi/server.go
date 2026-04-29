package httpapi

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/app"
	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/events"
	"github.com/alanweb7/openclaw-2026-api-go/internal/hermes"
	"github.com/alanweb7/openclaw-2026-api-go/internal/webhook"
	"github.com/alanweb7/openclaw-2026-api-go/internal/wsclient"
)

type Server struct {
	cfg    config.Config
	app    *app.App
	logger *slog.Logger
	hermes *hermes.Client
	hook   *webhook.Client
}

type healthResponse struct {
	Status string `json:"status"`
}

type sendRequest struct {
	SessionKey    string `json:"sessionKey"`
	AgentID       string `json:"agentId,omitempty"`
	CustomerID    string `json:"customerId,omitempty"`
	Workspace     string `json:"workspace,omitempty"`
	Message       string `json:"message"`
	CreateSession bool   `json:"createSession,omitempty"`
	CallbackURL   string `json:"callbackUrl,omitempty"`
	Stream        *bool  `json:"stream,omitempty"`
	DedupeKey     string `json:"dedupeKey,omitempty"`
}

type sendResponse struct {
	OK        bool   `json:"ok"`
	RequestID string `json:"requestId,omitempty"`
}

type errorResponse struct {
	OK      bool   `json:"ok"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type createSessionRequest struct {
	SessionKey string `json:"sessionKey,omitempty"`
	AgentID    string `json:"agentId,omitempty"`
	CustomerID string `json:"customerId,omitempty"`
	Workspace  string `json:"workspace,omitempty"`
}

type createSessionResponse struct {
	OK         bool   `json:"ok"`
	SessionKey string `json:"sessionKey"`
	SessionID  string `json:"sessionId,omitempty"`
	RequestID  string `json:"requestId,omitempty"`
}

type hermesSendRequest struct {
	Command     string         `json:"command"`
	CallbackURL string         `json:"callbackUrl"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type hermesSendResponse struct {
	OK        bool   `json:"ok"`
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
}

func New(cfg config.Config, application *app.App, logger *slog.Logger) *Server {
	return &Server{
		cfg:    cfg,
		app:    application,
		logger: logger.With("component", "httpapi"),
		hermes: hermes.New(cfg),
		hook:   webhook.New(cfg, logger),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/v1/sessions/create", s.handleCreateSession)
	mux.HandleFunc("/v1/sessions/send", s.handleSend)
	mux.HandleFunc("/v1/hermes/send", s.handleHermesSend)
	return mux
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := ":" + s.cfg.HTTPListenPort
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("http api listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http api server failed: %w", err)
		}
		return nil
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	// Never fallback to a global session key for public send requests.
	// This avoids accidental cross-tenant/shared-memory conversations.
	sessionKey := resolveSessionKey(req.SessionKey, req.AgentID, req.CustomerID, req.Workspace, "")
	if sessionKey == "" {
		http.Error(w, "sessionKey is required", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Message) == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}

	dedupeKey := strings.TrimSpace(req.DedupeKey)
	if dedupeKey == "" {
		dedupeKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	}
	if dedupeKey != "" {
		scopedDedupeKey := "in:" + sessionKey + ":" + dedupeKey
		isNew, dedupeErr := s.app.RegisterInboundDedupe(r.Context(), scopedDedupeKey)
		if dedupeErr != nil {
			s.logger.Warn("inbound dedupe check failed", "error", dedupeErr.Error())
		} else if !isNew {
			writeJSON(w, http.StatusOK, sendResponse{OK: true, RequestID: "duplicate"})
			return
		}
	}
	if callbackURL := strings.TrimSpace(req.CallbackURL); callbackURL != "" {
		if !strings.HasPrefix(strings.ToLower(callbackURL), "http://") && !strings.HasPrefix(strings.ToLower(callbackURL), "https://") {
			http.Error(w, "callbackUrl must start with http:// or https://", http.StatusBadRequest)
			return
		}
	}

	stream := true
	if req.Stream != nil {
		stream = *req.Stream
	}
	s.app.SetSessionDelivery(sessionKey, app.SessionDeliveryOptions{
		CallbackURL: strings.TrimSpace(req.CallbackURL),
		Stream:      stream,
	})

	requestID, err := s.app.SendSessionMessage(r.Context(), sessionKey, req.Message)
	if err != nil && req.CreateSession && strings.Contains(strings.ToLower(err.Error()), "session not found") {
		s.logger.Info("session missing, creating automatically before retry", "session_key", sessionKey)
		if _, createErr := s.app.CreateSessionWithOptions(r.Context(), sessionKey, wsclient.CreateSessionOptions{
			Workspace: strings.TrimSpace(req.Workspace),
			AgentID:   strings.TrimSpace(req.AgentID),
		}); createErr != nil {
			code, message := classifyGatewayError(createErr)
			s.logger.Error("session_create_rejected", "error", createErr.Error(), "session_key", sessionKey, "code", code)
			writeJSON(w, http.StatusBadGateway, errorResponse{OK: false, Code: code, Message: message})
			return
		}
		requestID, err = s.app.SendSessionMessage(r.Context(), sessionKey, req.Message)
	}
	if err != nil {
		code, message := classifyGatewayError(err)
		s.logger.Error(code, "error", err.Error(), "session_key", sessionKey, "code", code)
		writeJSON(w, http.StatusBadGateway, errorResponse{OK: false, Code: code, Message: message})
		return
	}

	writeJSON(w, http.StatusOK, sendResponse{
		OK:        true,
		RequestID: requestID,
	})
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createSessionRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
	}

	sessionKey := resolveSessionKey(req.SessionKey, req.AgentID, req.CustomerID, req.Workspace, "")

	res, err := s.app.CreateSessionWithOptions(r.Context(), sessionKey, wsclient.CreateSessionOptions{
		Workspace: strings.TrimSpace(req.Workspace),
		AgentID:   strings.TrimSpace(req.AgentID),
	})
	if err != nil {
		code, message := classifyGatewayError(err)
		s.logger.Error(code, "error", err.Error(), "session_key", sessionKey, "code", code)
		writeJSON(w, http.StatusBadGateway, errorResponse{OK: false, Code: code, Message: message})
		return
	}

	writeJSON(w, http.StatusOK, createSessionResponse{
		OK:         true,
		SessionKey: res.Key,
		SessionID:  res.SessionID,
		RequestID:  res.RequestID,
	})
}

func (s *Server) handleHermesSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req hermesSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Command) == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}
	callbackURL := strings.TrimSpace(req.CallbackURL)
	if callbackURL == "" {
		http.Error(w, "callbackUrl is required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(strings.ToLower(callbackURL), "http://") && !strings.HasPrefix(strings.ToLower(callbackURL), "https://") {
		http.Error(w, "callbackUrl must start with http:// or https://", http.StatusBadRequest)
		return
	}

	requestID := "hermes-" + hex.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	writeJSON(w, http.StatusAccepted, hermesSendResponse{
		OK:        true,
		RequestID: requestID,
		Status:    "queued",
	})

	go s.runHermesAndCallback(requestID, callbackURL, req)
}

func (s *Server) runHermesAndCallback(requestID, callbackURL string, req hermesSendRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HermesTimeout+5*time.Second)
	defer cancel()

	result := map[string]any{
		"ok":        false,
		"eventType": "hermes.command.result",
		"requestId": requestID,
		"command":   req.Command,
		"metadata":  req.Metadata,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	output, err := s.hermes.SendCommand(ctx, req.Command)
	if err != nil {
		result["code"] = "hermes_command_failed"
		result["error"] = err.Error()
	} else {
		result["ok"] = true
		result["code"] = "hermes_command_ok"
		result["output"] = output
	}

	payload := events.WebhookPayload{
		Source:     "hermes",
		ReceivedAt: time.Now().UTC(),
		EventType:  "hermes.command.result",
		RequestID:  requestID,
		Raw:        result,
		Normalized: result,
	}
	if sendErr := s.hook.SendToURL(ctx, callbackURL, payload); sendErr != nil {
		s.logger.Error("hermes callback delivery failed", "request_id", requestID, "error", sendErr.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

var nonIDChars = regexp.MustCompile(`[^a-zA-Z0-9._:-]+`)

func buildSessionKey(agentID, customerID string) string {
	agent := sanitizeID(agentID)
	customer := sanitizeID(customerID)
	if agent == "" || customer == "" {
		return ""
	}
	return agent + ":" + customer
}

func resolveSessionKey(sessionKey, agentID, customerID, workspace, fallbackSessionKey string) string {
	key := strings.TrimSpace(sessionKey)
	if key != "" {
		return key
	}
	if fromIDs := buildSessionKey(agentID, customerID); fromIDs != "" {
		return fromIDs
	}
	if fromWorkspace := buildWorkspaceSessionKey(workspace); fromWorkspace != "" {
		return fromWorkspace
	}
	return strings.TrimSpace(fallbackSessionKey)
}

func buildWorkspaceSessionKey(workspace string) string {
	normalized := strings.TrimSpace(strings.ToLower(workspace))
	if normalized == "" {
		return ""
	}
	sum := sha1.Sum([]byte(normalized))
	// Keep a short deterministic suffix while preserving readability.
	return "workspace:" + hex.EncodeToString(sum[:8])
}

func sanitizeID(v string) string {
	out := strings.TrimSpace(strings.ToLower(v))
	if out == "" {
		return ""
	}
	out = nonIDChars.ReplaceAllString(out, "-")
	out = strings.Trim(out, "-:.")
	return out
}

func classifyGatewayError(err error) (string, string) {
	if err == nil {
		return "gateway_connectivity_error", "gateway connectivity error"
	}
	switch {
	case wsclient.IsCode(err, wsclient.ErrCodeWSNotConnected):
		return "ws_not_connected", "websocket is not connected"
	case wsclient.IsCode(err, wsclient.ErrCodeConnectRejected):
		text := strings.ToLower(err.Error())
		if strings.Contains(text, "device") && strings.Contains(text, "identity") {
			return "device_identity_required", "gateway rejected connection: device identity required"
		}
		return "connect_rejected", "gateway rejected websocket connect"
	case wsclient.IsCode(err, wsclient.ErrCodeSendRejected):
		return "send_rejected", "gateway rejected message send"
	case wsclient.IsCode(err, wsclient.ErrCodeSessionCreateReject):
		return "session_create_rejected", "gateway rejected session creation"
	case wsclient.IsCode(err, wsclient.ErrCodeGatewayConnectivity):
		return "gateway_connectivity_error", "gateway connectivity error"
	}

	text := strings.ToLower(err.Error())
	if strings.Contains(text, "session not found") {
		return "send_rejected", "session not found"
	}
	if strings.Contains(text, "device") && strings.Contains(text, "identity") {
		return "device_identity_required", "gateway rejected connection: device identity required"
	}
	if strings.Contains(text, "not connected") {
		return "ws_not_connected", "websocket is not connected"
	}
	return "gateway_connectivity_error", "gateway connectivity error"
}
