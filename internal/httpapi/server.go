package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/app"
	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

type Server struct {
	cfg    config.Config
	app    *app.App
	logger *slog.Logger
}

type healthResponse struct {
	Status string `json:"status"`
}

type sendRequest struct {
	SessionKey    string `json:"sessionKey"`
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

type createSessionRequest struct {
	SessionKey string `json:"sessionKey,omitempty"`
}

type createSessionResponse struct {
	OK         bool   `json:"ok"`
	SessionKey string `json:"sessionKey"`
	SessionID  string `json:"sessionId,omitempty"`
	RequestID  string `json:"requestId,omitempty"`
}

func New(cfg config.Config, application *app.App, logger *slog.Logger) *Server {
	return &Server{
		cfg:    cfg,
		app:    application,
		logger: logger.With("component", "httpapi"),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/v1/sessions/create", s.handleCreateSession)
	mux.HandleFunc("/v1/sessions/send", s.handleSend)
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

	sessionKey := strings.TrimSpace(req.SessionKey)
	if sessionKey == "" {
		sessionKey = s.cfg.OpenClawSessionKey
	}
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
		isNew, dedupeErr := s.app.RegisterInboundDedupe(r.Context(), "in:"+dedupeKey)
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
		if _, createErr := s.app.CreateSession(r.Context(), sessionKey); createErr != nil {
			s.logger.Error("failed to auto-create missing session", "error", createErr.Error(), "session_key", sessionKey)
			http.Error(w, "failed to create missing openclaw session", http.StatusBadGateway)
			return
		}
		requestID, err = s.app.SendSessionMessage(r.Context(), sessionKey, req.Message)
	}
	if err != nil {
		s.logger.Error("failed to send session message", "error", err.Error(), "session_key", sessionKey)
		http.Error(w, "failed to deliver message to openclaw", http.StatusBadGateway)
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

	sessionKey := strings.TrimSpace(req.SessionKey)
	res, err := s.app.CreateSession(r.Context(), sessionKey)
	if err != nil {
		s.logger.Error("failed to create session", "error", err.Error(), "session_key", sessionKey)
		http.Error(w, "failed to create openclaw session", http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusOK, createSessionResponse{
		OK:         true,
		SessionKey: res.Key,
		SessionID:  res.SessionID,
		RequestID:  res.RequestID,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
