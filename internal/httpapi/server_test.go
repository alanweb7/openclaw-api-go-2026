package httpapi

import (
	"errors"
	"testing"

	"github.com/alanweb7/openclaw-2026-api-go/internal/wsclient"
)

func TestResolveSessionKey(t *testing.T) {
	tests := []struct {
		name     string
		session  string
		agent    string
		customer string
		ws       string
		fallback string
		want     string
	}{
		{
			name:    "uses explicit session key",
			session: "custom:key",
			agent:   "design",
			ws:      "/data/work/a",
			want:    "custom:key",
		},
		{
			name:     "builds from agent and customer",
			agent:    "Design",
			customer: "Cliente-A",
			want:     "design:cliente-a",
		},
		{
			name: "builds deterministic key from workspace",
			ws:   "/data/.openclaw/workspaces/design/cliente-a",
			want: "workspace:0969d038a43b8daf",
		},
		{
			name:     "falls back to config key",
			fallback: "agent:main:guardian",
			want:     "agent:main:guardian",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveSessionKey(tt.session, tt.agent, tt.customer, tt.ws, tt.fallback)
			if got != tt.want {
				t.Fatalf("resolveSessionKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyGatewayError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		want    string
		wantMsg string
	}{
		{
			name:    "ws not connected",
			err:     &wsclient.OpError{Code: wsclient.ErrCodeWSNotConnected, Op: "sessions.send", Cause: errors.New("websocket is not connected")},
			want:    "ws_not_connected",
			wantMsg: "websocket is not connected",
		},
		{
			name:    "connect rejected device identity",
			err:     &wsclient.OpError{Code: wsclient.ErrCodeConnectRejected, Op: "connect", Cause: errors.New("CONTROL_UI_DEVICE_IDENTITY_REQUIRED")},
			want:    "device_identity_required",
			wantMsg: "gateway rejected connection: device identity required",
		},
		{
			name:    "session create rejected",
			err:     &wsclient.OpError{Code: wsclient.ErrCodeSessionCreateReject, Op: "sessions.create", Cause: errors.New("policy denied")},
			want:    "session_create_rejected",
			wantMsg: "gateway rejected session creation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCode, gotMsg := classifyGatewayError(tt.err)
			if gotCode != tt.want {
				t.Fatalf("classifyGatewayError() code = %q, want %q", gotCode, tt.want)
			}
			if gotMsg != tt.wantMsg {
				t.Fatalf("classifyGatewayError() msg = %q, want %q", gotMsg, tt.wantMsg)
			}
		})
	}
}
