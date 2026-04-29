package httpapi

import "testing"

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
