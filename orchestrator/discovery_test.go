package orchestrator

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryLocalInventory(t *testing.T) {
	registry := NewRegistry("bootstrap", "https://orch.example/deployment", time.Second, time.Minute)
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "local", RunnerURL: "http://127.0.0.1:9000", App: "local", Mode: "single-shot", Capacity: 2}))
	policy, err := destination.New("test", nil)
	require.NoError(t, err)
	s := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Close()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/deployment/discovery", nil))
	require.Equal(t, 200, w.Code)
	require.NotContains(t, w.Body.String(), `"nodes"`)
	var entries []discoveryEntry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, registry.service, entries[0].Address)
	require.Len(t, entries[0].Runners, 1)
	require.Equal(t, "https://orch.example/deployment/apps/local/app", entries[0].Runners[0].URL)
}
