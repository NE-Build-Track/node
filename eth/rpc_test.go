package eth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRPCTransportPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		respond func(http.ResponseWriter, *http.Request)
		error   string
	}{
		{"bounded response", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(bytes.Repeat([]byte("x"), maxRPCResponse+1))
		}, "exceeds 1 MiB"},
		{"HTTP error redaction", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "secret-password secret-token", http.StatusServiceUnavailable)
		}, "HTTP 503"},
		{"RPC error redaction", func(w http.ResponseWriter, r *http.Request) {
			var request struct{ ID json.RawMessage }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32000, "message": "secret-password secret-token"}})
		}, "RPC error -32000"},
		{"redirect is not replayed", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/replayed" {
				t.Error("RPC request followed a redirect")
			}
			http.Redirect(w, r, "/replayed", http.StatusTemporaryRedirect)
		}, "HTTP 307"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.respond))
			defer server.Close()
			parsed, err := url.Parse(server.URL + "?token=secret-token")
			require.NoError(t, err)
			parsed.User = url.UserPassword("operator", "secret-password")
			rpc, err := NewRPC(parsed, nil)
			require.NoError(t, err)
			defer rpc.Close()
			_, err = rpc.ChainID(t.Context())
			require.ErrorContains(t, err, tc.error)
			require.NotContains(t, err.Error(), "secret-password")
			require.NotContains(t, err.Error(), "secret-token")
		})
	}
}

func TestOptionalChainID(t *testing.T) {
	for _, tc := range []struct {
		expected *big.Int
		valid    bool
	}{
		{nil, true}, {big.NewInt(42161), true}, {big.NewInt(1), false},
	} {
		t.Run(fmt.Sprint(tc.expected), func(t *testing.T) {
			rpc := testRPC(t, func(method string, _ []json.RawMessage) any {
				require.Equal(t, "eth_chainId", method)
				return "0xa4b1"
			})
			err := rpc.CheckChainID(t.Context(), tc.expected)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// testRPC keeps adapter tests focused on their requests and responses.
func testRPC(t *testing.T, reply func(string, []json.RawMessage) any) *RPC {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage
			Method string
			Params []json.RawMessage
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": reply(req.Method, req.Params)}))
	}))
	t.Cleanup(server.Close)
	rpc, err := OpenRPC(server.URL)
	require.NoError(t, err)
	t.Cleanup(rpc.Close)
	return rpc
}

type ticketRevertRPCError struct{ message string }

func (e ticketRevertRPCError) Error() string  { return e.message }
func (e ticketRevertRPCError) ErrorCode() int { return -32000 }

func TestTerminalTicketErrorRetainsOnlyKnownClassification(t *testing.T) {
	original := ticketRevertRPCError{message: "secret https://user:password@rpc.example: ticket creationRound does not have a block hash; private calldata"}
	require.ErrorIs(t, safeRPCError(original), ErrTicketCreationRound)
	require.Equal(t, "ticket creationRound does not have a block hash", safeRPCError(original).Error())
	require.Equal(t, "ethereum RPC error -32000", safeRPCError(ticketRevertRPCError{message: "nonce too low https://user:password@rpc.example"}).Error())
}
