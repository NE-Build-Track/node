package signer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm/wire"
)

type maxPrice struct {
	Price    json.Number `json:"price"`
	Currency string      `json:"currency"`
	Unit     string      `json:"unit"`
}

func validateAuthWebhook(endpoint *url.URL) error {
	if err := destination.ValidateURL(endpoint); err != nil {
		return errors.New("invalid signer auth webhook URL")
	}
	if endpoint.User != nil || endpoint.Fragment != "" {
		return errors.New("invalid signer auth webhook URL")
	}
	return nil
}

func (s *Service) SetAuthWebhook(endpoint *url.URL, headers Headers) error {
	if err := validateAuthWebhook(endpoint); err != nil {
		return err
	}
	if _, err := headers.MarshalText(); err != nil {
		return err
	}
	s.authClient = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: 5 * time.Second}
	// Authentication is one explicit call, including when a target redirects.
	s.authClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s.authURL, s.authHeaders = endpoint.Clone(), Headers(http.Header(headers).Clone())
	encoded, _ := json.Marshal(struct {
		URL     string
		Headers Headers
	}{endpoint.String(), s.authHeaders})
	digest := sha256.Sum256(encoded)
	s.authPolicy = hex.EncodeToString(digest[:])
	return nil
}

func (s *Service) authorizePayment(r *http.Request, req paymentRequest, price wire.PriceInfo, state *paymentState) error {
	logger := slog.With("component", "signer", "event", "authorization", "operation", "authorization")
	identity := r.Header.Get("Signer-Auth-Id")
	refresh := s.authURL != nil && (state.AuthPolicy != s.authPolicy || state.AuthExpiry == 0 || time.Now().Unix() > state.AuthExpiry)
	if s.authURL != nil && !refresh {
		s.metrics.Add("signer_auth_cache_hits_total", 1, nil)
	}
	if refresh {
		started := time.Now()
		status, outcome := 0, "error"
		defer func() {
			s.metrics.Add("signer_auth_webhook_requests_total", 1, observability.Labels{"status": strconv.Itoa(status), "outcome": outcome})
			s.metrics.Observe("signer_auth_webhook_duration_seconds", time.Since(started).Seconds(), nil)
		}()
		body, err := json.Marshal(struct {
			Headers http.Header   `json:"headers"`
			State   *paymentState `json:"state"`
		}{r.Header, state})
		if err != nil {
			logger.ErrorContext(r.Context(), "signer auth payload encoding failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return err
		}
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.authURL.String(), bytes.NewReader(body))
		if err != nil {
			logger.ErrorContext(r.Context(), "signer auth request construction failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		for name, values := range s.authHeaders {
			request.Header.Del(name)
			for _, value := range values {
				request.Header.Add(name, value)
			}
		}
		response, err := s.authClient.Do(request)
		if err != nil {
			logger.ErrorContext(r.Context(), "signer auth webhook unavailable", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return serviceFailure(502, "signer auth webhook unavailable")
		}
		defer response.Body.Close()
		status = response.StatusCode
		if response.StatusCode != 200 {
			logger.ErrorContext(r.Context(), "signer auth webhook returned non-200 HTTP status", "outcome", "error", "error_kind", "http_status", "http_status", response.StatusCode)
			return serviceFailure(502, "signer auth webhook failed")
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if err != nil {
			logger.ErrorContext(r.Context(), "signer auth response read failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return serviceFailure(502, "invalid signer auth response")
		}
		if len(data) > 1<<20 {
			logger.ErrorContext(r.Context(), "signer auth response exceeds size limit", "outcome", "error", "error_kind", "response_limit")
			return serviceFailure(502, "invalid signer auth response")
		}
		var auth struct {
			Status   int       `json:"status"`
			Reason   string    `json:"reason"`
			Expiry   int64     `json:"expiry"`
			AuthID   string    `json:"auth_id"`
			MaxPrice *maxPrice `json:"maxPrice"`
		}
		if err := json.Unmarshal(data, &auth); err != nil {
			logger.ErrorContext(r.Context(), "signer auth response decoding failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return serviceFailure(502, "invalid signer auth response")
		}
		if auth.Status != 200 && (auth.Status < 400 || auth.Status > 599) {
			logger.ErrorContext(r.Context(), "signer auth response has invalid decision status", "outcome", "error", "error_kind", "invalid_decision", "decision_status", auth.Status)
			return serviceFailure(502, "invalid signer auth response")
		}
		if auth.Status != 200 {
			outcome = "denied"
			logger.WarnContext(r.Context(), "signer authorization rejected", "outcome", "denied", "error_kind", "policy_rejected", "decision_status", auth.Status)
			return errors.Join(paymentFailure{auth.Status, fmt.Sprintf("signer auth rejected request with status %d", auth.Status)}, errAuthorizationDenied)
		}
		if auth.AuthID != "" {
			identity = auth.AuthID
		}
		state.AuthExpiry, state.AuthPolicy, state.AuthMaxPrice = auth.Expiry, s.authPolicy, ""
		if auth.MaxPrice != nil {
			if err := checkMaxPrice(paymentRequest{Type: req.Type, MaxPrice: auth.MaxPrice}, price); err != nil {
				if f, ok := errors.AsType[paymentFailure](err); ok && f.status == http.StatusBadRequest {
					logger.ErrorContext(r.Context(), "signer auth invalid maxPrice", "outcome", "error", "error_kind", "invalid_policy")
					return serviceFailure(http.StatusBadGateway, "signer auth invalid maxPrice")
				}
				outcome = "denied"
				logger.WarnContext(r.Context(), "signer auth price ceiling rejected", "outcome", outcome, "error_kind", "price_ceiling")
				return errors.Join(err, errAuthorizationDenied)
			}
			state.AuthMaxPrice = auth.MaxPrice.Price.String()
		}
		outcome = "success"
	}
	// Apply webhook identity precedence before checking for an established ID change.
	if identity != "" && state.AuthID != identity {
		if state.AuthID != "" {
			logger.WarnContext(r.Context(), "signer authorization identity changed", "outcome", "denied", "error_kind", "identity_changed")
			return paymentFailure{403, "signer auth ID changed"}
		}
		state.AuthID = identity
	}
	if s.authURL != nil && state.AuthMaxPrice != "" {
		return checkMaxPrice(paymentRequest{Type: req.Type, MaxPrice: &maxPrice{Price: json.Number(state.AuthMaxPrice), Currency: "wei", Unit: map[string]string{"live": "seconds", "fixed": "fixed"}[req.Type]}}, price)
	}
	return nil
}
