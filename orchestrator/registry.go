package orchestrator

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
)

type priceInfo struct {
	Price    json.Number `json:"price"`
	PriceUSD json.Number `json:"price_usd,omitempty" toml:"-"`
	Currency string      `json:"currency"`
	Unit     string      `json:"unit"`
}

type runnerGPU struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	VRAMMB int    `json:"vram_mb,omitempty"`
}

func (g *runnerGPU) UnmarshalJSON(data []byte) error {
	var index int
	if err := json.Unmarshal(data, &index); err == nil {
		if index < 0 {
			return errors.New("GPU index must be nonnegative")
		}
		*g = runnerGPU{ID: fmt.Sprintf("%d", index)}
		return nil
	}
	type plain runnerGPU
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.VRAMMB < 0 {
		return errors.New("GPU VRAM must be nonnegative")
	}
	*g = runnerGPU(value)
	return nil
}

type heartbeatRequest struct {
	RunnerID   string     `json:"runner_id"`
	RunnerURL  string     `json:"runner_url"`
	Version    string     `json:"version"`
	Metadata   string     `json:"metadata"`
	GPU        *runnerGPU `json:"gpu,omitempty"`
	Status     string     `json:"status"`
	Mode       string     `json:"mode"`
	Proxy      bool       `json:"proxy"`
	App        string     `json:"app"`
	Capacity   int        `json:"capacity"`
	PriceInfo  priceInfo  `json:"price_info"`
	SessionIDs []string   `json:"session_ids"`
}

type heartbeatResponse struct {
	RunnerID          string          `json:"runner_id"`
	Orchestrator      string          `json:"orchestrator,omitempty"`
	HeartbeatInterval string          `json:"heartbeat_interval"`
	HeartbeatTTL      string          `json:"heartbeat_ttl"`
	HeartbeatSecret   string          `json:"heartbeat_secret,omitempty"`
	SessionIDs        []string        `json:"session_ids"`
	O2R               *trickleChannel `json:"o2r,omitempty"`
}

type trickleChannel struct {
	Name        string `json:"name"`
	ChannelName string `json:"channel_name"`
	URL         string `json:"url"`
	InternalURL string `json:"internal_url,omitempty"`
	MimeType    string `json:"mime_type"`
}

type runner struct {
	USDQuote   priceInfo
	PriceError bool
	heartbeatRequest
	Credential string
	Last       time.Time
	Static     bool
	Healthy    bool
	HealthURL  string
	HealthCode int
	Sessions   map[string]*session

	healthFailures int
}

type session struct {
	identity  observability.Identity
	pending   bool
	ID        string
	Token     string
	Proxies   map[string]sessionProxy
	Created   time.Time
	PriceInfo priceInfo
	ctx       context.Context
	cancel    context.CancelCauseFunc
}

var errInvalidSessionToken = errors.New("invalid session token")
var errRunnerUnavailable = errors.New("runner unavailable")

var (
	errRunnerCapacity  = errors.New("runner at capacity")
	errSessionCapacity = errors.New("session capacity reached")
)

func capacityError(err error) bool {
	return errors.Is(err, errRunnerCapacity) || errors.Is(err, errSessionCapacity)
}

// runnerStopped bounds the time existing proxy requests have to finish.
type runnerStopped struct{ deadline time.Time }

func (runnerStopped) Error() string { return "session stopped by runner" }

type sessionProxy struct {
	target *url.URL
	runner bool
}

type Registry struct {
	logger        *slog.Logger
	metrics       *observability.Metrics
	server        *Server
	mu            sync.Mutex
	runners       map[string]*runner
	secret        string
	service       string
	runnerService string
	interval      time.Duration
	ttl           time.Duration
	onEvent       func(runnerID, event, sessionID, sessionToken string)
	weiPerUSD     *big.Rat
	rateUntil     time.Time
	proxyTemplate string
	closed        bool
}

const maxRunners = 256

func NewRegistry(secret, service string, interval, ttl time.Duration) *Registry {
	r := &Registry{metrics: observability.New("orchestrator"), runners: make(map[string]*runner), secret: secret, service: strings.TrimRight(service, "/"), interval: interval, ttl: ttl}
	r.metrics.Snapshot(r.collectMetrics)
	r.metrics.Legacy(func(emit observability.Emit) {
		runners, sessions := r.Counts()
		emit("runners", float64(runners), nil)
		emit("sessions", float64(sessions), nil)
	})
	return r
}

func (r *Registry) runnerServiceURL() string {
	if r.runnerService != "" {
		return r.runnerService
	}
	return r.service
}

func (r *Registry) SetWeiPerUSD(rate *big.Rat) { r.setRate(rate, time.Time{}) }

func (r *Registry) setRate(rate *big.Rat, until time.Time) {
	if rate == nil || rate.Sign() <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.weiPerUSD = new(big.Rat).Set(rate)
	r.rateUntil = until
	r.metrics.Gauge("price_feed_valid_until_timestamp_seconds", nil).Set(float64(until.Unix()) * observability.Bool(!until.IsZero()))
	for _, item := range r.runners {
		quote := item.USDQuote
		item.PriceError = normalizePriceAt(&quote, r.weiPerUSD) != nil
		if !item.PriceError {
			item.PriceInfo = quote
		}
	}
}

func (r *Registry) priceAvailable(item *runner) bool {
	return !item.PriceError && (item.PriceInfo.Price == "" || r.rateUntil.IsZero() || time.Now().Before(r.rateUntil))
}

func (r *Registry) normalizePrice(price *priceInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return normalizePriceAt(price, r.weiPerUSD)
}

func normalizePriceAt(price *priceInfo, rate *big.Rat) error {
	value := strings.TrimSpace(price.Price.String())
	numeric, ok := new(big.Rat).SetString(value)
	if rate != nil && (!ok || numeric.Sign() <= 0) {
		return errors.New("runner price must be positive in on-chain mode")
	}
	if value == "" || (ok && numeric.Sign() == 0) {
		*price = priceInfo{}
		return nil
	}
	if !ok || numeric.Sign() < 0 {
		return errors.New("runner price must be nonnegative")
	}
	if price.Currency != "" && strings.ToLower(strings.TrimSpace(price.Currency)) != "usd" {
		return errors.New("runner price currency must be USD")
	}
	if rate == nil {
		// Off-chain registration accepts the runner's usual quote, but does
		// not advertise or enforce payment requirements.
		*price = priceInfo{}
		return nil
	}
	wei, unit, err := pm.ConvertRunnerPrice(value, price.Unit, rate)
	if err != nil {
		return err
	}
	usd := numeric
	if unit == "seconds" {
		usd.Quo(usd, big.NewRat(3600, 1))
	}
	usdString := strings.TrimRight(strings.TrimRight(usd.FloatString(36), "0"), ".")
	*price = priceInfo{Price: json.Number(wei.String()), PriceUSD: json.Number(usdString), Currency: "wei", Unit: unit}
	return nil
}

func randomIDWithPrefix(prefix string, n int) (string, error) {
	id, err := randomID(n)
	return prefix + id, err
}

func randomID(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)), nil
}

func equalSecret(a, b string) bool {
	return a != "" && b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (r *Registry) acceptsHeartbeatCredential(auth string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if equalSecret(auth, r.secret) {
		return true
	}
	for _, item := range r.runners {
		if !item.Static && equalSecret(auth, item.Credential) {
			return true
		}
	}
	return false
}

func validateHeartbeat(req *heartbeatRequest) error {
	if req.Capacity == 0 {
		req.Capacity = 1
	}
	req.PriceInfo.Unit = strings.ToLower(strings.TrimSpace(req.PriceInfo.Unit))
	req.PriceInfo.Currency = strings.ToLower(strings.TrimSpace(req.PriceInfo.Currency))
	if req.RunnerID != "" && !validRouteID(req.RunnerID) {
		return errors.New("runner_id must contain only ASCII letters, digits, '-' or '_' and be at most 128 bytes")
	}
	if req.App == "" || req.Capacity < 1 || req.Capacity > 10000 {
		return errors.New("app and capacity between 1 and 10000 are required")
	}
	if _, err := destination.ParseURL(req.RunnerURL); err != nil {
		return errors.New("runner_url must be an absolute HTTP or HTTPS URL")
	}
	if req.Mode != "" && req.Mode != "persistent" && req.Mode != "single-shot" {
		return errors.New("mode must be persistent or single-shot")
	}
	if req.PriceInfo.Unit == "720p" {
		return errors.New("720p pricing is unsupported")
	}
	if req.PriceInfo.Unit != "" && req.PriceInfo.Unit != "hour" && req.PriceInfo.Unit != "fixed" {
		return errors.New("price_info.unit must be hour or fixed")
	}
	return nil
}

func validRouteID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func (r *Registry) Heartbeat(req heartbeatRequest, auth string) (heartbeatResponse, int, error) {
	if err := validateHeartbeat(&req); err != nil {
		return heartbeatResponse{}, http.StatusBadRequest, err
	}
	if err := r.validateProxyRunner(req); err != nil {
		return heartbeatResponse{}, http.StatusBadRequest, err
	}
	quote := req.PriceInfo
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := normalizePriceAt(&req.PriceInfo, r.weiPerUSD); err != nil {
		return heartbeatResponse{}, http.StatusBadRequest, err
	}
	if r.closed {
		return heartbeatResponse{}, http.StatusServiceUnavailable, errors.New("orchestrator shutting down")
	}
	current := r.runners[req.RunnerID]
	if current != nil && current.Static {
		return heartbeatResponse{}, http.StatusForbidden, errors.New("static runner cannot heartbeat")
	}
	initial := current == nil
	if initial {
		if !equalSecret(auth, r.secret) {
			return heartbeatResponse{}, http.StatusUnauthorized, errors.New("invalid runner credential")
		}
		if len(r.runners) >= maxRunners {
			return heartbeatResponse{}, http.StatusServiceUnavailable, errors.New("runner capacity reached")
		}
		id := req.RunnerID
		if id == "" {
			var err error
			id, err = randomIDWithPrefix("runner_", 5)
			if err != nil {
				return heartbeatResponse{}, http.StatusInternalServerError, err
			}
		}
		credential, err := randomID(32)
		if err != nil {
			return heartbeatResponse{}, http.StatusInternalServerError, err
		}
		current = &runner{Credential: credential, Sessions: make(map[string]*session), Healthy: true}
		req.RunnerID = id
		r.runners[id] = current
	} else if !equalSecret(auth, current.Credential) {
		return heartbeatResponse{}, http.StatusUnauthorized, errors.New("invalid runner credential")
	}
	if req.Mode == "" {
		req.Mode = "persistent"
	}
	if req.Status == "" {
		req.Status = "ready"
	}
	current.USDQuote, current.PriceError = quote, false
	current.heartbeatRequest = req
	current.Last = time.Now()
	// The SDK sends its active session IDs for reconciliation. Preserve
	// reservations until an explicit stop or runner expiry; the runner may
	// have missed a callback and will receive the authoritative list.
	ids := make([]string, 0, len(current.Sessions))
	for id := range current.Sessions {
		ids = append(ids, id)
	}
	resp := heartbeatResponse{RunnerID: req.RunnerID, Orchestrator: r.runnerServiceURL(), HeartbeatInterval: r.interval.String(), HeartbeatTTL: r.ttl.String(), SessionIDs: ids}
	if initial {
		resp.HeartbeatSecret = current.Credential
		r.runnerEvent(req.RunnerID, current, "registered")
	}
	return resp, http.StatusOK, nil
}

func (r *Registry) Unregister(id, auth string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.runners[id]
	if current == nil {
		return http.StatusNotFound, errors.New("runner not found")
	}
	if current.Static || !equalSecret(current.Credential, auth) {
		return http.StatusUnauthorized, errors.New("invalid runner credential")
	}
	for sid := range current.Sessions {
		r.releaseLocked(id, sid, nil, "runner_unregistered")
	}
	if r.onEvent != nil {
		r.onEvent(id, "unregistered", "", "")
	}
	r.runnerEvent(id, current, "unregistered")
	delete(r.runners, id)
	return http.StatusOK, nil
}

type discoveryRunner struct {
	ObservedAt        string     `json:"observed_at"`
	URL               string     `json:"url"`
	App               string     `json:"app"`
	Version           string     `json:"version,omitempty"`
	Metadata          string     `json:"metadata,omitempty"`
	GPU               *runnerGPU `json:"gpu,omitempty"`
	Mode              string     `json:"mode"`
	Capacity          int        `json:"capacity"`
	CapacityUsed      int        `json:"capacity_used"`
	CapacityAvailable int        `json:"capacity_available"`
	PriceInfo         priceInfo  `json:"price_info,omitzero"`
}

func (r discoveryRunner) DiscoveryIdentity() (string, string, string) {
	return r.URL, r.App, r.ObservedAt
}

type discoveryEntry = discovery.Entry[discoveryRunner]

func (r *Registry) Discovery() []discoveryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := []discoveryRunner{}
	observed := time.Now().UTC().Format(time.RFC3339Nano)
	for _, id := range slices.Sorted(maps.Keys(r.runners)) {
		item := r.runners[id]
		if !r.usable(item) || !r.priceAvailable(item) {
			continue
		}
		available := max(0, item.Capacity-len(item.Sessions))
		result = append(result, discoveryRunner{ObservedAt: observed, URL: r.discoveryURL(id, item), App: item.App, Version: item.Version, Metadata: item.Metadata, GPU: item.GPU, Mode: item.Mode, Capacity: item.Capacity, CapacityUsed: len(item.Sessions), CapacityAvailable: available, PriceInfo: item.PriceInfo})
	}
	return []discoveryEntry{{Address: r.service, Runners: result}}
}

func (r *Registry) usable(item *runner) bool {
	return !r.closed && (item.Static || time.Since(item.Last) <= r.ttl) && item.Healthy && item.Status == "ready"
}

func (r *Registry) appURL(id, mode, sessionID string) string {
	if mode == "single-shot" {
		return r.service + "/apps/" + url.PathEscape(id) + "/app"
	}
	if sessionID == "" {
		return r.service + "/apps/" + url.PathEscape(id) + "/session"
	}
	return r.service + "/apps/" + url.PathEscape(id) + "/session/" + url.PathEscape(sessionID) + "/app"
}

func (r *Registry) Reserve(id string) (string, string, string, int, error) {
	return r.ReserveWithID(id, "")
}

func (r *Registry) ReserveWithID(id, requestedID string) (string, string, string, int, error) {
	return r.reserveWithPrice(id, requestedID, nil)
}

func (r *Registry) reserveWithPrice(id, requestedID string, agreed *priceInfo) (sid, app, control string, status int, result error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item != nil {
		defer func() {
			outcome := "success"
			if result != nil {
				outcome = "rejected"
				if capacityError(result) {
					outcome = "capacity"
				} else if status == 503 {
					outcome = "unavailable"
				}
			}
			r.metrics.Add("session_reservations_total", 1, runnerIdentity(id, item).Labels().With("outcome", outcome))
		}()
	}
	if item == nil || !r.usable(item) {
		return "", "", "", http.StatusNotFound, errors.New("runner unavailable")
	}
	if agreed == nil && !r.priceAvailable(item) {
		return "", "", "", http.StatusServiceUnavailable, errors.New("runner price unavailable")
	}
	if item.Mode != "persistent" && !(item.Mode == "single-shot" && requestedID != "") {
		return "", "", "", http.StatusBadRequest, errors.New("only persistent runners have sessions")
	}
	total := 0
	for _, registered := range r.runners {
		total += len(registered.Sessions)
	}
	if total >= 10000 {
		return "", "", "", http.StatusServiceUnavailable, errSessionCapacity
	}
	if len(item.Sessions) >= item.Capacity {
		return "", "", "", http.StatusServiceUnavailable, errRunnerCapacity
	}
	sessionID := requestedID
	if sessionID == "" {
		var err error
		sessionID, err = randomIDWithPrefix("session_", 10)
		if err != nil {
			return "", "", "", http.StatusInternalServerError, err
		}
	}
	if !validRouteID(sessionID) || item.Sessions[sessionID] != nil {
		return "", "", "", http.StatusConflict, errors.New("session already exists or invalid")
	}
	token, err := randomID(32)
	if err != nil {
		return "", "", "", http.StatusInternalServerError, err
	}
	price := item.PriceInfo
	if agreed != nil {
		price = *agreed
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	item.Sessions[sessionID] = &session{identity: runnerIdentity(id, item), pending: agreed != nil, ID: sessionID, Token: token, Created: time.Now(), Proxies: map[string]sessionProxy{}, PriceInfo: price, ctx: ctx, cancel: cancel}
	appURL := r.appURL(id, item.Mode, sessionID)
	if item.Proxy && item.Mode == "persistent" {
		target, _ := url.Parse(item.RunnerURL)
		proxyID, err := randomID(10)
		if err != nil {
			cancel(nil)
			delete(item.Sessions, sessionID)
			return "", "", "", http.StatusInternalServerError, err
		}
		item.Sessions[sessionID].Proxies[proxyID] = sessionProxy{target: target, runner: true}
		appURL = r.proxyURL(proxyID)
	}
	if agreed == nil && r.onEvent != nil {
		r.onEvent(id, "reserved", sessionID, token)
	}
	return sessionID, appURL, r.service + "/apps/" + url.PathEscape(id) + "/session/" + url.PathEscape(sessionID), http.StatusOK, nil
}

func (r *Registry) PriceForRunner(id string) (priceInfo, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || !r.usable(item) {
		return priceInfo{}, http.StatusNotFound, errors.New("runner unavailable")
	}
	if !r.priceAvailable(item) {
		return priceInfo{}, http.StatusServiceUnavailable, errors.New("runner price unavailable")
	}
	return item.PriceInfo, http.StatusOK, nil
}

func (r *Registry) ModeForRunner(id string) (string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || !r.usable(item) {
		return "", http.StatusNotFound, errors.New("runner unavailable")
	}
	return item.Mode, http.StatusOK, nil
}

func (r *Registry) PriceForSession(id, sid string) (priceInfo, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || item.Sessions[sid] == nil || item.Sessions[sid].pending {
		return priceInfo{}, http.StatusNotFound, errors.New("session not found")
	}
	return item.Sessions[sid].PriceInfo, http.StatusOK, nil
}

func (r *Registry) PaidSessions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []string
	for _, item := range r.runners {
		for id, sess := range item.Sessions {
			if !sess.pending && sess.PriceInfo.Price != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func (r *Registry) ReleaseBySession(sid string, reasons ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, item := range r.runners {
		if item.Sessions[sid] != nil {
			r.releaseLocked(id, sid, nil, reasons...)
			return
		}
	}
}

func (r *Registry) release(id, sid, token string, callback bool) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || item.Sessions[sid] == nil {
		return http.StatusNotFound, errors.New("session not found")
	}
	if callback && !equalSecret(item.Sessions[sid].Token, token) {
		return http.StatusUnauthorized, errInvalidSessionToken
	}
	var cause error
	reason := "client_stop"
	if callback {
		cause, reason = runnerStopped{deadline: time.Now().Add(time.Second)}, "runner_stop"
	}
	r.releaseLocked(id, sid, cause, reason)
	return http.StatusOK, nil
}

// releaseLocked is the single teardown path for capacity and active requests.
// The caller holds r.mu; cancellation never waits for the request to finish.
func (r *Registry) releaseLocked(id, sid string, cause error, reasons ...string) {
	item := r.runners[id]
	if item == nil || item.Sessions[sid] == nil {
		return
	}
	sess := item.Sessions[sid]
	reason := "released"
	if len(reasons) != 0 {
		reason = reasons[0]
	}
	if sess.pending && (reason == "released" || reason == "completed") {
		reason = "reservation_failed"
	}
	r.metrics.Add("session_ends_total", 1, sess.identity.Labels().With("reason", reason))
	if !sess.pending {
		r.metrics.Observe("session_duration_seconds", time.Since(sess.Created).Seconds(), sess.identity.Labels())
	}
	if r.logger != nil {
		r.logger.Info("session ended", "event", "session_ended", "operation", "release", "outcome", reason, "app", sess.identity.App, "runner_id", id, "session_id", sid)
	}
	sess.cancel(cause)
	delete(item.Sessions, sid)
	if r.onEvent != nil {
		r.onEvent(id, "released", sid, "")
	}
}

func (r *Registry) sessionContext(id, sid string) (context.Context, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || item.Sessions[sid] == nil {
		return nil, false
	}
	return item.Sessions[sid].ctx, true
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for id, item := range r.runners {
		for sid := range item.Sessions {
			r.releaseLocked(id, sid, nil, "shutdown")
		}
		if r.onEvent != nil {
			r.onEvent(id, "unregistered", "", "")
		}
	}
}

func (r *Registry) sessionTarget(id, sid string) (*url.URL, string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || !r.usable(item) || item.Sessions[sid] == nil || item.Sessions[sid].pending {
		return nil, "", http.StatusNotFound, errors.New("session not found")
	}
	target, err := destination.ParseURL(item.RunnerURL)
	if err != nil {
		return nil, "", http.StatusBadGateway, err
	}
	return target, item.Sessions[sid].Token, http.StatusOK, nil
}

func (r *Registry) validSessionToken(id, sid, token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	return item != nil && item.Sessions[sid] != nil && !item.Sessions[sid].pending && equalSecret(item.Sessions[sid].Token, token)
}

func (r *Registry) addProxy(id, sid, token string, target *url.URL) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || item.Sessions[sid] == nil || !equalSecret(item.Sessions[sid].Token, token) {
		return "", errInvalidSessionToken
	}
	proxyID, err := randomID(10)
	if err != nil {
		return "", err
	}
	if len(item.Sessions[sid].Proxies) >= 25 {
		return "", errors.New("too many session proxies")
	}
	item.Sessions[sid].Proxies[proxyID] = sessionProxy{target: target}
	return proxyID, nil
}

func (r *Registry) proxyTarget(proxyID string) (sessionProxy, string, string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for runnerID, item := range r.runners {
		if !r.usable(item) {
			continue
		}
		for sid, sess := range item.Sessions {
			if sess.pending {
				continue
			}
			if target, ok := sess.Proxies[proxyID]; ok {
				return target, runnerID, sid, sess.Token, true
			}
		}
	}
	return sessionProxy{}, "", "", "", false
}

func (r *Registry) Expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, item := range r.runners {
		if !item.Static && time.Since(item.Last) > r.ttl {
			for sid := range item.Sessions {
				r.releaseLocked(id, sid, errRunnerUnavailable, "runner_expired")
			}
			if r.onEvent != nil {
				r.onEvent(id, "expired", "", "")
			}
			r.runnerEvent(id, item, "expired")
			delete(r.runners, id)
		}
	}
}

type StaticRunner struct {
	ID         string
	Proxy      bool
	RunnerURL  string
	Version    string
	Metadata   string
	GPU        *runnerGPU
	App        string
	Mode       string
	Status     string
	Capacity   int
	HealthURL  string
	HealthCode int
	PriceInfo  priceInfo
}

func (r *Registry) AddStatic(config StaticRunner) error {
	if !validRouteID(config.ID) {
		return errors.New("static runner id must be a route-safe identifier")
	}
	req := heartbeatRequest{RunnerID: config.ID, Proxy: config.Proxy, RunnerURL: config.RunnerURL, Version: config.Version, Metadata: config.Metadata, GPU: config.GPU, App: config.App, Mode: config.Mode, Status: config.Status, Capacity: config.Capacity, PriceInfo: config.PriceInfo}
	if err := validateHeartbeat(&req); err != nil {
		return fmt.Errorf("static runner %s: %w", config.ID, err)
	}
	if err := r.validateProxyRunner(req); err != nil {
		return err
	}
	quote := req.PriceInfo
	if config.HealthURL != "" {
		if _, err := destination.ParseURL(config.HealthURL); err != nil {
			return fmt.Errorf("static runner %s has invalid health URL", config.ID)
		}
	}
	if req.Mode == "" {
		req.Mode = "persistent"
	}
	if req.Status == "" {
		req.Status = "ready"
	}
	if config.HealthCode == 0 {
		config.HealthCode = http.StatusOK
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := normalizePriceAt(&req.PriceInfo, r.weiPerUSD); err != nil {
		return fmt.Errorf("static runner %s: %w", config.ID, err)
	}
	if r.runners[config.ID] != nil {
		return fmt.Errorf("duplicate static runner id %s", config.ID)
	}
	if len(r.runners) >= maxRunners {
		return errors.New("static runner capacity reached")
	}
	r.runners[config.ID] = &runner{USDQuote: quote, heartbeatRequest: req, Static: true, Healthy: config.HealthURL == "", HealthURL: config.HealthURL, HealthCode: config.HealthCode, Sessions: map[string]*session{}}
	r.runnerEvent(config.ID, r.runners[config.ID], "registered")
	return nil
}

func (r *Registry) CheckStaticHealth(ctx context.Context, client *http.Client) {
	r.mu.Lock()
	checks := make(map[string]string)
	for id, item := range r.runners {
		if item.Static && item.HealthURL != "" {
			checks[id] = item.HealthURL
		}
	}
	r.mu.Unlock()
	var workers sync.WaitGroup
	jobs := make(chan string)
	for range min(8, len(checks)) {
		workers.Go(func() {
			for id := range jobs {
				r.checkStaticHealth(ctx, client, id, checks[id])
			}
		})
	}
	for id := range checks {
		select {
		case jobs <- id:
		case <-ctx.Done():
		}
	}
	close(jobs)
	workers.Wait()
}

func (r *Registry) checkStaticHealth(ctx context.Context, client *http.Client, id, raw string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return
	}
	response, err := client.Do(request)
	healthy := false
	if err == nil {
		_ = response.Body.Close()
		r.mu.Lock()
		item := r.runners[id]
		healthy = item != nil && response.StatusCode == item.HealthCode
		r.mu.Unlock()
	}
	r.mu.Lock()
	if item := r.runners[id]; item != nil && item.Static {
		r.metrics.Add("runner_health_checks_total", 1, runnerIdentity(id, item).Labels().With("outcome", map[bool]string{true: "success", false: "error"}[healthy]))
		if healthy {
			item.healthFailures = 0
		} else {
			item.healthFailures++
			healthy = item.Healthy && item.healthFailures < 3
		}
		if item.Healthy != healthy {
			event := "unhealthy"
			if healthy {
				event = "healthy"
			}
			r.runnerEvent(id, item, event)
		}
		item.Healthy = healthy
		if !healthy {
			for sid := range item.Sessions {
				r.releaseLocked(id, sid, errRunnerUnavailable, "runner_unhealthy")
			}
		}
	}
	r.mu.Unlock()
}

func (r *Registry) Counts() (runners, sessions int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range r.runners {
		runners++
		sessions += len(item.Sessions)
	}
	return
}

func (r *Registry) String() string {
	runners, sessions := r.Counts()
	return fmt.Sprintf("runners=%d sessions=%d", runners, sessions)
}

func (r *Registry) activateSession(id, sid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.runners[id]
	if item == nil || !r.usable(item) || item.Sessions[sid] == nil {
		return false
	}
	sess := item.Sessions[sid]
	if sess.pending {
		sess.pending = false
		if r.onEvent != nil {
			r.onEvent(id, "reserved", sid, sess.Token)
		}
	}
	return true
}
