package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/trickle"
)

const (
	maxControlBody     = 1 << 20
	maxTrickleChannels = 1024
)

type Server struct {
	basePath        string
	runnerBasePath  string
	registry        *Registry
	runnerTransport *http.Transport
	proxyTransport  *http.Transport
	logger          *slog.Logger
	payment         *PaymentEngine
	mux             *http.ServeMux
	mu              sync.Mutex
	channels        map[string]*channel
	o2r             map[string]string
	trickleServer   *trickle.Server
	controlSlots    chan struct{}
	streamSlots     chan struct{}
}

func NewServer(registry *Registry, runnerPolicy, proxyPolicy destination.Policy, logger *slog.Logger) *Server {
	logger = logger.With("component", "orchestrator")
	registry.logger = logger
	s := &Server{registry: registry, logger: logger, mux: http.NewServeMux(), channels: map[string]*channel{}, o2r: map[string]string{}}
	serviceURL, _ := url.Parse(registry.service)
	s.basePath = strings.TrimRight(serviceURL.Path, "/")
	runnerURL, _ := url.Parse(registry.runnerServiceURL())
	s.runnerBasePath = strings.TrimRight(runnerURL.Path, "/")
	s.runnerTransport = runnerPolicy.Transport(time.Minute)
	s.proxyTransport = proxyPolicy.Transport(time.Minute)
	// A stalled stream must not take the slot needed to stop or fund it.
	s.controlSlots = make(chan struct{}, 64)
	s.streamSlots = make(chan struct{}, 256)
	registry.onEvent = s.emitSessionEvent
	s.mux.HandleFunc("POST /runners/heartbeat", s.heartbeat)
	s.mux.HandleFunc("POST /runners/{runner_id}/unregister", s.unregister)
	s.mux.HandleFunc("GET /discovery", s.discovery)
	s.mux.HandleFunc("POST /apps/{runner_id}/session", s.reserve)
	s.mux.HandleFunc("POST /apps/{runner_id}/session/{session_id}/stop", s.clientStop)
	s.mux.HandleFunc("POST /apps/{runner_id}/session/{session_id}/payment", s.sessionPayment)
	s.mux.HandleFunc("POST /refresh-payment", s.refreshPayment)
	s.mux.HandleFunc("POST /runner/{runner_id}/session/{session_id}/stop", s.runnerStop)
	s.mux.HandleFunc("POST /runner/{runner_id}/session/{session_id}/proxy", s.createProxy)
	s.mux.HandleFunc("POST /runner/{runner_id}/session/{session_id}/channels", s.createChannels)
	s.mux.HandleFunc("DELETE /runner/{runner_id}/session/{session_id}/channels", s.deleteChannels)
	s.mux.HandleFunc("/apps/{runner_id}/session/{session_id}/app", s.proxySession)
	s.mux.HandleFunc("/apps/{runner_id}/session/{session_id}/app/{app_path...}", s.proxySession)
	s.mux.HandleFunc("/apps/{runner_id}/app", s.proxySingleShot)
	s.mux.HandleFunc("/apps/{runner_id}/app/{app_path...}", s.proxySingleShot)
	s.mux.HandleFunc("/proxy/{proxy_id}", s.proxyGenerated)
	s.mux.HandleFunc("/proxy/{proxy_id}/{app_path...}", s.proxyGenerated)
	s.mux.HandleFunc("/run/{proxy_id}", s.proxyGenerated)
	s.mux.HandleFunc("/run/{proxy_id}/{app_path...}", s.proxyGenerated)
	s.trickleServer = trickle.ConfigureServer(trickle.TrickleServerConfig{
		BasePath: "/ai/trickle/", Mux: s.mux, BeforeDelete: s.beforeDeleteChannel,
	})
	registry.mu.Lock()
	registry.server = s
	registry.mu.Unlock()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.registry.metrics.HTTP(w, r, s.serveHTTP)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path+"/", "/../") {
		fail(w, http.StatusBadRequest, "invalid path")
		return
	}
	proxyID, appPath := s.registry.matchProxy(r)
	if proxyID == "" {
		basePath := ""
		for _, candidate := range []string{s.basePath, s.runnerBasePath} {
			if len(candidate) > len(basePath) && strings.HasPrefix(r.URL.Path, candidate+"/") {
				basePath = candidate
			}
		}
		if basePath == "" && s.basePath != "" && s.runnerBasePath != "" {
			fail(w, 404, "route not found")
			return
		}
		if basePath != "" {
			r = r.Clone(r.Context())
			r.URL.Path = strings.TrimPrefix(r.URL.Path, basePath)
			r.URL.RawPath = ""
		}
	}
	stream := proxyID != "" || strings.HasPrefix(r.URL.Path, "/ai/trickle/") || strings.HasPrefix(r.URL.Path, "/proxy/") || strings.HasPrefix(r.URL.Path, "/run/") || strings.HasSuffix(r.URL.Path, "/app") || strings.Contains(r.URL.Path, "/app/")
	_, pattern := s.mux.Handler(r)
	observability.Operation(r, pattern)
	if proxyID != "" {
		observability.Operation(r, "generated_proxy")
	}
	// Resolve only known routes before admission so rejected application work is
	// attributed without turning attacker-supplied runner IDs into label values.
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if pattern != "" && len(parts) >= 3 && parts[0] == "apps" {
		if parts[2] == "app" {
			observability.App(r, s.registry.identity(parts[1], ""))
		} else if len(parts) >= 5 && parts[2] == "session" && parts[4] == "app" {
			observability.App(r, s.registry.identity(parts[1], parts[3]))
		}
	}
	generated := proxyID
	if generated == "" && pattern != "" && len(parts) >= 2 && (parts[0] == "proxy" || parts[0] == "run") {
		generated = parts[1]
	}
	if generated != "" {
		if s.registry.singleShotProxy(generated) {
			observability.App(r, s.registry.identity(generated, ""))
		} else if _, id, sid, _, ok := s.registry.proxyTarget(generated); ok {
			observability.App(r, s.registry.identity(id, sid))
		}
	}
	if strings.HasPrefix(r.URL.Path, "/ai/trickle/") {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/ai/trickle/"), "/")
		s.mu.Lock()
		ch := s.channels[parts[0]]
		var identity observability.Identity
		if ch != nil {
			identity = ch.identity
		}
		s.mu.Unlock()
		observability.Stream(r, identity, "trickle")
	}
	slots := s.controlSlots
	if stream {
		slots = s.streamSlots
	} else {
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		s.registry.metrics.Add("http_admission_rejections_total", 1, observability.Labels{"pool": map[bool]string{true: "stream", false: "control"}[stream]})
		observability.Outcome(r, "capacity")
		fail(w, http.StatusServiceUnavailable, "server busy")
		return
	}
	if proxyID != "" {
		r.SetPathValue("proxy_id", proxyID)
		r.SetPathValue("app_path", appPath)
		s.proxyGenerated(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) SetPayment(engine *PaymentEngine) {
	s.payment = engine
	if engine != nil {
		engine.metrics, engine.identity = s.registry.metrics, s.registry.identity
		engine.store.logger = s.logger
		if engine.store.metrics == nil {
			engine.store.metrics = s.registry.metrics
			engine.store.metrics.Snapshot(engine.store.collectMetrics)
		}
	}
}

func (s *Server) Close() {
	s.registry.Close()
	s.runnerTransport.CloseIdleConnections()
	s.proxyTransport.CloseIdleConnections()
}

func writeJSON(w http.ResponseWriter, status int, value any) (int, error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	return w.Write(append(body, '\n'))
}

func fail(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func failReservation(w http.ResponseWriter, r *http.Request, status int, err error) {
	if capacityError(err) {
		observability.Outcome(r, "capacity")
	}
	fail(w, status, err.Error())
}

func decode(r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxControlBody))
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if !s.registry.acceptsHeartbeatCredential(r.Header.Get("Authorization")) {
		w.Header().Set("Connection", "close")
		fail(w, http.StatusUnauthorized, "invalid runner credential")
		return
	}
	var req heartbeatRequest
	if err := decode(r, &req); err != nil {
		fail(w, http.StatusBadRequest, "invalid heartbeat JSON")
		return
	}
	resp, status, err := s.registry.Heartbeat(req, r.Header.Get("Authorization"))
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	if resp.HeartbeatSecret != "" {
		id, err := randomIDWithPrefix(resp.RunnerID+"-", 10)
		if err != nil {
			fail(w, http.StatusInternalServerError, "runner channel creation failed")
			return
		}
		id += "-o2r"
		s.registry.mu.Lock()
		current := s.registry.runners[resp.RunnerID]
		if s.registry.closed || current == nil || current.Credential != resp.HeartbeatSecret {
			s.registry.mu.Unlock()
			fail(w, 503, "runner registration expired")
			return
		}
		s.mu.Lock()
		s.newChannel(id, "o2r", "application/json", resp.RunnerID, "").identity = runnerIdentity(resp.RunnerID, current)
		s.o2r[resp.RunnerID] = id
		s.mu.Unlock()
		s.registry.mu.Unlock()
		address := s.registry.runnerServiceURL() + "/ai/trickle/" + id
		resp.O2R = &trickleChannel{Name: "o2r", ChannelName: id, URL: address, MimeType: "application/json"}
	}
	writeJSON(w, status, resp)
}

func (s *Server) emitSessionEvent(runnerID, event, sid, token string) {
	if event == "expired" || event == "unregistered" {
		s.mu.Lock()
		for id, ch := range s.channels {
			if ch.runnerID == runnerID {
				s.closeChannel(id)
			}
		}
		delete(s.o2r, runnerID)
		s.mu.Unlock()
		return
	}
	filterEmpty := func(m map[string]string) map[string]string {
		maps.DeleteFunc(m, func(_, value string) bool { return value == "" })
		return m
	}
	data, err := json.Marshal(filterEmpty(map[string]string{
		"event":         event,
		"session":       sid,
		"session_token": token,
		"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
	}))
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishSessionEvent(s.o2r[runnerID], data)
	if event == "released" {
		for id, owned := range s.channels {
			if owned.runnerID == runnerID && owned.sessionID == sid {
				s.closeChannel(id)
			}
		}
	}
}

func (s *Server) unregister(w http.ResponseWriter, r *http.Request) {
	status, err := s.registry.Unregister(r.PathValue("runner_id"), r.Header.Get("Authorization"))
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) discovery(w http.ResponseWriter, r *http.Request) {
	if n, _ := writeJSON(w, http.StatusOK, s.registry.Discovery()); n > discovery.MaxBody {
		s.logger.WarnContext(r.Context(), "orchestrator discovery response exceeds signer size limit", "event", "discovery_response", "operation", "discovery", "outcome", "error", "error_kind", "response_limit", "response_bytes", n, "max_bytes", discovery.MaxBody)
	}
}

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	if s.reservePaid(w, r) {
		return
	}
	id, appURL, controlURL, status, err := s.registry.Reserve(r.PathValue("runner_id"))
	if err != nil {
		failReservation(w, r, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": id, "app_url": appURL, "control_url": controlURL})
}

func (s *Server) clientStop(w http.ResponseWriter, r *http.Request) {
	status, err := s.registry.release(r.PathValue("runner_id"), r.PathValue("session_id"), "", false)
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) runnerStop(w http.ResponseWriter, r *http.Request) {
	status, err := s.registry.release(r.PathValue("runner_id"), r.PathValue("session_id"), r.Header.Get("Livepeer-Session-Token"), true)
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) callbackAuthorized(r *http.Request) bool {
	return s.registry.validSessionToken(r.PathValue("runner_id"), r.PathValue("session_id"), r.Header.Get("Livepeer-Session-Token"))
}

func (s *Server) createProxy(w http.ResponseWriter, r *http.Request) {
	if !s.callbackAuthorized(r) {
		fail(w, http.StatusUnauthorized, "invalid session token")
		return
	}
	var body struct {
		TargetURL string `json:"target_url"`
	}
	if err := decode(r, &body); err != nil {
		fail(w, http.StatusBadRequest, "invalid proxy JSON")
		return
	}
	var target *url.URL
	var err error
	if body.TargetURL == "" {
		target, _, _, err = s.registry.sessionTarget(r.PathValue("runner_id"), r.PathValue("session_id"))
	} else {
		target, err = destination.ParseURL(body.TargetURL)
	}
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := s.registry.addProxy(r.PathValue("runner_id"), r.PathValue("session_id"), r.Header.Get("Livepeer-Session-Token"), target)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, errInvalidSessionToken) {
			status = http.StatusUnauthorized
		}
		fail(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"proxy_id": id, "url": s.registry.proxyURL(id)})
}

func (s *Server) proxySession(w http.ResponseWriter, r *http.Request) {
	observability.App(r, s.registry.identity(r.PathValue("runner_id"), r.PathValue("session_id")))
	target, token, status, err := s.registry.sessionTarget(r.PathValue("runner_id"), r.PathValue("session_id"))
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	control := s.registry.runnerServiceURL() + "/runner/" + url.PathEscape(r.PathValue("runner_id")) + "/session/" + url.PathEscape(r.PathValue("session_id"))
	s.proxy(w, r, s.runnerTransport, target, r.PathValue("app_path"), r.PathValue("runner_id"), r.PathValue("session_id"), token, control)
}

func (s *Server) proxySingleShot(w http.ResponseWriter, r *http.Request) {
	observability.App(r, s.registry.identity(r.PathValue("runner_id"), ""))
	if s.proxyPaidSingleShot(w, r) {
		return
	}
	runnerID := r.PathValue("runner_id")
	id, err := randomIDWithPrefix("session_", 10)
	if err != nil {
		fail(w, http.StatusInternalServerError, "session creation failed")
		return
	}
	id, _, _, status, err := s.registry.ReserveWithID(runnerID, id)
	if err != nil {
		failReservation(w, r, status, err)
		return
	}
	defer s.registry.ReleaseBySession(id, "completed")
	target, token, status, err := s.registry.sessionTarget(runnerID, id)
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	control := s.registry.runnerServiceURL() + "/runner/" + url.PathEscape(runnerID) + "/session/" + id
	s.proxy(w, r, s.runnerTransport, target, r.PathValue("app_path"), runnerID, id, token, control)
}

func (s *Server) proxyGenerated(w http.ResponseWriter, r *http.Request) {
	if s.registry.singleShotProxy(r.PathValue("proxy_id")) {
		r.SetPathValue("runner_id", r.PathValue("proxy_id"))
		s.proxySingleShot(w, r)
		return
	}
	target, runnerID, sid, token, ok := s.registry.proxyTarget(r.PathValue("proxy_id"))
	if !ok {
		fail(w, http.StatusNotFound, "proxy not found")
		return
	}
	observability.App(r, s.registry.identity(runnerID, sid))
	control := s.registry.runnerServiceURL() + "/runner/" + url.PathEscape(runnerID) + "/session/" + url.PathEscape(sid)
	transport := s.proxyTransport
	if target.runner {
		transport = s.runnerTransport
	}
	s.proxy(w, r, transport, target.target, r.PathValue("app_path"), runnerID, sid, token, control)
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request, transport http.RoundTripper, target *url.URL, appPath, runnerID, sid, token, control string) {
	kind := "runner"
	if transport == s.proxyTransport {
		kind = "session_proxy"
	}
	transport = upstreamObserver{transport, s.registry.metrics, s.registry.identity(runnerID, sid), kind}
	sessionCtx, ok := s.registry.sessionContext(runnerID, sid)
	if !ok || sessionCtx.Err() != nil {
		fail(w, http.StatusNotFound, "session not found")
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(sessionCtx, func() {
		if stopped, ok := context.Cause(sessionCtx).(runnerStopped); ok {
			// The runner may be awaiting its stop callback before sending its
			// final response. Revoke the session now, but let that response finish.
			timer := time.NewTimer(time.Until(stopped.deadline))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
		}
		cancel()
	})
	defer stop()
	defer cancel()
	defer func() {
		if errors.Is(context.Cause(sessionCtx), errRunnerUnavailable) {
			observability.Outcome(r, "transport_error")
		} else if sessionCtx.Err() != nil {
			observability.Outcome(r, "session_stopped")
		}
	}()
	proxy := &httputil.ReverseProxy{
		ErrorLog:      log.New(proxyLog{s.logger.With("runner_id", runnerID, "session_id", sid)}, "", 0),
		Transport:     transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			observability.Outcome(r, "transport_error")
			s.logger.Warn("runner proxy failed", "event", "proxy_failed", "operation", "proxy", "outcome", "error", "runner_id", runnerID, "session_id", sid, "error_kind", "upstream_failure")
			fail(w, http.StatusBadGateway, "runner destination unavailable")
		},
		Rewrite: func(req *httputil.ProxyRequest) {
			u := target.Clone()
			u.RawQuery = target.RawQuery
			if req.In.URL.RawQuery != "" {
				if u.RawQuery != "" {
					u.RawQuery += "&"
				}
				u.RawQuery += req.In.URL.RawQuery
			}
			req.SetURL(u)
			req.Out.URL.Path = strings.TrimRight(target.Path, "/") + "/" + strings.TrimLeft(appPath, "/")
			req.Out.URL.RawPath = ""
			req.Out.URL.RawQuery = u.RawQuery
			req.SetXForwarded()
			req.Out.Header.Set("Livepeer-Runner-Route", runnerID)
			req.Out.Header.Set("Livepeer-Session-Id", sid)
			req.Out.Header.Set("Livepeer-Session-Token", token)
			req.Out.Header.Set("Livepeer-Session-Control", control)
		},
	}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

// ReverseProxy's internal messages may include transport-provided error text.
type proxyLog struct{ logger *slog.Logger }

func (l proxyLog) Write(p []byte) (int, error) {
	l.logger.Warn("proxy stream interrupted", "event", "proxy_stream", "operation", "proxy", "outcome", "error", "error_kind", "stream_error")
	return len(p), nil
}

func (s *Server) createChannels(w http.ResponseWriter, r *http.Request) {
	if !s.callbackAuthorized(r) {
		fail(w, http.StatusUnauthorized, "invalid session token")
		return
	}
	var body struct {
		Channels []struct {
			Name     string `json:"name"`
			MimeType string `json:"mime_type"`
		} `json:"channels"`
	}
	if err := decode(r, &body); err != nil || len(body.Channels) > 25 {
		fail(w, http.StatusBadRequest, "invalid channels request")
		return
	}
	ctx, ok := s.registry.sessionContext(r.PathValue("runner_id"), r.PathValue("session_id"))
	if !ok {
		fail(w, http.StatusNotFound, "session not found")
		return
	}
	identity := s.registry.identity(r.PathValue("runner_id"), r.PathValue("session_id"))
	for _, item := range body.Channels {
		if item.Name == "" {
			fail(w, http.StatusBadRequest, "channel name required")
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Release cancels the context before acquiring s.mu for cleanup. Checking
	// it under this lock prevents a late callback from recreating owned data.
	if ctx.Err() != nil {
		fail(w, http.StatusNotFound, "session not found")
		return
	}
	existing := map[string]string{}
	for id, ch := range s.channels {
		if ch.runnerID == r.PathValue("runner_id") && ch.sessionID == r.PathValue("session_id") {
			existing[ch.name] = id
		}
	}
	missing := map[string]string{}
	for _, item := range body.Channels {
		if existing[item.Name] == "" && missing[item.Name] == "" {
			id, err := randomIDWithPrefix("channel_", 10)
			if err != nil {
				fail(w, http.StatusInternalServerError, "channel creation failed")
				return
			}
			if validRouteID(item.Name) {
				id += "-" + item.Name
			}
			missing[item.Name] = id
		}
	}
	// Reserve space for one O2R channel per possible runner.
	if len(s.channels)+len(missing) > maxTrickleChannels-maxRunners {
		fail(w, http.StatusServiceUnavailable, "too many channels")
		return
	}
	result := []map[string]string{}
	for _, item := range body.Channels {
		id := existing[item.Name]
		if id == "" {
			id = missing[item.Name]
			if item.MimeType == "" {
				item.MimeType = "application/octet-stream"
			}
			s.newChannel(id, item.Name, item.MimeType, r.PathValue("runner_id"), r.PathValue("session_id")).identity = identity
			existing[item.Name] = id
		}
		ch := s.channels[id]
		address := s.registry.service + "/ai/trickle/" + id
		result = append(result, map[string]string{"name": ch.name, "channel_name": id, "url": address, "internal_url": s.registry.runnerServiceURL() + "/ai/trickle/" + id, "mime_type": ch.mime})
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": result})
}

func (s *Server) deleteChannels(w http.ResponseWriter, r *http.Request) {
	if !s.callbackAuthorized(r) {
		fail(w, http.StatusUnauthorized, "invalid session token")
		return
	}
	var body struct {
		Channels []string `json:"channels"`
	}
	if err := decode(r, &body); err != nil {
		fail(w, http.StatusBadRequest, "invalid channels request")
		return
	}
	deleted := []string{}
	s.mu.Lock()
	for _, name := range body.Channels {
		for id, ch := range s.channels {
			if ch.runnerID == r.PathValue("runner_id") && ch.sessionID == r.PathValue("session_id") && (id == name || ch.name == name) {
				s.closeChannel(id)
				deleted = append(deleted, name)
			}
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) String() string { return fmt.Sprint(s.registry) }
