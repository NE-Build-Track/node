package signer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
)

const authTokenValidPeriod = 30 * time.Minute

type signedState struct {
	State []byte `json:"state"`
	Sig   []byte `json:"sig"`
}
type paymentRequest struct {
	State        signedState `json:"state"`
	Orchestrator []byte      `json:"orchestrator"`
	ManifestID   string
	App          string    `json:"app,omitempty"`
	Type         string    `json:"type"`
	MaxPrice     *maxPrice `json:"maxPrice,omitempty"`
	Capabilities []byte    `json:"capabilities,omitempty"`
}
type paymentResponse struct {
	Payment  string      `json:"payment"`
	SegCreds string      `json:"segCreds,omitempty"`
	State    signedState `json:"state"`
}

type Service struct {
	authClient      *http.Client
	authURL         *url.URL
	authPolicy      string
	authHeaders     Headers
	key             pm.TicketSigner
	mux             *http.ServeMux
	discoveryURLs   []*url.URL
	discoveryClient *http.Client
	paymentChain    pm.PayerChain
	payerPolicy     pm.PayerPolicy
	pricePolicy     *pricePolicy
	events          signedTicketSink
	slots           chan struct{}
}

func (s *Service) SetPaymentChain(chain pm.PayerChain) { s.paymentChain = chain }

func (s *Service) SetPayerPolicy(policy pm.PayerPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	s.payerPolicy = policy
	return nil
}

func validateDiscoveryURL(endpoint *url.URL) error {
	if err := destination.ValidateURL(endpoint); err != nil {
		return errors.New("invalid signer discovery orchestrator URL")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("invalid signer discovery orchestrator URL")
	}
	return nil
}

// SetDiscovery configures discovery sources. Private destinations, including
// redirect targets, require an IP or IP:port grant for discovery.
func (s *Service) SetDiscovery(orchestrators []*url.URL, grants ...string) error {
	policy, err := destination.New("signer-discovery", grants)
	if err != nil {
		return err
	}
	urls := make([]*url.URL, 0, len(orchestrators))
	for _, candidate := range orchestrators {
		if err := validateDiscoveryURL(candidate); err != nil {
			return err
		}
		parsed := candidate.Clone()
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/discovery"
		urls = append(urls, parsed)
	}
	client := policy.Client()
	client.Timeout = 5 * time.Second
	previous := s.discoveryClient
	s.discoveryClient = client
	s.discoveryURLs = urls
	if previous != nil {
		previous.CloseIdleConnections()
	}
	return nil
}

func NewService(key *eth.Key) (*Service, error) {
	if key == nil {
		return nil, errors.New("signer key is required")
	}
	return newService(key), nil
}

func (s *Service) Close() error {
	if s.discoveryClient != nil {
		s.discoveryClient.CloseIdleConnections()
	}
	if s.authClient != nil {
		s.authClient.CloseIdleConnections()
	}
	return nil
}

func newService(key *eth.Key) *Service {
	s := &Service{key: key, mux: http.NewServeMux(), payerPolicy: pm.DefaultPayerPolicy(), slots: make(chan struct{}, 64)}
	s.mux.HandleFunc("POST /sign-orchestrator-info", s.signInfo)
	s.mux.HandleFunc("POST /generate-live-payment", s.generate)
	s.mux.HandleFunc("GET /discover-orchestrators", s.discover)
	return s
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	control := http.NewResponseController(w)
	_ = control.SetReadDeadline(time.Now().Add(5 * time.Second))
	deadline, _ := ctx.Deadline()
	_ = control.SetWriteDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = control.SetWriteDeadline(time.Now())
		close(done)
	})
	defer func() {
		if !stop() {
			<-done // Do not change deadlines after the connection is reused.
		}
	}()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		signerError(w, http.StatusServiceUnavailable, "signer busy")
		return
	}
	s.mux.ServeHTTP(w, r.WithContext(ctx))
}
func signerError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status == http.StatusInternalServerError {
		reason = "Internal Server Error"
	} else if status == http.StatusServiceUnavailable {
		reason = "Service Unavailable Error"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": reason}})
}

func (s *Service) signInfo(w http.ResponseWriter, r *http.Request) {
	address := s.key.Address().Hex()
	sig, err := s.key.SignMessage([]byte(address))
	if err != nil {
		slog.ErrorContext(r.Context(), "signer orchestrator info signing failed", "error", err)
		signerError(w, 500, "signing failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"address": address, "signature": eth.HexSignature(sig)})
}

type discoveredGPU struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	VRAMMB int    `json:"vram_mb,omitempty"`
}

type discoveredRunner struct {
	URL               string         `json:"url"`
	App               string         `json:"app"`
	Version           string         `json:"version,omitempty"`
	Metadata          string         `json:"metadata,omitempty"`
	GPU               *discoveredGPU `json:"gpu,omitempty"`
	Mode              string         `json:"mode"`
	Capacity          int            `json:"capacity"`
	CapacityUsed      int            `json:"capacity_used"`
	CapacityAvailable int            `json:"capacity_available"`
	PriceInfo         struct {
		Price    json.Number `json:"price"`
		PriceUSD json.Number `json:"price_usd,omitempty"`
		Currency string      `json:"currency"`
		Unit     string      `json:"unit"`
	} `json:"price_info"`
}
type discoveredOrchestrator struct {
	Address  string             `json:"address"`
	Score    float32            `json:"score"`
	Runners  []discoveredRunner `json:"runners"`
	LastSeen time.Time          `json:"last_seen"`
}

func (s *Service) discover(w http.ResponseWriter, r *http.Request) {
	if len(s.discoveryURLs) == 0 {
		slog.WarnContext(r.Context(), "signer discovery has no configured sources")
		signerError(w, http.StatusServiceUnavailable, "no orchestrator discovery source configured")
		return
	}
	appFilter := r.URL.Query()["app"]
	gpuFilter := r.URL.Query()["gpu"]
	available := false
	result := make([]discoveredOrchestrator, 0, len(s.discoveryURLs))
	for source, endpoint := range s.discoveryURLs {
		u := *endpoint
		query := u.Query()
		for _, app := range appFilter {
			query.Add("app", app)
		}
		for _, gpu := range gpuFilter {
			query.Add("gpu", gpu)
		}
		u.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
		if err != nil {
			slog.ErrorContext(r.Context(), "signer discovery request construction failed", "source_index", source, "error", err)
			continue
		}
		response, err := s.discoveryClient.Do(request)
		if err != nil {
			slog.WarnContext(r.Context(), "signer discovery source unavailable", "source_index", source, "error", err)
			continue
		}
		if response.StatusCode != 200 {
			slog.WarnContext(r.Context(), "signer discovery source returned non-200 HTTP status", "source_index", source, "http_status", response.StatusCode)
			_ = response.Body.Close()
			continue
		}
		var entries []struct {
			Address string             `json:"address"`
			Runners []discoveredRunner `json:"runners"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&entries)
		_ = response.Body.Close()
		if decodeErr != nil {
			slog.WarnContext(r.Context(), "signer discovery response decoding failed", "source_index", source, "error", decodeErr)
			continue
		}
		available = true
		for _, entry := range entries {
			if _, err := destination.ParseURL(entry.Address); err != nil {
				continue
			}
			filtered := make([]discoveredRunner, 0, len(entry.Runners))
			for _, runner := range entry.Runners {
				if runner.URL == "" || runner.App == "" {
					continue
				}
				if len(appFilter) > 0 {
					match := slices.Contains(appFilter, runner.App)
					if !match {
						continue
					}
				}
				if len(gpuFilter) > 0 {
					name := ""
					if runner.GPU != nil {
						name = strings.TrimSpace(runner.GPU.Name)
					}
					match := slices.Contains(gpuFilter, name)
					if !match {
						continue
					}
				}
				filtered = append(filtered, runner)
			}
			if len(filtered) > 0 {
				result = append(result, discoveredOrchestrator{Address: entry.Address, Score: 1, Runners: filtered, LastSeen: time.Now().UTC()})
			}
		}
	}
	if !available {
		signerError(w, http.StatusServiceUnavailable, "orchestrator discovery unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func randomStateID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// paymentFailure carries a client-safe message and HTTP status.
type paymentFailure struct {
	status int
	reason string
}

func (e paymentFailure) Error() string { return e.reason }
func invalid(reason string) error      { return paymentFailure{http.StatusBadRequest, reason} }

func checkMaxPrice(req paymentRequest, price wire.PriceInfo) error {
	if req.MaxPrice == nil {
		return nil
	}
	ceiling, ok := new(big.Rat).SetString(req.MaxPrice.Price.String())
	if !ok || ceiling.Sign() <= 0 || strings.ToLower(strings.TrimSpace(req.MaxPrice.Currency)) != "wei" {
		return invalid("maxPrice requires a positive wei price")
	}
	unit := map[string]string{"live": "seconds", "fixed": "fixed"}[req.Type]
	if strings.ToLower(strings.TrimSpace(req.MaxPrice.Unit)) != unit {
		return invalid("maxPrice unit does not match payment type")
	}
	actual := new(big.Rat).SetFrac64(price.PricePerUnit, price.UnitsPerPrice)
	if actual.Cmp(ceiling) > 0 {
		return paymentFailure{481, "orchestrator price exceeds maxPrice"}
	}
	return nil
}

func (s *Service) generate(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		signerError(w, 400, "payment request too large")
		return
	}
	var req paymentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		signerError(w, 400, "invalid payment request")
		return
	}
	if req.Type != "live" && req.Type != "fixed" {
		signerError(w, 400, "payment type must be live or fixed")
		return
	}
	if len(req.Capabilities) > 0 {
		signerError(w, 400, "payment capabilities are unsupported")
		return
	}
	info, err := wire.DecodeOrchestratorInfo(req.Orchestrator)
	if err != nil {
		signerError(w, 400, "invalid orchestrator Protobuf")
		return
	}
	// NB: Once go-livepeer is phased out, ensure OrchestratorInfo.Address matches info.TicketParams.Recipient
	address := ethcommon.BytesToAddress(info.TicketParams.Recipient)
	if address == (ethcommon.Address{}) || len(info.Auth.Token) == 0 || info.TicketParams.Expiration.CreationRound <= 0 || len(info.TicketParams.Expiration.CreationRoundBlockHash) != 32 || ethcommon.BytesToHash(info.TicketParams.Expiration.CreationRoundBlockHash) == (ethcommon.Hash{}) || len(info.TicketParams.Recipient) != 20 || info.Price.PricePerUnit <= 0 || info.Price.UnitsPerPrice <= 0 || len(info.TicketParams.RecipientRandHash) != 32 || info.Auth.SessionID == "" {
		signerError(w, 400, "incomplete or expired orchestrator payment params")
		return
	}
	authTokenExpireBuffer := 0.1
	refreshPoint := info.Auth.Expiration - int64(authTokenValidPeriod.Seconds()*authTokenExpireBuffer)
	if time.Now().After(time.Unix(refreshPoint, 0)) {
		w.Header().Set("Livepeer-Orchestrator-URL", info.Transcoder)
		signerError(w, 480, "refresh expired orchestrator authentication")
		return
	}
	if req.ManifestID == "" {
		req.ManifestID = info.Auth.SessionID
	}
	if req.ManifestID != info.Auth.SessionID {
		signerError(w, 400, "manifest ID does not match payment params")
		return
	}
	if err := checkMaxPrice(req, info.Price); err != nil {
		if f, ok := errors.AsType[paymentFailure](err); ok {
			signerError(w, f.status, f.reason)
		} else {
			signerError(w, 400, "invalid maxPrice")
		}
		return
	}
	var accountingRate *big.Rat
	if s.pricePolicy != nil {
		var err error
		accountingRate, err = s.pricePolicy.checkWithRate(req.Type, info.Price)
		if err != nil {
			f := err.(paymentFailure)
			signerError(w, f.status, f.reason)
			return
		}
	}
	var state paymentState
	oldSequence := int64(-1)
	if len(req.State.State) > 0 || len(req.State.Sig) > 0 {
		if !(pm.DefaultSigVerifier{}).Verify(s.key.Address(), req.State.State, req.State.Sig) {
			signerError(w, 400, "invalid state signature")
			return
		}
		if err := json.Unmarshal(req.State.State, &state); err != nil || state.StateID == "" || state.SequenceNumber > math.MaxInt64 {
			signerError(w, 400, "invalid payment state")
			return
		}
		if state.OrchestratorAddress != address || state.App != req.App || state.Type != req.Type || state.ManifestID != req.ManifestID {
			signerError(w, 400, "payment state scope mismatch")
			return
		}
		oldSequence = int64(state.SequenceNumber)
	} else {
		state = paymentState{StateID: randomStateID(), OrchestratorAddress: address, App: req.App, Type: req.Type, ManifestID: req.ManifestID, InitialPricePerUnit: info.Price.PricePerUnit, InitialPixelsPerUnit: info.Price.UnitsPerPrice}
	}
	if state.InitialPricePerUnit <= 0 || state.InitialPixelsPerUnit <= 0 || new(big.Rat).SetFrac64(info.Price.PricePerUnit, info.Price.UnitsPerPrice).Cmp(new(big.Rat).SetFrac64(state.InitialPricePerUnit, state.InitialPixelsPerUnit)) > 0 {
		signerError(w, 481, "orchestrator price exceeds initial session price")
		return
	}
	draft, err := s.preparePayment(r.Context(), req.Type, req.ManifestID, info, state, oldSequence, time.Now())
	if err != nil {
		switch {
		case errors.Is(err, pm.ErrRefreshRequired):
			w.Header().Set("Livepeer-Orchestrator-URL", info.Transcoder)
			signerError(w, 480, "refresh session for remote signer")
		case errors.Is(err, pm.ErrNoTickets):
			signerError(w, 482, "no tickets needed")
		default:
			if f, ok := errors.AsType[paymentFailure](err); ok {
				signerError(w, f.status, f.reason)
			} else {
				signerError(w, 500, "payment generation failed")
			}
		}
		return
	}
	if err := s.authorizePayment(r, req, info.Price, &draft.State); err != nil {
		if f, ok := errors.AsType[paymentFailure](err); ok {
			signerError(w, f.status, f.reason)
		} else {
			signerError(w, 500, "payment authorization failed")
		}
		return
	}
	signed, err := draft.Sign(r.Context(), s.key)
	if err != nil {
		signerError(w, 500, "payment generation failed")
		return
	}
	stateBytes, err := json.Marshal(draft.State)
	if err != nil {
		slog.ErrorContext(r.Context(), "signer payment state encoding failed", "error", err)
		signerError(w, 500, "payment state encoding failed")
		return
	}
	stateSig, err := s.key.SignMessage(stateBytes)
	if err != nil {
		slog.ErrorContext(r.Context(), "signer payment state signing failed", "error", err)
		signerError(w, 500, "payment state signing failed")
		return
	}
	response, err := json.Marshal(paymentResponse{Payment: signed.Payment, SegCreds: signed.SegCreds, State: signedState{State: stateBytes, Sig: stateSig}})
	if err != nil {
		slog.ErrorContext(r.Context(), "signer payment response encoding failed", "error", err)
		signerError(w, 500, "payment response encoding failed")
		return
	}
	if s.events != nil {
		event := newSignedTicketEvent(s.key.Address(), req, info, draft, accountingRate)
		if err := s.events.Enqueue(r.Context(), event); err != nil {
			if errors.Is(err, errEventTooLarge) {
				slog.WarnContext(r.Context(), "signer accounting event exceeds size limit; payment withheld", "error", err)
				signerError(w, http.StatusRequestEntityTooLarge, "signing event exceeds size limit")
			} else {
				slog.ErrorContext(r.Context(), "signer accounting event could not be persisted; payment withheld", "outbox_full", errors.Is(err, errOutboxFull), "error", err)
				signerError(w, http.StatusServiceUnavailable, "signer accounting outbox unavailable or full")
			}
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(response)
}
