package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
)

func paymentPrice(price priceInfo) (int64, bool) {
	if price.Price == "" || price.Price == "0" {
		return 0, false
	}
	value, err := price.Price.Int64()
	return value, err == nil && value > 0
}

func (s *Server) challenge(w http.ResponseWriter, r *http.Request, runnerID string, price priceInfo) {
	if s.payment == nil {
		fail(w, http.StatusServiceUnavailable, "on-chain payment is unavailable")
		return
	}
	payerRaw := r.Header.Get("Livepeer-Payer-Address")
	if !eth.ValidAddress(payerRaw) {
		fail(w, http.StatusPaymentRequired, "Livepeer-Payer-Address is required")
		return
	}
	manifest, err := randomIDWithPrefix("session_", 10)
	if err != nil {
		fail(w, 500, "cannot create payment challenge")
		return
	}
	wei, _ := paymentPrice(price)
	challenge, err := s.payment.MakeChallenge(r.Context(), runnerID, manifest, ethcommon.HexToAddress(payerRaw), wei, price.Unit, s.registry.service)
	if err != nil {
		observability.Outcome(r, paymentAppOutcome(err))
		s.logger.Warn("payment challenge failed", "event", "payment_challenge", "operation", "challenge", "runner_id", runnerID, "outcome", paymentOutcome(err), "error_kind", observability.ErrorKind(err))
		fail(w, 502, "payment challenge unavailable")
		return
	}
	writeJSON(w, http.StatusPaymentRequired, challenge)
}

func (s *Server) paidReceipt(w http.ResponseWriter, r *http.Request, runnerID string, price priceInfo) (string, bool) {
	manifest, err := ManifestFromSegment(r.Header.Get("Livepeer-Segment"))
	if err != nil {
		fail(w, http.StatusPaymentRequired, "invalid segment credentials")
		return "", false
	}
	info, err := s.payment.ChallengeInfo(manifest)
	wei, _ := paymentPrice(price)
	if err != nil || info.Price.PricePerUnit != wei || info.Price.UnitsPerPrice != 1 {
		fail(w, http.StatusConflict, "runner price changed; request a new payment challenge")
		return "", false
	}
	if _, _, err := s.payment.Receive(r.Context(), runnerID, manifest, r.Header.Get("Livepeer-Payment"), r.Header.Get("Livepeer-Segment")); err != nil {
		observability.Outcome(r, paymentAppOutcome(err))
		status := http.StatusBadRequest
		if errors.Is(err, ErrMissingChallenge) {
			status = http.StatusNotFound
		}
		if errors.Is(err, ErrInvalidPayment) {
			status = http.StatusForbidden
		}
		fail(w, status, err.Error())
		return "", false
	}
	return manifest, true
}

func (s *Server) reservePaid(w http.ResponseWriter, r *http.Request) bool {
	runnerID := r.PathValue("runner_id")
	mode, status, err := s.registry.ModeForRunner(runnerID)
	if err != nil {
		fail(w, status, err.Error())
		return true
	}
	if mode != "persistent" {
		fail(w, http.StatusBadRequest, "runner is not persistent")
		return true
	}
	price, status, err := s.reservationPrice(r, runnerID)
	if err != nil {
		fail(w, status, err.Error())
		return true
	}
	if _, paid := paymentPrice(price); !paid {
		return false
	}
	if r.Header.Get("Livepeer-Payment") == "" || r.Header.Get("Livepeer-Segment") == "" {
		s.challenge(w, r, runnerID, price)
		return true
	}
	manifest, err := ManifestFromSegment(r.Header.Get("Livepeer-Segment"))
	if err != nil {
		fail(w, 403, "invalid segment credentials")
		return true
	}
	id, appURL, controlURL, status, err := s.registry.reserveWithPrice(runnerID, manifest, &price)
	if err != nil {
		failReservation(w, r, status, err)
		return true
	}
	if _, ok := s.paidReceipt(w, r, runnerID, price); !ok {
		s.registry.ReleaseBySession(id)
		return true
	}
	if err := s.payment.Charge(r.Context(), id, time.Now()); err != nil {
		observability.Outcome(r, paymentAppOutcome(err))
		s.registry.ReleaseBySession(id)
		fail(w, 402, "insufficient payment balance")
		return true
	}
	if !s.registry.activateSession(runnerID, id) {
		fail(w, 404, "session ended during payment")
		return true
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": id, "app_url": appURL, "control_url": controlURL})
	return true
}

func (s *Server) proxyPaidSingleShot(w http.ResponseWriter, r *http.Request) bool {
	runnerID := r.PathValue("runner_id")
	mode, status, err := s.registry.ModeForRunner(runnerID)
	if err != nil {
		fail(w, status, err.Error())
		return true
	}
	if mode != "single-shot" {
		fail(w, http.StatusBadRequest, "runner is not single-shot")
		return true
	}
	price, status, err := s.reservationPrice(r, runnerID)
	if err != nil {
		fail(w, status, err.Error())
		return true
	}
	if _, paid := paymentPrice(price); !paid {
		return false
	}
	if r.Header.Get("Livepeer-Payment") == "" || r.Header.Get("Livepeer-Segment") == "" {
		s.challenge(w, r, runnerID, price)
		return true
	}
	manifest, err := ManifestFromSegment(r.Header.Get("Livepeer-Segment"))
	if err != nil {
		fail(w, 403, "invalid segment credentials")
		return true
	}
	id, _, _, status, err := s.registry.reserveWithPrice(runnerID, manifest, &price)
	if err != nil {
		failReservation(w, r, status, err)
		return true
	}
	defer s.registry.ReleaseBySession(id, "completed")
	if _, ok := s.paidReceipt(w, r, runnerID, price); !ok {
		return true
	}
	if err := s.payment.Charge(r.Context(), id, time.Now()); err != nil {
		observability.Outcome(r, paymentAppOutcome(err))
		fail(w, 402, "insufficient payment balance")
		return true
	}
	if !s.registry.activateSession(runnerID, id) {
		fail(w, 404, "session ended during payment")
		return true
	}
	target, token, status, err := s.registry.sessionTarget(runnerID, id)
	if err != nil {
		fail(w, status, err.Error())
		return true
	}
	control := s.registry.runnerServiceURL() + "/runner/" + url.PathEscape(runnerID) + "/session/" + url.PathEscape(id)
	s.proxy(w, r, s.runnerTransport, target, r.PathValue("app_path"), runnerID, id, token, control)
	return true
}

func (s *Server) sessionPayment(w http.ResponseWriter, r *http.Request) {
	if s.payment == nil {
		fail(w, 404, "payment unavailable")
		return
	}
	runnerID, manifest := r.PathValue("runner_id"), r.PathValue("session_id")
	price, status, err := s.registry.PriceForSession(runnerID, manifest)
	if err != nil {
		fail(w, status, err.Error())
		return
	}
	if price.Unit == "fixed" {
		fail(w, http.StatusConflict, "fixed-price sessions do not accept follow-up payments")
		return
	}
	if _, ok := s.paidReceipt(w, r, runnerID, price); !ok {
		return
	}
	challenge, err := s.payment.ChallengeForManifest(runnerID, manifest, s.registry.service)
	if err != nil {
		fail(w, 500, "payment params unavailable")
		return
	}
	writeJSON(w, http.StatusOK, challenge)
}

func (s *Server) refreshPayment(w http.ResponseWriter, r *http.Request) {
	if s.payment == nil {
		fail(w, 404, "payment unavailable")
		return
	}
	var request struct {
		PayerAddress string `json:"sender"`
		ManifestID   string `json:"manifest_id"`
	}
	if err := decode(r, &request); err != nil || !eth.ValidAddress(request.PayerAddress) || !validRouteID(request.ManifestID) {
		fail(w, 400, "invalid refresh request")
		return
	}
	challenge, err := s.payment.RefreshChallenge(r.Context(), request.ManifestID, ethcommon.HexToAddress(request.PayerAddress), s.registry.service)
	if err != nil {
		fail(w, 404, "payment challenge not found")
		return
	}
	writeJSON(w, http.StatusOK, challenge)
}

func (s *Server) ChargePaidSessions(ctx context.Context) {
	if s.payment == nil {
		return
	}
	for _, id := range s.registry.PaidSessions() {
		if err := s.payment.Charge(ctx, id, time.Now()); err != nil && !errors.Is(err, ErrFixedAlreadyCharged) {
			s.logger.Warn("paid session ended", "event", "payment_billing", "operation", "charge", "session_id", id, "outcome", paymentOutcome(err), "error_kind", observability.ErrorKind(err))
			reason := "billing_error"
			if errors.Is(err, ErrInsufficientBalance) {
				reason = "insufficient_balance"
			}
			s.registry.ReleaseBySession(id, reason)
		}
	}
}

func (s *Server) reservationPrice(r *http.Request, runnerID string) (priceInfo, int, error) {
	if s.payment != nil && r.Header.Get("Livepeer-Payment") != "" && r.Header.Get("Livepeer-Segment") != "" {
		manifest, err := ManifestFromSegment(r.Header.Get("Livepeer-Segment"))
		if err != nil {
			return priceInfo{}, 403, err
		}
		price, unit, err := s.payment.ChallengePrice(runnerID, manifest)
		if err != nil {
			return priceInfo{}, 403, ErrMissingChallenge
		}
		return priceInfo{Price: json.Number(strconv.FormatInt(price, 10)), Currency: "wei", Unit: unit}, 200, nil
	}
	return s.registry.PriceForRunner(runnerID)
}
