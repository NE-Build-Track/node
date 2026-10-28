package signer

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm/wire"
)

// pricePolicy holds only a local observation of the exchange rate. Signer
// replicas use the same configured limits and feed, without sharing state.
type pricePolicy struct {
	metrics                 *observability.Metrics
	mu                      sync.RWMutex
	maxLiveUSD, maxFixedUSD *big.Rat
	weiPerUSD               *big.Rat
	validUntil              time.Time
}

func newPricePolicy(hourlyUSD, fixedUSD *big.Rat) (*pricePolicy, error) {
	if hourlyUSD == nil || hourlyUSD.Sign() <= 0 {
		return nil, errors.New("max-hourly-price must be positive")
	}
	if fixedUSD == nil || fixedUSD.Sign() <= 0 {
		return nil, errors.New("max-fixed-price must be positive")
	}
	return &pricePolicy{
		maxLiveUSD:  new(big.Rat).Quo(hourlyUSD, big.NewRat(3600, 1)),
		maxFixedUSD: new(big.Rat).Set(fixedUSD),
	}, nil
}

func (p *pricePolicy) setRate(rate *big.Rat, until time.Time) error {
	if rate == nil || rate.Sign() <= 0 || (!until.IsZero() && !time.Now().Before(until)) {
		return errors.New("positive current wei-per-usd rate is required")
	}
	p.mu.Lock()
	p.weiPerUSD = new(big.Rat).Set(rate)
	p.validUntil = until
	p.mu.Unlock()
	return nil
}

func (p *pricePolicy) rate() (*big.Rat, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.weiPerUSD == nil || (!p.validUntil.IsZero() && !time.Now().Before(p.validUntil)) {
		return nil, false
	}
	return new(big.Rat).Set(p.weiPerUSD), true
}

func (p *pricePolicy) ready() bool {
	_, ok := p.rate()
	return ok
}

// Return the observation used for validation so accounting cannot race a feed refresh.
func (p *pricePolicy) checkWithRate(paymentType string, price wire.PriceInfo) (*big.Rat, error) {
	rate, ok := p.rate()
	if !ok {
		return nil, serviceFailure(503, "signer USD conversion rate unavailable")
	}
	ceiling := p.maxLiveUSD
	if paymentType == "fixed" {
		ceiling = p.maxFixedUSD
	}
	actual := new(big.Rat).SetFrac64(price.PricePerUnit, price.UnitsPerPrice)
	if actual.Cmp(new(big.Rat).Mul(ceiling, rate)) > 0 {
		return nil, paymentFailure{481, "orchestrator price exceeds signer USD maximum"}
	}
	return rate, nil
}

func refreshPriceFeed(ctx context.Context, period time.Duration, policy *pricePolicy, read func(context.Context) (*big.Rat, time.Time, error)) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = policy.refreshFeed(ctx, read)
		}
	}
}

// Startup and scheduled reads each publish exactly one feed observation.
func (p *pricePolicy) refreshFeed(ctx context.Context, read func(context.Context) (*big.Rat, time.Time, error)) error {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rate, until, err := read(readCtx)
	if err == nil {
		err = p.setRate(rate, until)
	}
	if err == nil {
		p.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "success"})
	} else {
		p.metrics.Add("price_feed_updates_total", 1, observability.Labels{"outcome": "error"})
		slog.With("component", "signer", "event", "price_feed", "operation", "refresh").Error("signer price feed unavailable; check --eth-usd-feed, --eth-usd-max-age and RPC configuration", "outcome", "error", "error_kind", observability.ErrorKind(err))
	}
	return err
}
