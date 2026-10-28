package signer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/orchestrator"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func TestPaymentEncoding(t *testing.T) {
	data, err := os.ReadFile("testdata/payment-before-authorization.json")
	require.NoError(t, err)
	var fixtures map[string]struct{ Payment, SegCreds string }
	require.NoError(t, json.Unmarshal(data, &fixtures))
	keyFile, passwordFile := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(keyFile, passwordFile)
	require.NoError(t, err)
	s, info := testService(t)
	s.key = key
	info.Auth.Expiration = 1900000000
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, paymentState{}, -1, time.Now())
			require.NoError(t, err)
			signed, err := draft.Sign(t.Context(), key, nil)
			require.NoError(t, err)
			require.Equal(t, fixtures[kind].Payment, signed.Payment)
			require.Equal(t, fixtures[kind].SegCreds, signed.SegCreds)
		})
	}
}

func TestDraftPaymentPreservesValues(t *testing.T) {
	s, original := testService(t)
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			// Decode a separate envelope so mutation cannot affect another case.
			info, err := wire.DecodeOrchestratorInfo(wire.EncodeOrchestratorInfo(original))
			require.NoError(t, err)
			// Retain noncanonical integer bytes rather than re-encoding big.Ints.
			info.TicketParams.FaceValue = append([]byte{0}, info.TicketParams.FaceValue...)
			now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			state := paymentState{PMSessionID: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash).Hex(), TicketNonce: 41, Balance: "1/3", LastUpdate: now.Add(-1500 * time.Millisecond), SequenceNumber: 7}
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, state, 7, now)
			require.NoError(t, err)
			require.Equal(t, now, draft.State.LastUpdate)
			require.Equal(t, uint64(8), draft.State.SequenceNumber)
			fee := "10"
			count := 1
			if kind == "live" {
				fee = "20"
				count = 2
			}
			require.Equal(t, fee, draft.Usage.Fee.RatString())
			require.Equal(t, count, draft.Usage.NumTickets)
			require.Equal(t, uint32(41+count), draft.State.TicketNonce)
			paymentBefore := wire.EncodePayment(draft.payment)
			segmentBefore := wire.EncodeSegData(draft.segment)
			signatureMessage := append([]byte(info.Auth.SessionID), make([]byte, 32)...)
			signatureMessage = append(signatureMessage, crypto.Keccak256(nil)...)
			for _, value := range [][]byte{info.TicketParams.Recipient, info.TicketParams.FaceValue, info.TicketParams.WinProb, info.TicketParams.RecipientRandHash, info.TicketParams.Seed, info.TicketParams.ExpirationBlock, info.TicketParams.Expiration.CreationRoundBlockHash, info.Auth.Token} {
				value[0] ^= 0xff
			}
			info.Price.PricePerUnit = 999
			info.Auth.Expiration = 0
			signed, err := draft.Sign(t.Context(), s.key, nil)
			require.NoError(t, err)
			paymentBytes, err := base64.StdEncoding.DecodeString(signed.Payment)
			require.NoError(t, err)
			payment, err := wire.DecodePayment(paymentBytes)
			require.NoError(t, err)
			require.Equal(t, draft.Usage.NumTickets, len(payment.PayerParams))
			require.Equal(t, draft.State.TicketNonce, payment.PayerParams[len(payment.PayerParams)-1].TicketNonce)
			payment.PayerParams = nil
			require.Equal(t, paymentBefore, wire.EncodePayment(payment))
			segmentBytes, err := base64.StdEncoding.DecodeString(signed.SegCreds)
			require.NoError(t, err)
			segment, err := wire.DecodeSegData(segmentBytes)
			require.NoError(t, err)
			require.True(t, (pm.DefaultSigVerifier{}).Verify(s.key.Address(), crypto.Keccak256(signatureMessage), segment.Signature))
			segment.Signature = nil
			require.Equal(t, segmentBefore, wire.EncodeSegData(segment))
		})
	}
}

type collateralChain struct{ withdraw *big.Int }

func (c collateralChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(50), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, nil
}
func (c collateralChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return true, nil
}
func (c collateralChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	return pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(10), Reserve: big.NewInt(10), WithdrawRound: c.withdraw}, err
}

func TestRecipientReservesWinningLiabilityAcrossConcurrentSessions(t *testing.T) {
	key, _, _ := testSignerKey(t)
	store, err := orchestrator.OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	chain := collateralChain{new(big.Int)}
	engine, err := orchestrator.NewPaymentEngine(store, chain, ethcommon.HexToAddress("0x1234"), big.NewInt(10), new(big.Int).Sub(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), big.NewInt(1)))
	require.NoError(t, err)
	s := newService(key)
	s.SetPaymentChain(chain)
	payments := map[string]signedPayment{}
	for _, manifest := range []string{"one", "two", "three"} {
		_, err := engine.MakeChallenge(t.Context(), "runner", manifest, key.Address(), 9, "fixed", "https://orch.example")
		require.NoError(t, err)
		info, err := engine.ChallengeInfo(manifest)
		require.NoError(t, err)
		draft, err := s.preparePayment(t.Context(), "fixed", manifest, info, paymentState{}, -1, time.Now())
		require.NoError(t, err)
		payment, err := draft.Sign(t.Context(), key, nil)
		require.NoError(t, err)
		payments[manifest] = payment
	}
	var workers sync.WaitGroup
	errors := make(chan error, 3)
	for manifest, payment := range payments {
		workers.Go(func() {
			_, _, err := engine.Receive(t.Context(), "runner", manifest, payment.Payment, payment.SegCreds)
			errors <- err
		})
	}
	workers.Wait()
	close(errors)
	accepted, rejected := 0, 0
	for err := range errors {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, pm.ErrPayerUnavailable)
			rejected++
		}
	}
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, rejected)
	count, err := store.WinningTicketCount(key.Address(), 0)
	require.NoError(t, err)
	require.Equal(t, 2, count, "rejected batches must roll back tickets and credit")
}
