package signer

import (
	"context"
	"math/big"
	"strconv"
	"time"
	"uuid"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/pm/wire"
)

type signedTicketSink interface {
	Enqueue(context.Context, signingEvent) error
}

// signingEvent is the create_signed_ticket Kafka envelope.
type signingEvent struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Timestamp string            `json:"timestamp"`
	Gateway   string            `json:"gateway"`
	Data      signedTicketEvent `json:"data"`
}

type signedTicketEvent struct {
	SessionID        string    `json:"session_id"`
	SessionStatus    string    `json:"session_status"`
	App              string    `json:"app"`
	Pipeline         string    `json:"pipeline"`
	RequestID        string    `json:"request_id"`
	PayerAddress     string    `json:"payer_address"`
	OrchAddress      string    `json:"orch_address"`
	OrchURL          string    `json:"orch_url"`
	ManifestID       string    `json:"manifest_id"`
	PMSessionID      string    `json:"pm_session_id"`
	CurrentTime      time.Time `json:"current_time"`
	CurrentTimeUnix  int64     `json:"current_time_unix"`
	PreviousTime     time.Time `json:"previous_time"`
	PreviousTimeUnix int64     `json:"previous_time_unix"`
	BillableSecs     float64   `json:"billable_secs"`
	Pixels           int64     `json:"pixels"`
	SessionBalance   string    `json:"session_balance"`
	ComputedFee      string    `json:"computed_fee"`
	ComputedFeeUSD   *string   `json:"computed_fee_usd"`
	Cost             string    `json:"cost"`
	SequenceNumber   uint64    `json:"sequence_number"`
	NumTickets       int       `json:"num_tickets"`
	AuthID           string    `json:"auth_id"`
}

func newSignedTicketEvent(payer ethcommon.Address, req paymentRequest, info wire.OrchestratorInfo, draft draftPayment) signingEvent {
	state, usage := draft.State, draft.Usage
	status := "continuing"
	if state.SequenceNumber == 0 {
		status = "new"
	}
	return signingEvent{
		ID: uuid.NewV4().String(), Type: "create_signed_ticket", Timestamp: strconv.FormatInt(time.Now().UnixMilli(), 10), Gateway: "",
		Data: signedTicketEvent{
			SessionID: state.StateID, SessionStatus: status, App: state.App, Pipeline: req.Type, RequestID: randomStateID(),
			PayerAddress: payer.Hex(), OrchAddress: state.OrchestratorAddress.Hex(), OrchURL: info.Transcoder,
			ManifestID: state.ManifestID, PMSessionID: state.PMSessionID, CurrentTime: state.LastUpdate.UTC(), CurrentTimeUnix: state.LastUpdate.UnixMilli(),
			PreviousTime: usage.PreviousTime.UTC(), PreviousTimeUnix: usage.PreviousTime.UnixMilli(), BillableSecs: usage.BillableSecs,
			SessionBalance: usage.Balance.FloatString(0), ComputedFee: state.FeeWei, ComputedFeeUSD: state.FeeUSD,
			Cost:           new(big.Rat).SetFrac64(info.Price.PricePerUnit, info.Price.UnitsPerPrice).FloatString(10),
			SequenceNumber: state.SequenceNumber, NumTickets: usage.NumTickets, AuthID: state.AuthID,
		},
	}
}
