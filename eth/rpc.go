package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/node/destination"
)

const maxRPCResponse = 1 << 20

// RPC applies bounded HTTP transport and error redaction to geth's client.
type RPC struct {
	ethereum *ethclient.Client
	http     *http.Client
	observer func(string, time.Duration, error)
}

// SetObserver installs a hook before the client is used. Observations contain
// allowlisted methods and redacted errors, never RPC payloads or URLs.
func (r *RPC) SetObserver(observer func(string, time.Duration, error)) { r.observer = observer }

func OpenRPC(endpoint string) (*RPC, error) {
	u, err := destination.ParseURL(endpoint)
	if err != nil {
		return nil, errors.New("invalid Ethereum RPC URL")
	}
	return NewRPC(u, nil)
}

// NewRPC opens a bounded RPC client using the supplied transport. A nil
// transport uses the system HTTP transport, including normal TLS verification.
func NewRPC(endpoint *url.URL, transport http.RoundTripper) (*RPC, error) {
	if err := destination.ValidateURL(endpoint); err != nil {
		return nil, errors.New("invalid Ethereum RPC URL")
	}
	if transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	r := new(RPC)
	client := &http.Client{Transport: boundedRPCTransport{transport, r}, Timeout: 15 * time.Second}
	// Redirects can replay eth_sendRawTransaction POST bodies.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	rpc, err := gethrpc.DialOptions(context.Background(), endpoint.String(), gethrpc.WithHTTPClient(client))
	if err != nil {
		return nil, errors.New("invalid Ethereum RPC endpoint")
	}
	r.ethereum, r.http = ethclient.NewClient(rpc), client
	return r, nil
}

func (r *RPC) Close() {
	r.ethereum.Close()
	r.http.CloseIdleConnections()
}

var errRPCResponseLimit = errors.New("ethereum RPC response exceeds 1 MiB")

// Bound the HTTP body before geth decodes it or includes it in an HTTP error.
type boundedRPCTransport struct {
	http.RoundTripper
	rpc *RPC
}

func (t boundedRPCTransport) RoundTrip(request *http.Request) (response *http.Response, err error) {
	observer := t.rpc.observer
	var observed error
	if observer != nil {
		start, method := time.Now(), "unknown"
		if request.GetBody != nil {
			if body, err := request.GetBody(); err == nil {
				var envelope struct {
					Method string `json:"method"`
				}
				if json.NewDecoder(body).Decode(&envelope) == nil {
					switch envelope.Method {
					case "eth_chainId", "eth_call", "eth_getCode", "eth_blockNumber", "eth_getBalance", "eth_getTransactionCount", "eth_getTransactionReceipt", "eth_getTransactionByHash", "eth_getBlockByNumber", "eth_estimateGas", "eth_sendRawTransaction", "eth_maxPriorityFeePerGas", "eth_gasPrice":
						method = envelope.Method
					}
				}
				_ = body.Close()
			}
		}
		defer func() {
			if err != nil {
				observed = err
			}
			observer(method, time.Since(start), safeRPCError(observed))
		}()
	}
	response, err = t.RoundTripper.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxRPCResponse+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRPCResponse {
		return nil, errRPCResponseLimit
	}
	if observer != nil {
		var envelope struct {
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 || json.Unmarshal(data, &envelope) != nil || envelope.Error != nil {
			observed = errors.New("ethereum RPC request failed")
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}

func (t boundedRPCTransport) CloseIdleConnections() {
	if closer, ok := t.RoundTripper.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// geth errors may include an authenticated endpoint or server-supplied text.
func safeRPCError(err error) error {
	if err == nil {
		return nil
	}
	for _, safe := range []error{errRPCResponseLimit, context.Canceled, context.DeadlineExceeded, ethereum.NotFound} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	var httpError gethrpc.HTTPError
	if errors.As(err, &httpError) {
		return fmt.Errorf("ethereum RPC returned HTTP %d", httpError.StatusCode)
	}
	var rpcError gethrpc.Error
	if errors.As(err, &rpcError) {
		// Preserve only upstream's fixed terminal ticket classification. Arbitrary
		// provider text (including endpoints or calldata) must remain redacted.
		if strings.Contains(rpcError.Error(), "ticket creationRound does not have a block hash") {
			return ErrTicketCreationRound
		}
		return fmt.Errorf("ethereum RPC error %d", rpcError.ErrorCode())
	}
	return errors.New("ethereum RPC request failed")
}

var ErrTicketCreationRound = errors.New("ticket creationRound does not have a block hash")

func (r *RPC) TransactionByHash(ctx context.Context, hash ethcommon.Hash) (*types.Transaction, bool, error) {
	tx, pending, err := r.ethereum.TransactionByHash(ctx, hash)
	return tx, pending, safeRPCError(err)
}

func ValidAddress(address string) bool {
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return false
	}
	return ethcommon.IsHexAddress(address)
}

// ChainID uses ethclient's standard call and quantity decoding.
func (r *RPC) ChainID(ctx context.Context) (*big.Int, error) {
	id, err := r.ethereum.ChainID(ctx)
	return id, safeRPCError(err)
}

func (r *RPC) CodeAt(ctx context.Context, address ethcommon.Address, number *big.Int) ([]byte, error) {
	code, err := r.ethereum.CodeAt(ctx, address, number)
	return code, safeRPCError(err)
}

func (r *RPC) BlockNumber(ctx context.Context) (uint64, error) {
	number, err := r.ethereum.BlockNumber(ctx)
	return number, safeRPCError(err)
}

func (r *RPC) BalanceAt(ctx context.Context, address ethcommon.Address, number *big.Int) (*big.Int, error) {
	balance, err := r.ethereum.BalanceAt(ctx, address, number)
	return balance, safeRPCError(err)
}

func (r *RPC) PendingNonceAt(ctx context.Context, address ethcommon.Address) (uint64, error) {
	nonce, err := r.ethereum.PendingNonceAt(ctx, address)
	return nonce, safeRPCError(err)
}

func (r *RPC) TransactionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	// Geth leaves Status unchanged when omitted/null; seed an invalid value.
	receipt := &types.Receipt{Status: 2}
	if err := r.ethereum.Client().CallContext(ctx, &receipt, "eth_getTransactionReceipt", hash); err != nil {
		return nil, safeRPCError(err)
	}
	if receipt == nil {
		return nil, ethereum.NotFound
	}
	if receipt.Status > types.ReceiptStatusSuccessful || receipt.TxHash != hash || receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() || receipt.BlockHash == (ethcommon.Hash{}) {
		return nil, errors.New("invalid transaction receipt")
	}
	return receipt, nil
}

// miniHeader retains the RPC hash and Arbitrum's L1 clock, which geth's Header
// does not expose. All quantities use geth's JSON decoders.
type miniHeader struct {
	Number        *hexutil.Big   `json:"number"`
	BaseFee       *hexutil.Big   `json:"baseFeePerGas"`
	L1BlockNumber *hexutil.Big   `json:"l1BlockNumber"`
	Hash          ethcommon.Hash `json:"hash"`
}

func (r *RPC) header(ctx context.Context, block any) (*miniHeader, error) {
	var header *miniHeader
	if err := r.ethereum.Client().CallContext(ctx, &header, "eth_getBlockByNumber", block, false); err != nil {
		return nil, safeRPCError(err)
	}
	if header == nil {
		return nil, ethereum.NotFound
	}
	return header, nil
}

// CheckChainID validates the RPC chain ID, optionally pinning it to expected.
func (r *RPC) CheckChainID(ctx context.Context, expected *big.Int) error {
	id, err := r.ChainID(ctx)
	if err != nil {
		return err
	}
	if expected != nil && id.Cmp(expected) != 0 {
		return errors.New("RPC chain ID does not match configured chain-id")
	}
	return nil
}
