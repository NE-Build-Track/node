// Ported from livepeer/go-livepeer at 0ff08af1bb4cb268f428d619cc968b5eaee0d093.
// Original implementation by Yondon Fu and Rafal Leszko; MIT notice in LICENSE.
package eth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

// The upstream tests use testify/mock; this fixed reader avoids adding its
// separate objx dependency. The table retains the upstream nonce assertions.
type mockRemoteNonceReader struct {
	nonce uint64
	err   error
}

func (m *mockRemoteNonceReader) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	return m.nonce, m.err
}

func TestNonceManagerNextAndUpdate(t *testing.T) {
	for _, tc := range []struct {
		name             string
		remote           uint64
		last             uint64
		update           bool
		readBeforeUpdate bool
		want             uint64
		err              error
	}{
		{name: "RPC error", err: errors.New("PendingNonceAt error")},
		{name: "zero"},
		{name: "increment local", update: true, readBeforeUpdate: true, want: 1},
		{name: "remote ahead", remote: 10, want: 10},
		{name: "last equals local", update: true, want: 1},
		{name: "last ahead of local", update: true, last: 10, want: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nm := NewNonceManager(&mockRemoteNonceReader{nonce: tc.remote, err: tc.err})
			addr := randomNonceAddress()
			if tc.update {
				// Preserve both upstream paths: Update after Next, and Update
				// before any nonce has been read for the account.
				if tc.readBeforeUpdate {
					nonce, err := nm.Next(addr)
					require.NoError(t, err)
					require.Zero(t, nonce)
				}
				nm.Update(addr, tc.last)
			}
			nonce, err := nm.Next(addr)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, nonce)
		})
	}
}

func TestNonceManagerConcurrent(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			nm := NewNonceManager(&mockRemoteNonceReader{})
			var wg sync.WaitGroup
			var errCount atomic.Uint64
			var usedMu sync.Mutex
			used := map[uint64]bool{}
			var addrs [50]common.Address
			sharedAddr := randomNonceAddress()
			for i := range addrs {
				addrs[i] = sharedAddr
				if !shared {
					addrs[i] = randomNonceAddress()
				}
				wg.Add(1)
				go func(addr common.Address) {
					defer wg.Done()
					nm.Lock(addr)
					defer nm.Unlock(addr)
					nonce, err := nm.Next(addr)
					if err != nil {
						errCount.Add(1)
						return
					}
					if shared {
						usedMu.Lock()
						used[nonce] = true
						usedMu.Unlock()
					}
					nm.Update(addr, nonce)
				}(addrs[i])
			}
			wg.Wait()
			require.Zero(t, errCount.Load())
			for _, addr := range addrs {
				nonce, err := nm.Next(addr)
				require.NoError(t, err)
				want := uint64(1)
				if shared {
					want = 50
				}
				require.Equal(t, want, nonce)
			}
			if shared {
				require.Len(t, used, 50)
				for i := uint64(0); i < 50; i++ {
					require.True(t, used[i])
				}
			}
		})
	}
}

func randomNonceAddress() common.Address { var a common.Address; _, _ = rand.Read(a[:]); return a }
