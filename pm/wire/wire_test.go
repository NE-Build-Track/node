package wire

import "testing"

func FuzzDecodeEnvelopes(f *testing.F) {
	f.Add([]byte{0x0a, 0x01, 0x78})
	f.Add([]byte{0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeOrchestratorInfo(data)
		_, _ = DecodePayment(data)
		_, _ = DecodeSegData(data)
	})
}
