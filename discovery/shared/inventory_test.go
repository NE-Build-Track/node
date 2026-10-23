package shared

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type record struct {
	URL      string `json:"url"`
	App      string `json:"app"`
	Observed string `json:"observed_at"`
	Capacity int    `json:"capacity"`
}

func (r record) DiscoveryIdentity() (string, string, string) { return r.URL, r.App, r.Observed }
func candidate(stamp string) string {
	return fmt.Sprintf(`{"url":"https://runner.example/app","app":"test","observed_at":%q}`, stamp)
}
func envelope(records string) string {
	return `[{"address":"https://orch.example","runners":[` + records + `]}]`
}

func TestDecodeValidation(t *testing.T) {
	now := time.Date(2026, 10, 22, 18, 0, 0, 0, time.UTC)
	valid := candidate(now.Format(time.RFC3339Nano))
	for _, test := range []struct {
		name, body string
		count      int
		failed     bool
	}{
		{"empty", "[]", 0, false}, {"omitted", `[{"address":"https://orch.example"}]`, 0, false},
		{"node metadata only", `[{"nodes":[]}]`, 0, false},
		{"node metadata and runners", `[{"nodes":[]},` + strings.TrimPrefix(envelope(valid), "["), 1, false},
		{"invalid node metadata", `[{"nodes":{}}]`, 0, true},
		{"metadata cannot hide invalid runners", `[{"nodes":[]},` + strings.TrimPrefix(envelope(`{}`), "["), 0, true},
		{"mixed", envelope(valid + `,null,{"url":9},` + candidate("broken")), 1, false},
		{"all invalid", envelope(candidate("broken") + `,{}`), 0, true},
		{"missing timestamp", envelope(`{"url":"https://r.example","app":"test"}`), 0, true},
		{"wrong timestamp type", envelope(`{"url":"https://r.example","app":"test","observed_at":123}`), 0, true},
		{"bad address", strings.ReplaceAll(envelope(valid), "https://orch.example", "file:///bad"), 0, true},
		{"wrong runner list", `[{"address":"https://orch.example","runners":{}}]`, 0, true},
		{"trailing JSON", envelope(valid) + `[]`, 0, true}, {"null", "null", 0, true},
		{"before skew", envelope(candidate(now.Add(ClockSkew - time.Nanosecond).Format(time.RFC3339Nano))), 1, false},
		{"at skew", envelope(candidate(now.Add(ClockSkew).Format(time.RFC3339Nano))), 1, false},
		{"after skew", envelope(candidate(now.Add(ClockSkew + time.Nanosecond).Format(time.RFC3339Nano))), 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries, err := Decode[record]([]byte(test.body), now)
			if test.failed {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			count := 0
			for _, entry := range entries {
				count += len(entry.Runners)
			}
			require.Equal(t, test.count, count)
		})
	}
	_, err := Read(strings.NewReader(strings.Repeat(" ", MaxBody+1)))
	require.Error(t, err)
}

func TestMergePreservesFreshestWholeRecord(t *testing.T) {
	old := record{"https://r.example", "old", "2026-01-01T00:00:00Z", 4}
	newest := record{old.URL, "new", "2026-01-01T02:00:00.123+01:00", 1}
	tie := newest
	tie.App = "tie"
	entries := []Entry[record]{{"https://a.example", []record{old}}, {"https://b.example", []record{newest}}, {"https://a.example", []record{tie, {"https://other.example", "other", old.Observed, 2}}}}
	merged := Merge(entries)
	require.Len(t, merged, 2)
	require.Equal(t, "https://a.example", merged[0].Address)
	require.Equal(t, "https://other.example", merged[0].Runners[0].URL)
	require.Equal(t, []record{newest}, merged[1].Runners)
	// A second forwarding step preserves the original text and ownership.
	require.Equal(t, merged, Merge(merged))
}
