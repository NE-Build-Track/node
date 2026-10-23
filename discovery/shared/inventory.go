package shared

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/livepeer/node/destination"
)

const MaxBody = 1 << 20
const ClockSkew = 30 * time.Second

// Runner exposes the fields used to validate and compare an inventory record.
type Runner interface {
	DiscoveryIdentity() (url, app, observed string)
}

type Entry[R Runner] struct {
	Address string `json:"address"`
	Runners []R    `json:"runners"`
}

// ValidateURL checks discovery base URLs, retaining deployment paths.
func ValidateURL(u *url.URL) error {
	if destination.ValidateURL(u) != nil || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("discovery URL must be absolute HTTP(S) without credentials, query or fragment")
	}
	return nil
}

func Read(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, MaxBody+1))
	if err != nil || len(data) > MaxBody {
		return nil, errors.New("discovery response unreadable or exceeds 1 MiB")
	}
	return data, nil
}

// Decode ignores source metadata and isolates runner errors from envelope errors.
// Empty inventories succeed; advertised runners with no valid candidates fail.
func Decode[R Runner](data []byte, now time.Time) ([]Entry[R], error) {
	var wire []struct {
		Address string            `json:"address"`
		Runners []json.RawMessage `json:"runners"`
		Nodes   json.RawMessage   `json:"nodes"`
	}
	if err := json.Unmarshal(data, &wire); err != nil || wire == nil {
		return nil, errors.New("discovery response must be an array of entries")
	}
	result := make([]Entry[R], 0, len(wire))
	candidates, accepted := 0, 0
	metadata := false
	for _, entry := range wire {
		candidates += len(entry.Runners)
		if entry.Address == "" && len(entry.Runners) == 0 && len(entry.Nodes) > 0 {
			var nodes []json.RawMessage
			if json.Unmarshal(entry.Nodes, &nodes) == nil && nodes != nil {
				metadata = true
				continue
			}
		}
		address, err := url.Parse(entry.Address)
		if err != nil || ValidateURL(address) != nil {
			continue
		}
		out := Entry[R]{Address: entry.Address, Runners: make([]R, 0, len(entry.Runners))}
		for _, raw := range entry.Runners {
			var runner R
			if json.Unmarshal(raw, &runner) != nil {
				continue
			}
			endpoint, app, stamp := runner.DiscoveryIdentity()
			if _, err := destination.ParseURL(endpoint); err != nil || app == "" {
				continue
			}
			observed, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil || observed.After(now.Add(ClockSkew)) {
				continue
			}
			out.Runners = append(out.Runners, runner)
			accepted++
		}
		result = append(result, out)
	}
	if candidates > 0 && accepted == 0 {
		return nil, errors.New("discovery inventory has no valid runners")
	}
	if len(wire) > 0 && len(result) == 0 && !metadata {
		return nil, errors.New("discovery inventory has no valid addresses")
	}
	return result, nil
}

// Merge chooses whole records by original observation time, breaking ties by
// input order. It retains empty groups, letting each caller control presentation.
func Merge[R Runner](entries []Entry[R]) []Entry[R] {
	type winner struct {
		entry, runner int
		observed      time.Time
	}
	winners := make(map[string]winner)
	for i, entry := range entries {
		for j, runner := range entry.Runners {
			endpoint, _, stamp := runner.DiscoveryIdentity()
			observed, _ := time.Parse(time.RFC3339Nano, stamp)
			prior, exists := winners[endpoint]
			if !exists || observed.After(prior.observed) {
				winners[endpoint] = winner{i, j, observed}
			}
		}
	}
	result := make([]Entry[R], 0, len(entries))
	groups := make(map[string]int)
	for i, entry := range entries {
		group, exists := groups[entry.Address]
		if !exists {
			group = len(result)
			groups[entry.Address] = group
			result = append(result, Entry[R]{Address: entry.Address, Runners: []R{}})
		}
		for j, runner := range entry.Runners {
			endpoint, _, _ := runner.DiscoveryIdentity()
			win := winners[endpoint]
			if win.entry == i && win.runner == j {
				result[group].Runners = append(result[group].Runners, runner)
			}
		}
	}
	return result
}
