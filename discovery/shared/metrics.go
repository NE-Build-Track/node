package shared

import (
	"strconv"

	"github.com/livepeer/node/internal/observability"
)

// CollectMetrics includes configured sources that have never succeeded and
// sources whose inventory has been evicted. Labels never contain source URLs.
func (c *Collector[R]) CollectMetrics(emit observability.Emit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, source := range c.sources {
		labels := observability.Labels{"source_index": strconv.Itoa(index)}
		state := "healthy"
		if source.QueriedAt.IsZero() {
			state = "uninitialized"
			if !source.LastSuccess.IsZero() {
				state = "evicted"
			}
		} else if source.Failures > 0 {
			state = "retained"
		}
		for _, candidate := range []string{"healthy", "uninitialized", "evicted", "retained"} {
			emit("discovery_source_state", observability.Bool(state == candidate), labels.With("state", candidate))
		}
		emit("discovery_source_failures", float64(source.Failures), labels)
		last := 0.0
		if !source.LastSuccess.IsZero() {
			last = float64(source.LastSuccess.Unix())
		}
		emit("discovery_last_success_timestamp_seconds", last, labels)
		count := 0
		for _, entry := range source.Entries {
			count += len(entry.Runners)
		}
		emit("discovery_cached_runners", float64(count), labels)
	}
}
