package test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// MetricSum sums samples matching a label subset. Histograms return their
// observation count, allowing behavioral tests to verify exactly-once recording.
func MetricSum(t *testing.T, registry prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	var sum float64
	for _, metric := range MetricSamples(t, registry, name, labels) {
		if metric.Counter != nil {
			sum += metric.Counter.GetValue()
		} else if metric.Gauge != nil {
			sum += metric.Gauge.GetValue()
		} else if metric.Histogram != nil {
			sum += float64(metric.Histogram.GetSampleCount())
		}
	}
	return sum
}

// MetricSamples distinguishes absent series from samples whose value is zero.
func MetricSamples(t *testing.T, registry prometheus.Gatherer, name string, labels map[string]string) []*dto.Metric {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	var samples []*dto.Metric
	for _, family := range families {
		if family.GetName() != "livepeer_"+name {
			continue
		}
		for _, metric := range family.Metric {
			matched := true
			for key, value := range labels {
				found := false
				for _, label := range metric.Label {
					if label.GetName() == key && label.GetValue() == value {
						found = true
					}
				}
				matched = matched && found
			}
			if !matched {
				continue
			}
			samples = append(samples, metric)
		}
	}
	return samples
}
