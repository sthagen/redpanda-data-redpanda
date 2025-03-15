// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimeRange(t *testing.T) {
	baseTime := time.Now().UTC()
	tests := []struct {
		name     string
		rs       string
		tr       timeRange
		hasError bool
	}{
		{name: "no lhs", rs: ":-24h", hasError: true},
		{name: "no rhs", rs: "-24h:", hasError: true},
		{name: "end:end", rs: "end:end", hasError: true},
		{name: "just :", rs: ":", hasError: true},
		{name: "single date", rs: "2022-03-24", hasError: true},
		{name: "same date", rs: "2022-03-24:2022-03-24", hasError: true},
		{name: "out of order date", rs: "2022-03-25:2022-03-24", hasError: true},
		{name: "past end", rs: "24h:end", hasError: true},
		{name: "nonsense", rs: "-24h:-48h", hasError: true},
		{
			name: "relative to end",
			rs:   "-24h:end",
			tr:   timeRange{Time{baseTime.Add(-24 * time.Hour)}, Time{baseTime}},
		},
		{
			name: "two negative durations",
			rs:   "-48h:-24h",
			tr:   timeRange{Time{baseTime.Add(-48 * time.Hour)}, Time{baseTime.Add(-24 * time.Hour)}},
		},
		{
			name: "date til end",
			rs:   "2022-03-24:end",
			tr:   timeRange{Time{time.Unix(1648080000, 0).UTC()}, Time{baseTime}},
		},
		{
			name: "date to date",
			rs:   "2022-03-24:2022-03-25",
			tr:   timeRange{Time{time.Unix(1648080000, 0).UTC()}, Time{time.Unix(1648080000, 0).Add(24 * time.Hour).UTC()}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tr, err := parseTimeRange(test.rs, baseTime)
			if test.hasError {
				require.Error(t, err)
				return
			}

			require.Equal(t, test.tr.Start, tr.Start)
			require.Equal(t, test.tr.End, tr.End)
			require.NoError(t, err)
		})
	}
}

func TestEmptyCollector(t *testing.T) {
	cfg := collectorConfig{
		topicPartitions: []topicPartition{},
		consumeRanges:   offsetRanges{},
		timeRange:       timeRange{Time{time.Now().Add(-24 * time.Hour)}, Time{time.Now()}},
	}
	c, err := newCollector(cfg)
	require.NoError(t, err)
	res := c.results()

	_, err = res.summarizeGlobal()
	require.NoError(t, err)
	_, err = res.summarizeTopics()
	require.NoError(t, err)
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		name     string
		data     []int
		percent  float64
		output   int
		hasError bool
	}{
		{"test0", []int{11, 12, 13, 14}, 0.0, 11, false},
		{"test1", []int{11, 12, 13, 14}, 25.0, 11, false},
		{"test2", []int{11, 12, 13, 14}, 45.0, 12, false},
		{"test4", []int{11, 12, 13, 14}, 100.0, 14, false},
		{"empty data", []int{}, 0.0, 0, true},
		{"positive out of bounds percent", []int{}, 1000.0, 0, true},
		{"negative out of bounds percent", []int{}, -1000.0, 0, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := percentile(test.data, test.percent)
			require.Equal(t, test.output, res)
			require.Equal(t, test.hasError, err != nil)
		})
	}
}
