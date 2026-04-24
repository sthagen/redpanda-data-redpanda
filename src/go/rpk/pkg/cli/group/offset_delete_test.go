// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"errors"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestBuildOffsetDeleteResults(t *testing.T) {
	tests := []struct {
		name      string
		responses kadm.DeleteOffsetsResponses
		wantOK    bool
		wantOrder []offsetDeleteResult
	}{
		{
			name:      "empty responses",
			responses: kadm.DeleteOffsetsResponses{},
			wantOK:    true,
			wantOrder: nil,
		},
		{
			name: "single topic single partition success",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {0: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
			},
		},
		{
			name: "single topic single partition error",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {0: errors.New("some error")},
			},
			wantOK: false,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "some error"},
			},
		},
		{
			name: "topics sorted alphabetically",
			responses: kadm.DeleteOffsetsResponses{
				"zebra": {0: nil},
				"apple": {0: nil},
				"mango": {0: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "apple", Partition: 0, Status: "OK"},
				{Topic: "mango", Partition: 0, Status: "OK"},
				{Topic: "zebra", Partition: 0, Status: "OK"},
			},
		},
		{
			name: "partitions sorted numerically",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {3: nil, 1: nil, 0: nil, 2: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
				{Topic: "foo", Partition: 1, Status: "OK"},
				{Topic: "foo", Partition: 2, Status: "OK"},
				{Topic: "foo", Partition: 3, Status: "OK"},
			},
		},
		{
			name: "mixed success and error marks ok=false",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {
					0: nil,
					1: errors.New("bad partition"),
				},
			},
			wantOK: false,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
				{Topic: "foo", Partition: 1, Status: "bad partition"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := buildOffsetDeleteResults(tt.responses)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantOrder, got)
		})
	}
}

func TestPrintOffsetDeleteResults(t *testing.T) {
	results := []offsetDeleteResult{
		{Topic: "apple", Partition: 0, Status: "OK"},
		{Topic: "apple", Partition: 1, Status: "some error"},
		{Topic: "zebra", Partition: 0, Status: "OK"},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printOffsetDeleteResults(f, results, b)
	require.Equal(t, [][]string{
		{"apple", "0", "OK"},
		{"apple", "1", "some", "error"},
		{"zebra", "0", "OK"},
	}, out.TableRows(b.String()))
}
