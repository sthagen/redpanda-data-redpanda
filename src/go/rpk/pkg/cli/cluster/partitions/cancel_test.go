// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func Test_buildMovementCancelResult(t *testing.T) {
	tests := []struct {
		name      string
		movements []rpadmin.PartitionsMovementResult
		want      []movementCancelResult
	}{
		{
			name:      "empty",
			movements: []rpadmin.PartitionsMovementResult{},
			want:      []movementCancelResult{},
		},
		{
			name: "single result",
			movements: []rpadmin.PartitionsMovementResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
			},
			want: []movementCancelResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
			},
		},
		{
			name: "multiple results",
			movements: []rpadmin.PartitionsMovementResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
				{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
				{Namespace: "redpanda_internal", Topic: "tx", Partition: 2, Result: "success"},
			},
			want: []movementCancelResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
				{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
				{Namespace: "redpanda_internal", Topic: "tx", Partition: 2, Result: "success"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMovementCancelResult(tt.movements)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_printMovementsResult(t *testing.T) {
	results := []movementCancelResult{
		{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
		{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
	}

	t.Run("text", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, printMovementsResult(config.OutFormatter{Kind: "text"}, results, &buf))
		require.Equal(t, [][]string{
			{"NAMESPACE", "TOPIC", "PARTITION", "RESULT"},
			{"kafka", "foo", "0", "success"},
			{"kafka", "bar", "1", "failed"},
		}, out.TableRows(buf.String()))
	})

	t.Run("text empty", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, printMovementsResult(config.OutFormatter{Kind: "text"}, []movementCancelResult{}, &buf))
		require.Equal(t, "There are no ongoing partition movements to cancel\n", buf.String())
	})
}
