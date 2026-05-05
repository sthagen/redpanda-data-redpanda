// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestBuildDecommissionStatus(t *testing.T) {
	t.Run("basic partitions", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:              "kafka",
					Topic:           "test",
					Partition:       0,
					MovingTo:        rpadmin.DecommissionMovingTo{NodeID: 3},
					PartitionSize:   1000,
					BytesMoved:      100,
					BytesLeftToMove: 900,
				},
			},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Len(t, resp.Partitions, 1)
		require.Equal(t, "kafka/test/0", resp.Partitions[0].Partition)
		require.Equal(t, 3, resp.Partitions[0].MovingTo)
		require.Equal(t, 10, resp.Partitions[0].CompletionPercent)
		require.Equal(t, 1000, resp.Partitions[0].PartitionSize)
		require.Nil(t, resp.Partitions[0].BytesMoved)
		require.Nil(t, resp.Partitions[0].BytesRemaining)
	})

	t.Run("detailed partitions", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:              "kafka",
					Topic:           "test",
					Partition:       1,
					MovingTo:        rpadmin.DecommissionMovingTo{NodeID: 5},
					PartitionSize:   2000,
					BytesMoved:      500,
					BytesLeftToMove: 1500,
				},
			},
		}

		resp := buildDecommissionStatus(dbs, true)

		require.Len(t, resp.Partitions, 1)
		require.NotNil(t, resp.Partitions[0].BytesMoved)
		require.Equal(t, 500, *resp.Partitions[0].BytesMoved)
		require.NotNil(t, resp.Partitions[0].BytesRemaining)
		require.Equal(t, 1500, *resp.Partitions[0].BytesRemaining)
	})

	t.Run("zero partition size completion", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:        "kafka",
					Topic:     "t",
					Partition: 0,
					MovingTo:  rpadmin.DecommissionMovingTo{NodeID: 1},
				},
			},
		}

		resp := buildDecommissionStatus(dbs, false)
		require.Equal(t, 0, resp.Partitions[0].CompletionPercent)
	})

	t.Run("reallocation failures", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			ReallocationFailureDetails: []rpadmin.ReallocationFailedPartition{
				{NS: "kafka", Topic: "foo", Partition: 1, Error: "not enough space"},
			},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Len(t, resp.ReallocationFailures, 1)
		require.Equal(t, "kafka/foo/1", resp.ReallocationFailures[0].Partition)
		require.Equal(t, "not enough space", resp.ReallocationFailures[0].Reason)
		require.Empty(t, resp.AllocationFailures)
	})

	t.Run("allocation failures", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			AllocationFailures: []string{"kafka/bar/0", "kafka/bar/1"},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Empty(t, resp.ReallocationFailures)
		require.Equal(t, []string{"kafka/bar/0", "kafka/bar/1"}, resp.AllocationFailures)
	})
}

func TestPrintDecommissionStatus(t *testing.T) {
	f := config.OutFormatter{Kind: "text"}
	resp := decommissionStatusResponse{
		Partitions: []decommissionPartition{
			{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 10, PartitionSize: 1000},
			{Partition: "kafka/test/1", MovingTo: 3, CompletionPercent: 50, PartitionSize: 2000},
		},
	}

	t.Run("basic", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(f, resp, false, false, &buf)
		require.Equal(t, [][]string{
			{"DECOMMISSION", "PROGRESS"},
			{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE"},
			{"kafka/test/0", "3", "10", "1000"},
			{"kafka/test/1", "3", "50", "2000"},
		}, out.TableRows(buf.String()))
	})

	t.Run("detailed adds bytes columns", func(t *testing.T) {
		moved, remaining := 100, 900
		respDetailed := decommissionStatusResponse{
			Partitions: []decommissionPartition{
				{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 10, PartitionSize: 1000, BytesMoved: &moved, BytesRemaining: &remaining},
			},
		}
		var buf bytes.Buffer
		printDecommissionStatus(f, respDetailed, true, false, &buf)
		require.Equal(t, [][]string{
			{"DECOMMISSION", "PROGRESS"},
			{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE", "BYTES-MOVED", "BYTES-REMAINING"},
			{"kafka/test/0", "3", "10", "1000", "100", "900"},
		}, out.TableRows(buf.String()))
	})

	t.Run("reallocation failures section precedes progress", func(t *testing.T) {
		respFail := decommissionStatusResponse{
			ReallocationFailures: []reallocationFailure{{Partition: "kafka/foo/1", Reason: "no space"}},
			Partitions:           []decommissionPartition{{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 5, PartitionSize: 100}},
		}
		var buf bytes.Buffer
		printDecommissionStatus(f, respFail, false, false, &buf)
		require.Equal(t, [][]string{
			{"REALLOCATION", "FAILURE", "DETAILS"},
			{"PARTITION", "REASON"},
			{"kafka/foo/1", "no", "space"},
			{},
			{"DECOMMISSION", "PROGRESS"},
			{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE"},
			{"kafka/test/0", "3", "5", "100"},
		}, out.TableRows(buf.String()))
	})

	t.Run("allocation failures section precedes progress", func(t *testing.T) {
		respFail := decommissionStatusResponse{
			AllocationFailures: []string{"kafka/bar/0"},
			Partitions:         []decommissionPartition{{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 5, PartitionSize: 100}},
		}
		var buf bytes.Buffer
		printDecommissionStatus(f, respFail, false, false, &buf)
		require.Equal(t, [][]string{
			{"ALLOCATION", "FAILURES"},
			{"kafka/bar/0"},
			{},
			{"DECOMMISSION", "PROGRESS"},
			{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE"},
			{"kafka/test/0", "3", "5", "100"},
		}, out.TableRows(buf.String()))
	})

	t.Run("human readable sizes", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(f, resp, false, true, &buf)
		rows := out.TableRows(buf.String())
		// Data row's PARTITION-SIZE column should no longer be raw integer.
		require.NotEqual(t, "1000", rows[2][3])
		require.NotEqual(t, "2000", rows[3][3])
	})

	t.Run("json empty partitions", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(config.OutFormatter{Kind: "json"}, decommissionStatusResponse{Partitions: []decommissionPartition{}}, false, false, &buf)
		require.Equal(t, `{"partitions":[]}`+"\n", buf.String())
	})
}
