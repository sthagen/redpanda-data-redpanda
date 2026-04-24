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
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func ptrInt(v int) *int { return &v }

func TestPrintBalancerStatus(t *testing.T) {
	pbs := rpadmin.PartitionBalancerStatus{
		Status:                    "ready",
		SecondsSinceLastTick:      5,
		CurrentReassignmentsCount: 2,
		Violations: rpadmin.PartitionBalancerViolations{
			UnavailableNodes:   []int{1, 2},
			OverDiskLimitNodes: []int{3},
		},
	}
	clusterPartitions := []rpadmin.ClusterPartition{
		{Replicas: []rpadmin.Replica{{NodeID: 1}, {NodeID: 2}}},
		{Replicas: []rpadmin.Replica{{NodeID: 1}}},
	}

	resp := buildBalancerStatusResponse(pbs, clusterPartitions)

	require.Equal(t, "ready", resp.Status)
	require.Equal(t, 5, resp.SecondsSinceLastTick)
	require.Equal(t, 2, resp.CurrentReassignmentsCount)
	require.Equal(t, []int{1, 2}, resp.UnavailableNodes)
	require.Equal(t, []int{3}, resp.OverDiskLimitNodes)
	require.Len(t, resp.BrokerReplicaDistribution, 2)

	f := config.OutFormatter{Kind: "text"}
	var buf strings.Builder
	printBalancerStatus(f, resp, &buf)
	require.Equal(t, [][]string{
		{"Status:", "ready"},
		{"Seconds", "Since", "Last", "Tick:", "5"},
		{"Current", "Reassignment", "Count:", "2"},
		{"Unavailable", "Nodes:", "[1", "2]"},
		{"Over", "Disk", "Limit", "Nodes:", "[3]"},
		{"BROKER", "PARTITION-COUNT"},
		{"1", "2"},
		{"2", "1"},
	}, out.TableRows(buf.String()))
}

func TestPrintBalancerStatusNoBrokerDist(t *testing.T) {
	pbs := rpadmin.PartitionBalancerStatus{
		Status:               "off",
		SecondsSinceLastTick: 0,
	}

	resp := buildBalancerStatusResponse(pbs, nil)

	require.Equal(t, "off", resp.Status)
	require.Empty(t, resp.BrokerReplicaDistribution)

	// JSON should omit broker_replica_distribution when empty.
	jsonBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	require.NotContains(t, string(jsonBytes), "broker_replica_distribution")

	f := config.OutFormatter{Kind: "text"}
	var buf strings.Builder
	printBalancerStatus(f, resp, &buf)
	require.Equal(t, [][]string{
		{"Status:", "off"},
		{"Seconds", "Since", "Last", "Tick:", "0"},
		{"Current", "Reassignment", "Count:", "0"},
	}, out.TableRows(buf.String()))
}

func TestPrintBalancerStatusPendingRecovery(t *testing.T) {
	count := 3
	pbs := rpadmin.PartitionBalancerStatus{
		Status:                         "stalled",
		PartitionsPendingForceRecovery: &count,
		PartitionsPendingRecoveryList:  []string{"foo/0/0", "bar/1/0"},
	}

	resp := buildBalancerStatusResponse(pbs, nil)
	require.Equal(t, ptrInt(3), resp.PartitionsPendingForceRecovery)
	require.Equal(t, []string{"foo/0/0", "bar/1/0"}, resp.PartitionsPendingRecoverySample)

	f := config.OutFormatter{Kind: "text"}
	var buf strings.Builder
	printBalancerStatus(f, resp, &buf)
	require.Equal(t, [][]string{
		{"Status:", "stalled"},
		{"Seconds", "Since", "Last", "Tick:", "0"},
		{"Current", "Reassignment", "Count:", "0"},
		{"Partitions", "Pending", "Recovery", "(3):", "[foo/0/0", "bar/1/0]"},
	}, out.TableRows(buf.String()))
}
