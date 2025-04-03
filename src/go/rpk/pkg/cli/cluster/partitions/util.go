// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

// LeaderDistribution represents the number of partition leaders assigned to a broker.
type LeaderDistribution struct {
	NodeID int `json:"node_id" yaml:"node_id"`
	Count  int `json:"count" yaml:"count"`
}

// ReplicaDistribution represents the number of partitions assigned to a broker.
type ReplicaDistribution struct {
	NodeID int `json:"node_id" yaml:"node_id"`
	Count  int `json:"count" yaml:"count"`
}

func buildLeaderPerBroker(clusterPartitions []rpadmin.ClusterPartition) []LeaderDistribution {
	brokerLeaders := make(map[int]int)
	for _, p := range clusterPartitions {
		brokerLeaders[*p.LeaderID]++
	}
	var distribution []LeaderDistribution
	for brokerID, leaderCount := range brokerLeaders {
		distribution = append(distribution, LeaderDistribution{
			NodeID: brokerID,
			Count:  leaderCount,
		})
	}
	return distribution
}

func printLeaderDistribution(leaderDist []LeaderDistribution) {
	tw := out.NewTable("BROKER", "LEADER-COUNT")
	defer tw.Flush()
	for _, p := range leaderDist {
		tw.PrintStructFields(struct {
			NodeID int
			Count  int
		}{p.NodeID, p.Count})
	}
}

func buildReplicaPerBroker(clusterPartitions []rpadmin.ClusterPartition) []ReplicaDistribution {
	brokerReplicas := make(map[int]int)
	for _, p := range clusterPartitions {
		for _, r := range p.Replicas {
			brokerReplicas[r.NodeID]++
		}
	}
	var distribution []ReplicaDistribution
	for brokerID, replicaCount := range brokerReplicas {
		distribution = append(distribution, ReplicaDistribution{
			NodeID: brokerID,
			Count:  replicaCount,
		})
	}
	return distribution
}

func printReplicaDistribution(replicaDist []ReplicaDistribution) {
	tw := out.NewTable("BROKER", "PARTITION-COUNT")
	defer tw.Flush()
	for _, p := range replicaDist {
		tw.PrintStructFields(struct {
			NodeID int
			Count  int
		}{p.NodeID, p.Count})
	}
}
